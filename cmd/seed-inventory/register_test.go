package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeInventory implements the slice of inventory-storage's contract the seeder drives,
// including its invariants: a stow needs a registered bin and room (409), a hazmat SKU needs a
// hazmat-rated bin and a temperature-sensitive SKU a matching zone (409), DOT-classed SKUs never
// share a bin (409), PUT /bins converges and never shrinks below occupancy, and a repeated
// Idempotency-Key replays instead of stowing again.
type fakeInventory struct {
	mu      sync.Mutex
	bins    map[string]*fakeBin
	zones   map[string]Bin // bin id -> facility attributes
	prods   map[string]Product
	keys    map[string]int // idempotency key -> status replayed
	stows   int
	replays int
}

type fakeBin struct {
	capacity, occupied int
	skus               map[string]int
}

func newFakeInventory(sites []Site, products []Product) (*fakeInventory, *httptest.Server) {
	f := &fakeInventory{bins: map[string]*fakeBin{}, zones: map[string]Bin{}, prods: map[string]Product{}, keys: map[string]int{}}
	for _, s := range sites {
		for _, b := range s.Bins {
			f.zones[b.ID] = b
		}
	}
	for _, p := range products {
		f.prods[p.SKU] = p
	}
	return f, httptest.NewServer(f)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (f *fakeInventory) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch {
	case strings.HasPrefix(r.URL.Path, "/bins/") && r.Method == http.MethodGet:
		id := strings.TrimPrefix(r.URL.Path, "/bins/")
		b, ok := f.bins[id]
		if !ok {
			http.Error(w, `{"title":"Bin not found"}`, http.StatusNotFound)
			return
		}
		writeJSON(w, 200, map[string]any{"binId": id, "capacity": b.capacity, "occupied": b.occupied, "available": b.capacity - b.occupied})
	case strings.HasPrefix(r.URL.Path, "/bins/") && r.Method == http.MethodPut:
		id := strings.TrimPrefix(r.URL.Path, "/bins/")
		var body struct {
			Capacity int `json:"capacity"`
		}
		if json.NewDecoder(r.Body).Decode(&body) != nil || body.Capacity < 1 {
			http.Error(w, "bad capacity", http.StatusBadRequest)
			return
		}
		if _, known := f.zones[id]; !known {
			http.Error(w, "unknown location", http.StatusUnprocessableEntity)
			return
		}
		b, ok := f.bins[id]
		switch {
		case !ok:
			f.bins[id] = &fakeBin{capacity: body.Capacity, skus: map[string]int{}}
			writeJSON(w, 201, map[string]any{"binId": id, "capacity": body.Capacity})
		case body.Capacity < b.occupied:
			http.Error(w, "capacity-below-occupancy", http.StatusConflict)
		default:
			b.capacity = body.Capacity
			writeJSON(w, 200, map[string]any{"binId": id, "capacity": b.capacity})
		}
	case r.URL.Path == "/stock/stow" && r.Method == http.MethodPost:
		key := r.Header.Get("Idempotency-Key")
		if key == "" {
			http.Error(w, "idempotency key required", http.StatusBadRequest)
			return
		}
		if st, seen := f.keys[key]; seen {
			f.replays++
			w.Header().Set("Idempotent-Replayed", "true")
			writeJSON(w, st, map[string]any{})
			return
		}
		var body struct {
			SKU      string `json:"sku"`
			Quantity int    `json:"quantity"`
			BinID    string `json:"binId"`
		}
		if json.NewDecoder(r.Body).Decode(&body) != nil || body.Quantity < 1 {
			http.Error(w, "zero-quantity", http.StatusUnprocessableEntity)
			return
		}
		b, ok := f.bins[body.BinID]
		if !ok {
			http.Error(w, "bin-not-found", http.StatusNotFound)
			return
		}
		p, z := f.prods[body.SKU], f.zones[body.BinID]
		switch {
		case b.occupied+body.Quantity > b.capacity:
			http.Error(w, "capacity-exceeded", http.StatusConflict)
			return
		case p.has("Hazmat") && !z.Hazmat:
			http.Error(w, "hazmat-zone-required", http.StatusConflict)
			return
		case p.Temp != "" && z.Temp != p.Temp:
			http.Error(w, "temperature-class-mismatch", http.StatusConflict)
			return
		case p.DotClass != 0 && len(b.skus) > 0 && b.skus[body.SKU] == 0:
			http.Error(w, "dot-segregation", http.StatusConflict)
			return
		}
		b.occupied += body.Quantity
		b.skus[body.SKU] += body.Quantity
		f.keys[key] = 201
		f.stows++
		writeJSON(w, 201, map[string]any{"sku": body.SKU, "binId": body.BinID, "quantity": body.Quantity})
	case strings.HasPrefix(r.URL.Path, "/inventory/") && strings.HasSuffix(r.URL.Path, "/usable"):
		sku := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/inventory/"), "/usable")
		total := 0
		for _, b := range f.bins {
			total += b.skus[sku]
		}
		writeJSON(w, 200, map[string]any{"sku": sku, "usable": total})
	default:
		http.NotFound(w, r)
	}
}

func TestStockingHonoursInventoryStoragesInvariantsAndIsIdempotent(t *testing.T) {
	sites, products := testSites(), testProducts()
	for i := 0; i < 600; i++ { // enough small SKUs that bins are shared (chaotic storage)
		products = append(products, Product{SKU: "ABO-X" + strings.Repeat("0", 3-len(itoa(i))) + itoa(i), HasDims: true, VolumeL: 0.2 + float64(i%9)*0.4, WeightG: int64(100 + i*5)})
	}
	plan := BuildPlan(products, sites, Options{})
	if plan.Units == 0 {
		t.Fatal("empty plan")
	}
	fake, srv := newFakeInventory(sites, products)
	defer srv.Close()
	c := &invClient{base: srv.URL, http: &http.Client{Timeout: 10 * time.Second}}
	ctx := context.Background()

	rep := c.stock(ctx, plan.Bins, 8, nil)
	if len(rep.failures) > 0 {
		t.Fatalf("%d failures, first: %s", len(rep.failures), rep.failures[0])
	}
	if int(rep.units.Load()) != plan.Units || int(rep.binsCreated.Load()) != len(plan.Bins) {
		t.Fatalf("stowed %d units into %d new bins, planned %d into %d", rep.units.Load(), rep.binsCreated.Load(), plan.Units, len(plan.Bins))
	}
	problems, occupied := c.verify(ctx, plan, 8, 200)
	if len(problems) > 0 || occupied != plan.Units {
		t.Fatalf("verify: %v (occupied %d, planned %d)", problems, occupied, plan.Units)
	}

	// second run: nothing is stowed again
	stows := fake.stows
	rep = c.stock(ctx, plan.Bins, 8, nil)
	if len(rep.failures) > 0 {
		t.Fatalf("re-run failed: %s", rep.failures[0])
	}
	if fake.stows != stows || int(rep.binsAlreadyStocked.Load()) != len(plan.Bins) || rep.units.Load() != 0 {
		t.Fatalf("re-run stowed again: stows %d->%d, skipped %d of %d", stows, fake.stows, rep.binsAlreadyStocked.Load(), len(plan.Bins))
	}
}

func TestAnInterruptedBinIsCompletedByTheIdempotencyKeys(t *testing.T) {
	sites := testSites()
	var products []Product
	for i := 0; i < 120; i++ {
		products = append(products, Product{SKU: "ABO-T" + itoa(1000+i), HasDims: true, VolumeL: 0.03, WeightG: 20})
	}
	plan := BuildPlan(products, sites, Options{})
	var shared *BinPlan
	for _, bp := range plan.Bins {
		if len(bp.Lines) >= 2 {
			shared = bp
			break
		}
	}
	if shared == nil {
		t.Fatal("expected chaotic storage to share a bin between SKUs")
	}
	fake, srv := newFakeInventory(sites, products)
	defer srv.Close()
	c := &invClient{base: srv.URL, http: &http.Client{Timeout: 10 * time.Second}}
	ctx := context.Background()
	// the first line landed before the interruption
	if _, err := c.do(ctx, http.MethodPut, "/bins/"+shared.Bin.ID, map[string]int{"capacity": shared.Capacity}, "", nil, 200, 201); err != nil {
		t.Fatal(err)
	}
	first := shared.Lines[0]
	if _, err := c.do(ctx, http.MethodPost, "/stock/stow", map[string]any{"sku": first.SKU, "quantity": first.Qty, "binId": shared.Bin.ID},
		"seed-inventory-v1:"+shared.Bin.ID+":"+first.SKU+":"+itoa(first.Qty), nil, 201); err != nil {
		t.Fatal(err)
	}
	rep := &stockReport{}
	c.stockBin(ctx, shared, rep)
	if len(rep.failures) > 0 {
		t.Fatal(rep.failures)
	}
	if rep.binsPartial.Load() != 1 || fake.bins[shared.Bin.ID].occupied != shared.Units || fake.replays != 1 {
		t.Fatalf("occupied %d want %d, partial %d, replays %d", fake.bins[shared.Bin.ID].occupied, shared.Units, rep.binsPartial.Load(), fake.replays)
	}
}

func itoa(i int) string {
	b, _ := json.Marshal(i)
	return string(b)
}
