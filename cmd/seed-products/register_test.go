package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeProductMaster implements the slice of product-master's published
// contract the seeder drives (apis/openapi.yaml), including its input rules,
// so the whole committed catalogue can be pushed through the real client
// without a database.
type fakeProductMaster struct {
	mu       sync.Mutex
	products map[string]*fakeProduct
}

type fakeProduct struct {
	Description string
	Dims        *Dimensions
	Tags        []string
	Temp        string
	Dot         *int
}

func newFakeProductMaster() *httptest.Server {
	f := &fakeProductMaster{products: map[string]*fakeProduct{}}
	return httptest.NewServer(f)
}

func (f *fakeProductMaster) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	path := r.URL.Path
	switch {
	case r.Method == http.MethodGet && path == "/healthz":
		w.WriteHeader(http.StatusOK)
	case r.Method == http.MethodGet && path == "/products":
		f.list(w, r)
	case strings.HasPrefix(path, "/products/") && r.Method == http.MethodPut:
		rest := strings.TrimPrefix(path, "/products/")
		sku, sub, _ := strings.Cut(rest, "/")
		switch sub {
		case "":
			f.register(w, r, sku)
		case "classification":
			f.classify(w, r, sku)
		case "dimensions/declared":
			f.declare(w, r, sku)
		default:
			http.NotFound(w, r)
		}
	default:
		http.NotFound(w, r)
	}
}

func bad(w http.ResponseWriter, why string) { http.Error(w, why, http.StatusBadRequest) }

func (f *fakeProductMaster) register(w http.ResponseWriter, r *http.Request, sku string) {
	var body struct {
		Description string `json:"description"`
	}
	if json.NewDecoder(r.Body).Decode(&body) != nil || !validSKU(sku) {
		bad(w, "invalid sku or body")
		return
	}
	p, existed := f.products[sku]
	if !existed {
		p = &fakeProduct{}
		f.products[sku] = p
	}
	p.Description = body.Description
	if existed {
		w.WriteHeader(http.StatusOK)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func (f *fakeProductMaster) declare(w http.ResponseWriter, r *http.Request, sku string) {
	p, ok := f.products[sku]
	if !ok {
		http.NotFound(w, r)
		return
	}
	var d Dimensions
	if json.NewDecoder(r.Body).Decode(&d) != nil {
		bad(w, "body")
		return
	}
	for _, v := range []int64{d.LengthMm, d.WidthMm, d.HeightMm} {
		if v < 1 || v > maxDimensionMm {
			bad(w, "dimension out of range")
			return
		}
	}
	if d.WeightG < 1 || d.WeightG > maxWeightG {
		bad(w, "weight out of range")
		return
	}
	p.Dims = &d
	w.WriteHeader(http.StatusOK)
}

func (f *fakeProductMaster) classify(w http.ResponseWriter, r *http.Request, sku string) {
	p, ok := f.products[sku]
	if !ok {
		http.NotFound(w, r)
		return
	}
	var body struct {
		HandlingTags     []string `json:"handlingTags"`
		TemperatureClass string   `json:"temperatureClass"`
		DotHazardClass   *int     `json:"dotHazardClass"`
	}
	if json.NewDecoder(r.Body).Decode(&body) != nil || len(body.HandlingTags) == 0 {
		bad(w, "handlingTags must be non-empty")
		return
	}
	seen := map[string]bool{}
	for _, tg := range body.HandlingTags {
		if !contains(handlingTagOrder, tg) || seen[tg] {
			bad(w, "unknown or duplicate tag")
			return
		}
		seen[tg] = true
	}
	if seen["TemperatureSensitive"] != (body.TemperatureClass != "") {
		bad(w, "temperatureClass iff TemperatureSensitive")
		return
	}
	if body.DotHazardClass != nil && (!seen["Hazmat"] || *body.DotHazardClass < 1 || *body.DotHazardClass > 9) {
		bad(w, "dotHazardClass only with Hazmat, 1..9")
		return
	}
	created := p.Tags == nil
	p.Tags, p.Temp, p.Dot = body.HandlingTags, body.TemperatureClass, body.DotHazardClass
	if created {
		w.WriteHeader(http.StatusCreated)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (f *fakeProductMaster) list(w http.ResponseWriter, r *http.Request) {
	skus := make([]string, 0, len(f.products))
	for s := range f.products {
		skus = append(skus, s)
	}
	sort.Strings(skus)
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 || limit > 500 {
		limit = 500
	}
	start := 0
	if c := r.URL.Query().Get("cursor"); c != "" {
		start, _ = strconv.Atoi(c)
	}
	end := start + limit
	if end > len(skus) {
		end = len(skus)
	}
	items := []map[string]any{}
	for _, s := range skus[start:end] {
		p := f.products[s]
		it := map[string]any{"sku": s, "description": p.Description, "physicalProfile": map[string]any{}}
		if p.Dims != nil {
			it["physicalProfile"] = map[string]any{"declared": p.Dims}
		}
		if p.Tags != nil {
			c := map[string]any{"handlingTags": p.Tags}
			if p.Temp != "" {
				c["temperatureClass"] = p.Temp
			}
			if p.Dot != nil {
				c["dotHazardClass"] = *p.Dot
			}
			it["classification"] = c
		}
		items = append(items, it)
	}
	page := map[string]any{"items": items}
	if end < len(skus) {
		page["nextCursor"] = strconv.Itoa(end)
	}
	_ = json.NewEncoder(w).Encode(page)
}

// Registers the WHOLE committed catalogue through the real client against the
// contract fake, twice (the second run must be a no-op), then verifies it by
// reading it back - the same path as `make seed-products`, minus the database.
func TestWholeCatalogueRegistersAndVerifiesAgainstTheContract(t *testing.T) {
	if testing.Short() {
		t.Skip("pushes ~13,700 products through the client (~20 s)")
	}
	products, err := loadFixtures([]string{
		"../../fixtures/products/openfoodfacts-products.json",
		"../../fixtures/products/fulfilment-products.json",
	})
	if err != nil {
		t.Fatal(err)
	}
	srv := newFakeProductMaster()
	defer srv.Close()
	c := &pmClient{base: srv.URL, http: &http.Client{Timeout: 15 * time.Second}}
	ctx := context.Background()

	for run := 1; run <= 2; run++ {
		rep := c.register(ctx, products, 16, true, nil)
		if len(rep.failures) > 0 {
			t.Fatalf("run %d: %d failures, first: %s", run, len(rep.failures), rep.failures[0])
		}
		dims, _, _ := countFulfilment(products)
		if got := int(rep.dimensioned.Load()); got != dims {
			t.Fatalf("run %d: declared dimensions for %d products, want %d", run, got, dims)
		}
		if run == 1 && int(rep.created.Load()) != len(products) {
			t.Fatalf("run 1 created %d, want %d", rep.created.Load(), len(products))
		}
		if run == 2 && rep.created.Load() != 0 {
			t.Fatalf("run 2 created %d products, want 0 (idempotent)", rep.created.Load())
		}
	}
	listed, err := c.listAll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != len(products) {
		t.Fatalf("listed %d products, want %d", len(listed), len(products))
	}
	if problems := verify(products, listed, true); len(problems) > 0 {
		t.Fatalf("%d problems, first: %s", len(problems), problems[0])
	}
}
