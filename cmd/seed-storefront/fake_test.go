package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// fakeCluster is a contract fake of the slice of the storefront the seeder drives, behind one
// base URL laid out like Kong (/api/<context>/...) plus the BFF (/api/storefront/...). It ENFORCES
// the published invariants the seeder must respect, independently of the seeder's own checks:
//
//   - every POST to a context needs an Idempotency-Key of 1..128 characters (400
//     idempotency-key-required); a repeated key replays the stored answer; the same key with a
//     different request is 422;
//   - pricing: one configured currency (422 currency-mismatch), tax table and charge schedule
//     versions strictly increase (409 non-increasing-version), a schedule names every tier (422
//     incomplete-charge-schedule), a price needs a known, non-discontinued item, a WITHDRAWN price
//     cannot be changed (409 illegal-transition), the optional version guard (409
//     concurrent-update);
//   - product-content: unknown category or parent (422 unknown-category), duplicate sibling name
//     (422), content limits (422), publish completeness (422 incomplete-content), RETIRED is
//     terminal (409 content-retired), a second create is 409;
//   - offer-availability: keyset paging by catalogItemId with nextCursor, page size capped;
//   - product-discovery: purchasable = item ACTIVE + content PUBLISHED + price ACTIVE + in stock,
//     visible only after `lag` reads (the projection is event-driven);
//   - BFF: search over purchasable products, delivery dates only for a 3-character served prefix;
//   - process-path-management (fake_wms_test.go): GET /process-paths lists Active paths only; PUT
//     /sites/{siteId}/cpt-schedule is a wholesale replacement (no Idempotency-Key, no version),
//     400 on a missing/invalid body, 422 on an invalid timezone, no cutoff, an empty/duplicate
//     cptId, a malformed localTime, empty/invalid days, an empty shipMethod or an eligible path
//     that is not Active; an identical revision is a 200 that changes (and publishes) nothing;
//   - network-fulfillment: GET /capability-offers advertises stock (PHYSICAL) only while the
//     site's schedule agrees with the network node profile (and the pre-cutoff gap is not
//     simulated), THROUGHPUT_CONSTRAINED 0 otherwise.
type fakeCluster struct {
	mu       sync.Mutex
	currency string
	items    map[string]*fakeItem
	tax      *TaxTable
	charges  *ChargeSchedule
	cats     []Category
	catVer   int
	prices   map[string]*ItemPrice
	contents map[string]*ProductContent
	idem     map[string]fakeStored
	srv      *httptest.Server // one server per fake: its URL is part of the derived media URLs
	conns    int              // TCP connections accepted (a client that does not reuse them exhausts ports)
	pageCap  int              // the most items a catalogue page returns, whatever the limit
	lag      int              // discovery reads that still answer purchasable=false once eligible
	seen     map[string]int

	// WMS (process-path-management) and network-fulfillment: see fake_wms_test.go
	paths       map[string]string       // pathId -> status (ACTIVE, DEACTIVATED)
	cpt         map[string]*CPTSchedule // siteId -> CPT schedule
	puts        int                     // every PUT received
	cptChanges  int                     // PUTs that changed a schedule (CPTScheduleChanged published)
	node        NodeProfile             // the network's node profile the ACL's offers depend on
	offerSKUs   []string                // SKUs network-fulfillment offers at node.VendorNodeCode
	offersStuck bool                    // the pre-cutoff gap: THROUGHPUT_CONSTRAINED 0 whatever the schedule
	posts       int                     // every POST received by a context (replays included)
	mutations   int                     // POSTs that changed state
	byKind      map[string]int          // mutations by route kind
	targets     map[string]int          // mutations by catalogItemId

	// afterMutation runs (outside the lock) after every state change; tests interrupt runs with it.
	afterMutation func(kind string, n int)
	// dropResponse, when it returns true, applies a write but answers 503 (a lost response).
	dropResponse func(kind string, n int) bool
}

type fakeItem struct {
	ID, Title, State string
	InStock          bool
}

type fakeStored struct {
	fingerprint string
	status      int
	body        []byte
}

func newFakeCluster() *fakeCluster {
	return &fakeCluster{
		currency: "USD", items: map[string]*fakeItem{}, prices: map[string]*ItemPrice{},
		contents: map[string]*ProductContent{}, idem: map[string]fakeStored{}, seen: map[string]int{},
		byKind: map[string]int{}, targets: map[string]int{}, pageCap: 7, lag: 1,
		paths: map[string]string{"PICK": "ACTIVE", "PACK": "ACTIVE", "FULL_CASE": "ACTIVE", "OLD_PATH": "DEACTIVATED"},
		cpt:   map[string]*CPTSchedule{},
		node:  NodeProfile{VendorNodeCode: "site-1", Timezone: "America/Chicago", CutoffTimes: []string{"11:00", "17:00"}},
	}
}

func (f *fakeCluster) addItem(title, state string) string {
	sum := sha256.Sum256([]byte(title))
	id := "item-" + hex.EncodeToString(sum[:])[:12]
	f.items[id] = &fakeItem{ID: id, Title: title, State: state, InStock: true}
	return id
}

func (f *fakeCluster) start() *httptest.Server {
	if f.srv == nil {
		f.srv = httptest.NewUnstartedServer(f)
		f.srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
			if s == http.StateNew {
				f.mu.Lock()
				f.conns++
				f.mu.Unlock()
			}
		}
		f.srv.Start()
	}
	return f.srv
}

func problem(w http.ResponseWriter, status int, slug string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"type": "/problems/" + slug, "title": slug, "status": status})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (f *fakeCluster) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p := r.URL.Path
	switch {
	case strings.HasPrefix(p, "/media/"):
		w.WriteHeader(http.StatusOK)
		return
	case strings.HasPrefix(p, "/api/storefront/"):
		f.mu.Lock()
		defer f.mu.Unlock()
		f.bff(w, r, strings.TrimPrefix(p, "/api/storefront"))
		return
	case r.Method == http.MethodGet:
		f.mu.Lock()
		defer f.mu.Unlock()
		f.read(w, r, strings.TrimPrefix(p, "/api"))
		return
	case r.Method == http.MethodPut:
		f.mu.Lock()
		defer f.mu.Unlock()
		f.putCPT(w, r, strings.TrimPrefix(p, "/api"))
		return
	case r.Method != http.MethodPost:
		problem(w, http.StatusMethodNotAllowed, "method-not-allowed")
		return
	}

	// a write: Idempotency-Key, replay, then the command
	body, _ := io.ReadAll(r.Body)
	key := r.Header.Get("Idempotency-Key")
	f.mu.Lock()
	f.posts++
	if key == "" || len(key) > 128 {
		f.mu.Unlock()
		problem(w, http.StatusBadRequest, "idempotency-key-required")
		return
	}
	fp := r.Method + " " + p + " " + string(body)
	if st, ok := f.idem[key]; ok {
		f.mu.Unlock()
		if st.fingerprint != fp {
			problem(w, http.StatusUnprocessableEntity, "idempotency-key-reuse")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(st.status)
		_, _ = w.Write(st.body)
		return
	}
	rec := httptest.NewRecorder()
	kind, target := f.command(rec, strings.TrimPrefix(p, "/api"), body)
	changed := rec.Code < 300
	n := 0
	if changed {
		f.idem[key] = fakeStored{fingerprint: fp, status: rec.Code, body: rec.Body.Bytes()}
		f.mutations++
		f.byKind[kind]++
		n = f.byKind[kind]
		if target != "" {
			f.targets[target]++
		}
	}
	drop := changed && f.dropResponse != nil && f.dropResponse(kind, n)
	hook := f.afterMutation
	f.mu.Unlock()

	if drop {
		problem(w, http.StatusServiceUnavailable, "unavailable")
	} else {
		for k, v := range rec.Header() {
			w.Header()[k] = v
		}
		w.WriteHeader(rec.Code)
		_, _ = w.Write(rec.Body.Bytes())
	}
	if changed && hook != nil {
		hook(kind, n)
	}
}

// ---- reads ----

func (f *fakeCluster) read(w http.ResponseWriter, r *http.Request, p string) {
	if f.readWMS(w, p) {
		return
	}
	switch {
	case p == "/offer-availability/catalog":
		f.catalog(w, r)
	case p == "/pricing/tax-tables/current":
		if f.tax == nil {
			problem(w, http.StatusNotFound, "not-found")
			return
		}
		writeJSON(w, http.StatusOK, f.tax)
	case p == "/pricing/delivery-charge-schedules/current":
		if f.charges == nil {
			problem(w, http.StatusNotFound, "not-found")
			return
		}
		writeJSON(w, http.StatusOK, f.charges)
	case strings.HasPrefix(p, "/pricing/item-prices/"):
		ip := f.prices[strings.TrimPrefix(p, "/pricing/item-prices/")]
		if ip == nil {
			problem(w, http.StatusNotFound, "not-found")
			return
		}
		writeJSON(w, http.StatusOK, ip)
	case p == "/product-content/categories":
		writeJSON(w, http.StatusOK, CategoryTree{Version: f.catVer, Categories: f.sortedCats()})
	case strings.HasPrefix(p, "/product-content/product-contents/"):
		pc := f.contents[strings.TrimPrefix(p, "/product-content/product-contents/")]
		if pc == nil {
			problem(w, http.StatusNotFound, "not-found")
			return
		}
		writeJSON(w, http.StatusOK, pc)
	case strings.HasPrefix(p, "/product-discovery/products/"):
		id := strings.TrimPrefix(p, "/product-discovery/products/")
		v, ok := f.view(id, true)
		if !ok {
			problem(w, http.StatusNotFound, "not-found")
			return
		}
		writeJSON(w, http.StatusOK, v)
	default:
		problem(w, http.StatusNotFound, "not-found")
	}
}

func (f *fakeCluster) catalog(w http.ResponseWriter, r *http.Request) {
	limit := 50
	if s := r.URL.Query().Get("limit"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n < 1 || n > 500 {
			problem(w, http.StatusUnprocessableEntity, "validation-failed")
			return
		}
		limit = n
	}
	if limit > f.pageCap {
		limit = f.pageCap
	}
	after := r.URL.Query().Get("after")
	ids := make([]string, 0, len(f.items))
	for id := range f.items {
		if id > after {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	page := map[string]any{"items": []map[string]any{}}
	var items []map[string]any
	for i, id := range ids {
		if i == limit {
			page["nextCursor"] = ids[i-1]
			break
		}
		it := f.items[id]
		items = append(items, map[string]any{"catalogItemId": it.ID, "title": it.Title, "state": it.State})
	}
	if items != nil {
		page["items"] = items
	}
	writeJSON(w, http.StatusOK, page)
}

func (f *fakeCluster) sortedCats() []Category {
	out := append([]Category(nil), f.cats...)
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out
}

func (f *fakeCluster) catByID(id string) (Category, bool) {
	for _, c := range f.cats {
		if c.CategoryID == id {
			return c, true
		}
	}
	return Category{}, false
}

// view is product-discovery's ProductView; count=true consumes one read of the lag.
func (f *fakeCluster) view(id string, count bool) (ProductView, bool) {
	it := f.items[id]
	if it == nil {
		return ProductView{}, false
	}
	v := ProductView{CatalogItemID: id, Title: it.Title, InStock: it.InStock}
	pc := f.contents[id]
	published := pc != nil && pc.State == "PUBLISHED" && pc.PublishedRevision > 0
	if published {
		v.Description = pc.Description
		v.CategoryID = pc.PrimaryCategoryID
		if c, ok := f.catByID(pc.PrimaryCategoryID); ok {
			v.CategoryPath = strings.Split(c.Path, "/")
		}
	}
	ip := f.prices[id]
	if ip != nil && ip.State == "ACTIVE" {
		m := ip.EffectivePrice
		v.EffectivePrice = &m
	}
	eligible := it.State == "ACTIVE" && published && v.EffectivePrice != nil && it.InStock
	if eligible && count {
		f.seen[id]++
	}
	v.Purchasable = eligible && f.seen[id] > f.lag
	if !count {
		v.Purchasable = eligible
	}
	return v, true
}

// ---- BFF ----

func (f *fakeCluster) bff(w http.ResponseWriter, r *http.Request, p string) {
	switch {
	case r.Method == http.MethodGet && p == "/products":
		q := strings.ToLower(r.URL.Query().Get("q"))
		catID := r.URL.Query().Get("categoryId")
		ids := make([]string, 0, len(f.items))
		for id := range f.items {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		hits := []ProductView{}
		for _, id := range ids {
			v, _ := f.view(id, false)
			if !v.Purchasable {
				continue
			}
			text := strings.ToLower(v.Title + " " + strings.Join(v.CategoryPath, " "))
			if q != "" && !strings.Contains(text, q) {
				continue
			}
			if catID != "" {
				c, _ := f.catByID(v.CategoryID)
				if c.CategoryID != catID && c.ParentID != catID {
					continue
				}
			}
			hits = append(hits, v)
		}
		writeJSON(w, http.StatusOK, map[string]any{"hits": hits})
	case r.Method == http.MethodPost && strings.HasSuffix(p, ":delivery-dates"):
		var req struct {
			PostalPrefix string `json:"postalPrefix"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		if f.items[strings.TrimSuffix(strings.TrimPrefix(p, "/products/"), ":delivery-dates")] == nil {
			problem(w, http.StatusNotFound, "not-found")
			return
		}
		out := map[string]any{"postalPrefix": req.PostalPrefix, "tiers": []any{}, "unavailable": []any{}}
		if req.PostalPrefix == "786" || req.PostalPrefix == "787" {
			out["tiers"] = []map[string]any{{"tier": "TWO_DAY"}, {"tier": "STANDARD"}}
		} else {
			out["unavailable"] = []map[string]any{{"tier": "STANDARD", "constraint": "DESTINATION_NOT_SERVED"}}
		}
		writeJSON(w, http.StatusOK, out)
	default:
		problem(w, http.StatusNotFound, "not-found")
	}
}

// ---- commands ----

var fakeIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,63}$`)

// command applies one POST and returns its kind and the catalogue item it targets.
func (f *fakeCluster) command(w http.ResponseWriter, p string, body []byte) (kind, target string) {
	switch {
	case p == "/pricing/tax-tables":
		var in TaxTable
		if json.Unmarshal(body, &in) != nil || len(in.Bands) == 0 {
			problem(w, http.StatusUnprocessableEntity, "validation-failed")
			return "tax-table", ""
		}
		if f.tax != nil && in.Version <= f.tax.Version {
			problem(w, http.StatusConflict, "non-increasing-version")
			return "tax-table", ""
		}
		f.tax = &TaxTable{TaxTableID: fmt.Sprintf("tax-%d", in.Version), Version: in.Version, State: "CURRENT", Bands: in.Bands}
		writeJSON(w, http.StatusCreated, f.tax)
		return "tax-table", ""

	case p == "/pricing/delivery-charge-schedules":
		var in ChargeSchedule
		if json.Unmarshal(body, &in) != nil {
			problem(w, http.StatusUnprocessableEntity, "validation-failed")
			return "charge-schedule", ""
		}
		if f.charges != nil && in.Version <= f.charges.Version {
			problem(w, http.StatusConflict, "non-increasing-version")
			return "charge-schedule", ""
		}
		tiers := map[string]bool{}
		for _, t := range in.Tiers {
			if t.Charge.Currency != f.currency || (t.FreeOver != nil && t.FreeOver.Currency != f.currency) {
				problem(w, http.StatusUnprocessableEntity, "currency-mismatch")
				return "charge-schedule", ""
			}
			tiers[t.Tier] = true
		}
		for _, t := range serviceTiers {
			if !tiers[t] {
				problem(w, http.StatusUnprocessableEntity, "incomplete-charge-schedule")
				return "charge-schedule", ""
			}
		}
		f.charges = &ChargeSchedule{ScheduleID: fmt.Sprintf("sched-%d", in.Version), Version: in.Version, State: "CURRENT", Tiers: in.Tiers}
		writeJSON(w, http.StatusCreated, f.charges)
		return "charge-schedule", ""

	case p == "/pricing/item-prices":
		var in struct {
			CatalogItemID string `json:"catalogItemId"`
			ListPrice     Money  `json:"listPrice"`
		}
		if json.Unmarshal(body, &in) != nil {
			problem(w, http.StatusUnprocessableEntity, "validation-failed")
			return "item-price", ""
		}
		it := f.items[in.CatalogItemID]
		switch {
		case it == nil || it.State != "ACTIVE":
			problem(w, http.StatusUnprocessableEntity, "unknown-catalog-item")
		case in.ListPrice.Currency != f.currency:
			problem(w, http.StatusUnprocessableEntity, "currency-mismatch")
		case in.ListPrice.AmountMinor < 0:
			problem(w, http.StatusUnprocessableEntity, "negative-amount")
		case f.prices[in.CatalogItemID] != nil:
			problem(w, http.StatusConflict, "concurrent-update")
		default:
			ip := &ItemPrice{CatalogItemID: in.CatalogItemID, State: "ACTIVE", ListPrice: in.ListPrice, EffectivePrice: in.ListPrice, Version: 1}
			f.prices[in.CatalogItemID] = ip
			writeJSON(w, http.StatusCreated, ip)
		}
		return "item-price", in.CatalogItemID

	case strings.HasPrefix(p, "/pricing/item-prices/") && strings.HasSuffix(p, ":set-list-price"):
		id := strings.TrimSuffix(strings.TrimPrefix(p, "/pricing/item-prices/"), ":set-list-price")
		var in struct {
			ListPrice Money `json:"listPrice"`
			Version   int   `json:"version"`
		}
		_ = json.Unmarshal(body, &in)
		ip := f.prices[id]
		switch {
		case ip == nil:
			problem(w, http.StatusNotFound, "not-found")
		case ip.State == "WITHDRAWN":
			problem(w, http.StatusConflict, "illegal-transition")
		case in.Version != 0 && in.Version != ip.Version:
			problem(w, http.StatusConflict, "concurrent-update")
		case in.ListPrice.Currency != f.currency:
			problem(w, http.StatusUnprocessableEntity, "currency-mismatch")
		case in.ListPrice.AmountMinor < 0:
			problem(w, http.StatusUnprocessableEntity, "negative-amount")
		default:
			ip.ListPrice, ip.EffectivePrice = in.ListPrice, in.ListPrice
			ip.Version++
			writeJSON(w, http.StatusOK, ip)
		}
		return "item-price", id

	case p == "/product-content/categories":
		var in struct {
			ParentID *string `json:"parentId"`
			Name     string  `json:"name"`
		}
		if json.Unmarshal(body, &in) != nil || strings.TrimSpace(in.Name) == "" || in.Name != strings.TrimSpace(in.Name) || len(in.Name) > 100 {
			problem(w, http.StatusUnprocessableEntity, "validation-failed")
			return "category", ""
		}
		parent, path, depth := "", in.Name, 1
		if in.ParentID != nil {
			pc, ok := f.catByID(*in.ParentID)
			if !ok {
				problem(w, http.StatusUnprocessableEntity, "unknown-category")
				return "category", ""
			}
			parent, path, depth = pc.CategoryID, pc.Path+"/"+in.Name, strings.Count(pc.Path, "/")+2
		}
		if depth > 4 {
			problem(w, http.StatusUnprocessableEntity, "max-depth")
			return "category", ""
		}
		for _, c := range f.cats {
			if c.ParentID == parent && strings.EqualFold(c.Name, in.Name) {
				problem(w, http.StatusUnprocessableEntity, "duplicate-sibling-name")
				return "category", ""
			}
		}
		c := Category{CategoryID: fmt.Sprintf("cat-%04d", len(f.cats)+1), ParentID: parent, Name: in.Name, Path: path}
		f.cats = append(f.cats, c)
		f.catVer++
		writeJSON(w, http.StatusCreated, c)
		return "category", ""

	case p == "/product-content/product-contents":
		var in struct {
			CatalogItemID string `json:"catalogItemId"`
			ContentBody
		}
		if json.Unmarshal(body, &in) != nil {
			problem(w, http.StatusUnprocessableEntity, "validation-failed")
			return "content-create", ""
		}
		it := f.items[in.CatalogItemID]
		switch {
		case it == nil:
			problem(w, http.StatusUnprocessableEntity, "unknown-catalog-item")
		case it.State != "ACTIVE":
			problem(w, http.StatusUnprocessableEntity, "item-discontinued")
		case f.contents[in.CatalogItemID] != nil:
			problem(w, http.StatusConflict, "concurrent-update")
		default:
			if slug := f.invalidBody(in.ContentBody); slug != "" {
				problem(w, http.StatusUnprocessableEntity, slug)
				return "content-create", in.CatalogItemID
			}
			pc := &ProductContent{CatalogItemID: in.CatalogItemID, State: "DRAFT", Revision: 1, ContentBody: in.ContentBody}
			f.contents[in.CatalogItemID] = pc
			writeJSON(w, http.StatusCreated, pc)
		}
		return "content-create", in.CatalogItemID

	case strings.HasPrefix(p, "/product-content/product-contents/") && strings.HasSuffix(p, ":revise"):
		id := strings.TrimSuffix(strings.TrimPrefix(p, "/product-content/product-contents/"), ":revise")
		var in ContentBody
		_ = json.Unmarshal(body, &in)
		pc := f.contents[id]
		switch {
		case pc == nil:
			problem(w, http.StatusNotFound, "not-found")
		case pc.State == "RETIRED":
			problem(w, http.StatusConflict, "content-retired")
		default:
			if slug := f.invalidBody(in); slug != "" {
				problem(w, http.StatusUnprocessableEntity, slug)
				return "content-revise", id
			}
			if pc.State == "PUBLISHED" && !fakeComplete(in) {
				problem(w, http.StatusUnprocessableEntity, "incomplete-content")
				return "content-revise", id
			}
			pc.ContentBody = in
			pc.Revision++
			writeJSON(w, http.StatusOK, pc)
		}
		return "content-revise", id

	case strings.HasPrefix(p, "/product-content/product-contents/") && strings.HasSuffix(p, ":publish"):
		id := strings.TrimSuffix(strings.TrimPrefix(p, "/product-content/product-contents/"), ":publish")
		pc := f.contents[id]
		switch {
		case pc == nil:
			problem(w, http.StatusNotFound, "not-found")
		case pc.State == "RETIRED":
			problem(w, http.StatusConflict, "content-retired")
		case !fakeComplete(pc.ContentBody):
			problem(w, http.StatusUnprocessableEntity, "incomplete-content")
		case f.items[id] == nil || f.items[id].State != "ACTIVE":
			problem(w, http.StatusUnprocessableEntity, "item-discontinued")
		default:
			pc.State, pc.PublishedRevision = "PUBLISHED", pc.Revision
			writeJSON(w, http.StatusOK, pc)
		}
		return "content-publish", id
	}
	problem(w, http.StatusNotFound, "not-found")
	return "unknown", ""
}

// invalidBody is product-content's body validation (limits, ids, URLs, known category).
func (f *fakeCluster) invalidBody(b ContentBody) string {
	switch {
	case len(b.Bullets) > 10:
		return "too-many-bullets"
	case len(b.Attributes) > 50:
		return "too-many-attributes"
	case len(b.Media) > 12:
		return "too-many-media"
	case len([]rune(b.Description)) > 4000:
		return "validation-failed"
	}
	for _, s := range b.Bullets {
		if s == "" || len([]rune(s)) > 300 {
			return "validation-failed"
		}
	}
	for _, a := range b.Attributes {
		if a.Name == "" || a.Value == "" || len([]rune(a.Name)) > 100 || len([]rune(a.Value)) > 300 || len([]rune(a.Unit)) > 20 {
			return "validation-failed"
		}
	}
	for _, m := range b.Media {
		if !fakeIDPattern.MatchString(m.MediaID) || m.AltText == "" || len([]rune(m.AltText)) > 200 || len(m.URL) > 2048 ||
			!(strings.HasPrefix(m.URL, "http://") || strings.HasPrefix(m.URL, "https://")) || (m.Role != "PRIMARY" && m.Role != "GALLERY") {
			return "validation-failed"
		}
	}
	if b.PrimaryCategoryID != "" {
		if _, ok := f.catByID(b.PrimaryCategoryID); !ok {
			return "unknown-category"
		}
	}
	return ""
}

func fakeComplete(b ContentBody) bool {
	primary := 0
	for _, m := range b.Media {
		if m.Role == "PRIMARY" {
			primary++
		}
	}
	return b.Description != "" && b.PrimaryCategoryID != "" && primary == 1
}

// post is a raw request helper for the fake's own contract tests.
func post(t interface{ Fatal(...any) }, url, key string, body any) (int, string) {
	var raw []byte
	if body != nil {
		raw, _ = json.Marshal(body)
	}
	req, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}
