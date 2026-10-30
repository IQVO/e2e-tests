package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

const repoRoot = "../.."

// siblingNetworkData is the warehouse-infra data file next to this repo (or $STOREFRONT_NETWORK_DATA).
func siblingNetworkData() string {
	if v := os.Getenv("STOREFRONT_NETWORK_DATA"); v != "" {
		return v
	}
	return filepath.Join(repoRoot, "..", "warehouse-infra", "scripts", "data", "retail-network-seed.json")
}

type inputs struct {
	fx      *Fixture
	phys    map[string]PhysicalProduct
	titles  map[string]string // sku -> catalogue title used by the fake
	network string            // network data file handed to the seeder
}

// loadInputs reads the REAL committed fixtures. Titles come from the sibling warehouse-infra data
// file when it carries every fixture SKU; otherwise (CI: single-repo checkout) neutral stand-in
// titles are written to a temporary network data file, so the whole fixture still goes through.
func loadInputs(t *testing.T) inputs {
	t.Helper()
	fx, err := LoadFixture(filepath.Join(repoRoot, "fixtures/storefront/catalogue.json"))
	if err != nil {
		t.Fatal(err)
	}
	phys, err := LoadPhysical(filepath.Join(repoRoot, "fixtures/products/fulfilment-products.json"))
	if err != nil {
		t.Fatal(err)
	}
	titles := map[string]string{}
	if real, err := LoadNetworkTitles(siblingNetworkData()); err == nil {
		titles = real
	}
	complete := true
	for _, p := range fx.Products {
		if titles[p.SKU] == "" {
			complete = false
		}
	}
	if !complete {
		titles = map[string]string{}
		for _, p := range fx.Products {
			titles[p.SKU] = "Stand-in catalogue title for " + p.SKU
		}
	}
	return inputs{fx: fx, phys: phys, titles: titles, network: writeNetwork(t, titles)}
}

func writeNetwork(t *testing.T, titles map[string]string) string {
	t.Helper()
	type row struct {
		SKU   string `json:"sku"`
		Title string `json:"title"`
	}
	var doc struct {
		Catalog []row `json:"catalog"`
	}
	for sku, title := range titles {
		doc.Catalog = append(doc.Catalog, row{sku, title})
	}
	raw, _ := json.Marshal(doc)
	path := filepath.Join(t.TempDir(), "retail-network-seed.json")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// clusterLike is the fake in the state of the live cluster before the first storefront seed:
// the network's items (ACTIVE, in stock), 5 legacy items, tax table and charge schedule v1 equal
// to the fixture, and the top-level Jewelry category.
func clusterLike(in inputs) (*fakeCluster, map[string]string) {
	f := newFakeCluster()
	ids := map[string]string{}
	for _, p := range in.fx.Products {
		ids[p.SKU] = f.addItem(in.titles[p.SKU], "ACTIVE")
	}
	for _, legacy := range []string{"LEG-0001 legacy item one", "LEG-0002 legacy item two", "LEG-0003 legacy item three", "LEG-0004 legacy item four", "LEG-0005 legacy item five"} {
		ids[legacy] = f.addItem(legacy, "ACTIVE")
	}
	f.tax = &TaxTable{TaxTableID: "tax-1", Version: 1, State: "CURRENT", Bands: in.fx.TaxBands}
	f.charges = &ChargeSchedule{ScheduleID: "sched-1", Version: 1, State: "CURRENT", Tiers: desiredTiers(in.fx.DeliveryCharges, "USD")}
	f.cats = []Category{{CategoryID: "cat-80c58ef626f7", Name: "Jewelry", Path: "Jewelry"}}
	f.catVer = 1
	return f, ids
}

func config(base string, in inputs, out *bytes.Buffer) Config {
	return Config{
		BaseURL: base + "/api", StorefrontURL: base + "/api/storefront",
		FixturePath:     filepath.Join(repoRoot, "fixtures/storefront/catalogue.json"),
		ProductsPath:    filepath.Join(repoRoot, "fixtures/products/fulfilment-products.json"),
		NetworkDataPath: in.network, MediaBase: base + "/media/departments",
		VerifyTimeout: 5 * time.Second, VerifyInterval: time.Millisecond, QuotePrefix: "787",
		HTTP: nil, Out: out,
	}
}

func run(t *testing.T, ctx context.Context, cfg Config, f *fakeCluster) (*Result, error) {
	t.Helper()
	srv := f.start()
	t.Cleanup(func() {
		if f.srv != nil {
			f.srv.Close()
			f.srv = nil
		}
	})
	cfg.BaseURL = srv.URL + "/api"
	cfg.StorefrontURL = srv.URL + "/api/storefront"
	cfg.MediaBase = srv.URL + "/media/departments"
	cfg.HTTP = srv.Client()
	return Run(ctx, cfg)
}

func TestSeedsTheWholeFixtureTwiceSecondRunWritesNothing(t *testing.T) {
	in := loadInputs(t)
	f, ids := clusterLike(in)
	var out bytes.Buffer
	cfg := config("", in, &out)

	res, err := run(t, context.Background(), cfg, f)
	if err != nil {
		t.Fatalf("first run: %v\n%s", err, out.String())
	}
	n := len(in.fx.Products)
	subcats := map[string]bool{}
	for _, p := range in.fx.Products {
		subcats[p.Department+"/"+p.Subcategory] = true
	}
	// Jewelry exists: departments - 1 + subcategories; tax and charges equal: skipped
	wantWrites := (len(in.fx.Departments) - 1) + len(subcats) + n + 2*n
	if res.Writes != wantWrites || res.Plan.Writes() != wantWrites || f.mutations != wantWrites {
		t.Fatalf("first run: %d writes sent, %d planned, %d applied by the fake; want %d\n%s", res.Writes, res.Plan.Writes(), f.mutations, wantWrites, out.String())
	}
	if f.tax.Version != 1 || f.charges.Version != 1 {
		t.Fatalf("an equal tax table / charge schedule must be skipped, got v%d / v%d", f.tax.Version, f.charges.Version)
	}
	if res.Verify == nil || res.Verify.Purchasable != n || len(res.Verify.QuoteTiers) == 0 {
		t.Fatalf("verify: %+v", res.Verify)
	}
	for _, legacy := range []string{"LEG-0001 legacy item one", "LEG-0005 legacy item five"} {
		if f.targets[ids[legacy]] != 0 || f.prices[ids[legacy]] != nil || f.contents[ids[legacy]] != nil {
			t.Fatalf("legacy item %q was touched", legacy)
		}
	}
	for _, p := range in.fx.Products {
		ip, pc := f.prices[ids[p.SKU]], f.contents[ids[p.SKU]]
		if ip == nil || ip.ListPrice != (Money{p.ListPriceMinor, "USD"}) {
			t.Fatalf("%s price %+v, want %d USD", p.SKU, ip, p.ListPriceMinor)
		}
		if pc == nil || pc.State != "PUBLISHED" || pc.PublishedRevision != pc.Revision {
			t.Fatalf("%s content %+v", p.SKU, pc)
		}
		c, _ := f.catByID(pc.PrimaryCategoryID)
		dept := in.fx.departmentBySlug()[p.Department].Name
		if c.Path != dept+"/"+p.Subcategory {
			t.Fatalf("%s category path %q", p.SKU, c.Path)
		}
	}
	if c, _ := f.catByID("cat-80c58ef626f7"); c.Path != "Jewelry" {
		t.Fatal("the existing Jewelry category must be adopted")
	}

	if f.conns > 3 {
		t.Fatalf("first run opened %d TCP connections for %d POSTs: the client must reuse its connections", f.conns, f.posts)
	}
	posts := f.posts
	out.Reset()
	res, err = run(t, context.Background(), cfg, f)
	if err != nil {
		t.Fatalf("second run: %v\n%s", err, out.String())
	}
	if res.Writes != 0 || res.Plan.Writes() != 0 || f.posts != posts {
		t.Fatalf("second run must write nothing: %d sent, %d planned, %d POSTs reached the fake\n%s", res.Writes, res.Plan.Writes(), f.posts-posts, out.String())
	}
	if !strings.Contains(out.String(), "apply: 0 writes") {
		t.Fatalf("the second run must print 0 writes:\n%s", out.String())
	}
}

func TestEmptyOrDifferentPricingTablesArePublishedWithTheNextVersion(t *testing.T) {
	in := loadInputs(t)
	f, _ := clusterLike(in)
	f.tax, f.charges = nil, nil
	var out bytes.Buffer
	if _, err := run(t, context.Background(), config("", in, &out), f); err != nil {
		t.Fatalf("%v\n%s", err, out.String())
	}
	if f.tax.Version != 1 || f.charges.Version != 1 {
		t.Fatalf("from empty: tax v%d, charges v%d, want v1", f.tax.Version, f.charges.Version)
	}

	f2, _ := clusterLike(in)
	f2.tax = &TaxTable{Version: 3, Bands: []TaxBand{{"786", "787", 800}}}
	f2.charges.Version = 2
	f2.charges.Tiers[0].Charge.AmountMinor++
	out.Reset()
	if _, err := run(t, context.Background(), config("", in, &out), f2); err != nil {
		t.Fatalf("%v\n%s", err, out.String())
	}
	if f2.tax.Version != 4 || f2.tax.Bands[0].RateBasisPoints != 825 || f2.charges.Version != 3 {
		t.Fatalf("different tables must be republished with version+1: tax %+v charges v%d", f2.tax, f2.charges.Version)
	}
}

func TestInterruptedRunConverges(t *testing.T) {
	in := loadInputs(t)
	f, _ := clusterLike(in)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// kill the run in the middle of the content phase: after the 10th content create
	f.afterMutation = func(kind string, n int) {
		if kind == "content-create" && n == 10 {
			cancel()
		}
	}
	var out bytes.Buffer
	cfg := config("", in, &out)
	if _, err := run(t, ctx, cfg, f); err == nil {
		t.Fatal("the interrupted run must fail")
	}
	if f.byKind["content-create"] != 10 || f.byKind["content-publish"] != 9 {
		t.Fatalf("interruption point: %v", f.byKind)
	}
	f.afterMutation = nil

	out.Reset()
	res, err := run(t, context.Background(), cfg, f)
	if err != nil {
		t.Fatalf("rerun: %v\n%s", err, out.String())
	}
	n := len(in.fx.Products)
	if res.Plan.ContentsToCreate != n-10 || res.Plan.ContentsToPublish != 1 || res.Plan.PricesToCreate != 0 || len(res.Plan.Categories) != 0 {
		t.Fatalf("rerun plan: %+v", res.Plan.Summary())
	}
	if f.byKind["content-create"] != n || f.byKind["content-publish"] != n || res.Verify.Purchasable != n {
		t.Fatalf("after the rerun: %v, purchasable %d", f.byKind, res.Verify.Purchasable)
	}
	out.Reset()
	if res, err = run(t, context.Background(), cfg, f); err != nil || res.Writes != 0 {
		t.Fatalf("third run: %v, %d writes", err, res.Writes)
	}
}

func TestLostResponseIsRetriedWithTheSameKey(t *testing.T) {
	in := loadInputs(t)
	f, _ := clusterLike(in)
	// the 3rd price and the 5th publish are applied but their answer is lost (503)
	f.dropResponse = func(kind string, n int) bool {
		return (kind == "item-price" && n == 3) || (kind == "content-publish" && n == 5)
	}
	var out bytes.Buffer
	res, err := run(t, context.Background(), config("", in, &out), f)
	if err != nil {
		t.Fatalf("%v\n%s", err, out.String())
	}
	n := len(in.fx.Products)
	if f.byKind["item-price"] != n || f.byKind["content-publish"] != n || res.Verify.Purchasable != n {
		t.Fatalf("a retried write must replay, not apply twice: %v", f.byKind)
	}
}

func TestMissingCatalogueItemFailsBeforeAnyWrite(t *testing.T) {
	in := loadInputs(t)
	sku := in.fx.Products[len(in.fx.Products)/2].SKU

	cases := map[string]func(f *fakeCluster, ids map[string]string) inputs{
		"title not in the catalogue": func(f *fakeCluster, ids map[string]string) inputs {
			delete(f.items, ids[sku])
			return in
		},
		"catalogue item discontinued": func(f *fakeCluster, ids map[string]string) inputs {
			f.items[ids[sku]].State = "DISCONTINUED"
			return in
		},
		"sku not in the network data file": func(f *fakeCluster, ids map[string]string) inputs {
			titles := map[string]string{}
			for k, v := range in.titles {
				if k != sku {
					titles[k] = v
				}
			}
			out := in
			out.network = writeNetwork(t, titles)
			return out
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			f, ids := clusterLike(in)
			f.tax, f.charges = nil, nil // a write would be planned for them too
			use := mutate(f, ids)
			var out bytes.Buffer
			_, err := run(t, context.Background(), config("", use, &out), f)
			if err == nil {
				t.Fatal("want a CHECK failure")
			}
			if f.posts != 0 {
				t.Fatalf("%d POSTs before the failure; CHECK must fail before any write", f.posts)
			}
			if !strings.Contains(err.Error(), sku) {
				t.Fatalf("the error must name %s: %v", sku, err)
			}
			if name != "sku not in the network data file" && !strings.Contains(err.Error(), "run the retail-network seed first") {
				t.Fatalf("the error must tell the user to run the network seed: %v", err)
			}
		})
	}
}

func TestWithdrawnPriceAndRetiredContentAreReportedNotResurrected(t *testing.T) {
	in := loadInputs(t)
	f, ids := clusterLike(in)
	a, b := in.fx.Products[0].SKU, in.fx.Products[1].SKU
	f.prices[ids[a]] = &ItemPrice{CatalogItemID: ids[a], State: "WITHDRAWN", ListPrice: Money{1, "USD"}, EffectivePrice: Money{1, "USD"}, Version: 2}
	f.contents[ids[b]] = &ProductContent{CatalogItemID: ids[b], State: "RETIRED", Revision: 1, PublishedRevision: 1}
	var out bytes.Buffer
	cfgB := config("", in, &out)
	cfgB.VerifyTimeout = 500 * time.Millisecond
	res, err := run(t, context.Background(), cfgB, f)
	if err == nil {
		t.Fatal("blocked products must fail the run")
	}
	if len(res.Plan.Blocked) != 2 {
		t.Fatalf("want 2 blocked products, got %v", res.Plan.Blocked)
	}
	if f.prices[ids[a]].State != "WITHDRAWN" || f.prices[ids[a]].Version != 2 {
		t.Fatal("a WITHDRAWN price must be left alone")
	}
	if f.contents[ids[b]].State != "RETIRED" || f.contents[ids[b]].Revision != 1 {
		t.Fatal("RETIRED content must be left alone")
	}
	if res.Verify == nil || res.Verify.Purchasable != len(in.fx.Products)-2 {
		t.Fatalf("every other product must still be provisioned: %+v", res.Verify)
	}
	if !strings.Contains(out.String(), "BLOCKED") {
		t.Fatalf("blocked products must be reported:\n%s", out.String())
	}
}

func TestChangedPriceAndDriftedContentConverge(t *testing.T) {
	in := loadInputs(t)
	f, ids := clusterLike(in)
	var out bytes.Buffer
	cfg := config("", in, &out)
	if _, err := run(t, context.Background(), cfg, f); err != nil {
		t.Fatalf("%v\n%s", err, out.String())
	}
	a, b := in.fx.Products[2], in.fx.Products[3]
	f.prices[ids[a.SKU]].ListPrice.AmountMinor = 1
	f.prices[ids[a.SKU]].Version = 7
	f.contents[ids[b.SKU]].Description = "edited by hand"
	f.contents[ids[b.SKU]].Revision++
	f.contents[ids[b.SKU]].PublishedRevision++

	out.Reset()
	res, err := run(t, context.Background(), cfg, f)
	if err != nil {
		t.Fatalf("%v\n%s", err, out.String())
	}
	if res.Plan.PricesToChange != 1 || res.Plan.ContentsToRevise != 1 || res.Writes != 3 {
		t.Fatalf("want 1 set-list-price + 1 revise + 1 publish, got %d writes: %v", res.Writes, res.Plan.Summary())
	}
	if f.prices[ids[a.SKU]].ListPrice.AmountMinor != a.ListPriceMinor || f.prices[ids[a.SKU]].Version != 8 {
		t.Fatalf("price not converged: %+v", f.prices[ids[a.SKU]])
	}
	pc := f.contents[ids[b.SKU]]
	if pc.Description == "edited by hand" || pc.State != "PUBLISHED" || pc.PublishedRevision != pc.Revision {
		t.Fatalf("content not converged: %+v", pc)
	}
}

func TestDryRunAndVerifyOnlyWriteNothing(t *testing.T) {
	in := loadInputs(t)
	f, _ := clusterLike(in)
	f.tax = nil
	var out bytes.Buffer
	cfg := config("", in, &out)
	cfg.DryRun = true
	res, err := run(t, context.Background(), cfg, f)
	if err != nil || f.posts != 0 || res.Plan.Writes() == 0 {
		t.Fatalf("dry run: err %v, %d POSTs, %d planned", err, f.posts, res.Plan.Writes())
	}
	cfg.DryRun, cfg.VerifyOnly = false, true
	cfg.VerifyTimeout = 50 * time.Millisecond
	if _, err := run(t, context.Background(), cfg, f); err == nil || f.posts != 0 {
		t.Fatalf("verify-only on an unseeded storefront must fail without writing: err %v, %d POSTs", err, f.posts)
	}
}

func TestPlanIsPureAndKeysAreDeterministic(t *testing.T) {
	in := loadInputs(t)
	f, _ := clusterLike(in)
	byTitle := map[string][]CatalogItem{}
	for _, it := range f.items {
		byTitle[it.Title] = append(byTitle[it.Title], CatalogItem{CatalogItemID: it.ID, Title: it.Title, State: it.State})
	}
	d1, miss := buildDesired(in.fx, in.phys, in.titles, byTitle, "http://storefront.localhost/media/departments")
	d2, _ := buildDesired(in.fx, in.phys, in.titles, byTitle, "http://storefront.localhost/media/departments")
	if len(miss) != 0 || !reflect.DeepEqual(d1, d2) {
		t.Fatalf("desired state is not deterministic (missing %v)", miss)
	}
	obs := Observed{Tax: f.tax, Charges: f.charges, Tree: CategoryTree{Version: 1, Categories: f.cats}, Prices: map[string]*ItemPrice{}, Contents: map[string]*ProductContent{}}
	p1, p2 := BuildPlan(*d1, obs), BuildPlan(*d1, obs)
	if !reflect.DeepEqual(p1, p2) {
		t.Fatal("BuildPlan is not deterministic")
	}
	k1 := idempotencyKey("item-price", "/pricing/item-prices", map[string]any{"b": 1, "a": "x"})
	k2 := idempotencyKey("item-price", "/pricing/item-prices", map[string]any{"a": "x", "b": 1})
	k3 := idempotencyKey("item-price", "/pricing/item-prices", map[string]any{"a": "y", "b": 1})
	if k1 != k2 || k1 == k3 || !strings.HasPrefix(k1, "seed-sf:") || len(k1) != len("seed-sf:")+40 {
		t.Fatalf("keys: %s %s %s", k1, k2, k3)
	}
}

// TestFakeEnforcesTheContract pins the fake itself to the published invariants, so a seeder that
// passes against it respects them.
func TestFakeEnforcesTheContract(t *testing.T) {
	f := newFakeCluster()
	id := f.addItem("A real item", "ACTIVE")
	gone := f.addItem("A discontinued item", "DISCONTINUED")
	for i := 0; i < 20; i++ {
		f.addItem("Filler item "+string(rune('a'+i)), "ACTIVE")
	}
	srv := f.start()
	defer srv.Close()
	api := srv.URL + "/api"
	usd := func(n int64) Money { return Money{n, "USD"} }
	tiers := desiredTiers([]DeliveryCharge{{Tier: "SAME_DAY", ChargeMinor: 1}, {Tier: "NEXT_DAY", ChargeMinor: 1}, {Tier: "TWO_DAY", ChargeMinor: 1}, {Tier: "STANDARD", ChargeMinor: 1}}, "USD")

	steps := []struct {
		name, path, key string
		body            any
		want            int
		slug            string
	}{
		{"no idempotency key", "/pricing/tax-tables", "", map[string]any{"version": 1, "bands": []TaxBand{{"786", "787", 825}}}, 400, "idempotency-key-required"},
		{"key too long", "/pricing/tax-tables", strings.Repeat("k", 129), map[string]any{"version": 1, "bands": []TaxBand{{"786", "787", 825}}}, 400, "idempotency-key-required"},
		{"tax v2", "/pricing/tax-tables", "t2", map[string]any{"version": 2, "bands": []TaxBand{{"786", "787", 825}}}, 201, ""},
		{"tax v2 replayed", "/pricing/tax-tables", "t2", map[string]any{"version": 2, "bands": []TaxBand{{"786", "787", 825}}}, 201, ""},
		{"same key, other body", "/pricing/tax-tables", "t2", map[string]any{"version": 9, "bands": []TaxBand{{"786", "787", 825}}}, 422, "idempotency-key-reuse"},
		{"tax version not increasing", "/pricing/tax-tables", "t1", map[string]any{"version": 2, "bands": []TaxBand{{"786", "787", 825}}}, 409, "non-increasing-version"},
		{"schedule missing a tier", "/pricing/delivery-charge-schedules", "s1", map[string]any{"version": 1, "tiers": tiers[:3]}, 422, "incomplete-charge-schedule"},
		{"schedule in EUR", "/pricing/delivery-charge-schedules", "s2", map[string]any{"version": 1, "tiers": []TierCharge{{Tier: "SAME_DAY", Charge: Money{1, "EUR"}}}}, 422, "currency-mismatch"},
		{"price in EUR", "/pricing/item-prices", "p1", map[string]any{"catalogItemId": id, "listPrice": Money{100, "EUR"}}, 422, "currency-mismatch"},
		{"price of a discontinued item", "/pricing/item-prices", "p2", map[string]any{"catalogItemId": gone, "listPrice": usd(100)}, 422, "unknown-catalog-item"},
		{"price", "/pricing/item-prices", "p3", map[string]any{"catalogItemId": id, "listPrice": usd(100)}, 201, ""},
		{"second price create", "/pricing/item-prices", "p4", map[string]any{"catalogItemId": id, "listPrice": usd(100)}, 409, "concurrent-update"},
		{"stale price version", "/pricing/item-prices/" + id + ":set-list-price", "p5", map[string]any{"listPrice": usd(200), "version": 5}, 409, "concurrent-update"},
		{"unknown parent category", "/product-content/categories", "c1", map[string]any{"parentId": "cat-nope", "name": "X"}, 422, "unknown-category"},
		{"category", "/product-content/categories", "c2", map[string]any{"name": "Home"}, 201, ""},
		{"duplicate sibling", "/product-content/categories", "c3", map[string]any{"name": "home"}, 422, "duplicate-sibling-name"},
		{"content with unknown category", "/product-content/product-contents", "pc1", map[string]any{"catalogItemId": id, "description": "d", "primaryCategoryId": "cat-nope"}, 422, "unknown-category"},
		{"incomplete draft", "/product-content/product-contents", "pc2", map[string]any{"catalogItemId": id, "description": "d"}, 201, ""},
		{"publish incomplete", "/product-content/product-contents/" + id + ":publish", "pc3", nil, 422, "incomplete-content"},
		{"too many bullets", "/product-content/product-contents/" + id + ":revise", "pc4", map[string]any{"bullets": strings.Split("a b c d e f g h i j k", " ")}, 422, "too-many-bullets"},
		{"complete revise", "/product-content/product-contents/" + id + ":revise", "pc5", map[string]any{"description": "d", "primaryCategoryId": "cat-0001", "media": []Media{{"m1", "http://h/x.svg", "alt", "PRIMARY"}}}, 200, ""},
		{"publish", "/product-content/product-contents/" + id + ":publish", "pc6", nil, 200, ""},
	}
	for _, s := range steps {
		status, body := post(t, api+s.path, s.key, s.body)
		if status != s.want || (s.slug != "" && !strings.Contains(body, "/problems/"+s.slug)) {
			t.Fatalf("%s: HTTP %d %s, want %d %s", s.name, status, body, s.want, s.slug)
		}
	}
	f.contents[id].State = "RETIRED"
	if status, body := post(t, api+"/product-content/product-contents/"+id+":revise", "pc7", map[string]any{"description": "e"}); status != 409 || !strings.Contains(body, "content-retired") {
		t.Fatalf("revise RETIRED: %d %s", status, body)
	}
	f.prices[id].State = "WITHDRAWN"
	if status, body := post(t, api+"/pricing/item-prices/"+id+":set-list-price", "p6", map[string]any{"listPrice": usd(300)}); status != 409 || !strings.Contains(body, "illegal-transition") {
		t.Fatalf("set price of WITHDRAWN: %d %s", status, body)
	}

	// paging: the page size is capped, the cursor walks every item exactly once
	c := newClient(api, "", srv.Client(), true)
	all, err := readCatalog(context.Background(), c)
	if err != nil || len(all) != len(f.items) {
		t.Fatalf("paging read %d of %d items: %v", len(all), len(f.items), err)
	}
	seen := map[string]bool{}
	for _, it := range all {
		if seen[it.CatalogItemID] {
			t.Fatalf("item %s twice", it.CatalogItemID)
		}
		seen[it.CatalogItemID] = true
	}
	if err := c.write(context.Background(), "x", "/pricing/tax-tables", map[string]int{}, nil, nil, 201); !errors.Is(err, errDryRun) {
		t.Fatalf("a read-only client must refuse writes, got %v", err)
	}
}
