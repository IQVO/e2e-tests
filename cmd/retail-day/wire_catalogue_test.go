package main

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/claudioed/warehouse-e2e/internal/retailday/day"
	"github.com/claudioed/warehouse-e2e/internal/sim/shopper"
)

func TestResolveDictionaryPairsEachSKUWithItsOneActiveItem(t *testing.T) {
	titles := map[string]string{"S1": "t1", "S2": "t2", "S3": "t3", "S4": "t4", "S5": "t5", "S6": ""}
	catalog := []catalogItem{
		{"c1", "t1", "ACTIVE"},
		{"c2", "t2", "DISCONTINUED"},
		{"c3a", "t3", "ACTIVE"}, {"c3b", "t3", "ACTIVE"},
		{"c4old", "t4", "DISCONTINUED"}, {"c4", "t4", "ACTIVE"},
		// S5's title has no item at all; S6 has no title
	}
	d, err := resolveDictionary([]string{"S1", "S2", "S3", "S4", "S5", "S6"}, titles, catalog)
	if err != nil {
		t.Fatal(err)
	}
	if want := map[string]string{"S1": "c1", "S4": "c4"}; !reflect.DeepEqual(d.ItemBySKU, want) {
		t.Errorf("sku -> item %v, want %v", d.ItemBySKU, want)
	}
	if want := map[string]string{"c1": "S1", "c4": "S4"}; !reflect.DeepEqual(d.SKUByItem, want) {
		t.Errorf("item -> sku %v, want %v", d.SKUByItem, want)
	}
	if len(d.Unresolved) != 4 {
		t.Fatalf("unresolved %v", d.Unresolved)
	}
	for i, want := range []string{"S2", "S3", "S5", "S6"} {
		if !strings.HasPrefix(d.Unresolved[i], want+":") {
			t.Errorf("unresolved[%d] = %q, want it to name %s", i, d.Unresolved[i], want)
		}
	}
	if !strings.Contains(d.Unresolved[1], "2 ACTIVE") {
		t.Errorf("two ACTIVE items read: %q", d.Unresolved[1])
	}
}

func TestResolveDictionaryRefusesTwoSKUsBehindOneItem(t *testing.T) {
	_, err := resolveDictionary([]string{"S1", "S2"}, map[string]string{"S1": "same", "S2": "same"}, []catalogItem{{"c1", "same", "ACTIVE"}})
	if err == nil || !strings.Contains(err.Error(), "two SKUs") {
		t.Fatalf("err %v", err)
	}
}

func TestAnItemWithoutAnIdIsUnresolved(t *testing.T) {
	d, err := resolveDictionary([]string{"S1"}, map[string]string{"S1": "t"}, []catalogItem{{"", "t", "ACTIVE"}})
	if err != nil || len(d.ItemBySKU) != 0 || len(d.Unresolved) != 1 {
		t.Fatalf("%+v %v", d, err)
	}
}

func writeFile(t *testing.T, dir, name, body string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestTheFixturesTheDayReadsAreTheOnesInTheRepository(t *testing.T) {
	skus, err := fixtureSKUs("../../fixtures/storefront/catalogue.json")
	if err != nil {
		t.Fatal(err)
	}
	if len(skus) != 81 {
		t.Errorf("%d SKUs in the storefront fixture, the journey page says 81", len(skus))
	}
	site, err := loadSiteFixture("../../fixtures/storefront/fulfilment-site.json")
	if err != nil {
		t.Fatal(err)
	}
	if site.SiteID != "site-1" || site.WarehouseSiteID != "AUS3" || len(site.Cutoffs) != 2 || site.Cutoffs[1].CptID != "aus-1700" || site.Cutoffs[1].LocalTime != "17:00" || len(site.Cutoffs[1].EligiblePathIDs) == 0 {
		t.Errorf("site fixture %+v: the cutoffs did not decode into outbound.Cutoff", site)
	}
}

func TestFixtureReadersFailOnWhatTheyCannotUse(t *testing.T) {
	dir := t.TempDir()
	for name, fn := range map[string]func() error{
		"missing catalogue":    func() error { _, e := fixtureSKUs(filepath.Join(dir, "nope.json")); return e },
		"empty catalogue":      func() error { _, e := fixtureSKUs(writeFile(t, dir, "e.json", `{"products":[]}`)); return e },
		"broken catalogue":     func() error { _, e := fixtureSKUs(writeFile(t, dir, "b.json", `{`)); return e },
		"missing network data": func() error { _, e := networkTitles(filepath.Join(dir, "nope.json")); return e },
		"broken network data":  func() error { _, e := networkTitles(writeFile(t, dir, "n.json", `[`)); return e },
		"site without cutoffs": func() error {
			_, e := loadSiteFixture(writeFile(t, dir, "s.json", `{"siteId":"site-1","warehouseSiteId":"AUS3"}`))
			return e
		},
		"site without a building": func() error {
			_, e := loadSiteFixture(writeFile(t, dir, "s2.json", `{"siteId":"site-1","cutoffs":[{"cptId":"a","localTime":"11:00"}]}`))
			return e
		},
	} {
		if err := fn(); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
}

func TestLoadDictionaryDerivesItFromTheTwoFilesAndTheCatalog(t *testing.T) {
	dir := t.TempDir()
	fixture := writeFile(t, dir, "catalogue.json", `{"products":[{"sku":"A-1"},{"sku":"B-2"},{"sku":"C-3"}]}`)
	network := writeFile(t, dir, "seed.json", `{"catalog":[{"sku":"A-1","title":"title a"},{"sku":"B-2","title":"title b"},{"sku":"C-3","title":"title c"}]}`)
	f := newFakeCluster(t, map[string]http.HandlerFunc{
		// two pages: the walk must follow nextCursor
		"GET offer-availability/catalog": func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Query().Get("cursor") == "" {
				respondJSON(map[string]any{"items": []any{map[string]any{"catalogItemId": "ci-a", "title": "title a", "state": "ACTIVE"}}, "nextCursor": "p2"})(w, r)
				return
			}
			respondJSON(page(map[string]any{"catalogItemId": "ci-b", "title": "title b", "state": "ACTIVE"}))(w, r)
		},
	})
	d, err := loadDictionary(context.Background(), f.client(), fixture, network)
	if err != nil {
		t.Fatal(err)
	}
	if want := map[string]string{"A-1": "ci-a", "B-2": "ci-b"}; !reflect.DeepEqual(d.ItemBySKU, want) {
		t.Errorf("dictionary %v, want %v", d.ItemBySKU, want)
	}
	if len(d.Unresolved) != 1 || !strings.HasPrefix(d.Unresolved[0], "C-3:") {
		t.Errorf("unresolved %v", d.Unresolved)
	}
	if w := f.writes(); len(w) != 0 {
		t.Errorf("deriving the dictionary wrote: %v", w)
	}
}

func TestLoadDictionaryFailsWhenNothingPairsOrTheCatalogCannotBeRead(t *testing.T) {
	dir := t.TempDir()
	fixture := writeFile(t, dir, "catalogue.json", `{"products":[{"sku":"A-1"}]}`)
	network := writeFile(t, dir, "seed.json", `{"catalog":[{"sku":"A-1","title":"title a"}]}`)

	empty := newFakeCluster(t, map[string]http.HandlerFunc{"GET offer-availability/catalog": respondJSON(page())})
	if _, err := loadDictionary(context.Background(), empty.client(), fixture, network); err == nil || !strings.Contains(err.Error(), "none of the 1 fixture SKUs") {
		t.Errorf("nothing pairs: %v", err)
	}
	down := newFakeCluster(t, map[string]http.HandlerFunc{"GET offer-availability/catalog": func(w http.ResponseWriter, _ *http.Request) { problem(w, 503, "unavailable") }})
	if _, err := loadDictionary(context.Background(), down.client(), fixture, network); err == nil || !strings.Contains(err.Error(), "offer-availability GET /catalog") {
		t.Errorf("catalog down: %v", err)
	}
	if _, err := loadDictionary(context.Background(), down.client(), fixture, filepath.Join(dir, "missing.json")); err == nil {
		t.Error("a missing network data file did not fail")
	}
}

func TestCatalogueAdapterOffersOnlyPurchasableProductsWithASKU(t *testing.T) {
	products := []shopper.Product{
		{CatalogItemID: "ci-b", Purchasable: true},
		{CatalogItemID: "ci-a", Purchasable: true},
		{CatalogItemID: "ci-off", Purchasable: false},
		{CatalogItemID: "ci-stranger", Purchasable: true},
	}
	a := catalogueAdapter{
		list: func(context.Context) ([]shopper.Product, error) { return products, nil },
		dict: map[string]string{"ci-a": "SKU-A", "ci-b": "SKU-B", "ci-off": "SKU-OFF"},
	}
	got, err := a.Items(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := []day.Item{{CatalogItemID: "ci-a", SKU: "SKU-A"}, {CatalogItemID: "ci-b", SKU: "SKU-B"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("items %+v, want %+v", got, want)
	}
}

func TestCatalogueAdapterPassesTheListingErrorOn(t *testing.T) {
	a := catalogueAdapter{list: func(context.Context) ([]shopper.Product, error) { return nil, context.DeadlineExceeded }}
	if _, err := a.Items(context.Background()); err == nil {
		t.Fatal("a failed product listing read as an empty catalogue")
	}
}

func TestCatalogueListerReadsTheStorefrontsPurchasableProducts(t *testing.T) {
	f := newFakeCluster(t, map[string]http.HandlerFunc{
		"GET storefront/products": respondJSON(page(
			map[string]any{"catalogItemId": "ci-1", "title": "one", "purchasable": true},
			map[string]any{"catalogItemId": "ci-2", "title": "two", "purchasable": false},
		)),
	})
	got, err := newCatalogueLister(f.client())(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].CatalogItemID != "ci-1" {
		t.Errorf("products %+v", got)
	}
	if reqs := f.requests(); len(reqs) != 1 || reqs[0] != "GET storefront/products" {
		t.Errorf("requests %v: the shopper's catalogue is read through the BFF under the shopper's own key", reqs)
	}
}
