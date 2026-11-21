package collect

import (
	"slices"
	"strings"
	"testing"

	"github.com/claudioed/warehouse-e2e/internal/retailday"
)

func stockWorld(t *testing.T) *world {
	w := newWorld(t)
	seedDelivered(w, "1") // SKU-A usable 10, SKU-B 5, both on sale
	w.mu.Lock()
	w.bff.products["CAT-C"] = &bffProduct{CatalogItemID: "CAT-C", Title: "C", InStock: false, Purchasable: false}
	w.mu.Unlock()
	return w
}

func TestTheShelfIsUsableStockAndWhetherTheStorefrontSellsIt(t *testing.T) {
	w := stockWorld(t)
	got, err := newCollector(w).Shelf(t.Context(), []string{"SKU-B", "SKU-A", "SKU-C"})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]retailday.Shelf{"SKU-A": {Usable: 10, Purchasable: true}, "SKU-B": {Usable: 5, Purchasable: true}, "SKU-C": {Usable: 0, Purchasable: false}}
	if len(got) != len(want) {
		t.Fatalf("shelf %v want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("%s: %+v want %+v", k, got[k], v)
		}
	}
	noGets(t, w)

	// What the estate audit makes of it: stock and the listing agree.
	var stock []retailday.StockFacts
	for _, sku := range []string{"SKU-A", "SKU-B", "SKU-C"} {
		s := got[sku]
		stock = append(stock, retailday.StockFacts{SKU: sku, UsableBefore: s.Usable, UsableAfter: s.Usable, Purchasable: s.Purchasable})
	}
	if fs := retailday.AuditEstate(stock, 0, 0, 0); len(fs) != 0 {
		t.Fatalf("a consistent shelf has findings: %+v", fs)
	}
}

// A stock that ran out while the storefront still sells it is reported as it is: the estate audit,
// not the collector, calls it a finding.
func TestAShelfThatDisagreesWithItselfIsReportedAsItIs(t *testing.T) {
	w := stockWorld(t)
	w.mu.Lock()
	w.inv.usable["SKU-A"] = 0 // sold out, yet CAT-A still purchasable
	w.bff.products["CAT-B"].Purchasable = false
	w.mu.Unlock() // SKU-B has 5, yet no longer sold
	got, err := newCollector(w).Shelf(t.Context(), []string{"SKU-A", "SKU-B"})
	if err != nil {
		t.Fatal(err)
	}
	if got["SKU-A"] != (retailday.Shelf{Usable: 0, Purchasable: true}) || got["SKU-B"] != (retailday.Shelf{Usable: 5, Purchasable: false}) {
		t.Fatalf("shelf %+v", got)
	}
	var stock []retailday.StockFacts
	for _, sku := range []string{"SKU-A", "SKU-B"} {
		stock = append(stock, retailday.StockFacts{SKU: sku, UsableBefore: got[sku].Usable, UsableAfter: got[sku].Usable, Purchasable: got[sku].Purchasable})
	}
	if n := len(retailday.AuditEstate(stock, 0, 0, 0)); n != 2 {
		t.Fatalf("%d findings, want the two storefront/stock disagreements", n)
	}
}

// An unknown SKU is 200 with usable 0 (inventory-storage), a stub product is 404 (not sold): both are
// answers.
func TestAnUnknownSKUAndAnInvisibleProductAreAnswers(t *testing.T) {
	w := stockWorld(t)
	c := newCollector(w, func(s *Settings) { s.Catalogue["SKU-NEW"] = "CAT-NEW" })
	got, err := c.Shelf(t.Context(), []string{"SKU-NEW"})
	if err != nil {
		t.Fatal(err)
	}
	if got["SKU-NEW"] != (retailday.Shelf{}) {
		t.Fatalf("shelf %+v", got["SKU-NEW"])
	}
}

func TestTheSKUsAreReadOnceEachInAFixedOrder(t *testing.T) {
	w := stockWorld(t)
	w.reset()
	got, err := newCollector(w).Shelf(t.Context(), []string{"SKU-B", "SKU-A", "SKU-A", ""})
	if err != nil || len(got) != 2 {
		t.Fatalf("shelf %v err %v", got, err)
	}
	if n := w.requests("inventory-storage", "/inventory/"); n != 2 {
		t.Fatalf("%d inventory reads for two SKUs", n)
	}
	if n := w.requests("storefront-bff", "/products/"); n != 2 {
		t.Fatalf("%d product reads for two SKUs", n)
	}
	got, err = newCollector(w).Shelf(t.Context(), nil)
	if err != nil || len(got) != 0 {
		t.Fatalf("no SKUs: %v %v", got, err)
	}
}

// A SKU that cannot be read fails the call and no map comes back: a partial one would pass the
// estate rules as zero stock and "not sold".
func TestAShelfThatCannotBeReadIsAnErrorNeverAZero(t *testing.T) {
	for _, tc := range []struct {
		name string
		do   func(w *world)
		want []string
	}{
		{"inventory-storage answers 500", func(w *world) { w.fail("inventory-storage", 500) },
			[]string{"unread Usable SKU-A: inventory-storage /inventory/{sku}/usable: HTTP 500 internal-error", "unread Usable SKU-B: inventory-storage /inventory/{sku}/usable: HTTP 500"}},
		{"inventory-storage answers 404: its route declares none", func(w *world) { w.failRoute("inventory-storage", "/inventory/{sku}/usable", 404) },
			[]string{"unread Usable SKU-A: inventory-storage /inventory/{sku}/usable: HTTP 404"}},
		{"the BFF answers 503", func(w *world) { w.fail("storefront-bff", 503) },
			[]string{"unread Purchasable SKU-A: storefront-bff /products/{id}: HTTP 503 internal-error"}},
		{"the BFF drops the connection", func(w *world) { w.drop("storefront-bff") },
			[]string{"unread Purchasable SKU-A: storefront-bff /products/{id}: no usable answer"}},
		{"no usable count in the body", func(w *world) { w.rawBody("inventory-storage", "/inventory/{sku}/usable", `{"sku":"x"}`) },
			[]string{"unread Usable SKU-A: inventory-storage /inventory/{sku}/usable: the body has no usable count"}},
		{"no purchasable flag in the body", func(w *world) { w.rawBody("storefront-bff", "/products/{id}", `{"inStock":true}`) },
			[]string{"unread Purchasable SKU-A: storefront-bff /products/{id}: the body has no purchasable flag"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := stockWorld(t)
			tc.do(w)
			got, err := newCollector(w).Shelf(t.Context(), []string{"SKU-A", "SKU-B"})
			if err == nil || got != nil {
				t.Fatalf("shelf %v err %v: a failed read must be an error and no map", got, err)
			}
			for _, frag := range tc.want {
				if !strings.Contains(err.Error(), frag) {
					t.Fatalf("error %q lacks %q", err, frag)
				}
			}
			noGets(t, w)
		})
	}
}

func TestASKUTheCatalogueDoesNotKnowIsAnErrorAndEveryProblemIsNamedInOrder(t *testing.T) {
	w := stockWorld(t)
	w.failRoute("inventory-storage", "/inventory/{sku}/usable", 500)
	got, err := newCollector(w).Shelf(t.Context(), []string{"SKU-ZZ", "SKU-A", "SKU-B"})
	if err == nil || got != nil {
		t.Fatalf("shelf %v err %v", got, err)
	}
	msg := err.Error()
	order := []string{"unread Purchasable SKU-ZZ: Settings.Catalogue has no catalogItemId", "unread Usable SKU-A", "unread Usable SKU-B", "unread Usable SKU-ZZ"}
	last := -1
	for _, frag := range order {
		i := strings.Index(msg, frag)
		if i < 0 || i < last {
			t.Fatalf("error %q: %q missing or out of order", msg, frag)
		}
		last = i
	}
	if slices.Contains(strings.Split(msg, "; "), "") {
		t.Fatalf("error %q", msg)
	}
}
