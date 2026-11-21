package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestFixtureIsConsistentWithTheRealData: every fixture SKU is a real product of
// fulfilment-products.json with real dimensions and weight, every department referenced exists
// (and has a product), every price is > 0, and pricing gets a complete schedule. When the sibling
// warehouse-infra checkout is present, every SKU must also have its catalogue title there.
func TestFixtureIsConsistentWithTheRealData(t *testing.T) {
	fx, err := LoadFixture(filepath.Join(repoRoot, "fixtures/storefront/catalogue.json"))
	if err != nil {
		t.Fatal(err)
	}
	phys, err := LoadPhysical(filepath.Join(repoRoot, "fixtures/products/fulfilment-products.json"))
	if err != nil {
		t.Fatal(err)
	}
	if problems := fx.Validate(phys, nil); len(problems) > 0 {
		t.Fatalf("%d problems:\n%v", len(problems), problems)
	}
	if len(fx.Products) != 81 || len(fx.Departments) != 13 {
		t.Fatalf("the curated fixture has 81 products in 13 departments, got %d in %d", len(fx.Products), len(fx.Departments))
	}

	path := siblingNetworkData()
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Skipf("sibling warehouse-infra data file %s is absent (single-repo checkout, e.g. CI): the per-SKU title check needs it; set STOREFRONT_NETWORK_DATA to point at it", path)
	}
	titles, err := LoadNetworkTitles(path)
	if err != nil {
		t.Fatal(err)
	}
	if problems := fx.Validate(phys, titles); len(problems) > 0 {
		t.Fatalf("against %s, %d problems (is warehouse-infra up to date?):\n%v", path, len(problems), problems)
	}
}

func TestValidateCatchesInconsistencies(t *testing.T) {
	free := int64(10)
	fx := &Fixture{
		Currency: "EUR",
		TaxBands: []TaxBand{{"787", "786", 825}},
		DeliveryCharges: []DeliveryCharge{
			{Tier: "SAME_DAY", ChargeMinor: 1}, {Tier: "NEXT_DAY", ChargeMinor: 1}, {Tier: "TWO_DAY", ChargeMinor: 1, FreeOverMinor: &free},
		},
		DeliverablePostalPrefixes: []string{"787"},
		Departments:               []Department{{"home", "Home"}, {"empty", "Empty"}},
		Products: []FixtureProduct{
			{SKU: "A", Department: "home", Subcategory: "Lamps", ListPriceMinor: 0},
			{SKU: "B", Department: "garden", Subcategory: "Pots/Planters", ListPriceMinor: 100},
		},
	}
	phys := map[string]PhysicalProduct{"A": {SKU: "A", Dimensions: &Dimensions{1, 1, 1, 1}}}
	problems := fx.Validate(phys, map[string]string{"A": "Lamp"})
	want := []string{"currency", "taxBands[0]", "STANDARD missing", "price 0", "department \"garden\"", "Pots/Planters",
		"B is not in fixtures/products", "B has no catalogue title", "empty has no product"}
	for _, w := range want {
		found := false
		for _, p := range problems {
			if strings.Contains(p, w) {
				found = true
			}
		}
		if !found {
			t.Errorf("no problem mentions %q in %v", w, problems)
		}
	}
}
