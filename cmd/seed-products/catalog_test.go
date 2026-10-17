package main

import (
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestValidGTIN(t *testing.T) {
	tests := []struct {
		code string
		want bool
	}{
		{"4006381333931", true},  // EAN-13 (GS1 reference example)
		{"96385074", true},       // EAN-8
		{"036000291452", true},   // UPC-A
		{"10012345678902", true}, // GTIN-14
		{"4006381333932", false}, // wrong check digit
		{"036000291453", false},  // wrong check digit
		{"400638133393", false},  // EAN-13 minus a digit is a bad UPC-A
		{"12345", false},         // not a GTIN length
		{"40063813339A1", false}, // non-digit
		{"", false},
	}
	for _, tt := range tests {
		if got := validGTIN(tt.code); got != tt.want {
			t.Errorf("validGTIN(%q) = %v, want %v", tt.code, got, tt.want)
		}
	}
}

func TestBuildDescription(t *testing.T) {
	tests := []struct {
		name, brand, product, qty, want string
	}{
		{"brand name quantity", "Danone,Activia", "Nature yogurt", "4 x 125 g", "Danone Nature yogurt (4 x 125 g)"},
		{"brand already in name", "Nutella", "Nutella hazelnut spread", "400 g", "Nutella hazelnut spread (400 g)"},
		{"no quantity", "Barilla", "Spaghetti n.5", "", "Barilla Spaghetti n.5"},
		{"whitespace and control chars cleaned", " Kellogg's ", "Corn\tFlakes\x07  ", "500\u00a0g", "Kellogg's Corn Flakes (500 g)"},
		{"brand already in name despite punctuation", "Coca-Cola", "Coca Cola", "2 L", "Coca Cola (2 L)"},
		{"quantity already in name despite spacing", "Chergui", "Yaourt a Boire Daya Fraise 900g", "900 g", "Chergui Yaourt a Boire Daya Fraise 900g"},
		{"a brand that only prefixes a word is kept", "Yum", "Yummy noodles", "", "Yum Yummy noodles"},
		{"multi-word brand written as one word", "YumYum", "Yum Yum Chicken Flavour", "60 gram", "Yum Yum Chicken Flavour (60 gram)"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := buildDescription(tt.brand, tt.product, tt.qty); got != tt.want {
				t.Fatalf("buildDescription = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestBuildDescriptionFitsTheRuneLimit(t *testing.T) {
	long := strings.Repeat("é", 300) // 2 bytes each: the limit is runes, not bytes
	got := buildDescription("", long, "1 kg")
	if n := utf8.RuneCountInString(got); n != maxDescriptionRunes {
		t.Fatalf("description has %d runes, want exactly %d", n, maxDescriptionRunes)
	}
}

func TestDeriveTemperature(t *testing.T) {
	tests := []struct {
		name string
		tags []string
		want string
	}{
		{"frozen", []string{"en:frozen-foods", "en:pizzas"}, "Frozen"},
		{"frozen beats chilled", []string{"en:cheeses", "en:frozen-foods"}, "Frozen"},
		{"chilled yogurt", []string{"en:dairies", "en:yogurts"}, "Chilled"},
		{"shelf-stable dairy stays unclassified", []string{"en:dairies", "en:milks"}, ""},
		{"ambient", []string{"en:beverages", "en:waters"}, ""},
		{"no tags", nil, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, evidence := deriveTemperature(tt.tags)
			if got != tt.want {
				t.Fatalf("deriveTemperature = %q, want %q", got, tt.want)
			}
			if (got == "") != (len(evidence) == 0) {
				t.Fatalf("evidence %v must be present exactly when classified", evidence)
			}
		})
	}
}

func TestFixtureValidateRejectsBadProducts(t *testing.T) {
	good := FixtureProduct{SKU: "4006381333931", Description: "Stabilo pen"}
	tests := []struct {
		name string
		ps   []FixtureProduct
	}{
		{"bad gtin", []FixtureProduct{{SKU: "4006381333932", Description: "x"}}},
		{"duplicate", []FixtureProduct{good, good}},
		{"empty description", []FixtureProduct{{SKU: "4006381333931"}}},
		{"control char", []FixtureProduct{{SKU: "4006381333931", Description: "a\x07b"}}},
		{"unknown temperature", []FixtureProduct{{SKU: "4006381333931", Description: "x", TemperatureClass: "Warm"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := (Fixture{Products: tt.ps}).validate(); err == nil {
				t.Fatal("validate accepted an invalid fixture")
			}
		})
	}
	if err := (Fixture{Products: []FixtureProduct{good}}).validate(); err != nil {
		t.Fatalf("valid fixture rejected: %v", err)
	}
}

func TestToFixtureProductPrefersEnglishName(t *testing.T) {
	fp, ok := toFixtureProduct(offProduct{Code: "4006381333931", ProductName: "Yaourt nature", ProductNameEn: "Plain yogurt", Brands: "Danone", Quantity: "125 g", Categories: []string{"en:yogurts"}})
	if !ok {
		t.Fatal("valid product dropped")
	}
	if fp.Description != "Danone Plain yogurt (125 g)" || fp.TemperatureClass != "Chilled" {
		t.Fatalf("got %+v", fp)
	}
	if _, ok := toFixtureProduct(offProduct{Code: "4006381333932", ProductName: "x"}); ok {
		t.Fatal("invalid GTIN kept")
	}
	if _, ok := toFixtureProduct(offProduct{Code: "4006381333931", ProductName: "  "}); ok {
		t.Fatal("nameless product kept")
	}
}

func TestValidateFulfilmentFields(t *testing.T) {
	dims := &Dimensions{LengthMm: 300, WidthMm: 200, HeightMm: 100, WeightG: 1500}
	ok := FixtureProduct{SKU: "ABO-B07QDZN8VR", Description: "Wall mount", Source: "abo", Dimensions: dims}
	tests := []struct {
		name string
		mut  func(p *FixtureProduct)
	}{
		{"sku with slash", func(p *FixtureProduct) { p.SKU = "ABO/1" }},
		{"sku with space", func(p *FixtureProduct) { p.SKU = "ABO 1" }},
		{"dimension zero", func(p *FixtureProduct) { p.Dimensions = &Dimensions{0, 1, 1, 1} }},
		{"dimension over limit", func(p *FixtureProduct) { p.Dimensions = &Dimensions{20001, 1, 1, 1} }},
		{"weight over limit", func(p *FixtureProduct) { p.Dimensions = &Dimensions{1, 1, 1, 2000001} }},
		{"unknown tag", func(p *FixtureProduct) { p.HandlingTags = []string{"Shiny"} }},
		{"duplicate tag", func(p *FixtureProduct) { p.HandlingTags = []string{"Fragile", "Fragile"} }},
		{"temperature tag without class", func(p *FixtureProduct) { p.HandlingTags = []string{"TemperatureSensitive"} }},
		{"dot class without hazmat", func(p *FixtureProduct) { p.DotHazardClass = 3 }},
		{"dot class out of range", func(p *FixtureProduct) { p.HandlingTags = []string{"Hazmat"}; p.DotHazardClass = 10 }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := ok
			tt.mut(&p)
			if err := (Fixture{Products: []FixtureProduct{p}}).validate(); err == nil {
				t.Fatal("validate accepted an invalid fulfilment product")
			}
		})
	}
	good := ok
	good.HandlingTags = []string{"Hazmat", "TemperatureSensitive"}
	good.TemperatureClass = "Chilled"
	good.DotHazardClass = 3
	if err := (Fixture{Products: []FixtureProduct{good}}).validate(); err != nil {
		t.Fatalf("valid fulfilment product rejected: %v", err)
	}
}

func TestClassificationAddsTemperatureSensitiveForOpenFoodFacts(t *testing.T) {
	// An Open Food Facts fixture entry carries only the class.
	tags, temp, dot, ok := FixtureProduct{SKU: "4006381333931", TemperatureClass: "Frozen"}.classification()
	if !ok || temp != "Frozen" || dot != 0 || !sameSet(tags, []string{"TemperatureSensitive"}) {
		t.Fatalf("got %v %q %d %v", tags, temp, dot, ok)
	}
	// Tags come back in product-master's enum order whatever the fixture order.
	tags, _, _, _ = FixtureProduct{HandlingTags: []string{"HighValue", "Fragile", "Hazmat"}}.classification()
	if want := []string{"Hazmat", "Fragile", "HighValue"}; !equal(tags, want) {
		t.Fatalf("tags %v, want %v", tags, want)
	}
	if _, _, _, ok := (FixtureProduct{}).classification(); ok {
		t.Fatal("an unclassified product must not be classified")
	}
}

func TestLoadFixturesRejectsASkuInTwoFiles(t *testing.T) {
	dir := t.TempDir()
	write := func(name string, ps ...FixtureProduct) string {
		path := filepath.Join(dir, name)
		if err := writeFixture(path, Fixture{Products: ps}); err != nil {
			t.Fatal(err)
		}
		return path
	}
	a := write("a.json", FixtureProduct{SKU: "WANDS-1", Description: "Bed", Source: "wands"})
	b := write("b.json", FixtureProduct{SKU: "WANDS-1", Description: "Bed again", Source: "wands"})
	c := write("c.json", FixtureProduct{SKU: "WANDS-2", Description: "Chair", Source: "wands"})
	if _, err := loadFixtures([]string{a, b}); err == nil {
		t.Fatal("a SKU in two files was accepted")
	}
	got, err := loadFixtures([]string{a, c})
	if err != nil || len(got) != 2 {
		t.Fatalf("got %d products, err %v", len(got), err)
	}
}

func TestVerifyChecksDimensionsAndClassification(t *testing.T) {
	p := FixtureProduct{
		SKU: "WANDS-9", Description: "Sofa", Source: "wands",
		Dimensions:   &Dimensions{LengthMm: 2000, WidthMm: 900, HeightMm: 850, WeightG: 60000},
		HandlingTags: []string{"Oversized"},
	}
	listed := listedProduct{SKU: p.SKU, Description: p.Description}
	listed.PhysicalProfile.Declared = &struct {
		LengthMm int64 `json:"lengthMm"`
		WidthMm  int64 `json:"widthMm"`
		HeightMm int64 `json:"heightMm"`
		WeightG  int64 `json:"weightG"`
	}{2000, 900, 850, 60000}
	listed.Classification = &struct {
		HandlingTags     []string `json:"handlingTags"`
		TemperatureClass string   `json:"temperatureClass"`
		DotHazardClass   *int     `json:"dotHazardClass"`
	}{HandlingTags: []string{"Oversized"}}
	if probs := verify([]FixtureProduct{p}, map[string]listedProduct{p.SKU: listed}, true); len(probs) != 0 {
		t.Fatalf("matching product reported: %v", probs)
	}
	listed.PhysicalProfile.Declared.WeightG = 1
	if probs := verify([]FixtureProduct{p}, map[string]listedProduct{p.SKU: listed}, true); len(probs) != 1 {
		t.Fatalf("wrong weight not caught: %v", probs)
	}
}

// The committed fixtures are the contract: at least 10,000 products carry real
// dimensions and weight, every one validates, and no SKU repeats.
func TestCommittedFixturesMeetTheCatalogueContract(t *testing.T) {
	products, err := loadFixtures([]string{
		"../../fixtures/products/openfoodfacts-products.json",
		"../../fixtures/products/fulfilment-products.json",
	})
	if err != nil {
		t.Fatal(err)
	}
	dims, tagged, hazmat := countFulfilment(products)
	if dims < 10000 {
		t.Fatalf("only %d products have real dimensions, want >= 10000", dims)
	}
	if hazmat == 0 || tagged == 0 {
		t.Fatalf("no handling coverage: %d tagged, %d hazmat", tagged, hazmat)
	}
	for _, p := range products {
		if strings.Contains(strings.ToLower(p.Description), "amazon") {
			t.Errorf("%s: description carries the retailer name: %q", p.SKU, p.Description)
		}
	}
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
