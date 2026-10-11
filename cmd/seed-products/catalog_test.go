package main

import (
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
