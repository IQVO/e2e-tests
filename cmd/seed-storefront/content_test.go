package main

import (
	"reflect"
	"regexp"
	"strings"
	"testing"
)

// templateWords is the whole fixed vocabulary of DeriveContent. Every other word of a derived
// text must come from the product's own data (title, department, subcategory) or be a number:
// that is what "no invented text" means here.
var templateWords = strings.Fields(`it measures length x width height and weighs ships from the
	fulfillment center in size weight handling fragile high-value item sold by storefront cm kg lb
	austin texas department category`)

var wordRE = regexp.MustCompile(`[\p{L}][\p{L}'-]*`)

func words(s string) []string {
	var out []string
	for _, w := range wordRE.FindAllString(strings.ToLower(s), -1) {
		out = append(out, strings.Trim(w, "-'"))
	}
	return out
}

func derivedFor(t *testing.T, in inputs) map[string]ContentBody {
	t.Helper()
	depts := in.fx.departmentBySlug()
	out := map[string]ContentBody{}
	for _, p := range in.fx.Products {
		pp := in.phys[p.SKU]
		facts := ProductFacts{
			SKU: p.SKU, Title: in.titles[p.SKU], DepartmentSlug: p.Department, DepartmentName: depts[p.Department].Name,
			Subcategory: p.Subcategory, Dims: *pp.Dimensions, Fragile: hasTag(pp, "Fragile"), HighValue: hasTag(pp, "HighValue"),
		}
		out[p.SKU] = DeriveContent(facts, defaultMediaBase)
	}
	return out
}

func TestDerivedContentInventsNothing(t *testing.T) {
	in := loadInputs(t)
	depts := in.fx.departmentBySlug()
	allowed := map[string]bool{}
	for _, w := range templateWords {
		allowed[w] = true
	}
	for _, p := range in.fx.Products {
		b := derivedFor(t, in)[p.SKU]
		own := map[string]bool{}
		for _, w := range words(in.titles[p.SKU] + " " + depts[p.Department].Name + " " + p.Subcategory) {
			own[w] = true
		}
		text := b.Description + " " + strings.Join(b.Bullets, " ")
		for _, a := range b.Attributes {
			text += " " + a.Name + " " + a.Value + " " + a.Unit
		}
		for _, w := range words(text) {
			if !allowed[w] && !own[w] {
				t.Fatalf("%s: word %q comes neither from the template nor from the product's data\n%s", p.SKU, w, text)
			}
		}
	}
}

func TestDerivedContentRespectsTheLimitsAndIsDeterministic(t *testing.T) {
	in := loadInputs(t)
	a, b := derivedFor(t, in), derivedFor(t, in)
	if !reflect.DeepEqual(a, b) {
		t.Fatal("DeriveContent is not deterministic")
	}
	for sku, body := range a {
		if why := checkContent(body); why != "" {
			t.Fatalf("%s violates product-content's rules: %s", sku, why)
		}
		if len(body.Media) != 1 || body.Media[0].AltText != clip(strings.TrimRight(in.titles[sku], "."), maxAltText) {
			t.Fatalf("%s media %+v: one PRIMARY medium whose alt text is the title", sku, body.Media)
		}
		if !strings.HasPrefix(body.Media[0].URL, defaultMediaBase+"/") || !strings.HasSuffix(body.Media[0].URL, ".svg") {
			t.Fatalf("%s media url %s", sku, body.Media[0].URL)
		}
	}
}

func TestDerivedContentOfAKnownProduct(t *testing.T) {
	// ABO-B07GWVHX45: 283 x 205 x 165 mm, 2260 g, no handling tag (fulfilment-products.json)
	f := ProductFacts{SKU: "X", Title: "Plastic Food Storage Container Set, 6 Pieces", DepartmentSlug: "kitchen-dining",
		DepartmentName: "Kitchen & Dining", Subcategory: "Food Storage", Dims: Dimensions{283, 205, 165, 2260}}
	b := DeriveContent(f, "http://storefront.localhost/media/departments/")
	wantDesc := "Plastic Food Storage Container Set, 6 Pieces, in Kitchen & Dining > Food Storage. " +
		"It measures 28.3 x 20.5 x 16.5 cm (11.1 x 8.1 x 6.5 in), length x width x height, and weighs 2.26 kg (4.98 lb). " +
		"Ships from the Austin, Texas fulfillment center."
	if b.Description != wantDesc {
		t.Fatalf("description:\n got %s\nwant %s", b.Description, wantDesc)
	}
	wantBullets := []string{
		"Size: 28.3 x 20.5 x 16.5 cm (11.1 x 8.1 x 6.5 in), length x width x height",
		"Weight: 2.26 kg (4.98 lb)",
		"Ships from and sold by the storefront, from the Austin, Texas fulfillment center",
	}
	if !reflect.DeepEqual(b.Bullets, wantBullets) {
		t.Fatalf("bullets: %q", b.Bullets)
	}
	wantAttrs := []Attribute{{"Length", "28.3", "cm"}, {"Width", "20.5", "cm"}, {"Height", "16.5", "cm"}, {"Weight", "2.26", "kg"},
		{"Department", "Kitchen & Dining", ""}, {"Category", "Food Storage", ""}}
	if !reflect.DeepEqual(b.Attributes, wantAttrs) {
		t.Fatalf("attributes: %+v", b.Attributes)
	}
	if b.Media[0] != (Media{"dept-kitchen-dining", "http://storefront.localhost/media/departments/kitchen-dining.svg", f.Title, "PRIMARY"}) {
		t.Fatalf("media: %+v", b.Media[0])
	}

	f.Fragile, f.HighValue = true, true
	f.Dims.WeightG = 18
	b = DeriveContent(f, "http://h")
	if b.Bullets[2] != "Handling: fragile, high-value item" || b.Attributes[3].Value != "0.018" || !strings.Contains(b.Bullets[1], "(0.04 lb)") {
		t.Fatalf("handling / light weight: %q %+v", b.Bullets, b.Attributes[3])
	}
	f.HighValue = false
	if got := DeriveContent(f, "http://h").Bullets[2]; got != "Handling: fragile item" {
		t.Fatalf("fragile only: %q", got)
	}
}
