package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
)

// Fixture is fixtures/storefront/catalogue.json: the CURATED storefront merchandising (currency,
// tax bands, delivery charges, departments, and per product its department, subcategory and list
// price). Everything else about a product is real data read from the other two inputs.
type Fixture struct {
	Currency                  string           `json:"currency"`
	TaxBands                  []TaxBand        `json:"taxBands"`
	DeliveryCharges           []DeliveryCharge `json:"deliveryCharges"`
	DeliverablePostalPrefixes []string         `json:"deliverablePostalPrefixes"`
	Departments               []Department     `json:"departments"`
	Products                  []FixtureProduct `json:"products"`
}

// TaxBand is one band of pricing's tax table (also its wire shape).
type TaxBand struct {
	PostalPrefixFrom string `json:"postalPrefixFrom"`
	PostalPrefixTo   string `json:"postalPrefixTo"`
	RateBasisPoints  int    `json:"rateBasisPoints"`
}

// DeliveryCharge is the fixture's charge of one service tier, in minor units of the currency.
type DeliveryCharge struct {
	Tier          string `json:"tier"`
	ChargeMinor   int64  `json:"chargeMinor"`
	FreeOverMinor *int64 `json:"freeOverMinor,omitempty"`
}

// Department is a top-level category of the storefront tree; Slug names its media file.
type Department struct {
	Slug string `json:"slug"`
	Name string `json:"name"`
}

// FixtureProduct is one storefront product: a real product of fulfilment-products.json (by SKU)
// whose retail-network catalogue item is found by its title.
type FixtureProduct struct {
	SKU            string `json:"sku"`
	Department     string `json:"department"`
	Subcategory    string `json:"subcategory"`
	ListPriceMinor int64  `json:"listPriceMinor"`
}

// Dimensions is one unit's real size and weight (fulfilment-products.json).
type Dimensions struct {
	LengthMm int64 `json:"lengthMm"`
	WidthMm  int64 `json:"widthMm"`
	HeightMm int64 `json:"heightMm"`
	WeightG  int64 `json:"weightG"`
}

// PhysicalProduct is the slice of a fulfilment-products.json entry the content derivation uses.
// The source description is deliberately NOT read: it carries the source listing's brand names.
type PhysicalProduct struct {
	SKU          string      `json:"sku"`
	Dimensions   *Dimensions `json:"dimensions"`
	HandlingTags []string    `json:"handlingTags"`
}

// serviceTiers is pricing's closed Tier enum; a charge schedule names every one of them.
var serviceTiers = []string{"SAME_DAY", "NEXT_DAY", "TWO_DAY", "STANDARD"}

// LoadFixture reads the storefront fixture.
func LoadFixture(path string) (*Fixture, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("storefront fixture: %w", err)
	}
	var f Fixture
	if err := json.Unmarshal(raw, &f); err != nil {
		return nil, fmt.Errorf("storefront fixture %s: %w", path, err)
	}
	return &f, nil
}

// LoadPhysical reads fixtures/products/fulfilment-products.json and indexes it by SKU.
func LoadPhysical(path string) (map[string]PhysicalProduct, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("fulfilment products: %w", err)
	}
	var doc struct {
		Products []PhysicalProduct `json:"products"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("fulfilment products %s: %w", path, err)
	}
	out := make(map[string]PhysicalProduct, len(doc.Products))
	for _, p := range doc.Products {
		out[p.SKU] = p
	}
	return out, nil
}

// LoadNetworkTitles reads warehouse-infra's scripts/data/retail-network-seed.json and returns
// SKU -> catalogue item title (the network's natural key of a catalogue item).
func LoadNetworkTitles(path string) (map[string]string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("retail-network data file (warehouse-infra scripts/data/retail-network-seed.json, flag -network-data): %w", err)
	}
	var doc struct {
		Catalog []struct {
			SKU   string `json:"sku"`
			Title string `json:"title"`
		} `json:"catalog"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("retail-network data file %s: %w", path, err)
	}
	out := make(map[string]string, len(doc.Catalog))
	for _, c := range doc.Catalog {
		out[c.SKU] = c.Title
	}
	return out, nil
}

// Validate reports every inconsistency between the fixture and the real data it points at.
// titles may be nil (the network data file is not available): the title check is then skipped.
func (f *Fixture) Validate(phys map[string]PhysicalProduct, titles map[string]string) []string {
	var problems []string
	add := func(format string, args ...any) { problems = append(problems, fmt.Sprintf(format, args...)) }
	if f.Currency != "USD" {
		add("currency %q: the cluster prices in USD (helm-values/pricing.yaml PRICING_CURRENCY)", f.Currency)
	}
	if len(f.TaxBands) == 0 {
		add("no tax bands")
	}
	for i, b := range f.TaxBands {
		if b.PostalPrefixFrom == "" || b.PostalPrefixTo == "" || b.PostalPrefixFrom > b.PostalPrefixTo || b.RateBasisPoints < 0 || b.RateBasisPoints > 10000 {
			add("taxBands[%d] %+v is not a valid band", i, b)
		}
	}
	tiers := map[string]bool{}
	for _, c := range f.DeliveryCharges {
		if tiers[c.Tier] {
			add("deliveryCharges: tier %s twice", c.Tier)
		}
		tiers[c.Tier] = true
		if c.ChargeMinor < 0 || (c.FreeOverMinor != nil && *c.FreeOverMinor <= 0) {
			add("deliveryCharges: tier %s has a negative charge or a non-positive free-over", c.Tier)
		}
	}
	for _, t := range serviceTiers {
		if !tiers[t] {
			add("deliveryCharges: tier %s missing (pricing needs every tier: incomplete-charge-schedule)", t)
		}
	}
	if len(tiers) != len(serviceTiers) {
		add("deliveryCharges: %d tiers, pricing knows exactly %d", len(tiers), len(serviceTiers))
	}
	if len(f.DeliverablePostalPrefixes) == 0 {
		add("no deliverable postal prefixes")
	}
	depts := map[string]bool{}
	names := map[string]bool{}
	for _, d := range f.Departments {
		if !validSlug(d.Slug) || !validCategoryName(d.Name) {
			add("department %+v: slug must be [a-z0-9-] and the name a category name without '/'", d)
		}
		if depts[d.Slug] || names[strings.ToLower(d.Name)] {
			add("department %s declared twice", d.Slug)
		}
		depts[d.Slug] = true
		names[strings.ToLower(d.Name)] = true
	}
	seen := map[string]bool{}
	used := map[string]bool{}
	for _, p := range f.Products {
		if seen[p.SKU] {
			add("product %s listed twice", p.SKU)
		}
		seen[p.SKU] = true
		if !depts[p.Department] {
			add("product %s: department %q is not declared", p.SKU, p.Department)
		}
		used[p.Department] = true
		if !validCategoryName(p.Subcategory) {
			add("product %s: subcategory %q is not a category name", p.SKU, p.Subcategory)
		}
		if p.ListPriceMinor <= 0 {
			add("product %s: list price %d must be > 0", p.SKU, p.ListPriceMinor)
		}
		pp, ok := phys[p.SKU]
		switch {
		case !ok:
			add("product %s is not in fixtures/products/fulfilment-products.json", p.SKU)
		case pp.Dimensions == nil || pp.Dimensions.LengthMm <= 0 || pp.Dimensions.WidthMm <= 0 || pp.Dimensions.HeightMm <= 0 || pp.Dimensions.WeightG <= 0:
			add("product %s has no real dimensions and weight in fulfilment-products.json", p.SKU)
		}
		if titles != nil && strings.TrimSpace(titles[p.SKU]) == "" {
			add("product %s has no catalogue title in the retail-network data file", p.SKU)
		}
	}
	for _, d := range f.Departments {
		if !used[d.Slug] {
			add("department %s has no product (an empty department never shows up in search)", d.Slug)
		}
	}
	if len(f.Products) == 0 {
		add("no products")
	}
	sort.Strings(problems)
	return problems
}

func validSlug(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-') {
			return false
		}
	}
	return true
}

// validCategoryName mirrors product-content's rule (trimmed, 1..100 characters, no control
// character) plus the path separator: a path is the names joined with "/".
func validCategoryName(s string) bool {
	return validText(s, 100) && !strings.Contains(s, "/")
}

// validText is product-content's ValidTextN: non-blank, trimmed, at most limit runes, no control
// character.
func validText(s string, limit int) bool {
	if s == "" || s != strings.TrimSpace(s) || len([]rune(s)) > limit {
		return false
	}
	for _, r := range s {
		if r < 0x20 || r == 0x7f {
			return false
		}
	}
	return true
}

// departmentBySlug indexes the fixture's departments.
func (f *Fixture) departmentBySlug() map[string]Department {
	out := make(map[string]Department, len(f.Departments))
	for _, d := range f.Departments {
		out[d.Slug] = d
	}
	return out
}
