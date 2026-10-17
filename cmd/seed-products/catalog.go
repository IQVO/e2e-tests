package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"unicode"
	"unicode/utf8"
)

// The fixture is the contract between the fetch mode (internet, run rarely,
// by hand) and the register mode (the e2e run, offline and deterministic).
// Committing it means a suite run never depends on Open Food Facts being up
// (its search API answers 503 under load) and every run registers exactly
// the same real products.

// Fixture is the on-disk catalogue of real products to register.
type Fixture struct {
	Source    string           `json:"source"`
	License   string           `json:"license"`
	FetchedAt string           `json:"fetchedAt"`
	Products  []FixtureProduct `json:"products"`
}

// FixtureProduct is one real product, already reduced to what
// product-master's published contract accepts.
type FixtureProduct struct {
	// SKU is the product's GS1 GTIN (EAN-8, UPC-A, EAN-13 or GTIN-14),
	// check digit verified. Using the GTIN keeps every seeded SKU
	// traceable to a real, publicly listed product.
	SKU string `json:"sku"`
	// Description is "<brand> <name> (<net quantity>)", at most
	// maxDescriptionRunes runes, no control characters.
	Description string `json:"description"`
	// TemperatureClass is "Frozen", "Chilled" or empty (not
	// temperature-sensitive as far as the source categories say).
	TemperatureClass string `json:"temperatureClass,omitempty"`
	// Categories is the evidence for TemperatureClass: the source category
	// tags that drove it (empty when unclassified).
	Categories []string `json:"categories,omitempty"`

	// The fields below carry the fulfilment profile of the fulfilment
	// fixture (scripts/harvest-fulfilment-products.py). Open Food Facts
	// products leave them empty.

	// Source names the dataset the product came from ("abo", "wands").
	// Empty means an Open Food Facts product whose SKU is a GTIN; any other
	// source uses its own product id as the SKU.
	Source string `json:"source,omitempty"`
	// Category is the source's own category path.
	Category string `json:"category,omitempty"`
	// Dimensions are the real unit dimensions and weight from the source,
	// converted to whole millimetres and grams.
	Dimensions *Dimensions `json:"dimensions,omitempty"`
	// HandlingTags are product-master's handling tags, derived from the
	// category and name; Evidence records what triggered each one.
	HandlingTags []string `json:"handlingTags,omitempty"`
	// DotHazardClass is the top-level US DOT hazard class (1..9); only with Hazmat.
	DotHazardClass int      `json:"dotHazardClass,omitempty"`
	Evidence       []string `json:"evidence,omitempty"`
}

// Dimensions is one unit's size and weight, in the units product-master stores.
type Dimensions struct {
	LengthMm int64 `json:"lengthMm"`
	WidthMm  int64 `json:"widthMm"`
	HeightMm int64 `json:"heightMm"`
	WeightG  int64 `json:"weightG"`
}

// product-master's published dimension limits (apis/openapi.yaml).
const (
	maxDimensionMm = 20000
	maxWeightG     = 2000000
)

// handlingTagOrder is the closed HandlingTag enum in product-master's stable order.
var handlingTagOrder = []string{"Hazmat", "Fragile", "TemperatureSensitive", "Oversized", "HighValue"}

// classification returns the handling classification to PUT: the fixture's
// handling tags, plus TemperatureSensitive when a temperature class is set
// (Open Food Facts products carry only the class). ok is false when the
// product has nothing to classify.
func (p FixtureProduct) classification() (tags []string, temperatureClass string, dotHazardClass int, ok bool) {
	have := map[string]bool{}
	for _, tg := range p.HandlingTags {
		have[tg] = true
	}
	if p.TemperatureClass != "" {
		have["TemperatureSensitive"] = true
	}
	for _, tg := range handlingTagOrder {
		if have[tg] {
			tags = append(tags, tg)
		}
	}
	return tags, p.TemperatureClass, p.DotHazardClass, len(tags) > 0
}

// product-master's limits (apis/openapi.yaml; internal/domain/product/sku.go:
// MaxDescriptionLength counts runes).
const (
	maxDescriptionRunes = 200
	maxSKULength        = 64
)

// validGTIN reports whether code is a GS1 GTIN-8/12/13/14 with a correct
// mod-10 check digit. Weights 3,1,3,1... are applied from the digit next to
// the check digit, moving left.
func validGTIN(code string) bool {
	switch len(code) {
	case 8, 12, 13, 14:
	default:
		return false
	}
	sum := 0
	for i := len(code) - 2; i >= 0; i-- {
		c := code[i]
		if c < '0' || c > '9' {
			return false
		}
		d := int(c - '0')
		if (len(code)-2-i)%2 == 0 {
			d *= 3
		}
		sum += d
	}
	last := code[len(code)-1]
	if last < '0' || last > '9' {
		return false
	}
	return (10-sum%10)%10 == int(last-'0')
}

// cleanText collapses whitespace runs to one space, drops control
// characters (product-master rejects them) and trims.
func cleanText(s string) string {
	var b strings.Builder
	space := false
	for _, r := range s {
		if unicode.IsSpace(r) {
			space = true
			continue
		}
		if unicode.IsControl(r) || r == utf8.RuneError {
			continue
		}
		if space && b.Len() > 0 {
			b.WriteByte(' ')
		}
		space = false
		b.WriteRune(r)
	}
	return b.String()
}

// truncateRunes cuts s to at most n runes.
func truncateRunes(s string, n int) string {
	if utf8.RuneCountInString(s) <= n {
		return s
	}
	r := []rune(s)
	return strings.TrimSpace(string(r[:n]))
}

// buildDescription renders "<brand> <name> (<quantity>)". The brand is
// omitted when the name already starts with it, and the quantity when it is
// absent or already in the name. Both comparisons ignore case, spacing and
// punctuation ("Coca-Cola" vs "Coca Cola", "900g" vs "900 g"), so the real
// names never read "Coca-Cola Coca Cola" or "... 900g (900 g)". The result
// always fits product-master's rune limit.
func buildDescription(brand, name, quantity string) string {
	brand = cleanText(firstBrand(brand))
	name = cleanText(name)
	quantity = cleanText(quantity)
	desc := name
	if brand != "" && !startsWithWords(name, brand) {
		desc = brand + " " + name
	}
	if nq := alnum(quantity); nq != "" && !strings.Contains(alnum(name), nq) {
		desc += " (" + quantity + ")"
	}
	return truncateRunes(desc, maxDescriptionRunes)
}

// startsWithWords reports whether name's leading whole words spell brand,
// ignoring case, spacing and punctuation: "Coca Cola" starts with
// "Coca-Cola" and "Yum Yum Chicken" with "YumYum", but "Yummy noodles" does
// not start with "Yum" (word boundary).
func startsWithWords(name, brand string) bool {
	want := alnum(brand)
	if want == "" {
		return false
	}
	got := ""
	for _, w := range strings.Fields(name) {
		got += alnum(w)
		if got == want {
			return true
		}
		if len(got) >= len(want) {
			return false
		}
	}
	return false
}

// alnum lower-cases s and keeps only letters and digits, for comparisons
// that must not care about spacing or punctuation.
func alnum(s string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(s) {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// firstBrand keeps only the first of Open Food Facts' comma-separated
// brands ("Danone,Activia" -> "Danone").
func firstBrand(brands string) string {
	if i := strings.IndexByte(brands, ','); i >= 0 {
		return brands[:i]
	}
	return brands
}

// Category tags that put a product in a cold chain. Deliberately narrow:
// a tag only lands here when every product carrying it needs that storage
// band in a distribution centre. UHT milk, cured meats and "dairies" as a
// whole are left out because many of them are shelf-stable.
var (
	frozenTags  = []string{"en:frozen-foods", "en:ice-creams", "en:frozen-desserts"}
	chilledTags = []string{"en:yogurts", "en:cheeses", "en:fresh-meats", "en:fresh-fish", "en:fresh-poultry", "en:fresh-cheeses"}
)

// deriveTemperature returns the temperature class the source categories
// support, and the tags that justify it. Frozen wins over Chilled.
func deriveTemperature(categories []string) (string, []string) {
	if hits := intersect(categories, frozenTags); len(hits) > 0 {
		return "Frozen", hits
	}
	if hits := intersect(categories, chilledTags); len(hits) > 0 {
		return "Chilled", hits
	}
	return "", nil
}

func intersect(have, want []string) []string {
	set := make(map[string]bool, len(have))
	for _, h := range have {
		set[h] = true
	}
	var out []string
	for _, w := range want {
		if set[w] {
			out = append(out, w)
		}
	}
	return out
}

// validate checks every product against product-master's published input
// rules, so a bad fixture fails before the first request.
func (f Fixture) validate() error {
	seen := make(map[string]bool, len(f.Products))
	for i, p := range f.Products {
		if p.Source == "" {
			if !validGTIN(p.SKU) || len(p.SKU) > maxSKULength {
				return fmt.Errorf("product %d: sku %q is not a valid GTIN", i, p.SKU)
			}
		} else if !validSKU(p.SKU) {
			return fmt.Errorf("product %d: sku %q breaks product-master's SKU rules (1..%d chars, no whitespace, control characters or '/')", i, p.SKU, maxSKULength)
		}
		if seen[p.SKU] {
			return fmt.Errorf("product %d: duplicate sku %q", i, p.SKU)
		}
		seen[p.SKU] = true
		if p.Description == "" || utf8.RuneCountInString(p.Description) > maxDescriptionRunes {
			return fmt.Errorf("product %d (%s): description must be 1..%d runes", i, p.SKU, maxDescriptionRunes)
		}
		if cleanText(p.Description) != p.Description {
			return fmt.Errorf("product %d (%s): description has control characters or unnormalised whitespace", i, p.SKU)
		}
		switch p.TemperatureClass {
		case "", "Frozen", "Chilled":
		default:
			return fmt.Errorf("product %d (%s): unknown temperature class %q", i, p.SKU, p.TemperatureClass)
		}
		if err := p.validateFulfilment(); err != nil {
			return fmt.Errorf("product %d (%s): %w", i, p.SKU, err)
		}
	}
	return nil
}

// validateFulfilment checks the dimensions and handling classification
// against product-master's rules: each dimension 1..20000 mm, weight
// 1..2000000 g, tags from the closed enum without duplicates, a temperature
// class whenever TemperatureSensitive is present, a DOT class 1..9 only
// with Hazmat.
func (p FixtureProduct) validateFulfilment() error {
	if d := p.Dimensions; d != nil {
		for name, v := range map[string]int64{"lengthMm": d.LengthMm, "widthMm": d.WidthMm, "heightMm": d.HeightMm} {
			if v < 1 || v > maxDimensionMm {
				return fmt.Errorf("%s %d outside 1..%d", name, v, maxDimensionMm)
			}
		}
		if d.WeightG < 1 || d.WeightG > maxWeightG {
			return fmt.Errorf("weightG %d outside 1..%d", d.WeightG, maxWeightG)
		}
	}
	seen := map[string]bool{}
	for _, tg := range p.HandlingTags {
		if !contains(handlingTagOrder, tg) {
			return fmt.Errorf("unknown handling tag %q", tg)
		}
		if seen[tg] {
			return fmt.Errorf("duplicate handling tag %q", tg)
		}
		seen[tg] = true
	}
	// A temperature class implies TemperatureSensitive (Open Food Facts entries carry only the
	// class); the tag without a class is invalid.
	if seen["TemperatureSensitive"] && p.TemperatureClass == "" {
		return fmt.Errorf("handling tag TemperatureSensitive needs a temperatureClass")
	}
	if p.DotHazardClass != 0 && (p.DotHazardClass < 1 || p.DotHazardClass > 9 || !seen["Hazmat"]) {
		return fmt.Errorf("dotHazardClass %d must be 1..9 and only with Hazmat", p.DotHazardClass)
	}
	return nil
}

// validSKU applies product-master's SKU rule to non-GTIN identifiers.
func validSKU(s string) bool {
	if s == "" || utf8.RuneCountInString(s) > maxSKULength {
		return false
	}
	for _, r := range s {
		if r == '/' || unicode.IsSpace(r) || unicode.IsControl(r) {
			return false
		}
	}
	return true
}

func loadFixture(path string) (Fixture, error) {
	var f Fixture
	raw, err := os.ReadFile(path)
	if err != nil {
		return f, fmt.Errorf("read fixture: %w", err)
	}
	if err := json.Unmarshal(raw, &f); err != nil {
		return f, fmt.Errorf("parse fixture %s: %w", path, err)
	}
	return f, f.validate()
}

// loadFixtures loads and validates every fixture file and merges their
// products. A SKU may appear in only one file.
func loadFixtures(paths []string) ([]FixtureProduct, error) {
	var all []FixtureProduct
	seen := map[string]string{}
	for _, path := range paths {
		f, err := loadFixture(path)
		if err != nil {
			return nil, err
		}
		for _, p := range f.Products {
			if prev, dup := seen[p.SKU]; dup {
				return nil, fmt.Errorf("sku %q is in both %s and %s", p.SKU, prev, path)
			}
			seen[p.SKU] = path
		}
		all = append(all, f.Products...)
	}
	return all, nil
}

func writeFixture(path string, f Fixture) error {
	if err := f.validate(); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(raw, '\n'), 0o644)
}
