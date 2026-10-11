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
		if !validGTIN(p.SKU) || len(p.SKU) > maxSKULength {
			return fmt.Errorf("product %d: sku %q is not a valid GTIN", i, p.SKU)
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
	}
	return nil
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
