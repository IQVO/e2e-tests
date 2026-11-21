package main

import (
	"strconv"
	"strings"
)

// Content limits of product-content (apis/openapi.yaml, internal/domain/content).
const (
	maxDescription = 4000
	maxBullets     = 10
	maxBullet      = 300
	maxAttributes  = 50
	maxAttrName    = 100
	maxAttrValue   = 300
	maxAttrUnit    = 20
	maxMedia       = 12
	maxAltText     = 200
	maxURL         = 2048
	maxID          = 64
)

// shipsFrom is where every storefront product ships from: the network's only fulfillment node,
// site-1 (warehouse-infra scripts/data/retail-network-seed.json: postal code 78701, Austin, Texas,
// America/Chicago). It is a fact of the network data, not a marketing claim.
const shipsFrom = "Austin, Texas"

// ContentBody is product-content's editable content (ContentBody), as sent on create and revise.
// PrimaryCategoryID is filled at apply time, once the category exists.
type ContentBody struct {
	Description       string      `json:"description"`
	Bullets           []string    `json:"bullets"`
	Attributes        []Attribute `json:"attributes"`
	Media             []Media     `json:"media"`
	PrimaryCategoryID string      `json:"primaryCategoryId,omitempty"`
}

// Attribute is one name/value(/unit) row of the product page.
type Attribute struct {
	Name  string `json:"name"`
	Value string `json:"value"`
	Unit  string `json:"unit,omitempty"`
}

// Media is one medium; the storefront ships one PRIMARY department image per department.
type Media struct {
	MediaID string `json:"mediaId"`
	URL     string `json:"url"`
	AltText string `json:"altText"`
	Role    string `json:"role"`
}

// ProductFacts is everything the content of one product is derived from. Every field is real
// data (the network title, fulfilment-products.json dimensions and handling tags) or the curated
// merchandising of the fixture (department, subcategory).
type ProductFacts struct {
	SKU            string
	Title          string
	DepartmentSlug string
	DepartmentName string
	Subcategory    string
	Dims           Dimensions
	Fragile        bool
	HighValue      bool
}

// DeriveContent builds the content of one product, deterministically, from its facts only: no
// feature, material or claim that the data does not carry. mediaBase is the absolute URL of the
// department images (served by storefront-web, whose CSP only allows same-host images).
func DeriveContent(f ProductFacts, mediaBase string) ContentBody {
	title := strings.TrimRight(strings.TrimSpace(f.Title), ".")
	l, w, h := cm(f.Dims.LengthMm), cm(f.Dims.WidthMm), cm(f.Dims.HeightMm)
	li, wi, hi := inches(f.Dims.LengthMm), inches(f.Dims.WidthMm), inches(f.Dims.HeightMm)
	kg, lb := kilograms(f.Dims.WeightG), pounds(f.Dims.WeightG)
	size := l + " x " + w + " x " + h + " cm (" + li + " x " + wi + " x " + hi + " in)"
	weight := kg + " kg (" + lb + " lb)"

	desc := title + ", in " + f.DepartmentName + " > " + f.Subcategory + ". " +
		"It measures " + size + ", length x width x height, and weighs " + weight + ". " +
		"Ships from the " + shipsFrom + " fulfillment center."

	bullets := []string{
		"Size: " + size + ", length x width x height",
		"Weight: " + weight,
	}
	switch {
	case f.Fragile && f.HighValue:
		bullets = append(bullets, "Handling: fragile, high-value item")
	case f.Fragile:
		bullets = append(bullets, "Handling: fragile item")
	case f.HighValue:
		bullets = append(bullets, "Handling: high-value item")
	}
	bullets = append(bullets, "Ships from and sold by the storefront, from the "+shipsFrom+" fulfillment center")

	attrs := []Attribute{
		{Name: "Length", Value: l, Unit: "cm"},
		{Name: "Width", Value: w, Unit: "cm"},
		{Name: "Height", Value: h, Unit: "cm"},
		{Name: "Weight", Value: kg, Unit: "kg"},
		{Name: "Department", Value: f.DepartmentName},
		{Name: "Category", Value: f.Subcategory},
	}

	media := []Media{{
		MediaID: clip("dept-"+f.DepartmentSlug, maxID),
		URL:     strings.TrimRight(mediaBase, "/") + "/" + f.DepartmentSlug + ".svg",
		AltText: clip(title, maxAltText),
		Role:    "PRIMARY",
	}}
	return ContentBody{Description: clip(desc, maxDescription), Bullets: bullets, Attributes: attrs, Media: media}
}

// cm renders millimetres as centimetres with one decimal.
func cm(mm int64) string { return strconv.FormatFloat(float64(mm)/10, 'f', 1, 64) }

// inches renders millimetres as inches with one decimal.
func inches(mm int64) string { return strconv.FormatFloat(float64(mm)/25.4, 'f', 1, 64) }

// kilograms renders grams as kilograms, up to three decimals, trailing zeros trimmed.
func kilograms(g int64) string {
	s := strconv.FormatFloat(float64(g)/1000, 'f', 3, 64)
	s = strings.TrimRight(s, "0")
	return strings.TrimSuffix(s, ".")
}

// pounds renders grams as pounds with two decimals (never 0.00 for a real weight).
func pounds(g int64) string {
	lb := float64(g) / 453.59237
	if lb < 0.01 {
		lb = 0.01
	}
	return strconv.FormatFloat(lb, 'f', 2, 64)
}

// clip cuts s to at most n runes and trims the cut.
func clip(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return strings.TrimSpace(string(r[:n]))
}

// hasTag reports whether a fulfilment product carries a product-master handling tag.
func hasTag(p PhysicalProduct, tag string) bool {
	for _, t := range p.HandlingTags {
		if t == tag {
			return true
		}
	}
	return false
}

// checkContent applies product-content's publish gate and field limits to a derived body
// (category excluded: it is resolved at apply time). It returns the first violation or "".
func checkContent(b ContentBody) string {
	switch {
	case !validText(b.Description, maxDescription):
		return "description"
	case len(b.Bullets) > maxBullets:
		return "too many bullets"
	case len(b.Attributes) > maxAttributes:
		return "too many attributes"
	case len(b.Media) > maxMedia:
		return "too many media"
	}
	for _, s := range b.Bullets {
		if !validText(s, maxBullet) {
			return "bullet " + strconv.Quote(s)
		}
	}
	for _, a := range b.Attributes {
		if !validText(a.Name, maxAttrName) || !validText(a.Value, maxAttrValue) || (a.Unit != "" && !validText(a.Unit, maxAttrUnit)) {
			return "attribute " + a.Name
		}
	}
	primary := 0
	for _, m := range b.Media {
		if m.Role == "PRIMARY" {
			primary++
		}
		if !validID(m.MediaID) || !validText(m.AltText, maxAltText) || len(m.URL) > maxURL ||
			!(strings.HasPrefix(m.URL, "http://") || strings.HasPrefix(m.URL, "https://")) {
			return "medium " + m.MediaID
		}
	}
	if primary != 1 {
		return "exactly one PRIMARY medium"
	}
	return ""
}

// validID is product-content's opaque id rule: 1..64 of [A-Za-z0-9._:-], starting alphanumeric.
func validID(s string) bool {
	if s == "" || len(s) > maxID {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		alnum := c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9'
		if alnum || (i > 0 && (c == '.' || c == '_' || c == ':' || c == '-')) {
			continue
		}
		return false
	}
	return true
}
