package main

import (
	"fmt"
	"sort"
	"strings"
)

// ---- observed state (the services' wire shapes) ----

// Money is pricing's Money.
type Money struct {
	AmountMinor int64  `json:"amountMinor"`
	Currency    string `json:"currency"`
}

// TaxTable is pricing's current tax table.
type TaxTable struct {
	TaxTableID string    `json:"taxTableId,omitempty"`
	Version    int       `json:"version"`
	State      string    `json:"state,omitempty"`
	Bands      []TaxBand `json:"bands"`
}

// TierCharge is one tier of pricing's delivery charge schedule.
type TierCharge struct {
	Tier     string `json:"tier"`
	Charge   Money  `json:"charge"`
	FreeOver *Money `json:"freeOver,omitempty"`
}

// ChargeSchedule is pricing's current delivery charge schedule.
type ChargeSchedule struct {
	ScheduleID string       `json:"scheduleId,omitempty"`
	Version    int          `json:"version"`
	State      string       `json:"state,omitempty"`
	Tiers      []TierCharge `json:"tiers"`
}

// ItemPrice is pricing's price record of one catalogue item.
type ItemPrice struct {
	CatalogItemID  string `json:"catalogItemId"`
	State          string `json:"state"`
	ListPrice      Money  `json:"listPrice"`
	EffectivePrice Money  `json:"effectivePrice"`
	Version        int    `json:"version"`
}

// Category is one node of product-content's category tree.
type Category struct {
	CategoryID string `json:"categoryId"`
	ParentID   string `json:"parentId,omitempty"`
	Name       string `json:"name"`
	Path       string `json:"path"`
}

// CategoryTree is product-content's GET /categories.
type CategoryTree struct {
	Version    int        `json:"version"`
	Categories []Category `json:"categories"`
}

// ProductContent is product-content's content of one catalogue item.
type ProductContent struct {
	CatalogItemID     string `json:"catalogItemId"`
	State             string `json:"state"`
	Revision          int    `json:"revision"`
	PublishedRevision int    `json:"publishedRevision"`
	ContentBody
}

// CatalogItem is one item of offer-availability's GET /catalog.
type CatalogItem struct {
	CatalogItemID string `json:"catalogItemId"`
	Title         string `json:"title"`
	State         string `json:"state"`
}

// Observed is what CHECK read from the cluster.
type Observed struct {
	Tax      *TaxTable       // nil: no tax table yet (404)
	Charges  *ChargeSchedule // nil: no schedule yet (404)
	Tree     CategoryTree
	Prices   map[string]*ItemPrice      // by catalogItemId; nil: none (404)
	Contents map[string]*ProductContent // by catalogItemId; nil: none (404)
}

// ---- desired state ----

// DesiredProduct is one fixture product resolved to its catalogue item, with its derived content.
type DesiredProduct struct {
	SKU            string
	Title          string
	CatalogItemID  string
	DepartmentName string
	CategoryPath   string // "<department>/<subcategory>"
	ListPriceMinor int64
	Body           ContentBody // PrimaryCategoryID empty: resolved from CategoryPath at apply time
}

// Desired is the storefront state the fixture asks for.
type Desired struct {
	Currency   string
	TaxBands   []TaxBand
	Charges    []DeliveryCharge
	Categories []CategorySpec // departments first, then subcategories, each sorted by path
	Products   []DesiredProduct
}

// CategorySpec is one category of the desired tree; the natural key is the path.
type CategorySpec struct {
	Path       string
	Name       string
	ParentPath string // "" for a department (top level)
}

// ---- plan ----

// Plan is the diff between Desired and Observed: only what must be written.
type Plan struct {
	TaxTable       *TaxTable       // publish this version; nil = current table already matches
	ChargeSchedule *ChargeSchedule // publish this version; nil = current schedule already matches
	Categories     []CategorySpec  // to create, parents before children
	Prices         []PriceAction
	Contents       []ContentAction
	Blocked        []string // products the seeder must not (cannot) converge: WITHDRAWN price, RETIRED content

	TaxCurrent, ChargesCurrent         int // version observed (0 = none)
	CategoriesPresent                  int
	PricesUnchanged, ContentsUnchanged int
	PricesWithdrawn, ContentsRetired   int
	PricesToCreate, PricesToChange     int
	ContentsToCreate, ContentsToRevise int
	ContentsToPublish                  int
}

// PriceAction creates or changes the list price of one item.
type PriceAction struct {
	SKU           string
	CatalogItemID string
	Create        bool  // POST /item-prices; otherwise :set-list-price
	AmountMinor   int64 // the new list price
	Version       int   // the observed version (optimistic guard of :set-list-price)
}

// ContentAction converges the content of one item: create or revise if the body differs, then
// publish if the current revision is not the published one.
type ContentAction struct {
	SKU           string
	CatalogItemID string
	Create        bool // POST /product-contents (DRAFT, revision 1)
	Revise        bool // POST :revise (new revision)
	FromRevision  int  // the observed revision a revise applies to
	Publish       bool // POST :publish (always true once Create or Revise)
	Body          ContentBody
	CategoryPath  string
}

// Writes is the number of POSTs the plan sends.
func (p *Plan) Writes() int {
	n := len(p.Categories) + len(p.Prices)
	if p.TaxTable != nil {
		n++
	}
	if p.ChargeSchedule != nil {
		n++
	}
	for _, c := range p.Contents {
		if c.Create || c.Revise {
			n++
		}
		if c.Publish {
			n++
		}
	}
	return n
}

// treeIndex maps a lower-cased path to its category (product-content compares sibling names
// case-insensitively, so a path that differs only by case IS that category).
func treeIndex(t CategoryTree) map[string]Category {
	out := make(map[string]Category, len(t.Categories))
	for _, c := range t.Categories {
		out[strings.ToLower(c.Path)] = c
	}
	return out
}

// BuildPlan is the PURE, deterministic diff of the desired against the observed state.
func BuildPlan(d Desired, o Observed) *Plan {
	p := &Plan{}
	cur := d.Currency

	// tax table: compared as a set of bands
	if o.Tax != nil {
		p.TaxCurrent = o.Tax.Version
	}
	if o.Tax == nil || !sameBands(o.Tax.Bands, d.TaxBands) {
		p.TaxTable = &TaxTable{Version: p.TaxCurrent + 1, Bands: sortedBands(d.TaxBands)}
	}

	// delivery charge schedule: compared as a set keyed by tier
	want := desiredTiers(d.Charges, cur)
	if o.Charges != nil {
		p.ChargesCurrent = o.Charges.Version
	}
	if o.Charges == nil || !sameTiers(o.Charges.Tiers, want) {
		p.ChargeSchedule = &ChargeSchedule{Version: p.ChargesCurrent + 1, Tiers: want}
	}

	// categories by path
	idx := treeIndex(o.Tree)
	for _, c := range d.Categories {
		if _, ok := idx[strings.ToLower(c.Path)]; ok {
			p.CategoriesPresent++
			continue
		}
		p.Categories = append(p.Categories, c)
	}

	byID := map[string]Category{}
	for _, c := range o.Tree.Categories {
		byID[c.CategoryID] = c
	}
	for _, dp := range d.Products {
		// price
		switch ip := o.Prices[dp.CatalogItemID]; {
		case ip == nil:
			p.Prices = append(p.Prices, PriceAction{SKU: dp.SKU, CatalogItemID: dp.CatalogItemID, Create: true, AmountMinor: dp.ListPriceMinor})
			p.PricesToCreate++
		case ip.State == "WITHDRAWN":
			p.PricesWithdrawn++
			p.Blocked = append(p.Blocked, fmt.Sprintf("%s (%s): price is WITHDRAWN; pricing has no transition back to ACTIVE, so the item cannot be sold (left alone)", dp.SKU, dp.CatalogItemID))
		case ip.ListPrice.AmountMinor == dp.ListPriceMinor && ip.ListPrice.Currency == cur:
			p.PricesUnchanged++
		default:
			p.Prices = append(p.Prices, PriceAction{SKU: dp.SKU, CatalogItemID: dp.CatalogItemID, AmountMinor: dp.ListPriceMinor, Version: ip.Version})
			p.PricesToChange++
		}

		// content
		pc := o.Contents[dp.CatalogItemID]
		a := ContentAction{SKU: dp.SKU, CatalogItemID: dp.CatalogItemID, Body: dp.Body, CategoryPath: dp.CategoryPath}
		switch {
		case pc == nil:
			a.Create, a.Publish = true, true
			p.ContentsToCreate++
		case pc.State == "RETIRED":
			p.ContentsRetired++
			p.Blocked = append(p.Blocked, fmt.Sprintf("%s (%s): content is RETIRED; retired content never changes again, so the item cannot be published (left alone)", dp.SKU, dp.CatalogItemID))
			continue
		case !sameContent(pc, dp, idx, byID):
			a.Revise, a.FromRevision, a.Publish = true, pc.Revision, true
			p.ContentsToRevise++
		case pc.State != "PUBLISHED" || pc.Revision != pc.PublishedRevision:
			a.Publish, a.FromRevision = true, pc.Revision
			p.ContentsToPublish++
		default:
			p.ContentsUnchanged++
			continue
		}
		p.Contents = append(p.Contents, a)
	}
	sort.Strings(p.Blocked)
	return p
}

func sortedBands(bs []TaxBand) []TaxBand {
	out := append([]TaxBand(nil), bs...)
	sort.Slice(out, func(i, j int) bool { return out[i].PostalPrefixFrom < out[j].PostalPrefixFrom })
	return out
}

func sameBands(a, b []TaxBand) bool {
	if len(a) != len(b) {
		return false
	}
	sa, sb := sortedBands(a), sortedBands(b)
	for i := range sa {
		if sa[i] != sb[i] {
			return false
		}
	}
	return true
}

// desiredTiers turns the fixture's charges into pricing's tiers, in pricing's tier order.
func desiredTiers(cs []DeliveryCharge, currency string) []TierCharge {
	by := map[string]DeliveryCharge{}
	for _, c := range cs {
		by[c.Tier] = c
	}
	var out []TierCharge
	for _, t := range serviceTiers {
		c, ok := by[t]
		if !ok {
			continue
		}
		tc := TierCharge{Tier: t, Charge: Money{AmountMinor: c.ChargeMinor, Currency: currency}}
		if c.FreeOverMinor != nil {
			tc.FreeOver = &Money{AmountMinor: *c.FreeOverMinor, Currency: currency}
		}
		out = append(out, tc)
	}
	return out
}

func sameTiers(a, b []TierCharge) bool {
	if len(a) != len(b) {
		return false
	}
	by := map[string]TierCharge{}
	for _, t := range a {
		by[t.Tier] = t
	}
	for _, t := range b {
		o, ok := by[t.Tier]
		if !ok || o.Charge != t.Charge || (o.FreeOver == nil) != (t.FreeOver == nil) {
			return false
		}
		if o.FreeOver != nil && *o.FreeOver != *t.FreeOver {
			return false
		}
	}
	return true
}

// sameContent compares the observed content with the desired body, the category by path.
func sameContent(pc *ProductContent, dp DesiredProduct, idx map[string]Category, byID map[string]Category) bool {
	want, ok := idx[strings.ToLower(dp.CategoryPath)]
	if !ok || pc.PrimaryCategoryID != want.CategoryID {
		return false
	}
	if _, ok := byID[pc.PrimaryCategoryID]; !ok {
		return false
	}
	o, w := pc.ContentBody, dp.Body
	if o.Description != w.Description || len(o.Bullets) != len(w.Bullets) || len(o.Attributes) != len(w.Attributes) || len(o.Media) != len(w.Media) {
		return false
	}
	for i := range w.Bullets {
		if o.Bullets[i] != w.Bullets[i] {
			return false
		}
	}
	for i := range w.Attributes {
		if o.Attributes[i] != w.Attributes[i] {
			return false
		}
	}
	for i := range w.Media {
		if o.Media[i] != w.Media[i] {
			return false
		}
	}
	return true
}

// Summary renders the plan per kind with counts.
func (p *Plan) Summary() []string {
	tax := fmt.Sprintf("tax table         : current v%d matches, skip", p.TaxCurrent)
	if p.TaxTable != nil {
		tax = fmt.Sprintf("tax table         : publish v%d (current v%d differs or is missing)", p.TaxTable.Version, p.TaxCurrent)
	}
	ch := fmt.Sprintf("delivery charges  : current v%d matches, skip", p.ChargesCurrent)
	if p.ChargeSchedule != nil {
		ch = fmt.Sprintf("delivery charges  : publish v%d (current v%d differs or is missing)", p.ChargeSchedule.Version, p.ChargesCurrent)
	}
	return []string{
		tax,
		ch,
		fmt.Sprintf("categories        : %d to create, %d present", len(p.Categories), p.CategoriesPresent),
		fmt.Sprintf("item prices       : %d to create, %d to change, %d unchanged, %d WITHDRAWN (left alone)", p.PricesToCreate, p.PricesToChange, p.PricesUnchanged, p.PricesWithdrawn),
		fmt.Sprintf("product contents  : %d to create+publish, %d to revise+publish, %d to publish, %d unchanged, %d RETIRED (left alone)", p.ContentsToCreate, p.ContentsToRevise, p.ContentsToPublish, p.ContentsUnchanged, p.ContentsRetired),
		fmt.Sprintf("writes planned    : %d", p.Writes()),
	}
}
