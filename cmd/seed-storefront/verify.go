package main

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

// ProductView is the slice of product-discovery's ProductView (and the BFF's relay of it) that
// VERIFY reads.
type ProductView struct {
	CatalogItemID  string   `json:"catalogItemId"`
	Title          string   `json:"title"`
	Description    string   `json:"description"`
	CategoryID     string   `json:"categoryId"`
	CategoryPath   []string `json:"categoryPath"`
	EffectivePrice *Money   `json:"effectivePrice"`
	InStock        bool     `json:"inStock"`
	Purchasable    bool     `json:"purchasable"`
}

// VerifyReport is the outcome of VERIFY.
type VerifyReport struct {
	Products, Purchasable int
	Elapsed               time.Duration
	NotPurchasable        []string
	QuoteSKU, QuotePrefix string
	QuoteTiers            []string
	DepartmentHits        []string     // "<department> <n>"
	Offers                *OfferReport // network-fulfillment capability offers (nil: not read)
	Warnings              []string
	Failures              []string
}

// OK is true when every check passed.
func (r *VerifyReport) OK() bool { return len(r.Failures) == 0 }

// Lines renders the report.
func (r *VerifyReport) Lines() []string {
	out := []string{
		fmt.Sprintf("verify: %d/%d products purchasable in product-discovery (%s)", r.Purchasable, r.Products, r.Elapsed.Round(100*time.Millisecond)),
	}
	if r.Offers != nil {
		out = append([]string{r.Offers.Line()}, out...)
	}
	for i, n := range r.NotPurchasable {
		if i == 20 {
			out = append(out, fmt.Sprintf("  ... and %d more", len(r.NotPurchasable)-20))
			break
		}
		out = append(out, "  NOT PURCHASABLE "+n)
	}
	out = append(out, fmt.Sprintf("verify: delivery quote via the BFF for %s, postal prefix %s: %s", r.QuoteSKU, r.QuotePrefix, orNone(r.QuoteTiers)))
	out = append(out, fmt.Sprintf("verify: BFF search hits per department: %s", strings.Join(r.DepartmentHits, ", ")))
	for _, w := range r.Warnings {
		out = append(out, "  WARNING "+w)
	}
	if r.OK() {
		out = append(out, "verify: PASS")
	} else {
		out = append(out, fmt.Sprintf("verify: FAIL (%d)", len(r.Failures)))
	}
	return out
}

func orNone(s []string) string {
	if len(s) == 0 {
		return "no tier"
	}
	return strings.Join(s, ", ")
}

// verify proves the storefront can sell: every fixture product purchasable (bounded poll, the
// discovery projection is event-driven), one delivery quote through the BFF, and BFF search
// finding every department.
func verify(ctx context.Context, c *client, d *Desired, fx *Fixture, tree CategoryTree, cfg Config) *VerifyReport {
	rep := &VerifyReport{Products: len(d.Products), QuotePrefix: cfg.QuotePrefix}
	start := time.Now()
	timeout, interval := cfg.VerifyTimeout, cfg.VerifyInterval
	if interval <= 0 {
		interval = time.Second
	}
	pending := map[string]DesiredProduct{}
	for _, p := range d.Products {
		pending[p.CatalogItemID] = p
	}
	last := map[string]ProductView{}
	lastErr := map[string]error{}
	for {
		ids := make([]string, 0, len(pending))
		for id := range pending {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		for _, id := range ids {
			var v ProductView
			err := c.get(ctx, c.base+"/product-discovery/products/"+url.PathEscape(id), &v)
			lastErr[id] = err
			if err == nil {
				last[id] = v
				if v.Purchasable {
					delete(pending, id)
				}
			}
		}
		if len(pending) == 0 || time.Since(start) >= timeout || ctx.Err() != nil {
			break
		}
		select {
		case <-ctx.Done():
		case <-time.After(interval):
		}
	}
	rep.Elapsed = time.Since(start)
	rep.Purchasable = len(d.Products) - len(pending)
	for _, p := range d.Products {
		if _, ok := pending[p.CatalogItemID]; !ok {
			continue
		}
		v, seen := last[p.CatalogItemID]
		why := ""
		switch {
		case !seen:
			why = fmt.Sprintf("not in product-discovery (%v)", lastErr[p.CatalogItemID])
		default:
			var miss []string
			if !v.InStock {
				miss = append(miss, "out of stock (WMS usable stock -> network-fulfillment ACL, capped by the CPT schedule's pre-cutoff capacity -> listing feed)")
			}
			if v.EffectivePrice == nil {
				miss = append(miss, "no ACTIVE price")
			}
			if v.Description == "" || v.CategoryID == "" {
				miss = append(miss, "no PUBLISHED content")
			}
			if len(miss) == 0 {
				miss = append(miss, "purchasable=false")
			}
			why = strings.Join(miss, ", ")
		}
		rep.NotPurchasable = append(rep.NotPurchasable, fmt.Sprintf("%s %q (%s): %s", p.SKU, p.Title, p.CatalogItemID, why))
	}
	if len(pending) > 0 {
		rep.Failures = append(rep.Failures, fmt.Sprintf("%d products not purchasable within %s", len(pending), timeout))
	}

	// one delivery quote through the BFF, for the first fixture product
	if len(d.Products) > 0 {
		p := d.Products[0]
		rep.QuoteSKU = p.SKU
		var dates struct {
			Tiers []struct {
				Tier string `json:"tier"`
			} `json:"tiers"`
			Unavailable []map[string]any `json:"unavailable"`
		}
		body := map[string]string{"postalPrefix": cfg.QuotePrefix}
		u := c.sf + "/products/" + url.PathEscape(p.CatalogItemID) + ":delivery-dates"
		if _, err := c.do(ctx, http.MethodPost, u, body, "", &dates, http.StatusOK); err != nil {
			rep.Failures = append(rep.Failures, fmt.Sprintf("delivery quote: %v", err))
		} else {
			for _, t := range dates.Tiers {
				rep.QuoteTiers = append(rep.QuoteTiers, t.Tier)
			}
			if len(rep.QuoteTiers) == 0 {
				rep.Failures = append(rep.Failures, fmt.Sprintf("delivery quote for prefix %s returned no tier (unavailable: %v)", cfg.QuotePrefix, dates.Unavailable))
			}
		}
	}

	// BFF search: at least one hit of every department
	idx := treeIndex(tree)
	under := map[string]map[string]bool{} // department name -> category ids of its subtree
	for _, cat := range tree.Categories {
		top, _, _ := strings.Cut(cat.Path, "/")
		k := strings.ToLower(top)
		if under[k] == nil {
			under[k] = map[string]bool{}
		}
		under[k][cat.CategoryID] = true
	}
	for _, dept := range fx.Departments {
		n, err := searchDepartment(ctx, c, dept.Name, idx, under[strings.ToLower(dept.Name)])
		if err != nil {
			rep.Failures = append(rep.Failures, fmt.Sprintf("search %q: %v", dept.Name, err))
		} else if n == 0 {
			rep.Failures = append(rep.Failures, fmt.Sprintf("search %q: no hit of the department", dept.Name))
		}
		rep.DepartmentHits = append(rep.DepartmentHits, fmt.Sprintf("%s %d", dept.Name, n))
	}

	// the department image is served by storefront-web; a missing one only breaks the picture
	if len(fx.Departments) > 0 && cfg.MediaBase != "" {
		u := strings.TrimRight(cfg.MediaBase, "/") + "/" + fx.Departments[0].Slug + ".svg"
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u, bytes.NewReader(nil))
		if resp, err := c.http.Do(req); err != nil {
			rep.Warnings = append(rep.Warnings, fmt.Sprintf("department image %s: %v", u, err))
		} else {
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				rep.Warnings = append(rep.Warnings, fmt.Sprintf("department image %s answers HTTP %d (storefront-web ships public/media/departments/*.svg; the product page shows no picture until it does)", u, resp.StatusCode))
			}
		}
	}
	return rep
}

// searchDepartment counts the BFF search hits of a department: first a full-text query for the
// department name (category names are indexed), then the department's category filter.
func searchDepartment(ctx context.Context, c *client, name string, idx map[string]Category, subtree map[string]bool) (int, error) {
	type hits struct {
		Hits []ProductView `json:"hits"`
	}
	count := func(h hits) int {
		n := 0
		for _, v := range h.Hits {
			if subtree[v.CategoryID] || (len(v.CategoryPath) > 0 && strings.EqualFold(v.CategoryPath[0], name)) {
				n++
			}
		}
		return n
	}
	var h hits
	if err := c.get(ctx, c.sf+"/products?limit=100&q="+url.QueryEscape(name), &h); err != nil {
		return 0, err
	}
	if n := count(h); n > 0 {
		return n, nil
	}
	dept, ok := idx[strings.ToLower(name)]
	if !ok {
		return 0, nil
	}
	h = hits{}
	if err := c.get(ctx, c.sf+"/products?limit=100&categoryId="+url.QueryEscape(dept.CategoryID), &h); err != nil {
		return 0, err
	}
	return count(h), nil
}
