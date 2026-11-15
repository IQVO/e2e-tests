package collect

import (
	"context"
	"errors"
	"strings"

	"github.com/claudioed/warehouse-e2e/internal/retailday"
)

// Shelf reads each SKU's usable stock and whether the storefront still sells it.
//
// Usable is inventory-storage's GET /inventory/{sku}/usable (on hand minus active reservations
// minus held or unlocated stock, fleet-owned stock when ownerRef is omitted). The route answers
// 200 with usable 0 for a SKU it does not know, and declares no 404: a 404 there is a routing
// failure, not "no stock", so it is an error here and never a zero that would pass a rule.
//
// Purchasable is the BFF's GET /products/{catalogItemId} (product-discovery's ProductView), the
// catalogItemId taken from Settings.Catalogue. A stub row is invisible (404): the storefront does
// not sell it, which is an answer. A SKU the catalogue does not know cannot be looked up and is an
// error.
//
// Any SKU that cannot be read fails the whole call (the error names every one, in SKU order) and the
// map is nil: a partial map would read as zero stock and "not sold", which passes the estate rules.
func (c *Collector) Shelf(ctx context.Context, skus []string) (map[string]retailday.Shelf, error) {
	ctx, cancel := context.WithTimeout(ctx, c.set.Timeout)
	defer cancel()
	r := &run{c: c, ctx: ctx}
	list := unionOf(skus)
	out := make(map[string]retailday.Shelf, len(list))
	res := make([]retailday.Shelf, len(list))
	fns := make([]func(), len(list))
	for i, sku := range list {
		fns[i] = func() { res[i] = r.shelf(sku) }
	}
	r.par(fns...)
	if notes := r.sortedNotes(); len(notes) > 0 {
		return nil, errors.New("shelf not read: " + strings.Join(notes, "; "))
	}
	for i, sku := range list {
		out[sku] = res[i]
	}
	return out, nil
}

type usableView struct {
	Usable *int `json:"usable"`
}

type productView struct {
	Purchasable *bool `json:"purchasable"`
}

// shelf reads one SKU; what it cannot read is noted, and the result is then not used.
func (r *run) shelf(sku string) (s retailday.Shelf) {
	item, known := r.c.set.Catalogue[sku]
	if !known {
		r.note("unread Purchasable %s: Settings.Catalogue has no catalogItemId for it", sku)
	}
	r.par(
		func() {
			const route = "/inventory/{sku}/usable"
			resp := r.c.get(r.ctx, "inventory-storage", route, "/inventory/"+esc(sku)+"/usable")
			if !resp.OK() {
				r.unread("Usable "+sku, "inventory-storage", route, resp)
				return
			}
			var v usableView
			if err := resp.Decode(&v); err != nil || v.Usable == nil {
				r.note("unread Usable %s: inventory-storage %s: the body has no usable count", sku, route)
				return
			}
			s.Usable = *v.Usable
		},
		func() {
			if !known {
				return
			}
			const route = "/products/{id}"
			resp := r.c.get(r.ctx, retailday.BFF, route, "/products/"+esc(item))
			switch classify(resp) {
			case none:
				return // not visible, so not sold
			case failed:
				r.unread("Purchasable "+sku, retailday.BFF, route, resp)
				return
			}
			var v productView
			if err := resp.Decode(&v); err != nil || v.Purchasable == nil {
				r.note("unread Purchasable %s: %s %s: the body has no purchasable flag", sku, retailday.BFF, route)
				return
			}
			s.Purchasable = *v.Purchasable
		},
	)
	return s
}
