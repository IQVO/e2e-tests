package collect

import (
	"context"
	"fmt"

	"github.com/claudioed/warehouse-e2e/internal/retailday"
)

// Order follows one placed order across every context and returns what it found and what it
// could not read (see the package comment). It only reads, never panics, and returns the same
// facts and the same notes, in the same order, for the same state of the estate.
func (c *Collector) Order(ctx context.Context, p retailday.Placed) (f retailday.OrderFacts, notes []string) {
	ctx, cancel := context.WithTimeout(ctx, c.set.Timeout)
	defer cancel()
	r := &run{c: c, ctx: ctx, p: p}
	f = retailday.OrderFacts{OrderID: p.OrderID, Expect: p.Expect, Lines: p.Lines()}
	defer func() {
		if e := recover(); e != nil {
			r.note("unread order: the collector failed unexpectedly: %v", e)
			notes = r.sortedNotes()
		}
	}()

	id := p.OrderID
	var m moneyFacts
	moneyRead := false
	if id == "" {
		// Nothing was placed, so the shopper phase has no order id. The money records may still
		// name one; if they do, the order exists and is followed.
		m, moneyRead = r.money(""), true
		if id = m.bound; id == "" {
			if p.Expect == retailday.Delivered || p.Expect == retailday.Cancelled {
				r.note("unread order: the shopper phase has no order id for an order expected to be %s", p.Expect)
			}
			r.fillMoney(&f, m)
			return f, r.sortedNotes()
		}
		r.note("disagree OrderID: no order was placed, but the payment records name order %s", id)
	}

	var shop shopperFacts
	r.par(
		func() { shop = r.shopper(id) },
		func() {
			if !moneyRead {
				m = r.money(id)
			}
		},
	)
	r.fillMoney(&f, m)
	f.Status, f.PromiseEnd, f.TrackingNumbers = shop.status, shop.promiseEnd, shop.tracking
	return f, r.sortedNotes()
}

func (r *run) fillMoney(f *retailday.OrderFacts, m moneyFacts) {
	f.TotalMinor, f.AuthorizedMinor, f.CapturedMinor, f.VoidedMinor = m.total, m.authorized, m.captured, m.voided
}

// Shelf reads each SKU's usable stock and whether the storefront sells it. Not read yet.
func (c *Collector) Shelf(_ context.Context, skus []string) (map[string]retailday.Shelf, error) {
	return nil, fmt.Errorf("not read yet")
}
