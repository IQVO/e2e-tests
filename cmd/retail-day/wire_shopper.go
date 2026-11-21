package main

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/claudioed/warehouse-e2e/internal/retailday/day"
	"github.com/claudioed/warehouse-e2e/internal/sim"
	"github.com/claudioed/warehouse-e2e/internal/sim/shopper"
)

// The Shopper phase: one shopper.Shopper (one browser, one cookie jar) per Buyer.

// buyer is the one method of *shopper.Shopper the adapter uses.
type buyer interface {
	Buy(ctx context.Context, p shopper.Plan) (shopper.Outcome, error)
}

// shopperAdapter satisfies day.Shopper. newBuyer makes the browser of one e-mail address: the
// real one is shopper.New over the day's shared client (see newShopperFactory).
type shopperAdapter struct {
	runID    string
	newBuyer func(email string) (buyer, error)
	// orders learns where each placed order ships, for the carrier (it is given only an order id).
	orders *orderPrefixes
}

var _ day.Shopper = (*shopperAdapter)(nil)

// newShopperFactory builds shoppers on the shared client. Each gets its own cookie jar (shopper.New
// copies the client's HTTP settings around a jar of its own) and the shared call statistics.
func newShopperFactory(c *sim.Client, log sim.LogFunc) func(email string) (buyer, error) {
	return func(email string) (buyer, error) {
		return shopper.New(c, shopper.Options{Email: email, Log: log})
	}
}

// Buy plays one buyer's purchase. Every buyer has an e-mail address of its own, so two buyers never
// share a shopper account (and a cart) and a second run never signs in to the first one's account.
func (a *shopperAdapter) Buy(ctx context.Context, b day.Buyer) (day.Bought, error) {
	sh, err := a.newBuyer(buyerEmail(a.runID, b.Index))
	if err != nil {
		return day.Bought{}, err
	}
	out, err := sh.Buy(ctx, toPlan(b))
	if err != nil {
		return day.Bought{}, err
	}
	bought := toBought(out)
	if bought.OrderID != "" && a.orders != nil {
		a.orders.set(bought.OrderID, b.Prefix)
	}
	return bought, nil
}

// buyerEmail is unique per run id and buyer index: <run>-b<NN>@example.com under a fixed local part.
func buyerEmail(runID string, index int) string {
	return fmt.Sprintf("retailday-%s-b%02d@example.com", strings.ToLower(runID), index)
}

// toPlan turns a day buyer into the shopper's plan, field for field.
func toPlan(b day.Buyer) shopper.Plan {
	lines := make([]shopper.Line, len(b.Lines))
	for i, l := range b.Lines {
		lines[i] = shopper.Line{CatalogItemID: l.Item.CatalogItemID, Qty: l.Qty}
	}
	return shopper.Plan{
		Lines:                lines,
		Prefix:               b.Prefix,
		Tier:                 shopper.Tier(b.Tier),
		Token:                b.Token,
		FallbackToken:        b.FallbackToken,
		CancelAfterPlacement: b.CancelAfterPlacement,
		ExpectOutOfArea:      b.ExpectOutOfArea,
	}
}

// statuses is the explicit mapping of how a purchase ended (the two packages spell them alike;
// the adapter does not rely on that).
var statuses = map[shopper.Status]day.Status{
	shopper.StatusPlaced:           day.StatusPlaced,
	shopper.StatusDeclined:         day.StatusDeclined,
	shopper.StatusInDoubt:          day.StatusInDoubt,
	shopper.StatusRequoteExhausted: day.StatusRequoteExhausted,
	shopper.StatusRefused:          day.StatusRefused,
	shopper.StatusCancelled:        day.StatusCancelled,
	shopper.StatusNotServed:        day.StatusNotServed,
}

// toBought maps what a shopper learned to what the day judges. A status the day does not know is
// a refusal with a problem that says so, never a silent pass.
func toBought(o shopper.Outcome) day.Bought {
	st, known := statuses[o.Status]
	problems := append([]string(nil), o.Problems...)
	if !known {
		st = day.StatusRefused
		problems = append(problems, fmt.Sprintf("the shopper ended with the status %q, which the day does not know", o.Status))
	}
	return day.Bought{
		Status:          st,
		OrderID:         o.OrderID,
		TotalMinor:      o.TotalMinor,
		AuthorizationID: o.AuthorizationID,
		PromiseEnd:      promiseEnd(o.PromiseWindows),
		OrderStatus:     o.OrderStatus,
		CancelSettled:   o.CancelSettled,
		CancelRefused:   o.CancelRefused,
		Problems:        problems,
	}
}

// promiseEnd is the latest delivery instant any group of the order promised (deliverByLatest). It is
// zero ("unknown": the day then warns that lateness could not be judged) when the order has no group
// or when any group's instant cannot be read: taking the latest of the readable ones would understate
// the promise and fail an on-time order.
func promiseEnd(ws []shopper.PromiseWindow) time.Time {
	var end time.Time
	for _, w := range ws {
		t, ok := parseInstant(w.DeliverByLatest, w.TimeZone)
		if !ok {
			return time.Time{}
		}
		if t.After(end) {
			end = t
		}
	}
	return end
}

// parseInstant reads an RFC 3339 instant. One without an offset is a wall clock reading in tz (UTC
// when tz is empty); a tz that does not resolve makes it unreadable.
func parseInstant(s, tz string) (time.Time, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, false
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, true
	}
	loc := time.UTC
	if tz != "" {
		l, err := time.LoadLocation(tz)
		if err != nil {
			return time.Time{}, false
		}
		loc = l
	}
	for _, layout := range []string{"2006-01-02T15:04:05", "2006-01-02T15:04"} {
		if t, err := time.ParseInLocation(layout, s, loc); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

// orderPrefixes remembers the postal prefix each placed order ships to (customer-ordering order id ->
// prefix): the carrier is handed an order id and must find the delivery station that serves it.
type orderPrefixes struct{ m sync.Map }

func (p *orderPrefixes) set(orderID, prefix string) { p.m.Store(orderID, prefix) }

func (p *orderPrefixes) get(orderID string) (string, bool) {
	v, ok := p.m.Load(orderID)
	if !ok {
		return "", false
	}
	return v.(string), true
}
