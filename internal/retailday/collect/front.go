package collect

import (
	"slices"
	"sort"
	"strconv"
	"time"

	"github.com/claudioed/warehouse-e2e/internal/retailday"
)

// The shopper's side of the order: what customer-ordering shows (status, lines, promise,
// tracking numbers) and what the money did (customer-payments, checkout). Every reader returns
// its facts and notes what it could not read; a fact whose read failed is zero, as a whole.

// ---- customer-ordering ----

type orderView struct {
	OrderID string `json:"orderId"`
	Status  string `json:"status"`
	Lines   []struct {
		CatalogItemID string `json:"catalogItemId"`
		Quantity      int    `json:"quantity"`
	} `json:"lines"`
	Groups []struct {
		Status          string  `json:"status"`
		DeliverByLatest optTime `json:"deliverByLatest"`
	} `json:"groups"`
}

type trackingView struct {
	Groups []struct {
		Packages []struct {
			TrackingNumber string `json:"trackingNumber"`
		} `json:"packages"`
	} `json:"groups"`
}

// shopperFacts is what customer-ordering shows the shopper.
type shopperFacts struct {
	status     string
	promiseEnd time.Time
	tracking   []string
	// trackingRead is true when customer-ordering answered (an order with no packages yet answers
	// with none: tracking is empty and read).
	trackingRead bool
}

// shopper reads the order and its tracking at the same time.
func (r *run) shopper(id string) shopperFacts {
	var out shopperFacts
	r.par(
		func() {
			o, oc := fetchOne[orderView](r, "Status, PromiseEnd", "customer-ordering", "/orders/{orderId}", "/orders/"+esc(id))
			if oc != found {
				return
			}
			out.status = o.Status
			out.promiseEnd = promiseEnd(o)
			if !r.p.PromiseEnd.IsZero() && !out.promiseEnd.IsZero() && !r.p.PromiseEnd.Equal(out.promiseEnd) {
				r.note("disagree PromiseEnd: the shopper was promised %s, customer-ordering says %s", stamp(r.p.PromiseEnd), stamp(out.promiseEnd))
			}
			r.checkOrderLines(o)
		},
		func() {
			t, oc := fetchOne[trackingView](r, "TrackingNumbers", "customer-ordering", "/orders/{orderId}/tracking", "/orders/"+esc(id)+"/tracking")
			if oc != found {
				return
			}
			out.trackingRead = true
			set := map[string]bool{}
			for _, g := range t.Groups {
				for _, p := range g.Packages {
					if p.TrackingNumber != "" && !set[p.TrackingNumber] {
						set[p.TrackingNumber] = true
						out.tracking = append(out.tracking, p.TrackingNumber)
					}
				}
			}
			sort.Strings(out.tracking)
		},
	)
	return out
}

// promiseEnd is the latest delivery instant the order promised: the latest deliverByLatest of
// its groups that were not cancelled (a cancelled group is not delivered, so it owes no date).
func promiseEnd(o orderView) time.Time {
	var end time.Time
	for _, g := range o.Groups {
		if g.Status != "CANCELLED" && g.DeliverByLatest.After(end) {
			end = g.DeliverByLatest.Time
		}
	}
	return end
}

// checkOrderLines compares the order the storefront holds with what the shopper bought. The
// storefront sells by catalogItemId, the warehouse by SKU; Settings.Catalogue is the dictionary.
func (r *run) checkOrderLines(o orderView) {
	if len(o.Lines) != r.p.Lines() {
		r.note("disagree Lines: the shopper bought %d lines, customer-ordering holds %d", r.p.Lines(), len(o.Lines))
	}
	if len(r.p.SKUs) == 0 {
		return
	}
	if len(r.c.set.Catalogue) == 0 {
		r.note("unread Lines: no catalogue to turn catalogItemIds into SKUs, so the lines were not compared")
		return
	}
	skuOf := map[string]string{}
	for sku, item := range r.c.set.Catalogue {
		skuOf[item] = sku
	}
	got := map[string]int{}
	for _, l := range o.Lines {
		sku, known := skuOf[l.CatalogItemID]
		if !known {
			r.note("disagree Lines: customer-ordering holds catalogItemId %s, which the catalogue does not know", l.CatalogItemID)
			return
		}
		got[sku] += l.Quantity
	}
	if !sameQuantities(r.p.SKUs, got) {
		r.note("disagree Lines: the shopper bought %s, customer-ordering holds %s", showQuantities(r.p.SKUs), showQuantities(got))
	}
}

func sameQuantities(a, b map[string]int) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func showQuantities(m map[string]int) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	s := ""
	for i, k := range keys {
		if i > 0 {
			s += ", "
		}
		s += k + " x" + strconv.Itoa(m[k])
	}
	return "{" + s + "}"
}

// ---- customer-payments and checkout ----

type authView struct {
	AuthorizationID   string `json:"authorizationId"`
	CheckoutSessionID string `json:"checkoutSessionId"`
	OrderID           string `json:"orderId"`
	State             string `json:"state"`
	TotalMinor        int64  `json:"totalMinor"`
	Allocations       []struct {
		State       string `json:"state"`
		AmountMinor int64  `json:"amountMinor"`
	} `json:"allocations"`
}

type sessionView struct {
	SessionID  string `json:"sessionId"`
	TotalMinor int64  `json:"totalMinor"`
	OrderID    string `json:"orderId"`
}

// moneyFacts is what the money did and what the order cost.
type moneyFacts struct {
	authorized, captured, voided, total int64
	// bound is an order id the payment records name (for the case where the shopper phase knows
	// of none).
	bound string
}

const moneyFactNames = "AuthorizedMinor, CapturedMinor, VoidedMinor"

// money reads the order's payment authorizations and the checkout session they came from. orderID
// may be empty (a declined payment places no order): the shopper's authorization id is then the
// only key, and a record that names an order anyway is reported in moneyFacts.bound.
//
// Authorised is the total of every authorization the provider held (a DECLINED record held
// nothing); captured and released are the allocations in those states, in minor units
// (customer-payments openapi: allocations[].state PENDING | CAPTURED | RELEASED, amountMinor;
// a voided or expired authorization has its allocations RELEASED).
func (r *run) money(orderID string) moneyFacts {
	var out moneyFacts
	var auths []authView
	switch {
	case orderID != "":
		got, oc := fetchList[authView](r, moneyFactNames+", TotalMinor", "customer-payments", "/payment-authorizations?orderId", "/payment-authorizations"+query("orderId", orderID))
		if oc == failed {
			return out
		}
		auths = got
		if want := r.p.AuthorizationID; want != "" && !slices.ContainsFunc(auths, func(a authView) bool { return a.AuthorizationID == want }) {
			r.note("disagree AuthorizedMinor: the shopper's authorization %s is not bound to order %s", want, orderID)
		}
	case r.p.AuthorizationID != "":
		a, oc := fetchOne[authView](r, moneyFactNames+", TotalMinor", "customer-payments", "/payment-authorizations/{authorizationId}", "/payment-authorizations/"+esc(r.p.AuthorizationID))
		if oc == failed {
			return out
		}
		if oc == found {
			auths = []authView{a}
		}
	default:
		if r.p.Expect != retailday.NotServed {
			r.note("unread %s, TotalMinor: the shopper phase knows neither an order id nor an authorization id", moneyFactNames)
		}
		return out
	}
	sort.Slice(auths, func(i, j int) bool { return auths[i].AuthorizationID < auths[j].AuthorizationID })
	if len(auths) > 1 {
		r.note("disagree AuthorizedMinor: %d payment authorizations are bound to one order", len(auths))
	}
	for _, a := range auths {
		if a.State != "DECLINED" {
			out.authorized += a.TotalMinor
		}
		for _, al := range a.Allocations {
			switch al.State {
			case "CAPTURED":
				out.captured += al.AmountMinor
			case "RELEASED":
				out.voided += al.AmountMinor
			}
		}
		if out.bound == "" {
			out.bound = a.OrderID
		}
	}
	if len(auths) == 0 {
		return out
	}

	// The order total is the checkout session's: customer-ordering does not carry money.
	sid := auths[0].CheckoutSessionID
	if sid == "" {
		r.note("unread TotalMinor: authorization %s names no checkout session", auths[0].AuthorizationID)
		return out
	}
	s, oc := fetchOne[sessionView](r, "TotalMinor", "checkout", "/checkout-sessions/{sessionId}", "/checkout-sessions/"+esc(sid))
	if oc != found {
		return out
	}
	out.total = s.TotalMinor
	if s.TotalMinor != auths[0].TotalMinor {
		r.note("disagree TotalMinor: checkout session %s totals %d, authorization %s %d", sid, s.TotalMinor, auths[0].AuthorizationID, auths[0].TotalMinor)
	}
	if r.p.TotalMinor != 0 && r.p.TotalMinor != s.TotalMinor {
		r.note("disagree TotalMinor: the shopper was quoted %d, checkout session %s totals %d", r.p.TotalMinor, sid, s.TotalMinor)
	}
	if out.bound == "" {
		out.bound = s.OrderID
	}
	if orderID != "" && s.OrderID != "" && s.OrderID != orderID {
		r.note("disagree OrderID: checkout session %s is bound to order %s, not %s", sid, s.OrderID, orderID)
	}
	return out
}
