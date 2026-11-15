package collect

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"time"

	"github.com/claudioed/warehouse-e2e/internal/retailday"
)

// The network's side of the order: order-sourcing's assignment and its releases, the purchase
// orders vendor-connectivity raised for them, and the tracking journeys shipping-and-tracking
// keeps. An id one context holds that another denies is reported, never resolved silently.

// ---- order-sourcing ----

type assignmentView struct {
	AssignmentID string `json:"assignmentId"`
	OrderID      string `json:"orderId"`
	Releases     []struct {
		ReleaseNo int    `json:"releaseNo"`
		Outcome   string `json:"outcome"`
	} `json:"releases"`
	TrackingNumbers []string `json:"trackingNumbers"`
}

// release is one release order-sourcing made to a node, and the purchase order number it implies.
type release struct {
	assignmentID string
	no           int
	outcome      string
	po           string
}

// sourcingFacts is what order-sourcing holds for the order. ok is false when it could not be read.
type sourcingFacts struct {
	releases []release
	tracking []string
	ok       bool
}

// derivePONumber is vendor-connectivity's purchaseorder.NumberFor (ADR 0002 A1, domain code
// internal/domain/purchaseorder): "PO-" + the first 16 hex digits of
// sha256(assignmentId + ":" + releaseNo). No route lists purchase orders by order, so the
// number is derived from what order-sourcing publishes.
func derivePONumber(assignmentID string, releaseNo int) string {
	sum := sha256.Sum256([]byte(assignmentID + ":" + strconv.Itoa(releaseNo)))
	return "PO-" + hex.EncodeToString(sum[:])[:16]
}

func (r *run) sourcing(id string) sourcingFacts {
	as, oc := fetchList[assignmentView](r, "POs", "order-sourcing", "/assignments?orderId", "/assignments"+query("orderId", id))
	out := sourcingFacts{ok: oc != failed}
	if oc == failed {
		return out
	}
	sort.Slice(as, func(i, j int) bool { return as[i].AssignmentID < as[j].AssignmentID })
	seen := map[string]bool{}
	for _, a := range as {
		if a.OrderID != "" && a.OrderID != id {
			r.note("disagree POs: order-sourcing answered assignment %s of order %s for order %s", a.AssignmentID, a.OrderID, id)
			continue
		}
		for _, rl := range a.Releases {
			out.releases = append(out.releases, release{a.AssignmentID, rl.ReleaseNo, rl.Outcome, derivePONumber(a.AssignmentID, rl.ReleaseNo)})
		}
		for _, tn := range a.TrackingNumbers {
			if tn != "" && !seen[tn] {
				seen[tn] = true
				out.tracking = append(out.tracking, tn)
			}
		}
	}
	sort.Slice(out.releases, func(i, j int) bool {
		a, b := out.releases[i], out.releases[j]
		if a.assignmentID != b.assignmentID {
			return a.assignmentID < b.assignmentID
		}
		return a.no < b.no
	})
	sort.Strings(out.tracking)
	return out
}

// ---- vendor-connectivity ----

type poView struct {
	PONumber string `json:"poNumber"`
	State    string `json:"state"`
}

// readPO reads one purchase order. exists says why the number is believed to exist: when another
// context says so and vendor-connectivity answers 404, that is a disagreement and is noted; when
// nobody does (a release that was never accepted), a 404 is simply "none".
func (r *run) readPO(number, exists string) (po retailday.PO, ok, read bool) {
	v, oc := fetchOne[poView](r, "POs", "vendor-connectivity", "/vendor/purchase-orders/{poNumber}",
		"/vendor/purchase-orders/"+esc(number)+query("vendorId", r.c.set.VendorID))
	switch oc {
	case found:
		return retailday.PO{Number: number, State: v.State}, true, true
	case none:
		if exists != "" {
			r.note("disagree POs: %s, but vendor-connectivity has no purchase order %s for vendor %s", exists, number, r.c.set.VendorID)
		}
		return po, false, true
	default:
		return po, false, false
	}
}

// poFacts is the purchase orders found; ok is false when any read failed (the whole fact is then
// left out: a purchase order that could not be read must not be counted as absent).
type poFacts struct {
	pos []retailday.PO
	ok  bool
}

// purchaseOrders reads the purchase order of every release, at the same time.
func (r *run) purchaseOrders(s sourcingFacts) poFacts {
	if !s.ok {
		return poFacts{}
	}
	type result struct {
		po       retailday.PO
		ok, read bool
	}
	res := make([]result, len(s.releases))
	fns := make([]func(), len(s.releases))
	for i, rl := range s.releases {
		fns[i] = func() {
			exists := ""
			if rl.outcome == "ACCEPTED" {
				exists = fmt.Sprintf("order-sourcing says the node accepted release %d of assignment %s", rl.no, rl.assignmentID)
			}
			res[i].po, res[i].ok, res[i].read = r.readPO(rl.po, exists)
		}
	}
	r.par(fns...)
	out := poFacts{ok: true}
	for _, x := range res {
		if !x.read {
			out.ok = false
		}
		if x.ok {
			out.pos = append(out.pos, x.po)
		}
	}
	return out
}

// ---- shipping-and-tracking ----

type journeyView struct {
	TrackingNumber string  `json:"trackingNumber"`
	State          string  `json:"state"`
	PONumber       string  `json:"poNumber"`
	OrderID        string  `json:"orderId"`
	DeliveredAt    optTime `json:"deliveredAt"`
}

// journeyFacts is what the tracking journeys say. claimed are the purchase orders they name.
type journeyFacts struct {
	deliveredAt time.Time
	claimed     map[string]string // poNumber -> the tracking number whose journey names it
}

// journeys reads the journey of every tracking number at the same time. DeliveredAt is the
// latest delivery instant, and only when every package was delivered: an order with a package
// still moving is not delivered.
func (r *run) journeys(id string, numbers []string) journeyFacts {
	out := journeyFacts{claimed: map[string]string{}}
	js := make([]journeyView, len(numbers))
	ocs := make([]outcome, len(numbers))
	fns := make([]func(), len(numbers))
	for i, tn := range numbers {
		fns[i] = func() {
			js[i], ocs[i] = fetchOne[journeyView](r, "DeliveredAt", "shipping-and-tracking", "/journeys/{trackingNumber}", "/journeys/"+esc(tn))
		}
	}
	r.par(fns...)
	if len(numbers) == 0 {
		return out
	}
	delivered := true
	var latest time.Time
	for i, tn := range numbers {
		if ocs[i] != found {
			delivered = false // failed (noted) or not there yet
			continue
		}
		j := js[i]
		if j.OrderID != "" && j.OrderID != id {
			r.note("disagree OrderID: the journey of %s belongs to order %s, not %s", tn, j.OrderID, id)
		}
		if j.PONumber != "" {
			if _, dup := out.claimed[j.PONumber]; !dup {
				out.claimed[j.PONumber] = tn
			}
		}
		switch {
		case j.State != "DELIVERED":
			delivered = false
		case j.DeliveredAt.IsZero():
			delivered = false
			r.note("disagree DeliveredAt: the journey of %s is DELIVERED but carries no deliveredAt", tn)
		case j.DeliveredAt.After(latest):
			latest = j.DeliveredAt.Time
		}
	}
	if delivered {
		out.deliveredAt = latest
	}
	return out
}

// unionOf is the sorted union of the lists, without duplicates.
func unionOf(lists ...[]string) []string {
	set := map[string]bool{}
	var out []string
	for _, l := range lists {
		for _, s := range l {
			if s != "" && !set[s] {
				set[s] = true
				out = append(out, s)
			}
		}
	}
	sort.Strings(out)
	return out
}

// checkTracking compares what the shopper sees with what order-sourcing holds.
func (r *run) checkTracking(shop shopperFacts, s sourcingFacts) {
	if !shop.trackingRead || !s.ok {
		return
	}
	if !slices.Equal(shop.tracking, s.tracking) {
		r.note("disagree TrackingNumbers: customer-ordering shows %v, order-sourcing holds %v", shop.tracking, s.tracking)
	}
}

// network completes the network facts once the shopper's and sourcing's reads are in: the
// purchase orders (derived from the releases, plus any a journey names that the derivation
// missed) and the delivery.
func (r *run) network(id string, shop shopperFacts, s sourcingFacts) (pos []retailday.PO, deliveredAt time.Time) {
	r.checkTracking(shop, s)
	var pf poFacts
	var jf journeyFacts
	r.par(
		func() { pf = r.purchaseOrders(s) },
		func() { jf = r.journeys(id, unionOf(shop.tracking, s.tracking)) },
	)

	// A purchase order a journey names that the derivation did not produce.
	derived := map[string]bool{}
	for _, rl := range s.releases {
		derived[rl.po] = true
	}
	var extra []string
	for po := range jf.claimed {
		if !derived[po] {
			extra = append(extra, po)
		}
	}
	sort.Strings(extra)
	type result struct {
		po       retailday.PO
		ok, read bool
	}
	res := make([]result, len(extra))
	fns := make([]func(), len(extra))
	for i, po := range extra {
		fns[i] = func() {
			exists := fmt.Sprintf("the journey of %s names it", jf.claimed[po])
			res[i].po, res[i].ok, res[i].read = r.readPO(po, exists)
		}
	}
	r.par(fns...)
	// A fact that could not be read is zero as a whole: the derived half is unknown when sourcing
	// failed, and a purchase order that failed to read must not be counted as absent.
	all := s.ok && pf.ok
	pos = pf.pos
	for _, x := range res {
		if !x.read {
			all = false
		}
		if x.ok {
			pos = append(pos, x.po)
		}
	}
	if !all {
		pos = nil
	}
	sort.Slice(pos, func(i, j int) bool { return pos[i].Number < pos[j].Number })
	return pos, jf.deliveredAt
}
