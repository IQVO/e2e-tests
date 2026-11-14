package transport

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"time"

	"github.com/claudioed/warehouse-e2e/internal/sim"
)

// Tracker is the tracking reader. It answers two questions the other actors need:
//
//   - which tracking numbers does an order have? The tracking number (RN + 16
//     characters) is the ONLY id that links the warehouse side to the network
//     side. shipping-and-tracking has no route by order id or label (its only
//     read is GET /journeys/{trackingNumber}), so the numbers come from
//     customer-ordering's tracking view (GET /orders/{orderId}/tracking), which
//     is an event-fed read model and therefore fills in after the label and
//     shipment events: the reader polls, bounded;
//   - what does shipping-and-tracking say about a package? GET /journeys/{tn}:
//     the state and every stored scan, applied or not.
type Tracker struct {
	C        *sim.Client
	Clock    Clock
	Log      sim.LogFunc
	Settings Settings
	Timeline *Timeline
}

// NewTracker builds a tracker. timeline may be nil.
func NewTracker(c *sim.Client, clock Clock, log sim.LogFunc, s Settings, tl *Timeline) *Tracker {
	if tl == nil {
		tl = NewTimeline()
	}
	return &Tracker{C: c, Clock: clock, Log: log, Settings: s.withDefaults(), Timeline: tl}
}

// Scan is one scan of a journey as shipping-and-tracking stored it.
type Scan struct {
	ScanID   string    `json:"scanId"`
	NodeID   string    `json:"nodeId"`
	ScanType string    `json:"scanType"`
	At       time.Time `json:"at"`
	Applied  bool      `json:"applied"`
}

// Journey is the package journey: its state and every stored scan.
type Journey struct {
	TrackingNumber string    `json:"trackingNumber"`
	State          string    `json:"state"`
	OrderID        string    `json:"orderId"`
	DeliveredAt    time.Time `json:"deliveredAt"`
	Scans          []Scan    `json:"scans"`
}

// TrackedPackage is one package of an order's tracking view.
type TrackedPackage struct {
	TrackingNumber string `json:"trackingNumber"`
	State          string `json:"state"`
	GroupNo        int    `json:"-"`
	GroupStatus    string `json:"-"`
}

// OrderTracking is customer-ordering's tracking view of one order.
type OrderTracking struct {
	OrderID  string
	Status   string
	Packages []TrackedPackage // of the groups that are not CANCELLED, without VOIDED labels
}

// Order reads the order's tracking view once.
func (t *Tracker) Order(ctx context.Context, orderID string) (OrderTracking, error) {
	step := "customer-ordering GET /orders/{id}/tracking"
	r := t.C.Call(ctx, SvcOrdering, http.MethodGet, "GET /orders/{id}/tracking", "/orders/"+pathEscape(orderID)+"/tracking", nil)
	var v struct {
		OrderID string `json:"orderId"`
		Status  string `json:"status"`
		Groups  []struct {
			GroupNo  int              `json:"groupNo"`
			Status   string           `json:"status"`
			Packages []TrackedPackage `json:"packages"`
		} `json:"groups"`
	}
	if err := decode(step, r, &v); err != nil {
		return OrderTracking{}, err
	}
	out := OrderTracking{OrderID: v.OrderID, Status: v.Status}
	seen := map[string]bool{}
	for _, g := range v.Groups {
		if g.Status == "CANCELLED" {
			continue
		}
		for _, p := range g.Packages {
			if p.State == "VOIDED" || p.TrackingNumber == "" || seen[p.TrackingNumber] {
				continue
			}
			seen[p.TrackingNumber] = true
			p.GroupNo, p.GroupStatus = g.GroupNo, g.Status
			out.Packages = append(out.Packages, p)
		}
	}
	return out, nil
}

// OrderStatus reads the order's status (customer-ordering GET /orders/{id}).
func (t *Tracker) OrderStatus(ctx context.Context, orderID string) (string, error) {
	r := t.C.Call(ctx, SvcOrdering, http.MethodGet, "GET /orders/{id}", "/orders/"+pathEscape(orderID), nil)
	var v struct {
		Status string `json:"status"`
	}
	if err := decode("customer-ordering GET /orders/{id}", r, &v); err != nil {
		return "", err
	}
	return v.Status, nil
}

// OrderTrackingNumbers polls until the order's tracking view lists at least want
// packages (default 1) and returns their tracking numbers. An unknown order, a
// 5xx and a transport error are treated as "not yet" (the view is event-fed);
// any other refusal ends the wait. It gives up after Settings.TrackingTimeout.
func (t *Tracker) OrderTrackingNumbers(ctx context.Context, orderID string, want int) ([]string, error) {
	if want <= 0 {
		want = 1
	}
	var tns []string
	err := await(ctx, t.Clock, t.Settings.TrackingTimeout, t.Settings.PollInterval, func(ctx context.Context) (bool, string, error) {
		o, err := t.Order(ctx, orderID)
		if err != nil {
			if transient(err) {
				return false, err.Error(), nil
			}
			return false, "", err
		}
		tns = tns[:0]
		for _, p := range o.Packages {
			tns = append(tns, p.TrackingNumber)
		}
		return len(tns) >= want, fmt.Sprintf("%d of %d tracking numbers, order %s", len(tns), want, o.Status), nil
	})
	if err != nil {
		return nil, fmt.Errorf("tracking numbers of order %s: %w", orderID, err)
	}
	return slices.Clone(tns), nil
}

// PurchaseOrderTrackingNumbers is the vendor-side way to the same numbers:
// vendor-connectivity GET /vendor/purchase-orders/{poNumber}?vendorId= lists
// `trackingNumbers`, the ones CONFIRMED shipped (`[]` until the shipment
// confirmation succeeds; the labels issued earlier are in `labels`, not here).
// It polls, bounded by Settings.TrackingTimeout, until at least want (default 1)
// are confirmed. A refusal other than not-found/5xx (an unknown vendor, a
// purchase order of another vendor) ends the wait.
func (t *Tracker) PurchaseOrderTrackingNumbers(ctx context.Context, vendorID, poNumber string, want int) ([]string, error) {
	if want <= 0 {
		want = 1
	}
	var tns []string
	path := "/vendor/purchase-orders/" + pathEscape(poNumber) + "?vendorId=" + url.QueryEscape(vendorID)
	err := await(ctx, t.Clock, t.Settings.TrackingTimeout, t.Settings.PollInterval, func(ctx context.Context) (bool, string, error) {
		r := t.C.Call(ctx, SvcVendor, http.MethodGet, "GET /vendor/purchase-orders/{poNumber}", path, nil)
		var po struct {
			State           string   `json:"state"`
			TrackingNumbers []string `json:"trackingNumbers"`
		}
		if err := decode("vendor-connectivity GET /vendor/purchase-orders/{poNumber}", r, &po); err != nil {
			if transient(err) {
				return false, err.Error(), nil
			}
			return false, "", err
		}
		tns = dedupe(po.TrackingNumbers)
		return len(tns) >= want, fmt.Sprintf("%d of %d confirmed tracking numbers, purchase order %s", len(tns), want, po.State), nil
	})
	if err != nil {
		return nil, fmt.Errorf("tracking numbers of purchase order %s: %w", poNumber, err)
	}
	return tns, nil
}

// Journey reads one package's journey and raises the timeline to its newest scan,
// so a time the actors send later is never older than a scan already stored.
func (t *Tracker) Journey(ctx context.Context, tn string) (Journey, error) {
	r := t.C.Call(ctx, SvcTracking, http.MethodGet, "GET /journeys/{trackingNumber}", "/journeys/"+pathEscape(tn), nil)
	var j Journey
	if err := decode("shipping-and-tracking GET /journeys/{trackingNumber}", r, &j); err != nil {
		return Journey{}, err
	}
	for _, s := range j.Scans {
		t.Timeline.Observe(tn, s.At)
	}
	return j, nil
}

// AwaitJourney waits until the journey exists (the label event has arrived).
func (t *Tracker) AwaitJourney(ctx context.Context, tn string) (Journey, error) {
	var j Journey
	err := await(ctx, t.Clock, t.Settings.JourneyTimeout, t.Settings.PollInterval, func(ctx context.Context) (bool, string, error) {
		got, err := t.Journey(ctx, tn)
		if err != nil {
			if transient(err) {
				return false, err.Error(), nil
			}
			return false, "", err
		}
		j = got
		return true, "", nil
	})
	if err != nil {
		return Journey{}, fmt.Errorf("journey %s: %w", tn, err)
	}
	return j, nil
}

// AwaitState waits until the journey is in one of states.
func (t *Tracker) AwaitState(ctx context.Context, tn string, states ...string) (Journey, error) {
	var j Journey
	err := await(ctx, t.Clock, t.Settings.JourneyTimeout, t.Settings.PollInterval, func(ctx context.Context) (bool, string, error) {
		got, err := t.Journey(ctx, tn)
		if err != nil {
			if transient(err) {
				return false, err.Error(), nil
			}
			return false, "", err
		}
		j = got
		return slices.Contains(states, got.State), "state " + got.State, nil
	})
	if err != nil {
		return Journey{}, fmt.Errorf("journey %s to reach %v: %w", tn, states, err)
	}
	return j, nil
}

// ScanFindings lists every scan the journey stored without applying it: an
// out-of-order time or a scan with no edge from the state it met. After a run
// that never sends an older time, there are none.
func ScanFindings(j Journey) []string {
	var out []string
	for _, s := range j.Scans {
		if !s.Applied {
			out = append(out, fmt.Sprintf("journey %s: scan %s %s at %s stored but not applied (out of order or no edge from the state)",
				j.TrackingNumber, s.ScanID, s.ScanType, s.At.UTC().Format(time.RFC3339)))
		}
	}
	return out
}

// transient reports an answer worth polling through: not found yet, a server
// error or a transport error.
func transient(err error) bool {
	var se *StepError
	if !errors.As(err, &se) {
		return false
	}
	return se.Status == 0 || se.Status == http.StatusNotFound || se.Status >= 500
}
