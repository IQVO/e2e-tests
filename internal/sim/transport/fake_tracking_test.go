package transport

// Fakes of customer-ordering (the order's tracking view: the shopper-facing source of the
// tracking numbers) and shipping-and-tracking (the journey and its scans).
//
// Rules taken from the contracts:
//   - GET /orders/{orderId}/tracking is served from an EVENT-FED read model: a package appears
//     only once its label/shipment event arrived (modelled as "after N reads").
//   - shipping-and-tracking has NO route that lists journeys by order or label: GET
//     /journeys/{trackingNumber} is the only read.
//   - scans apply in TIME order; an older scan is stored with applied=false and the journey
//     does not move (409 scan-out-of-order on the scan route); a scan with no edge from the
//     current state is stored applied=false too (409 illegal-transition).
//   - edges: PICKUP_DEPARTURE LABELLED->HANDED_OVER, STATION_ARRIVAL HANDED_OVER|IN_TRANSIT->AT_STATION,
//     CARRIER_HANDOFF AT_STATION->WITH_CARRIER, OUT_FOR_DELIVERY AT_STATION|WITH_CARRIER->OUT_FOR_DELIVERY,
//     DELIVERED OUT_FOR_DELIVERY->DELIVERED, DELIVERY_FAILED OUT_FOR_DELIVERY->AT_STATION.

import (
	"net/http"
	"time"
)

type fpkg struct {
	TN    string
	State string
}

type fgroup struct {
	No     int
	Status string
	Pkgs   []fpkg
}

type forder struct {
	id        string
	status    string
	showAfter int // reads answered without packages before they appear
	reads     int
	groups    []fgroup
}

type orderingFake struct {
	w      *world
	orders map[string]*forder
}

func newOrderingFake(w *world) *orderingFake {
	o := &orderingFake{w: w, orders: map[string]*forder{}}
	w.route("GET", SvcOrdering, "/orders/{id}/tracking", func(r *http.Request, _ []byte) reply {
		ord, found := o.orders[r.PathValue("id")]
		if !found {
			return problem(404, "not-found", "unknown order")
		}
		ord.reads++
		groups := []map[string]any{}
		for _, g := range ord.groups {
			pk := []map[string]any{}
			if ord.reads > ord.showAfter {
				for _, p := range g.Pkgs {
					pk = append(pk, map[string]any{"trackingNumber": p.TN, "destinationKind": "SHIP_TO", "state": p.State})
				}
			}
			groups = append(groups, map[string]any{"groupNo": g.No, "promiseId": "p", "status": g.Status, "packages": pk})
		}
		return ok(map[string]any{"orderId": ord.id, "status": ord.status, "groups": groups})
	})
	w.route("GET", SvcOrdering, "/orders/{id}", func(r *http.Request, _ []byte) reply {
		ord, found := o.orders[r.PathValue("id")]
		if !found {
			return problem(404, "not-found", "unknown order")
		}
		gs := []map[string]any{}
		for _, g := range ord.groups {
			gs = append(gs, map[string]any{"groupNo": g.No, "status": g.Status})
		}
		return ok(map[string]any{"orderId": ord.id, "status": ord.status, "groups": gs})
	})
	return o
}

// addOrder registers an order with one SHIPPED group whose packages show after `showAfter` reads.
func (w *world) addOrder(id string, showAfter int, tns ...string) *forder {
	o := &forder{id: id, status: "SHIPPED", showAfter: showAfter}
	g := fgroup{No: 1, Status: "SHIPPED"}
	for _, tn := range tns {
		g.Pkgs = append(g.Pkgs, fpkg{TN: tn, State: "LABELLED"})
	}
	o.groups = []fgroup{g}
	w.ordering.orders[id] = o
	return o
}

type fscan struct {
	ScanID   string    `json:"scanId"`
	NodeID   string    `json:"nodeId"`
	ScanType string    `json:"scanType"`
	At       time.Time `json:"at"`
	Applied  bool      `json:"applied"`
}

type fjourney struct {
	tn          string
	state       string
	scans       []fscan
	lastApplied time.Time
	deliveredAt time.Time
	appearAfter int // reads answered 404 before the journey exists (the label event has not arrived)
	reads       int
}

type trackingFake struct {
	w        *world
	journeys map[string]*fjourney
}

var scanEdges = map[string]struct {
	from []string
	to   string
}{
	"PICKUP_DEPARTURE": {[]string{"LABELLED"}, "HANDED_OVER"},
	"STATION_ARRIVAL":  {[]string{"HANDED_OVER", "IN_TRANSIT"}, "AT_STATION"},
	"CARRIER_HANDOFF":  {[]string{"AT_STATION"}, "WITH_CARRIER"},
	"OUT_FOR_DELIVERY": {[]string{"AT_STATION", "WITH_CARRIER"}, "OUT_FOR_DELIVERY"},
	"DELIVERED":        {[]string{"OUT_FOR_DELIVERY"}, "DELIVERED"},
	"DELIVERY_FAILED":  {[]string{"OUT_FOR_DELIVERY"}, "AT_STATION"},
	"DEPARTURE":        {[]string{"HANDED_OVER", "IN_TRANSIT"}, "IN_TRANSIT"},
	"TRANSIT_ARRIVAL":  {[]string{"HANDED_OVER", "IN_TRANSIT"}, "IN_TRANSIT"},
	"SHIPMENT_CONFIRMED": {
		[]string{"LABELLED"}, "LABELLED"},
}

func (j *fjourney) view() map[string]any {
	scans := j.scans
	if scans == nil {
		scans = []fscan{}
	}
	v := map[string]any{"trackingNumber": j.tn, "state": j.state, "poNumber": "PO-1", "assignmentId": "asg-1", "orderId": "ord-1", "tier": "STANDARD", "scans": scans}
	if !j.deliveredAt.IsZero() {
		v["deliveredAt"] = rfc(j.deliveredAt)
	}
	return v
}

// record stores a scan and applies it when the time order and the state edge allow it.
// It returns the problem slug when the scan was stored without being applied.
func (t *trackingFake) record(tn, scanID, node, typ string, at time.Time) (j *fjourney, slug string) {
	j = t.journeys[tn]
	s := fscan{ScanID: scanID, NodeID: node, ScanType: typ, At: at.UTC()}
	edge, known := scanEdges[typ]
	switch {
	case !known:
		slug = "validation-failed"
	case at.Before(j.lastApplied):
		slug = "scan-out-of-order"
	default:
		okFrom := false
		for _, f := range edge.from {
			if f == j.state {
				okFrom = true
			}
		}
		if !okFrom {
			slug = "illegal-transition"
		} else {
			s.Applied = true
			j.state = edge.to
			j.lastApplied = at
			if typ == "DELIVERED" {
				j.deliveredAt = at
			}
		}
	}
	if known {
		j.scans = append(j.scans, s)
	}
	return j, slug
}

func newTrackingFake(w *world) *trackingFake {
	t := &trackingFake{w: w, journeys: map[string]*fjourney{}}
	w.route("GET", SvcTracking, "/journeys/{tn}", func(r *http.Request, _ []byte) reply {
		j, found := t.journeys[r.PathValue("tn")]
		if !found {
			return problem(404, "not-found", "unknown package")
		}
		j.reads++
		if j.reads <= j.appearAfter {
			return problem(404, "not-found", "unknown package")
		}
		return ok(j.view())
	})
	w.route("POST", SvcTracking, "/journeys/{tn}/scans", func(r *http.Request, body []byte) reply {
		var in struct {
			ScanID   string    `json:"scanId"`
			NodeID   string    `json:"nodeId"`
			ScanType string    `json:"scanType"`
			At       time.Time `json:"at"`
		}
		if bad := strict(body, &in); bad != nil {
			return *bad
		}
		if _, found := t.journeys[r.PathValue("tn")]; !found {
			return problem(404, "not-found", "unknown package")
		}
		for _, s := range t.journeys[r.PathValue("tn")].scans {
			if s.ScanID == in.ScanID {
				return problem(409, "duplicate-scan", in.ScanID)
			}
		}
		j, slug := t.record(r.PathValue("tn"), in.ScanID, in.NodeID, in.ScanType, in.At)
		if slug != "" {
			return problem(409, slug, "stored, not applied")
		}
		return ok(j.view())
	})
	w.route("POST", SvcTracking, "/carrier-events", func(_ *http.Request, body []byte) reply {
		var in struct {
			CarrierID      string    `json:"carrierId"`
			CarrierEventID string    `json:"carrierEventId"`
			TrackingNumber string    `json:"trackingNumber"`
			Status         string    `json:"status"`
			At             time.Time `json:"at"`
			ProofCode      string    `json:"proofCode,omitempty"`
		}
		if bad := strict(body, &in); bad != nil {
			return *bad
		}
		j, found := t.journeys[in.TrackingNumber]
		if !found {
			return problem(404, "not-found", "unknown package")
		}
		if in.At.After(w.now().Add(5 * time.Minute)) {
			return problem(422, "validation-failed", "at is more than five minutes in the future")
		}
		typ := map[string]string{"PICKED_UP": "CARRIER_HANDOFF", "OUT_FOR_DELIVERY": "OUT_FOR_DELIVERY", "DELIVERED": "DELIVERED", "DELIVERY_FAILED": "DELIVERY_FAILED"}[in.Status]
		if typ == "" {
			if in.Status == "IN_TRANSIT" {
				return ok(j.view())
			}
			return problem(422, "validation-failed", "unknown status")
		}
		id := in.CarrierID + ":" + in.CarrierEventID
		for _, s := range j.scans {
			if s.ScanID == id {
				return problem(409, "duplicate-scan", id)
			}
		}
		j, slug := t.record(in.TrackingNumber, id, in.CarrierID, typ, in.At)
		if slug != "" {
			return problem(409, slug, "stored, not applied")
		}
		return ok(j.view())
	})
	return t
}

// addJourney creates a LABELLED journey (the label was issued and the shipment confirmed).
func (w *world) addJourney(tn string, appearAfter int) *fjourney {
	j := &fjourney{tn: tn, state: "LABELLED", appearAfter: appearAfter}
	w.tracking.journeys[tn] = j
	return j
}

// scan applies a scan produced by another service's event (LoadDeparted, PackageOutForDelivery...).
func (w *world) scan(tn, typ, node string, at time.Time) {
	if _, found := w.tracking.journeys[tn]; !found {
		return
	}
	w.tracking.record(tn, sprintf("evt:%s:%s:%d", tn, typ, at.UnixNano()), node, typ, at)
}
