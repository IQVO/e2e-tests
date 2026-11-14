package transport

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/claudioed/warehouse-e2e/internal/sim"
)

// DefaultCarrierID is the carrier id of the simulated carrier's webhook events.
const DefaultCarrierID = "e2e-carrier"

// maxFuture is how far ahead of the service's clock a carrier event's `at` may be
// (shipping-and-tracking POST /carrier-events: "at most five minutes into the future").
const maxFuture = 5 * time.Minute

// Carrier is the external carrier of shipping-and-tracking: POST
// /journeys/{trackingNumber}/scans and POST /carrier-events. They exist only for
// CARRIER_HANDOFF service areas; the 786/787 seed areas run the network's own
// route (waves, partners, drivers), so the MAIN flow (Transport.Deliver) never
// uses it. It is here behind its own methods for a run that does hand a package
// over to a carrier.
//
// Every time it sends is stamped through the Timeline after reading the journey,
// so a scan is never older than one already stored (an older one is stored, not
// applied, and answers 409 scan-out-of-order).
type Carrier struct {
	C        *sim.Client
	Clock    Clock
	Log      sim.LogFunc
	Settings Settings
	Timeline *Timeline
	// ID is the carrierId of the events and the prefix of the generated scan ids.
	ID      string
	tracker *Tracker
}

// NewCarrier builds a carrier. tl may be nil.
func NewCarrier(c *sim.Client, clock Clock, log sim.LogFunc, s Settings, tl *Timeline) *Carrier {
	if tl == nil {
		tl = NewTimeline()
	}
	return &Carrier{C: c, Clock: clock, Log: log, Settings: s.withDefaults(), Timeline: tl, ID: DefaultCarrierID,
		tracker: NewTracker(c, clock, log, s, tl)}
}

// RecordScan records one scan of the given type at a node on a journey and returns the journey after it.
func (c *Carrier) RecordScan(ctx context.Context, tn, scanType, nodeID string) (Journey, error) {
	if _, err := c.tracker.Journey(ctx, tn); err != nil { // learn the newest scan time first
		return Journey{}, err
	}
	at := c.Timeline.Stamp(tn, c.Clock.Now())
	body := map[string]any{
		"scanId":   fmt.Sprintf("%s-%s-%s-%d", c.ID, tn, scanType, at.UnixNano()),
		"nodeId":   nodeID,
		"scanType": scanType,
		"at":       at.UTC(),
	}
	r := c.C.Call(ctx, SvcTracking, http.MethodPost, "POST /journeys/{trackingNumber}/scans", "/journeys/"+pathEscape(tn)+"/scans", body)
	var j Journey
	if err := decode("shipping-and-tracking POST /journeys/{tn}/scans", r, &j); err != nil {
		return Journey{}, err
	}
	return j, nil
}

// ReportEvent reports one carrier event (PICKED_UP, IN_TRANSIT, OUT_FOR_DELIVERY,
// DELIVERED or DELIVERY_FAILED) for a package and returns the journey after it.
// The event id is unique per call, so a re-sent report is never a duplicate scan.
func (c *Carrier) ReportEvent(ctx context.Context, tn, status string) (Journey, error) {
	if _, err := c.tracker.Journey(ctx, tn); err != nil {
		return Journey{}, err
	}
	at := c.Timeline.Stamp(tn, c.Clock.Now())
	if at.After(c.Clock.Now().Add(maxFuture)) {
		return Journey{}, fmt.Errorf("carrier event %s for %s: the time after the package's last scan (%s) is more than five minutes ahead of now; not sent",
			status, tn, at.UTC().Format(time.RFC3339))
	}
	body := map[string]any{
		"carrierId":      c.ID,
		"carrierEventId": fmt.Sprintf("%s-%s-%d", tn, status, at.UnixNano()),
		"trackingNumber": tn,
		"status":         status,
		"at":             at.UTC(),
	}
	r := c.C.Call(ctx, SvcTracking, http.MethodPost, "POST /carrier-events", "/carrier-events", body)
	var j Journey
	if err := decode("shipping-and-tracking POST /carrier-events", r, &j); err != nil {
		return Journey{}, err
	}
	return j, nil
}
