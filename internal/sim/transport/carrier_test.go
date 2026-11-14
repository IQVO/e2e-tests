package transport

import (
	"context"
	"strings"
	"testing"
	"time"
)

func atStation(w *world, tn string) {
	w.addJourney(tn, 0)
	w.scan(tn, "PICKUP_DEPARTURE", site, t0.Add(-2*time.Hour))
	w.scan(tn, "STATION_ARRIVAL", station, t0.Add(-time.Hour))
}

func TestCarrierScansAreKeyedUniqueAndStrictlyIncreasing(t *testing.T) {
	w := newWorld(t)
	atStation(w, tnA)
	c := NewCarrier(w.client(), w.clock, nil, Settings{}, NewTimeline())
	ctx := context.Background()

	j, err := c.RecordScan(ctx, tnA, "CARRIER_HANDOFF", station)
	if err != nil || j.State != "WITH_CARRIER" {
		t.Fatalf("journey = %+v, %v", j, err)
	}
	// Same instant on the actor's clock for the next scan: the timeline still moves it forward.
	j, err = c.RecordScan(ctx, tnA, "OUT_FOR_DELIVERY", "carrier-1")
	if err != nil || j.State != "OUT_FOR_DELIVERY" {
		t.Fatalf("journey = %+v, %v", j, err)
	}
	j, err = c.RecordScan(ctx, tnA, "DELIVERED", "carrier-1")
	if err != nil || j.State != "DELIVERED" {
		t.Fatalf("journey = %+v, %v", j, err)
	}
	scans := w.tracking.journeys[tnA].scans
	ids := map[string]bool{}
	for i, s := range scans {
		if !s.Applied {
			t.Fatalf("scan %+v stored but not applied", s)
		}
		if ids[s.ScanID] {
			t.Fatalf("scanId %q repeated", s.ScanID)
		}
		ids[s.ScanID] = true
		if i > 0 && !s.At.After(scans[i-1].At) {
			t.Fatalf("scan %d at %s is not after scan %d at %s", i, s.At, i-1, scans[i-1].At)
		}
	}
	w.assertEveryPostKeyed()
}

func TestCarrierRefusalsAreReturnedNotPanicked(t *testing.T) {
	w := newWorld(t)
	atStation(w, tnA)
	c := NewCarrier(w.client(), w.clock, nil, Settings{}, NewTimeline())
	ctx := context.Background()

	// DELIVERED has no edge from AT_STATION: stored not applied, 409 illegal-transition
	if _, err := c.RecordScan(ctx, tnA, "DELIVERED", "carrier-1"); !hasSlug(err, "illegal-transition") {
		t.Fatalf("err = %v, want illegal-transition", err)
	}
	if _, err := c.RecordScan(ctx, "RN0000000000NONE", "CARRIER_HANDOFF", station); err == nil || !strings.Contains(err.Error(), "404") {
		t.Fatalf("unknown package: %v, want 404", err)
	}
}

func TestCarrierEventsFollowTheWebhookContract(t *testing.T) {
	w := newWorld(t)
	atStation(w, tnA)
	c := NewCarrier(w.client(), w.clock, nil, Settings{}, NewTimeline())
	ctx := context.Background()

	for _, st := range []string{"PICKED_UP", "IN_TRANSIT", "OUT_FOR_DELIVERY", "DELIVERED"} {
		if _, err := c.ReportEvent(ctx, tnA, st); err != nil {
			t.Fatalf("%s: %v", st, err)
		}
	}
	j := w.tracking.journeys[tnA]
	if j.state != "DELIVERED" {
		t.Fatalf("journey = %s", j.state)
	}
	var ids []string
	for _, s := range j.scans {
		if strings.HasPrefix(s.ScanID, "e2e-carrier:") {
			ids = append(ids, s.ScanID)
		}
	}
	if len(ids) != 3 { // IN_TRANSIT is accepted without a scan
		t.Fatalf("carrier scans = %v, want 3", ids)
	}
	if _, err := c.ReportEvent(ctx, tnA, "LOST"); err == nil {
		t.Fatal("an unknown status must be refused")
	}
	w.assertEveryPostKeyed()
}

// at must not be more than five minutes ahead; a timeline pushed further than that by the service's clock
// cannot be honoured, and the carrier says so instead of sending a time the service refuses.
func TestCarrierEventNeverSendsAFutureTimeBeyondTheAllowance(t *testing.T) {
	w := newWorld(t)
	atStation(w, tnA)
	tl := NewTimeline()
	tl.Observe(tnA, w.clock.Now().Add(time.Hour))
	c := NewCarrier(w.client(), w.clock, nil, Settings{}, tl)

	_, err := c.ReportEvent(context.Background(), tnA, "PICKED_UP")
	if err == nil || !strings.Contains(err.Error(), "five minutes") {
		t.Fatalf("err = %v, want a refusal to send a time beyond the five-minute allowance", err)
	}
	if n := len(w.callsTo(SvcTracking, "POST /carrier-events")); n != 0 {
		t.Fatalf("%d carrier events sent, want none", n)
	}
}

func TestCarrierLearnsTheNewestScanBeforeStamping(t *testing.T) {
	w := newWorld(t)
	w.skew = 30 * time.Minute // the service's scans carry its clock, half an hour ahead of ours
	w.addJourney(tnA, 0)
	w.scan(tnA, "PICKUP_DEPARTURE", site, w.now())
	w.scan(tnA, "STATION_ARRIVAL", station, w.now().Add(time.Second))
	c := NewCarrier(w.client(), w.clock, nil, Settings{}, NewTimeline()) // a cold carrier: it knows nothing yet

	j, err := c.RecordScan(context.Background(), tnA, "CARRIER_HANDOFF", station)
	if err != nil || j.State != "WITH_CARRIER" {
		t.Fatalf("journey = %+v, %v: the scan was older than the stored ones", j, err)
	}
	for _, s := range w.tracking.journeys[tnA].scans {
		if !s.Applied {
			t.Fatalf("scan %+v stored but not applied", s)
		}
	}
}
