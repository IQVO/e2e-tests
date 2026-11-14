package transport

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"
)

func newTracker(w *world) *Tracker {
	return NewTracker(w.client(), w.clock, nil, Settings{}, NewTimeline())
}

// The tracking numbers are the only link between the warehouse and the network side. They are
// served by customer-ordering's event-fed tracking view, so they appear only after a few reads:
// the reader must poll, bounded, and not give up on the first empty answer.
func TestOrderTrackingNumbersPollsUntilTheViewHasThem(t *testing.T) {
	w := newWorld(t)
	w.addOrder("ord-1", 3, "RN0000000000000001", "RN0000000000000002")
	tr := newTracker(w)

	got, err := tr.OrderTrackingNumbers(context.Background(), "ord-1", 2)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"RN0000000000000001", "RN0000000000000002"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	if reads := len(w.callsTo(SvcOrdering, "GET /orders/ord-1/tracking")); reads != 4 {
		t.Fatalf("%d reads, want 4 (3 empty + 1 with the packages)", reads)
	}
	if elapsed := w.clock.Now().Sub(t0); elapsed != 3*DefaultSettings().PollInterval {
		t.Fatalf("waited %s of fake time, want 3 polls", elapsed)
	}
}

func TestOrderTrackingNumbersWaitsForTheExpectedCount(t *testing.T) {
	w := newWorld(t)
	o := w.addOrder("ord-1", 0, "RN0000000000000001")
	_ = o
	tr := newTracker(w)
	tr.Settings.TrackingTimeout = 30 * time.Second

	_, err := tr.OrderTrackingNumbers(context.Background(), "ord-1", 2)
	if !IsTimeout(err) {
		t.Fatalf("err = %v, want a timeout: only one of the two expected packages exists", err)
	}
	if !strings.Contains(err.Error(), "1 of 2") {
		t.Fatalf("err %q should say what it saw (1 of 2)", err)
	}
}

func TestOrderTrackingNumbersIsBoundedWhenTheyNeverAppear(t *testing.T) {
	w := newWorld(t)
	w.addOrder("ord-1", 1<<30, "RN0000000000000001")
	tr := newTracker(w)

	_, err := tr.OrderTrackingNumbers(context.Background(), "ord-1", 1)
	if !IsTimeout(err) {
		t.Fatalf("err = %v, want a timeout, not a hang", err)
	}
	if elapsed := w.clock.Now().Sub(t0); elapsed > DefaultSettings().TrackingTimeout+DefaultSettings().PollInterval {
		t.Fatalf("waited %s, want at most the tracking timeout", elapsed)
	}
}

func TestOrderTrackingNumbersTreatsAnUnknownOrderAsNotYetThenTimesOut(t *testing.T) {
	w := newWorld(t)
	tr := newTracker(w)
	tr.Settings.TrackingTimeout = 10 * time.Second
	_, err := tr.OrderTrackingNumbers(context.Background(), "ord-missing", 1)
	if !IsTimeout(err) || !strings.Contains(err.Error(), "not-found") {
		t.Fatalf("err = %v, want a timeout that says the order was not found", err)
	}
}

func TestOrderTrackingNumbersSkipsVoidedPackagesAndCancelledGroups(t *testing.T) {
	w := newWorld(t)
	o := w.addOrder("ord-1", 0, "RN0000000000000001", "RN0000000000000002")
	o.groups[0].Pkgs[1].State = "VOIDED"
	o.groups = append(o.groups, fgroup{No: 2, Status: "CANCELLED", Pkgs: []fpkg{{TN: "RN0000000000000009", State: "LABELLED"}}})
	tr := newTracker(w)

	got, err := tr.OrderTrackingNumbers(context.Background(), "ord-1", 1)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"RN0000000000000001"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v: a voided label and a cancelled group are not the order's parcels", got, want)
	}
}

// There is no shipping-and-tracking route by order id or label: the journey is read by tracking number only.
func TestJourneyReadsStateAndScansAndRaisesTheTimeline(t *testing.T) {
	w := newWorld(t)
	j := w.addJourney("RN0000000000000001", 0)
	w.scan("RN0000000000000001", "PICKUP_DEPARTURE", "site-1", t0.Add(time.Hour))
	tl := NewTimeline()
	tr := NewTracker(w.client(), w.clock, nil, Settings{}, tl)

	got, err := tr.Journey(context.Background(), "RN0000000000000001")
	if err != nil {
		t.Fatal(err)
	}
	if got.State != "HANDED_OVER" || len(got.Scans) != 1 || !got.Scans[0].Applied || got.Scans[0].ScanType != "PICKUP_DEPARTURE" {
		t.Fatalf("journey = %+v", got)
	}
	if !tl.Last("RN0000000000000001").Equal(j.scans[0].At) {
		t.Fatalf("timeline = %s, want the newest scan time %s", tl.Last("RN0000000000000001"), j.scans[0].At)
	}
}

func TestAwaitStatePollsAndAwaitJourneyWaitsForTheLabelEvent(t *testing.T) {
	w := newWorld(t)
	w.addJourney("RN0000000000000001", 2) // 404 twice: the label event has not arrived
	tr := newTracker(w)

	if _, err := tr.AwaitJourney(context.Background(), "RN0000000000000001"); err != nil {
		t.Fatalf("AwaitJourney: %v", err)
	}
	// The state never becomes DELIVERED: bounded.
	tr.Settings.JourneyTimeout = 20 * time.Second
	_, err := tr.AwaitState(context.Background(), "RN0000000000000001", "DELIVERED")
	if !IsTimeout(err) || !strings.Contains(err.Error(), "LABELLED") {
		t.Fatalf("err = %v, want a timeout that says the journey is still LABELLED", err)
	}
	w.scan("RN0000000000000001", "PICKUP_DEPARTURE", "site-1", t0)
	got, err := tr.AwaitState(context.Background(), "RN0000000000000001", "HANDED_OVER", "IN_TRANSIT")
	if err != nil || got.State != "HANDED_OVER" {
		t.Fatalf("AwaitState = %+v, %v", got, err)
	}
}

func TestScanFindingsNamesEveryScanStoredButNotApplied(t *testing.T) {
	j := Journey{TrackingNumber: "RN1", State: "AT_STATION", Scans: []Scan{
		{ScanID: "a", ScanType: "STATION_ARRIVAL", At: t0, Applied: true},
		{ScanID: "b", ScanType: "OUT_FOR_DELIVERY", At: t0.Add(-time.Hour), Applied: false},
	}}
	f := ScanFindings(j)
	if len(f) != 1 || !strings.Contains(f[0], "OUT_FOR_DELIVERY") || !strings.Contains(f[0], "RN1") || !strings.Contains(f[0], "not applied") {
		t.Fatalf("findings = %v", f)
	}
	if got := ScanFindings(Journey{TrackingNumber: "RN2", Scans: []Scan{{Applied: true}}}); len(got) != 0 {
		t.Fatalf("findings = %v, want none", got)
	}
}

func TestOrderStatusReadsTheOrder(t *testing.T) {
	w := newWorld(t)
	w.addOrder("ord-1", 0, "RN0000000000000001").status = "DELIVERED"
	tr := newTracker(w)
	st, err := tr.OrderStatus(context.Background(), "ord-1")
	if err != nil || st != "DELIVERED" {
		t.Fatalf("status = %q, %v", st, err)
	}
}
