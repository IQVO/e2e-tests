package inbound

import (
	"context"
	"strings"
	"testing"
)

// received docks a delivery and counts + closes its receipt: the state in which the yard may release the trailer.
func received(t *testing.T, c *Crew, lines ...Line) (ASN, Appointment, Visit, Receipt) {
	t.Helper()
	asn, a, v := docked(t, c, lines...)
	r, err := c.OpenReceipt(context.Background(), asn, a)
	if err != nil {
		t.Fatal(err)
	}
	if r, err = c.Receive(context.Background(), asn, r); err != nil {
		t.Fatal(err)
	}
	return asn, a, v, r
}

func TestReleaseWaitsForTheUnloadByReadingNeverByRetryingTheCheckOut(t *testing.T) {
	w := newWorld(t)
	w.lagUnloadReads = 3 // yard-management consumes ReceiptClosed a few reads late
	c := w.crew(w.seedSite())
	_, _, v, _ := received(t, c, Line{skuA, 10})
	if err := c.Release(context.Background(), v); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if n := w.served("yard-management POST /visits/{id}/check-outs"); n != 1 {
		t.Errorf("%d check-outs, want exactly 1", n)
	}
	if w.yard.refusedCheckouts != 0 {
		t.Errorf("the yard refused %d check-outs: the clerk must wait for the release by reading the trailer, not by retrying a refusal", w.yard.refusedCheckouts)
	}
	if n := w.served("yard-management GET /trailers/{id}"); n < 4 {
		t.Errorf("%d trailer reads, want at least 4 (3 still open, then released)", n)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if tr := w.yard.trailers[v.TrailerID]; tr.state != "DEPARTED" || w.yard.occupant[v.Door] != "" {
		t.Errorf("trailer %s door %q holds %q, want DEPARTED and the door free", tr.state, v.Door, w.yard.occupant[v.Door])
	}
}

func TestReleaseBeforeTheReceiptClosedTimesOutAndNeverChecksOut(t *testing.T) {
	w := newWorld(t)
	c := w.crew(w.seedSite())
	_, _, v := docked(t, c, Line{skuA, 10}) // nothing was received or closed
	err := c.Release(context.Background(), v)
	if err == nil || !IsTimeout(err) {
		t.Fatalf("err = %v, want a bounded timeout", err)
	}
	for _, want := range []string{"IN_PROGRESS", "ReceiptClosed", v.TrailerID} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q lacks %q", err, want)
		}
	}
	if n := w.served("yard-management POST /visits/{id}/check-outs"); n != 0 {
		t.Errorf("%d check-outs for a trailer whose unload is still open", n)
	}
}

func TestReleaseOfAnUnloadThatNeverCompletesAfterTheCloseSaysSo(t *testing.T) {
	w := newWorld(t)
	w.lagUnloadReads = 1 << 20
	c := w.crew(w.seedSite())
	_, _, v, _ := received(t, c, Line{skuA, 10})
	if err := c.Release(context.Background(), v); err == nil || !IsTimeout(err) {
		t.Fatalf("err = %v, want a bounded timeout", err)
	}
}

func TestAwaitCompletedReadsDockSchedulingUntilTheAppointmentIsCompleted(t *testing.T) {
	w := newWorld(t)
	w.lagCompleteReads = 3
	c := w.crew(w.seedSite())
	_, a, v, _ := received(t, c, Line{skuA, 10})
	if err := c.Release(context.Background(), v); err != nil {
		t.Fatal(err)
	}
	before := w.served("dock-scheduling GET /appointments/{id}")
	if err := c.AwaitCompleted(context.Background(), a); err != nil {
		t.Fatalf("AwaitCompleted: %v", err)
	}
	if n := w.served("dock-scheduling GET /appointments/{id}") - before; n != 4 {
		t.Errorf("%d reads, want 4 (3 still CHECKED_IN, then COMPLETED)", n)
	}
}

func TestAwaitCompletedOfATruckThatNeverLeftTimesOutNamingTheState(t *testing.T) {
	w := newWorld(t)
	c := w.crew(w.seedSite())
	_, a, _, _ := received(t, c, Line{skuA, 10}) // received, not released
	err := c.AwaitCompleted(context.Background(), a)
	if err == nil || !IsTimeout(err) || !strings.Contains(err.Error(), "CHECKED_IN") {
		t.Fatalf("err = %v, want a timeout that says the appointment is still CHECKED_IN", err)
	}
}

func TestAwaitCompletedStopsAtOnceOnAnAppointmentThatCanNeverComplete(t *testing.T) {
	w := newWorld(t)
	c := w.crew(w.seedSite())
	_, a := booked(t, c, Line{skuA, 10})
	w.with(func() { w.dock.appts[a.ID].state = "CANCELLED" })
	before := w.served("dock-scheduling GET /appointments/{id}")
	err := c.AwaitCompleted(context.Background(), a)
	if err == nil || IsTimeout(err) || !strings.Contains(err.Error(), "CANCELLED") {
		t.Fatalf("err = %v, want an immediate finding naming CANCELLED, not a timeout", err)
	}
	if n := w.served("dock-scheduling GET /appointments/{id}") - before; n != 1 {
		t.Errorf("%d reads, want 1", n)
	}
}

// ---------------------------------------------------------------------
// the staged handover, seen on the Kafka tap
// ---------------------------------------------------------------------

func TestStagedWaitIsNotFooledByOtherEventsOfTheSKU(t *testing.T) {
	w := newWorld(t)
	c := w.crew(w.seedSite())
	c.Staged = w.tap
	w.lagStagedSeen = 1 << 20 // StockReceived never shows up, but ReceiptLineReceived (same SKU in its payload) does
	lines := []Line{{skuA, 10}}
	base := c.trackStaged(lines)
	_, _, _, _ = received(t, c, lines...)
	err := c.awaitStaged(context.Background(), base, lines)
	if err == nil || !IsTimeout(err) || !strings.Contains(err.Error(), "StockReceived") || !strings.Contains(err.Error(), skuA) {
		t.Fatalf("err = %v, want a timeout naming StockReceived and %s", err, skuA)
	}
}

func TestStagedWaitPassesOnceStockReceivedCarriesTheSKU(t *testing.T) {
	w := newWorld(t)
	c := w.crew(w.seedSite())
	c.Staged = w.tap
	w.lagStagedSeen = 3
	lines := []Line{{skuA, 10}, {skuB, 5}}
	base := c.trackStaged(lines)
	_, _, _, _ = received(t, c, lines...)
	if err := c.awaitStaged(context.Background(), base, lines); err != nil {
		t.Fatalf("awaitStaged: %v", err)
	}
}

func TestStagedWaitDoesNotConfuseASKUWithOneItIsAPrefixOf(t *testing.T) {
	w := newWorld(t)
	c := w.crew(w.seedSite())
	c.Staged = w.tap
	lines := []Line{{skuA, 10}}
	base := c.trackStaged(lines)
	// A different SKU whose id starts with ours is staged; ours is not.
	w.with(func() { w.tap.publish("StockReceived", `{"sku":"`+skuA+`0","quantity":10}`) })
	err := c.awaitStaged(context.Background(), base, lines)
	if err == nil || !IsTimeout(err) {
		t.Fatalf("err = %v, want a timeout: %s0 is not %s", err, skuA, skuA)
	}
}

func TestStagedWaitCountsOnlyWhatArrivedAfterItsBaselineAndTracksASKUOnce(t *testing.T) {
	w := newWorld(t)
	c := w.crew(w.seedSite())
	c.Staged = w.tap
	first := []Line{{skuA, 10}}
	base1 := c.trackStaged(first)
	_, _, _, _ = received(t, c, first...)
	if err := c.awaitStaged(context.Background(), base1, first); err != nil {
		t.Fatalf("first delivery: %v", err)
	}
	// A second delivery of the same SKU: the first one's StockReceived must not satisfy its wait.
	second := []Line{{skuA, 7}}
	base2 := c.trackStaged(second)
	if base2[skuA] != 1 {
		t.Fatalf("baseline = %v, want the 1 event already seen for %s", base2, skuA)
	}
	if err := c.awaitStaged(context.Background(), base2, second); err == nil || !IsTimeout(err) {
		t.Fatalf("err = %v, want a timeout: nothing was staged since the baseline", err)
	}
	_, _, _, _ = received(t, c, second...)
	if err := c.awaitStaged(context.Background(), base2, second); err != nil {
		t.Fatalf("second delivery: %v", err)
	}
	// sim.EventTap.Track appends: tracking a ref twice would count every event twice.
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.tap.refs) != 1 {
		t.Errorf("tap tracks %v, want the SKU once", w.tap.refs)
	}
}

func TestWithoutATapNothingIsTrackedAndNothingIsAwaited(t *testing.T) {
	w := newWorld(t)
	c := w.crew(w.seedSite())
	base := c.trackStaged([]Line{{skuA, 1}})
	if base != nil {
		t.Errorf("baseline = %v, want nil without a tap", base)
	}
	if err := c.awaitStaged(context.Background(), base, []Line{{skuA, 1}}); err != nil {
		t.Errorf("awaitStaged without a tap = %v, want nil", err)
	}
}
