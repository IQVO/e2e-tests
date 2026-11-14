package inbound

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/claudioed/warehouse-e2e/internal/sim"
	"github.com/claudioed/warehouse-e2e/internal/sim/transport"
)

func mustDeliver(t *testing.T, c *Crew, lines ...Line) Delivery {
	t.Helper()
	d := c.Deliver(context.Background(), lines)
	if !d.OK() {
		t.Fatalf("delivery not OK: %+v", d)
	}
	return d
}

func TestDeliverRunsTheWholeChainInTheOrderTheRealServicesNeed(t *testing.T) {
	w := newWorld(t)
	site := w.seedSite()
	site.Bins = []string{binA}
	c := w.crew(site)
	d := mustDeliver(t, c, Line{skuA, 10}, Line{skuB, 5})

	if d.ASN == "" || d.AppointmentID == "" || d.Door != inDoorA || d.VisitID == "" || d.TrailerID == "" || d.ReceiptID == "" {
		t.Errorf("delivery ids = %+v", d)
	}
	if d.Received[skuA] != 10 || d.Received[skuB] != 5 {
		t.Errorf("received = %v", d.Received)
	}
	if got := d.StowedUnits(); got[skuA] != 10 || got[skuB] != 5 {
		t.Errorf("stowed = %v", got)
	}
	if !d.Released || !d.Completed {
		t.Errorf("released=%v completed=%v, want both", d.Released, d.Completed)
	}

	// The order is the contract: an appointment follows its ASN (dock-scheduling refuses an unknown one), the gate
	// follows the appointment, door work follows the docking move, the receipt follows door work, the stow follows the
	// close, and only the close lets the trailer out.
	order := []string{
		"inbound-receiving POST /asns",
		"dock-scheduling POST /appointments",
		"yard-management POST /visits",
		"yard-management POST /moves/{id}/completions",
		"yard-management POST /trailers/{id}/door-work-starts",
		"inbound-receiving POST /receipts",
		"inbound-receiving POST /receipts/{id}/lines",
		"inbound-receiving POST /receipts/{id}/closures",
		"inventory-storage POST /stowages",
		"yard-management POST /visits/{id}/check-outs",
		"dock-scheduling GET /appointments/{id}",
	}
	prev := -1
	for _, e := range order {
		i := w.first(e)
		if i < 0 {
			t.Fatalf("%s was never called", e)
		}
		if i <= prev {
			t.Errorf("%s came at %d, not after the previous step (%d)", e, i, prev)
		}
		prev = i
	}
	if w.last("inbound-receiving POST /receipts/{id}/closures") > w.first("inventory-storage POST /stowages") {
		t.Error("the stower started before the receipt was closed")
	}
	if w.last("inventory-storage POST /stowages") > w.first("yard-management POST /visits/{id}/check-outs") {
		t.Error("the truck was released before the goods were stowed")
	}
	if n := w.served("yard-management POST /visits/{id}/check-outs"); n != 1 {
		t.Errorf("%d check-outs, want 1", n)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.inv.usable(skuA) != 110 || w.inv.usable(skuB) != 105 || w.inv.usable(skuC) != 100 {
		t.Errorf("usable = %d/%d/%d, want 110/105/100", w.inv.usable(skuA), w.inv.usable(skuB), w.inv.usable(skuC))
	}
	if w.inv.staged[skuA] != 10 || w.inv.staged[skuB] != 5 {
		t.Errorf("staged = %v: every unit staged exactly once", w.inv.staged)
	}
}

func TestDeliverSurvivesEveryConsumerLagAtOnce(t *testing.T) {
	w := newWorld(t)
	w.lagASNToDock, w.lagDoorToDock, w.lagApptToYard, w.lagApptToReceiving = 2, 1, 2, 2
	w.lagUnloadReads, w.lagCompleteReads = 3, 3
	site := w.seedSite()
	w.recv.skuLag[skuA] = &lag{n: 2}
	w.dock.doorLag[inDoorA] = &lag{n: w.lagDoorToDock}
	c := w.crew(site)
	d := mustDeliver(t, c, Line{skuA, 10})
	if w.yard.refusedCheckouts != 0 {
		t.Errorf("the yard refused %d check-outs: lag is waited out by reading, not by trying the call", w.yard.refusedCheckouts)
	}
	if len(d.Problems) != 0 {
		t.Errorf("problems = %v", d.Problems)
	}
}

func TestDeliverOfNothingIsAFindingAndSendsNothing(t *testing.T) {
	w := newWorld(t)
	c := w.crew(w.seedSite())
	d := c.Deliver(context.Background(), []Line{{skuA, 0}})
	if d.OK() || len(d.Problems) == 0 || w.writes() != 0 {
		t.Errorf("delivery = %+v, writes = %d; want a finding and no write", d, w.writes())
	}
}

func TestDeliverStopsAtAnUnknownSKUBeforeBookingAnything(t *testing.T) {
	w := newWorld(t)
	c := w.crew(w.seedSite())
	d := c.Deliver(context.Background(), []Line{{"SKU-NOPE", 1}})
	if d.OK() || len(d.Problems) != 1 || !strings.Contains(d.Problems[0], "unknown-sku") {
		t.Fatalf("delivery = %+v, want one unknown-sku finding", d)
	}
	if d.AppointmentID != "" || w.served("dock-scheduling POST /appointments") != 0 {
		t.Error("a door was booked for an ASN that was never registered")
	}
}

func TestDeliverWithEveryDoorBusyKeepsTheASNAndStopsBeforeTheGate(t *testing.T) {
	w := newWorld(t)
	site := w.seedSite()
	c := w.crew(site)
	// Two other trucks hold both inbound doors for the window.
	win := c.NextWindow()
	for i, sku := range []string{skuB, skuC} {
		asn := registered(t, c, Line{sku, 1})
		if _, err := c.BookDoor(context.Background(), asn.Number, win); err != nil {
			t.Fatalf("booking %d: %v", i, err)
		}
	}
	d := c.Deliver(context.Background(), []Line{{skuA, 10}})
	if d.OK() || d.ASN == "" || d.AppointmentID != "" || d.VisitID != "" {
		t.Fatalf("delivery = %+v, want an ASN but no appointment and no visit", d)
	}
	if len(d.Problems) != 1 || !strings.Contains(d.Problems[0], "double-booked") {
		t.Errorf("problems = %v", d.Problems)
	}
}

func TestDeliverStopsAtARejectedVisit(t *testing.T) {
	w := newWorld(t)
	w.sealPolicyRejects = true
	site := w.seedSite()
	c := w.crew(site)
	w.with(func() { w.dock.storeSeal = func(string) string { return "SEAL-NOBODY-DECLARED" } }) // the booking recorded another seal than the carrier's
	d := c.Deliver(context.Background(), []Line{{skuA, 10}})
	if d.OK() || d.ReceiptID != "" || len(d.Problems) == 0 || !strings.Contains(d.Problems[0], "SEAL_MISMATCH") {
		t.Fatalf("delivery = %+v, want a SEAL_MISMATCH finding and no receipt", d)
	}
	if w.served("inbound-receiving POST /receipts") != 0 || w.served("inventory-storage POST /stowages") != 0 {
		t.Error("goods were received from a carrier the gate turned away")
	}
}

func TestDeliverThatCannotOpenTheReceiptLeavesTheTrailerDockedAndSaysSo(t *testing.T) {
	w := newWorld(t)
	w.lagApptToReceiving = 1 << 20
	c := w.crew(w.seedSite())
	d := c.Deliver(context.Background(), []Line{{skuA, 10}})
	if d.OK() || d.Released || d.ReceiptID != "" {
		t.Fatalf("delivery = %+v", d)
	}
	joined := strings.Join(d.Problems, " | ")
	for _, want := range []string{"unknown-appointment", "stays docked at door " + inDoorA} {
		if !strings.Contains(joined, want) {
			t.Errorf("problems %q lack %q", joined, want)
		}
	}
	if w.served("yard-management POST /visits/{id}/check-outs") != 0 {
		t.Error("the clerk tried to check out a trailer whose unload was never opened")
	}
}

func TestDeliverThatCannotStowStillReleasesTheTruckAndReportsWhatWasReceived(t *testing.T) {
	w := newWorld(t)
	site := w.seedSite()
	site.Bins = []string{binA}
	w.with(func() { w.inv.bins[binA].capacity = w.inv.bins[binA].occupied }) // full
	c := w.crew(site)
	d := c.Deliver(context.Background(), []Line{{skuA, 10}})
	if d.OK() || len(d.Stowed) != 0 || d.Received[skuA] != 10 {
		t.Fatalf("delivery = %+v, want 10 received and nothing stowed", d)
	}
	if !d.Released || !d.Completed {
		t.Errorf("released=%v completed=%v: a truck whose goods did not fit still leaves the dock", d.Released, d.Completed)
	}
	if len(d.Problems) != 1 || !strings.Contains(d.Problems[0], "only 0 of 10") {
		t.Errorf("problems = %v", d.Problems)
	}
}

func TestDeliverOfAnUnloadThatIsNeverReleasedReportsItAndDoesNotAwaitTheAppointment(t *testing.T) {
	w := newWorld(t)
	w.lagUnloadReads = 1 << 20
	c := w.crew(w.seedSite())
	d := c.Deliver(context.Background(), []Line{{skuA, 10}})
	if d.OK() || d.Released || d.Completed || len(d.Stowed) != 1 {
		t.Fatalf("delivery = %+v, want stowed but not released", d)
	}
	if w.served("dock-scheduling GET /appointments/{id}") != 0 {
		t.Error("the appointment was awaited although the truck never left")
	}
	if w.served("yard-management POST /visits/{id}/check-outs") != 0 {
		t.Error("a check-out was tried before the unload was released")
	}
}

func TestDeliverReportsADiscrepancyTheReceiptFound(t *testing.T) {
	w := newWorld(t)
	c := w.crew(w.seedSite())
	// The ASN was amended after the actor announced it: the receipt's snapshot expects one unit more than arrives.
	w.with(func() { w.recv.expectedDelta = 1 })
	d := c.Deliver(context.Background(), []Line{{skuA, 10}})
	if d.OK() {
		t.Fatalf("delivery OK with a Short receipt: %+v", d)
	}
	if !strings.Contains(strings.Join(d.Problems, "|"), "Short") {
		t.Errorf("problems = %v, want the Short discrepancy", d.Problems)
	}
	if d.Received[skuA] != 10 || d.StowedUnits()[skuA] != 10 {
		t.Errorf("received %v stowed %v, want the 10 units that arrived received and stowed", d.Received, d.StowedUnits())
	}
}

func TestDeliverWaitsForTheStagedHandoverBeforeItStows(t *testing.T) {
	w := newWorld(t)
	c := w.crew(w.seedSite())
	c.Staged = w.tap
	w.lagStagedSeen = 5
	d := mustDeliver(t, c, Line{skuA, 10})
	if d.StowedUnits()[skuA] != 10 {
		t.Errorf("stowed = %v", d.Stowed)
	}
}

func TestDeliverDoesNotStowWhenTheHandoverNeverReachesInventoryStorage(t *testing.T) {
	w := newWorld(t)
	c := w.crew(w.seedSite())
	c.Staged = w.tap
	w.lagStagedSeen = 1 << 20
	d := c.Deliver(context.Background(), []Line{{skuA, 10}})
	if d.OK() || len(d.Stowed) != 0 || w.served("inventory-storage POST /stowages") != 0 {
		t.Fatalf("delivery = %+v: stowing without the staged handover would hide a broken integration", d)
	}
	if !strings.Contains(strings.Join(d.Problems, "|"), "StockReceived") {
		t.Errorf("problems = %v, want the missing StockReceived named", d.Problems)
	}
	if !d.Released || !d.Completed {
		t.Errorf("released=%v completed=%v: the truck still leaves", d.Released, d.Completed)
	}
}

func TestDeliverWithACancelledContextIsAFindingNotAPanic(t *testing.T) {
	w := newWorld(t)
	c := w.crew(w.seedSite())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	d := c.Deliver(ctx, []Line{{skuA, 10}})
	if d.OK() || len(d.Problems) == 0 || w.writes() != 0 {
		t.Errorf("delivery = %+v writes = %d", d, w.writes())
	}
}

func TestDeliverAgainstServicesThatAnswerNonsenseIsAFindingNotAPanic(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		rw.WriteHeader(http.StatusCreated)
		_, _ = rw.Write([]byte(`["not","an","object"]`))
	}))
	t.Cleanup(srv.Close)
	base := map[string]string{SvcInbound: srv.URL, SvcDock: srv.URL, SvcYard: srv.URL, SvcInventory: srv.URL}
	c := &Crew{C: sim.NewClient(base, sim.NewCallStats()), Site: Site{Code: siteCode, Doors: []string{inDoorA}, Bins: []string{binA}}, RunID: "T1",
		Clock: transport.NewManualClock(t0), Wait: Waits{Timeout: time.Second, Interval: 100 * time.Millisecond}}
	d := c.Deliver(context.Background(), []Line{{skuA, 10}})
	if d.OK() || len(d.Problems) == 0 {
		t.Errorf("delivery = %+v", d)
	}
}

// Two trucks at once. Only what holds for EVERY interleaving is asserted: both complete, nothing is shared that
// must not be (ASN numbers, plates, units, keys), and the stock lands exactly once.
func TestTwoDeliveriesAtOnceDoNotShareWhatMustNotBeShared(t *testing.T) {
	w := newWorld(t)
	site := w.seedSite()
	site.Bins = []string{binA, binB}
	c := w.crew(site)
	var wg sync.WaitGroup
	out := make([]Delivery, 2)
	for i, sku := range []string{skuA, skuB} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out[i] = c.Deliver(context.Background(), []Line{{sku, 10 + i}})
		}()
	}
	wg.Wait()
	for i, d := range out {
		if !d.OK() {
			t.Fatalf("delivery %d not OK: %+v", i, d)
		}
	}
	if out[0].ASN == out[1].ASN || out[0].VisitID == out[1].VisitID || out[0].TrailerID == out[1].TrailerID || out[0].AppointmentID == out[1].AppointmentID {
		t.Errorf("two trucks shared an id: %+v / %+v", out[0], out[1])
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	plates := map[string]bool{}
	for _, v := range w.yard.visits {
		if plates[v.plate] {
			t.Errorf("plate %s checked in twice", v.plate)
		}
		plates[v.plate] = true
	}
	if w.inv.usable(skuA) != 110 || w.inv.usable(skuB) != 111 {
		t.Errorf("usable = %d/%d, want 110/111", w.inv.usable(skuA), w.inv.usable(skuB))
	}
}
