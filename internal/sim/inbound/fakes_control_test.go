package inbound

// Control tests: the fakes are only worth something if they refuse what the real services refuse. Each test here
// drives a fake with raw requests, no actor, and proves one rule an actor test relies on can actually fail.

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/claudioed/warehouse-e2e/internal/sim"
	"github.com/claudioed/warehouse-e2e/internal/sim/outbound"
)

func (w *world) do(svc, method, route, path string, body any, extra map[string]string) (int, string) {
	r := w.client.CallWith(context.Background(), svc, method, route, path, body, extra)
	return r.Status, outbound.Slug(r)
}

func (w *world) wantProblem(svc, method, path string, body any, status int, slug string) {
	w.t.Helper()
	if got, gotSlug := w.do(svc, method, path, path, body, nil); got != status || gotSlug != slug {
		w.t.Errorf("%s %s = %d %q, want %d %q", method, path, got, gotSlug, status, slug)
	}
}

func TestControlAPostWithoutAnIdempotencyKeyIsRefused(t *testing.T) {
	w := newWorld(t)
	w.seedSite()
	body := map[string]any{"asnNumber": "ASN-X", "supplierRef": "S", "lines": []map[string]any{{"lineNo": 1, "sku": skuA, "expectedQty": 1}}}
	status, slug := w.do(SvcInbound, http.MethodPost, "/asns", "/asns", body, map[string]string{"Idempotency-Key": ""})
	if status != 400 || slug != "idempotency-key-required" {
		t.Errorf("POST without a key = %d %q, want 400 idempotency-key-required", status, slug)
	}
	// the world's cleanup flags the keyless POST: forgive this one, it is the point of the test
	w.mu.Lock()
	w.posts = nil
	w.mu.Unlock()
}

func TestControlARefusalIsStoredUnderItsKeySoReSendingTheKeyNeverRecovers(t *testing.T) {
	w := newWorld(t)
	w.seedSite()
	w.recv.skuLag[skuA] = &lag{n: 1}
	body := map[string]any{"asnNumber": "ASN-X", "supplierRef": "S", "lines": []map[string]any{{"lineNo": 1, "sku": skuA, "expectedQty": 1}}}
	key := map[string]string{"Idempotency-Key": "k-1"}
	if st, slug := w.do(SvcInbound, http.MethodPost, "/asns", "/asns", body, key); st != 422 || slug != "unknown-sku" {
		t.Fatalf("first = %d %q", st, slug)
	}
	// The lag is over, but the same key replays the stored refusal.
	if st, slug := w.do(SvcInbound, http.MethodPost, "/asns", "/asns", body, key); st != 422 || slug != "unknown-sku" {
		t.Errorf("same key again = %d %q, want the stored 422 unknown-sku replayed", st, slug)
	}
	if st, _ := w.do(SvcInbound, http.MethodPost, "/asns", "/asns", body, map[string]string{"Idempotency-Key": "k-2"}); st != 201 {
		t.Errorf("a fresh key = %d, want 201", st)
	}
	// A key sent twice is exactly what the world's cleanup flags; forgive this deliberate one.
	w.mu.Lock()
	w.posts = nil
	w.mu.Unlock()
}

func TestControlTheSameKeyWithAnotherBodyIsRefused(t *testing.T) {
	w := newWorld(t)
	w.seedSite()
	mk := func(asn string) map[string]any {
		return map[string]any{"asnNumber": asn, "supplierRef": "S", "lines": []map[string]any{{"lineNo": 1, "sku": skuA, "expectedQty": 1}}}
	}
	key := map[string]string{"Idempotency-Key": "k-1"}
	w.do(SvcInbound, http.MethodPost, "/asns", "/asns", mk("ASN-A"), key)
	if st, slug := w.do(SvcInbound, http.MethodPost, "/asns", "/asns", mk("ASN-B"), key); st != 409 || slug != "idempotency-key-reused" {
		t.Errorf("= %d %q, want 409 idempotency-key-reused", st, slug)
	}
	w.mu.Lock()
	w.posts = nil
	w.mu.Unlock()
}

func TestControlTheYardRefusesACheckOutBeforeTheReceiptReleasedTheUnload(t *testing.T) {
	w := newWorld(t)
	c := w.crew(w.seedSite())
	_, _, v := docked(t, c, Line{skuA, 10})
	w.wantProblem(SvcYard, http.MethodPost, "/visits/"+v.ID+"/check-outs",
		map[string]any{"trailersOut": []map[string]any{{"trailerId": v.TrailerID}}}, 409, "trailer-not-released")
}

func TestControlDockSchedulingRefusesWhatItsContractRefuses(t *testing.T) {
	w := newWorld(t)
	site := w.seedSite()
	c := w.crew(site)
	asn := registered(t, c, Line{skuA, 1})
	win := c.NextWindow()
	book := func(door string, asns []string, ws, we time.Time) map[string]any {
		return map[string]any{
			"direction": "INBOUND", "doorCode": door, "carrier": "ACME", "carrierScac": "ACMF", "asnNumbers": asns,
			"windowStart": ws.Format(time.RFC3339), "windowEnd": we.Format(time.RFC3339),
		}
	}
	w.wantProblem(SvcDock, http.MethodPost, "/appointments", book(inDoorA, []string{"ASN-NOPE"}, win.Start, win.End), 422, "unknown-asn")
	w.wantProblem(SvcDock, http.MethodPost, "/appointments", book("AUS3-IB-DR-NOPE-01-01-A", []string{asn.Number}, win.Start, win.End), 422, "unknown-door")
	w.wantProblem(SvcDock, http.MethodPost, "/appointments", book(outDoor, []string{asn.Number}, win.Start, win.End), 422, "door-flow-mismatch")
	w.wantProblem(SvcDock, http.MethodPost, "/appointments", book(inDoorA, []string{asn.Number}, win.Start.Add(-time.Hour), win.End.Add(-time.Hour)), 422, "window-in-past")
	w.wantProblem(SvcDock, http.MethodPost, "/appointments", book(inDoorA, []string{asn.Number}, win.Start.Add(time.Minute), win.End.Add(time.Minute)), 422, "invalid-window") // off the 5 minute grid
	w.wantProblem(SvcDock, http.MethodPost, "/appointments", book(inDoorA, []string{asn.Number}, win.Start, win.Start.Add(5*time.Hour)), 422, "invalid-window")                // over the 4 h maximum

	if _, err := c.BookDoor(context.Background(), asn.Number, win); err != nil {
		t.Fatal(err)
	}
	other := registered(t, c, Line{skuB, 1})
	w.wantProblem(SvcDock, http.MethodPost, "/appointments", book(inDoorA, []string{other.Number}, win.Start, win.End), 409, "door-double-booked")
	w.wantProblem(SvcDock, http.MethodPost, "/appointments", book(inDoorB, []string{asn.Number}, win.Start, win.End), 409, "asn-already-scheduled")
}

func TestControlInventoryStorageRefusesWhatItsContractRefuses(t *testing.T) {
	w := newWorld(t)
	w.seedSite()
	w.with(func() {
		w.inv.class["SKU-HAZ"] = "HAZ"
		w.inv.bins[binB].capacity = 10
	})
	stow := func(sku, bin string, qty int) map[string]any {
		return map[string]any{"sku": sku, "quantity": qty, "binId": bin}
	}
	w.wantProblem(SvcInventory, http.MethodPost, "/stowages", stow(skuA, "AUS3-NOPE", 1), 404, "bin-not-found")
	w.wantProblem(SvcInventory, http.MethodPost, "/stowages", stow(skuA, binB, 11), 409, "bin-full")
	w.wantProblem(SvcInventory, http.MethodPost, "/stowages", stow("SKU-HAZ", binA, 1), 409, "hazmat-zone-required")
	w.wantProblem(SvcInventory, http.MethodPost, "/stowages", stow(skuA, binA, 0), 422, "invalid-quantity")
	w.wantProblem(SvcInventory, http.MethodPost, "/stowages", stow("", binA, 1), 400, "stow-requires-item-and-location")
}

func TestControlInboundReceivingRefusesWhatItsContractRefuses(t *testing.T) {
	w := newWorld(t)
	c := w.crew(w.seedSite())
	asn, a, _ := docked(t, c, Line{skuA, 10})
	other := registered(t, c, Line{skuB, 1})
	w.wantProblem(SvcInbound, http.MethodPost, "/receipts", map[string]any{"asnNumber": "ASN-NOPE"}, 422, "unknown-asn")
	w.wantProblem(SvcInbound, http.MethodPost, "/receipts", map[string]any{"asnNumber": other.Number, "appointmentId": a.ID}, 422, "asn-not-on-appointment")
	w.wantProblem(SvcInbound, http.MethodPost, "/receipts", map[string]any{"asnNumber": asn.Number, "appointmentId": "appt-NOPE"}, 422, "unknown-appointment")
	rc, err := c.OpenReceipt(context.Background(), asn, a)
	if err != nil {
		t.Fatal(err)
	}
	w.wantProblem(SvcInbound, http.MethodPost, "/receipts", map[string]any{"asnNumber": asn.Number, "appointmentId": a.ID}, 409, "receipt-already-open")
	w.wantProblem(SvcInbound, http.MethodPost, "/receipts/"+rc.ID+"/lines", map[string]any{"lineNo": 9, "quantity": 1, "condition": "Good"}, 422, "line-not-on-asn")
	w.wantProblem(SvcInbound, http.MethodPost, "/receipts/"+rc.ID+"/lines", map[string]any{"lineNo": 1, "quantity": 1, "condition": "Fine"}, 400, "invalid-condition")
	if _, err := c.CloseReceipt(context.Background(), rc); err != nil {
		t.Fatal(err)
	}
	w.wantProblem(SvcInbound, http.MethodPost, "/receipts/"+rc.ID+"/lines", map[string]any{"lineNo": 1, "quantity": 1, "condition": "Good"}, 409, "receipt-closed")
	w.wantProblem(SvcInbound, http.MethodPost, "/receipts", map[string]any{"asnNumber": asn.Number}, 409, "asn-not-receivable")
}

func TestControlAPageAboveTheServicesMaximumIsRefused(t *testing.T) {
	w := newWorld(t)
	w.seedSite()
	if st, slug := w.do(SvcInbound, http.MethodGet, "/docks", "/docks?limit=501", nil, nil); st != 400 || slug != "invalid-limit" {
		t.Errorf("limit=501 = %d %q, want 400 invalid-limit", st, slug)
	}
	// sim.ListAll asks for 500, the maximum of /docks: it must be accepted.
	if _, r := sim.ListAll[map[string]any](context.Background(), w.client, SvcInbound, "/docks", "/docks"); !r.OK() {
		t.Errorf("ListAll /docks = %d: the 500 it sends is the contract's maximum", r.Status)
	}
}

func TestControlTheYardRefusesAHostlerMoveCompletedUnderAnotherRef(t *testing.T) {
	w := newWorld(t)
	w.toleranceMinutes = 1
	c := w.crew(w.seedSite())
	_, a := booked(t, c, Line{skuA, 10})
	// Check in by hand, park, request and claim as hostler H1, then complete as H2.
	status, _ := w.do(SvcYard, http.MethodPost, "/visits", "/visits", map[string]any{
		"siteId": siteCode, "carrier": "ACME", "tractorPlate": "P-1", "purpose": "LIVE_UNLOAD", "appointmentId": a.ID,
		"trailers": []map[string]any{{"unitNumber": "U-1", "kind": "DRY_VAN", "loadState": "LOADED"}},
	}, nil)
	if status != 201 {
		t.Fatalf("check-in = %d", status)
	}
	w.mu.Lock()
	var trailer, drvMove string
	for id, m := range w.yard.moves {
		trailer, drvMove = m.trailer, id
	}
	w.mu.Unlock()
	w.do(SvcYard, http.MethodPost, "/moves/{id}/completions", "/moves/"+drvMove+"/completions", map[string]any{}, nil)
	w.do(SvcYard, http.MethodPost, "/moves", "/moves", map[string]any{"trailerId": trailer, "to": map[string]any{"kind": "DOOR", "code": a.Door}, "reason": "TO_DOOR"}, nil)
	r := w.client.Call(context.Background(), SvcYard, http.MethodPost, "/moves/claims", "/moves/claims", map[string]any{"siteId": siteCode, "hostlerRef": "H1"})
	var mv moveDTO
	if err := r.Decode(&mv); err != nil || mv.State != "CLAIMED" {
		t.Fatalf("claim = %d %s", r.Status, r.Body)
	}
	w.wantProblem(SvcYard, http.MethodPost, "/moves/"+mv.MoveID+"/completions", map[string]any{"hostlerRef": "H2"}, 409, "illegal-transition")
}
