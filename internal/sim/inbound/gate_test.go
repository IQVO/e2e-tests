package inbound

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

// booked registers an ASN for the lines and books its door.
func booked(t *testing.T, c *Crew, lines ...Line) (ASN, Appointment) {
	t.Helper()
	asn := registered(t, c, lines...)
	a, err := c.BookDoor(context.Background(), asn.Number, c.NextWindow())
	if err != nil {
		t.Fatalf("BookDoor: %v", err)
	}
	return asn, a
}

func TestGateInChecksTheCarrierInDocksTheTrailerAndStartsDoorWork(t *testing.T) {
	w := newWorld(t)
	c := w.crew(w.seedSite())
	asn, a := booked(t, c, Line{skuA, 10})
	v, err := c.GateIn(context.Background(), a)
	if err != nil {
		t.Fatalf("GateIn: %v", err)
	}
	if v.ID == "" || v.TrailerID == "" || v.AppointmentID != a.ID || v.Door != a.Door || v.MoveID == "" {
		t.Fatalf("visit = %+v", v)
	}
	// Run-scoped, and namespaced apart from the outbound crew's plates and units, which share the run id.
	if !strings.HasPrefix(v.Plate, "T1-IB-TRK-") || !strings.HasPrefix(v.Unit, "T1-IB-TRL-") {
		t.Errorf("plate %q / unit %q are not inbound-scoped", v.Plate, v.Unit)
	}

	var body map[string]any
	bodies := w.bodies("yard-management POST /visits")
	if err := json.Unmarshal(bodies[len(bodies)-1], &body); err != nil {
		t.Fatal(err)
	}
	trailers, _ := body["trailers"].([]any)
	tr, _ := trailers[0].(map[string]any)
	if body["purpose"] != "LIVE_UNLOAD" || body["appointmentId"] != a.ID || body["siteId"] != siteCode || len(trailers) != 1 ||
		tr["loadState"] != "LOADED" || tr["sealNumber"] != sealOf(asn.Number) || tr["kind"] != "DRY_VAN" {
		t.Errorf("check-in body = %v, want a LIVE_UNLOAD of one LOADED trailer carrying the booked seal", body)
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	trl := w.yard.trailers[v.TrailerID]
	if trl.state != "DOCKED" || trl.location == nil || trl.location.code != a.Door || trl.doorWork != "IN_PROGRESS" {
		t.Errorf("trailer = %+v, want DOCKED at %s with door work IN_PROGRESS", trl, a.Door)
	}
	if trl.sealState == "MISMATCH" {
		t.Error("the gate recorded a seal mismatch: the check-in must carry the seal the booking declared")
	}
	if w.dock.appts[a.ID].state != "CHECKED_IN" {
		t.Errorf("appointment is %s, want CHECKED_IN", w.dock.appts[a.ID].state)
	}
}

func TestGateInRetriesTheYardLagOfTheAppointment(t *testing.T) {
	w := newWorld(t)
	w.lagApptToYard = 3
	c := w.crew(w.seedSite())
	_, a := booked(t, c, Line{skuA, 10})
	if _, err := c.GateIn(context.Background(), a); err != nil {
		t.Fatalf("GateIn: %v", err)
	}
	if n := w.served("yard-management POST /visits"); n != 4 {
		t.Errorf("%d check-ins, want 4 (3 unknown-appointment then the visit)", n)
	}
}

func TestGateInOfAnAppointmentNeverConsumedTimesOut(t *testing.T) {
	w := newWorld(t)
	w.lagApptToYard = 1 << 20
	c := w.crew(w.seedSite())
	_, a := booked(t, c, Line{skuA, 10})
	_, err := c.GateIn(context.Background(), a)
	if err == nil || !IsTimeout(err) || !strings.Contains(err.Error(), "unknown-appointment") {
		t.Fatalf("err = %v, want a timeout naming unknown-appointment", err)
	}
	if n := w.served("yard-management POST /moves/{id}/completions"); n != 0 {
		t.Errorf("%d moves completed for a visit that never checked in", n)
	}
}

func TestGateInOfARejectedVisitIsAFindingAndMovesNothing(t *testing.T) {
	w := newWorld(t)
	w.sealPolicyRejects = true
	c := w.crew(w.seedSite())
	_, a := booked(t, c, Line{skuA, 10})
	a.Seal = "SEAL-OF-ANOTHER-TRUCK" // the carrier arrives with a seal the booking did not declare
	_, err := c.GateIn(context.Background(), a)
	if err == nil || !strings.Contains(err.Error(), "REJECTED") || !strings.Contains(err.Error(), "SEAL_MISMATCH") {
		t.Fatalf("err = %v, want the REJECTED visit and its SEAL_MISMATCH reason", err)
	}
	if n := w.served("yard-management POST /moves/{id}/completions") + w.served("yard-management POST /trailers/{id}/door-work-starts"); n != 0 {
		t.Errorf("%d yard steps after a rejected visit, want 0", n)
	}
}

func TestGateInOfAnEarlyCarrierIsParkedThenBroughtToTheDoorByAHostler(t *testing.T) {
	w := newWorld(t)
	w.toleranceMinutes = 1 // the window starts a few minutes ahead: earlier than the arrival tolerance
	c := w.crew(w.seedSite())
	_, a := booked(t, c, Line{skuA, 10})
	v, err := c.GateIn(context.Background(), a)
	if err != nil {
		t.Fatalf("GateIn: %v", err)
	}
	if n := w.served("yard-management POST /moves"); n != 1 {
		t.Errorf("%d hostler moves requested, want 1", n)
	}
	if n := w.served("yard-management POST /moves/claims"); n != 1 {
		t.Errorf("%d claims, want 1", n)
	}
	if n := w.served("yard-management POST /moves/{id}/completions"); n != 2 {
		t.Errorf("%d completions, want 2 (the driver's holding move, the hostler's door move)", n)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	trl := w.yard.trailers[v.TrailerID]
	if trl.state != "DOCKED" || trl.location.code != a.Door || trl.doorWork != "IN_PROGRESS" {
		t.Errorf("trailer = %+v, want DOCKED at the door with door work IN_PROGRESS", trl)
	}
	if mv := w.yard.moves[v.MoveID]; mv == nil || mv.performer != "HOSTLER" || mv.hostler != "T1-IB-HOSTLER" {
		t.Errorf("final move = %+v, want the hostler's, claimed as T1-IB-HOSTLER", mv)
	}
	if w.yard.occupant[spotA] != "" {
		t.Errorf("the holding spot still holds %s", w.yard.occupant[spotA])
	}
}

func TestGateInNeverCompletesAMoveThatIsNotItsOwnTrailers(t *testing.T) {
	w := newWorld(t)
	w.toleranceMinutes = 1
	site := w.seedSite()
	// Another visit's hostler move waits earlier in the queue: the claim is pull dispatch, the yard hands it out.
	w.with(func() {
		w.yard.trailers["trl-foreign"] = &trailerRec{id: "trl-foreign", site: siteCode, unit: "OTHER", state: "PARKED", location: &loc{"SPOT", spotB}, openMove: "mv-0000"}
		w.yard.occupant[spotB] = "trl-foreign"
		w.yard.moves["mv-0000"] = &moveRec{id: "mv-0000", site: siteCode, trailer: "trl-foreign", performer: "HOSTLER", reason: "TO_DOOR", state: "REQUESTED", to: loc{"DOOR", inDoorB}}
	})
	c := w.crew(site)
	_, a := booked(t, c, Line{skuA, 10})
	_, err := c.GateIn(context.Background(), a)
	if err == nil || !strings.Contains(err.Error(), "not ours") || !strings.Contains(err.Error(), "mv-0000") {
		t.Fatalf("err = %v, want the foreign claim reported", err)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if mv := w.yard.moves["mv-0000"]; mv.state != "CLAIMED" {
		t.Errorf("the foreign move is %s, want it left CLAIMED and not completed", mv.state)
	}
}
