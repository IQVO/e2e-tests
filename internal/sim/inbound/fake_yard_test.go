package inbound

import (
	"strings"
	"time"
)

// ---------------------------------------------------------------------
// yard-management
// ---------------------------------------------------------------------

type loc struct{ kind, code string }

type trailerRec struct {
	id, site, unit, kind, loadState, state, doorWork string
	visit, appt, seal, sealState                     string
	asns                                             []string
	location                                         *loc
	arrivedAt                                        time.Time
	openMove                                         string
	unloadPending                                    bool
	unloadLag                                        *lag // GET reads that still show the unload open after ReceiptClosed
}

type visitRec struct {
	id, site, carrier, plate, purpose, appt, state, reason string
	trailers, out                                          []string
	arrivedAt                                              time.Time
}

type moveRec struct {
	id, site, trailer, appt, performer, reason, state, hostler string
	to                                                         loc
}

type fakeYard struct {
	*fake
	doors       map[string]string // door -> dockFlow
	spots       []string
	apptCopy    map[string]*apptCopy // yard-management's copy of dock-scheduling's appointments
	receiptAppt map[string]string    // ReceiptOpened: receipt id -> appointment id
	trailers    map[string]*trailerRec
	visits      map[string]*visitRec
	moves       map[string]*moveRec
	occupant    map[string]string // location code -> trailer id
	// refusedCheckouts counts check-outs the yard refused: an actor that WAITS for the release reads, it does not
	// try the check-out and retry the refusal.
	refusedCheckouts int
}

func newFakeYard(w *world) *fakeYard {
	f := &fakeYard{
		fake: newFake(w, SvcYard), doors: map[string]string{}, apptCopy: map[string]*apptCopy{}, receiptAppt: map[string]string{},
		trailers: map[string]*trailerRec{}, visits: map[string]*visitRec{}, moves: map[string]*moveRec{}, occupant: map[string]string{},
	}
	f.handle("POST", "/visits", f.checkIn)
	f.handle("GET", "/visits/{id}", func(c *fctx) reply {
		v := f.visits[c.params["id"]]
		if v == nil {
			return refuse(404, "visit-not-found")
		}
		return ok(f.visitJSON(v))
	})
	f.handle("POST", "/visits/{id}/check-outs", f.checkOut)
	f.handle("GET", "/trailers/{id}", func(c *fctx) reply {
		t := f.trailers[c.params["id"]]
		if t == nil {
			return refuse(404, "trailer-not-found")
		}
		f.readTrailer(t)
		return ok(f.trailerJSON(t))
	})
	f.handle("POST", "/trailers/{id}/door-work-starts", func(c *fctx) reply {
		t := f.trailers[c.params["id"]]
		if t == nil {
			return refuse(404, "trailer-not-found")
		}
		if t.state != "DOCKED" || t.doorWork != "WAITING" {
			return refuse(409, "illegal-transition")
		}
		t.doorWork = "IN_PROGRESS"
		return ok(f.trailerJSON(t))
	})
	f.handle("POST", "/trailers/{id}/door-work-completions", func(c *fctx) reply {
		t := f.trailers[c.params["id"]]
		if t == nil {
			return refuse(404, "trailer-not-found")
		}
		if t.state != "DOCKED" || t.doorWork != "IN_PROGRESS" {
			return refuse(409, "illegal-transition")
		}
		t.doorWork = "DONE"
		return ok(f.trailerJSON(t))
	})
	f.handle("POST", "/moves", f.requestMove)
	f.handle("POST", "/moves/claims", f.claimMove)
	f.handle("POST", "/moves/{id}/completions", f.completeMove)
	f.handle("GET", "/sites/{site}/yard-board", func(c *fctx) reply {
		site := c.params["site"]
		var spots, doors []any
		add := func(code, kind, flow string) {
			e := map[string]any{"code": code, "kind": kind, "closed": false, "trailer": nil}
			if flow != "" {
				e["dockFlow"] = flow
			}
			if id := f.occupant[code]; id != "" {
				e["trailer"] = f.trailerJSON(f.trailers[id])
			}
			spots = append(spots, e)
			if kind == "DOOR" {
				doors = append(doors, e)
			}
		}
		for _, d := range sortedKeys(f.doors) {
			if strings.HasPrefix(d, site+"-") {
				add(d, "DOOR", f.doors[d])
			}
		}
		for _, s := range f.spots {
			if strings.HasPrefix(s, site+"-") {
				add(s, "SPOT", "")
			}
		}
		if len(spots) == 0 {
			return refuse(404, "site-not-found")
		}
		return ok(map[string]any{"siteId": site, "asOf": f.w.clock.Now().Format(time.RFC3339), "spots": spots, "doors": doors, "inMove": []any{}, "openMoves": []any{}})
	})
	return f
}

func (f *fakeYard) trailerJSON(t *trailerRec) map[string]any {
	out := map[string]any{
		"trailerId": t.id, "siteId": t.site, "unitNumber": t.unit, "kind": t.kind, "loadState": t.loadState, "state": t.state,
		"seal": map[string]any{"number": t.seal, "state": t.sealState}, "asnNumbers": t.asns, "visitId": t.visit, "appointmentId": t.appt,
		"arrivedAt": t.arrivedAt.Format(time.RFC3339Nano), "version": 1, "location": nil, "doorWork": nil,
	}
	if t.location != nil {
		out["location"] = map[string]any{"kind": t.location.kind, "code": t.location.code}
	}
	if t.doorWork != "" {
		out["doorWork"] = t.doorWork
	}
	return out
}

func (f *fakeYard) visitJSON(v *visitRec) map[string]any {
	out := map[string]any{
		"visitId": v.id, "siteId": v.site, "carrier": v.carrier, "tractorPlate": v.plate, "purpose": v.purpose, "state": v.state,
		"trailersIn": append([]string{}, v.trailers...), "trailersOut": append([]string{}, v.out...), "arrivedAt": v.arrivedAt.Format(time.RFC3339Nano), "version": 1,
	}
	if v.appt != "" {
		out["appointmentId"] = v.appt
	}
	if v.reason != "" {
		out["rejectionReason"] = v.reason
	}
	return out
}

func (f *fakeYard) moveJSON(m *moveRec) map[string]any {
	out := map[string]any{
		"moveId": m.id, "siteId": m.site, "trailerId": m.trailer, "performer": m.performer, "reason": m.reason, "state": m.state,
		"to": map[string]any{"kind": m.to.kind, "code": m.to.code}, "priority": map[string]any{"band": 5}, "version": 1,
	}
	if m.appt != "" {
		out["appointmentId"] = m.appt
	}
	if m.hostler != "" {
		out["hostlerRef"] = m.hostler
	}
	return out
}

// readTrailer applies AutoCompleteUnload to a read: ReceiptClosed was consumed, door work completes once the lag
// is over (WAITING or IN_PROGRESS to DONE, load state EMPTY).
func (f *fakeYard) readTrailer(t *trailerRec) {
	if t.unloadPending && t.state == "DOCKED" && (t.doorWork == "WAITING" || t.doorWork == "IN_PROGRESS") && !t.unloadLag.refuse() {
		t.doorWork, t.loadState = "DONE", "EMPTY"
	}
}

// onReceiptClosed is ReceiptClosed reaching yard-management: when the trailer of the receipt's appointment is
// DOCKED and every ASN on the appointment has a closed receipt, the unload is released.
func (f *fakeYard) onReceiptClosed(receiptID string) {
	appt := f.receiptAppt[receiptID]
	cp := f.apptCopy[appt]
	if appt == "" || cp == nil {
		return
	}
	for _, n := range cp.asns {
		if !f.w.recv.closed[n] {
			return
		}
	}
	for _, t := range f.trailers {
		if t.appt == appt && t.state == "DOCKED" {
			t.unloadPending, t.unloadLag = true, &lag{n: f.w.lagUnloadReads}
		}
	}
}

func (f *fakeYard) freeSpot() string {
	for _, s := range f.spots {
		if f.occupant[s] == "" {
			return s
		}
	}
	return ""
}

func (f *fakeYard) checkIn(c *fctx) reply {
	m, bad := c.obj()
	if bad != nil {
		return *bad
	}
	site, carrier, plate, purpose, apptID := str(m["siteId"]), str(m["carrier"]), str(m["tractorPlate"]), str(m["purpose"]), str(m["appointmentId"])
	raw, _ := m["trailers"].([]any)
	validPurpose := map[string]bool{"DROP": true, "LIVE_UNLOAD": true, "LIVE_LOAD": true, "PICKUP": true, "BOBTAIL": true}
	if site == "" || carrier == "" || plate == "" || !validPurpose[purpose] {
		return refuse(422, "validation")
	}
	if scac := str(m["carrierScac"]); scac != "" && !scacRE.MatchString(scac) {
		return refuse(422, "validation")
	}
	for _, v := range f.visits {
		if v.plate == plate && v.state == "CHECKED_IN" {
			return refuse(409, "visit-already-open")
		}
	}
	type tin struct{ unit, kind, load, seal string }
	var ins []tin
	for _, r := range raw {
		t, _ := r.(map[string]any)
		in := tin{str(t["unitNumber"]), str(t["kind"]), str(t["loadState"]), str(t["sealNumber"])}
		if in.unit == "" || in.kind == "" || in.load == "" {
			return refuse(422, "validation")
		}
		ins = append(ins, in)
	}
	for _, t := range f.trailers {
		for _, in := range ins {
			if t.unit == in.unit && t.state != "DEPARTED" {
				return refuse(409, "trailer-already-on-site")
			}
		}
	}
	var appt *apptRec
	if apptID != "" {
		cp := f.apptCopy[apptID]
		if cp == nil || cp.lag.refuse() {
			return refuse(422, "unknown-appointment")
		}
		appt = f.w.dock.appts[apptID]
		if len(ins) != 1 {
			return refuse(422, "validation")
		}
		if _, known := f.doors[appt.door]; !known {
			return refuse(422, "unknown-location")
		}
	} else if f.w.appointmentsRequired {
		return refuse(409, "appointment-required")
	}

	v := &visitRec{id: f.w.id("vis"), site: site, carrier: carrier, plate: plate, purpose: purpose, appt: apptID, state: "CHECKED_IN", arrivedAt: f.w.clock.Now()}
	f.visits[v.id] = v
	if appt != nil && len(ins) == 1 && ins[0].seal != "" && appt.seal != "" && ins[0].seal != appt.seal && f.w.sealPolicyRejects {
		v.state, v.reason = "REJECTED", "SEAL_MISMATCH"
		return created(map[string]any{"visit": f.visitJSON(v), "trailers": []any{}, "moves": []any{}})
	}
	var trailers, moves []any
	for _, in := range ins {
		t := &trailerRec{
			id: f.w.id("trl"), site: site, unit: in.unit, kind: in.kind, loadState: in.load, state: "IN_MOVE", visit: v.id, appt: apptID,
			seal: in.seal, sealState: "INTACT", arrivedAt: f.w.clock.Now(),
		}
		if appt != nil {
			t.asns = appt.asns
			if in.seal != "" && appt.seal != "" && in.seal != appt.seal {
				t.sealState = "MISMATCH"
			}
		}
		f.trailers[t.id] = t
		v.trailers = append(v.trailers, t.id)
		mv := &moveRec{id: f.w.id("mv"), site: site, trailer: t.id, appt: apptID, performer: "VISITING_DRIVER", state: "CLAIMED"}
		t.openMove = mv.id
		switch {
		case appt != nil && f.occupant[appt.door] == "" && f.doors[appt.door] != "Outbound" &&
			appt.start.Sub(f.w.clock.Now()) <= time.Duration(f.w.toleranceMinutes)*time.Minute:
			mv.reason, mv.to = "TO_DOOR", loc{"DOOR", appt.door}
		case f.freeSpot() != "":
			mv.reason, mv.to = "TO_HOLDING", loc{"SPOT", f.freeSpot()}
		default:
			return refuse(409, "no-free-spot")
		}
		f.moves[mv.id] = mv
		trailers, moves = append(trailers, f.trailerJSON(t)), append(moves, f.moveJSON(mv))
	}
	if appt != nil {
		appt.state = "CHECKED_IN" // VisitCheckedIn
	}
	return created(map[string]any{"visit": f.visitJSON(v), "trailers": trailers, "moves": moves})
}

func (f *fakeYard) completeMove(c *fctx) reply {
	mv := f.moves[c.params["id"]]
	if mv == nil {
		return refuse(404, "move-not-found")
	}
	m, bad := c.obj()
	if bad != nil {
		return *bad
	}
	if mv.state != "CLAIMED" {
		return refuse(409, "illegal-transition")
	}
	if mv.performer == "HOSTLER" && str(m["hostlerRef"]) != mv.hostler {
		return refuse(409, "illegal-transition")
	}
	t := f.trailers[mv.trailer]
	if holder := f.occupant[mv.to.code]; holder != "" && holder != t.id {
		return refuse(409, "place-occupied")
	}
	if t.location != nil && f.occupant[t.location.code] == t.id {
		delete(f.occupant, t.location.code)
	}
	mv.state, t.openMove = "COMPLETED", ""
	t.location = &loc{mv.to.kind, mv.to.code}
	f.occupant[mv.to.code] = t.id
	if mv.to.kind == "DOOR" {
		t.state, t.doorWork = "DOCKED", "WAITING" // TrailerDocked
	} else {
		t.state = "PARKED"
	}
	return ok(f.moveJSON(mv))
}

func (f *fakeYard) requestMove(c *fctx) reply {
	m, bad := c.obj()
	if bad != nil {
		return *bad
	}
	t := f.trailers[str(m["trailerId"])]
	if t == nil {
		return refuse(404, "trailer-not-found")
	}
	if t.openMove != "" {
		return refuse(409, "trailer-has-open-move")
	}
	to, _ := m["to"].(map[string]any)
	dest := loc{str(to["kind"]), str(to["code"])}
	if _, door := f.doors[dest.code]; dest.kind != "DOOR" || !door {
		return refuse(422, "unknown-location")
	}
	mv := &moveRec{id: f.w.id("mv"), site: t.site, trailer: t.id, appt: t.appt, performer: "HOSTLER", reason: str(m["reason"]), state: "REQUESTED", to: dest}
	t.openMove = mv.id
	f.moves[mv.id] = mv
	return created(f.moveJSON(mv))
}

func (f *fakeYard) claimMove(c *fctx) reply {
	m, bad := c.obj()
	if bad != nil {
		return *bad
	}
	site, hostler := str(m["siteId"]), str(m["hostlerRef"])
	var pick *moveRec
	for _, id := range sortedKeys(f.moves) {
		mv := f.moves[id]
		if mv.site == site && mv.performer == "HOSTLER" && mv.state == "REQUESTED" {
			pick = mv
			break
		}
	}
	if pick == nil {
		return refuse(404, "no-claimable-move")
	}
	pick.state, pick.hostler = "CLAIMED", hostler
	t := f.trailers[pick.trailer]
	t.state = "IN_MOVE"
	return created(f.moveJSON(pick))
}

func (f *fakeYard) checkOut(c *fctx) reply {
	v := f.visits[c.params["id"]]
	if v == nil {
		return refuse(404, "visit-not-found")
	}
	m, bad := c.obj()
	if bad != nil {
		return *bad
	}
	if v.state != "CHECKED_IN" {
		return refuse(409, "illegal-transition")
	}
	var out []string
	raw, _ := m["trailersOut"].([]any)
	for _, r := range raw {
		id := str(r.(map[string]any)["trailerId"])
		t := f.trailers[id]
		if t == nil || t.visit != v.id {
			return refuse(422, "validation")
		}
		f.readTrailer(t)
		released := t.openMove == "" && (t.state == "PARKED" || (t.state == "DOCKED" && t.doorWork == "DONE"))
		if !released {
			f.refusedCheckouts++
			return refuse(409, "trailer-not-released")
		}
		out = append(out, id)
	}
	for _, id := range out {
		t := f.trailers[id]
		if t.location != nil && f.occupant[t.location.code] == id {
			delete(f.occupant, t.location.code)
		}
		t.state, t.doorWork = "DEPARTED", ""
	}
	v.state, v.out = "CHECKED_OUT", out
	if a := f.w.dock.appts[v.appt]; a != nil {
		a.departed = true // TrailerDeparted: dock-scheduling completes the appointment after its lag
	}
	return ok(f.visitJSON(v))
}
