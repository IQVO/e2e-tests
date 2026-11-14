package inbound

import (
	"regexp"
	"strings"
	"time"
)

// ---------------------------------------------------------------------
// dock-scheduling
// ---------------------------------------------------------------------

type calendar struct {
	version                            int
	grid, maxWindow, tolerance, noShow int
}

type apptRec struct {
	id, site, dir, door, carrier, scac, seal, state string
	start, end                                      time.Time
	asns                                            []string
	completeLag                                     *lag // reads that still show CHECKED_IN after the yard check-out
	departed                                        bool
}

type fakeDock struct {
	*fake
	doors    map[string]string // door -> dockFlow
	doorLag  map[string]*lag
	asnLag   map[string]*lag // ASN -> refusals left (unknown-asn) while dock-scheduling has not consumed ASNRegistered
	calendar map[string]*calendar
	appts    map[string]*apptRec
}

var scacRE = regexp.MustCompile(`^[A-Z]{2,4}$`)

func newFakeDock(w *world) *fakeDock {
	f := &fakeDock{
		fake: newFake(w, SvcDock), doors: map[string]string{}, doorLag: map[string]*lag{}, asnLag: map[string]*lag{},
		calendar: map[string]*calendar{}, appts: map[string]*apptRec{},
	}
	f.handle("POST", "/appointments", f.book)
	f.handle("GET", "/appointments/{id}", func(c *fctx) reply {
		a := f.appts[c.params["id"]]
		if a == nil {
			return refuse(404, "appointment-not-found")
		}
		if a.departed && a.state == "CHECKED_IN" && !a.completeLag.refuse() {
			a.state = "COMPLETED"
		}
		return ok(f.apptJSON(a))
	})
	f.handle("GET", "/sites/{site}/dock-calendar", func(c *fctx) reply {
		cal := f.calendar[c.params["site"]]
		if cal == nil {
			// A never-declared site answers the defaults at version 0.
			cal = &calendar{grid: 15, maxWindow: 240, tolerance: 15, noShow: 60}
		}
		return ok(map[string]any{
			"siteId": c.params["site"], "version": cal.version,
			"settings":       map[string]any{"maxWindowMinutes": cal.maxWindow, "slotGranularityMinutes": cal.grid, "arrivalToleranceMinutes": cal.tolerance, "noShowGraceMinutes": cal.noShow},
			"operatingHours": []any{}, "closures": []any{},
		})
	})
	f.handle("PUT", "/sites/{site}/dock-calendar/settings", func(c *fctx) reply {
		m, bad := c.obj()
		if bad != nil {
			return *bad
		}
		cal := f.calendar[c.params["site"]]
		if cal == nil {
			cal = &calendar{}
			f.calendar[c.params["site"]] = cal
		}
		cal.version++
		cal.maxWindow, _ = intOf(m["maxWindowMinutes"])
		cal.grid, _ = intOf(m["slotGranularityMinutes"])
		cal.tolerance, _ = intOf(m["arrivalToleranceMinutes"])
		cal.noShow, _ = intOf(m["noShowGraceMinutes"])
		return ok(map[string]any{"siteId": c.params["site"], "version": cal.version})
	})
	return f
}

func (f *fakeDock) apptJSON(a *apptRec) map[string]any {
	out := map[string]any{
		"appointmentId": a.id, "siteId": a.site, "direction": a.dir, "doorCode": a.door, "carrier": a.carrier,
		"windowStart": a.start.Format(time.RFC3339), "windowEnd": a.end.Format(time.RFC3339), "asnNumbers": a.asns,
		"provenance": "native", "state": a.state, "version": 1, "atRisk": false,
	}
	if a.scac != "" {
		out["carrierScac"] = a.scac
	}
	if a.seal != "" {
		out["expectedSealNumber"] = a.seal
	}
	return out
}

func (f *fakeDock) book(c *fctx) reply {
	m, bad := c.obj()
	if bad != nil {
		return *bad
	}
	dir, _ := m["direction"].(string)
	door, _ := m["doorCode"].(string)
	carrier, _ := m["carrier"].(string)
	scac, _ := m["carrierScac"].(string)
	seal, _ := m["expectedSealNumber"].(string)
	ws, err1 := time.Parse(time.RFC3339, str(m["windowStart"]))
	we, err2 := time.Parse(time.RFC3339, str(m["windowEnd"]))
	var asns []string
	if raw, ok := m["asnNumbers"].([]any); ok {
		for _, r := range raw {
			asns = append(asns, str(r))
		}
	}
	if dir != "INBOUND" && dir != "OUTBOUND" || door == "" || strings.TrimSpace(carrier) == "" || err1 != nil || err2 != nil {
		return refuse(422, "invalid-window")
	}
	if scac != "" && !scacRE.MatchString(scac) {
		return refuse(422, "invalid-scac")
	}
	flow, known := f.doors[door]
	if !known || f.doorLag[door].refuse() {
		return refuse(422, "unknown-door")
	}
	if dir == "INBOUND" && flow == "Outbound" {
		return refuse(422, "door-flow-mismatch")
	}
	site := strings.SplitN(door, "-", 2)[0]
	cal := f.calendar[site]
	if cal == nil {
		cal = &calendar{grid: 15, maxWindow: 240}
	}
	if ws.Before(f.w.clock.Now()) {
		return refuse(422, "window-in-past")
	}
	onGrid := func(t time.Time) bool { return t.Second() == 0 && t.Minute()%cal.grid == 0 }
	if !onGrid(ws) || !onGrid(we) || !we.After(ws) || we.Sub(ws) > time.Duration(cal.maxWindow)*time.Minute {
		return refuse(422, "invalid-window")
	}
	if dir == "INBOUND" {
		if len(asns) == 0 {
			return refuse(422, "unknown-asn")
		}
		for _, n := range asns {
			if f.w.recv.asns[n] == nil || f.asnLag[n].refuse() {
				return refuse(422, "unknown-asn")
			}
		}
	}
	for _, o := range f.appts {
		if o.door == door && (o.state == "BOOKED" || o.state == "CHECKED_IN") && ws.Before(o.end) && o.start.Before(we) {
			return refuse(409, "door-double-booked")
		}
	}
	for _, o := range f.appts {
		if o.state != "BOOKED" && o.state != "CHECKED_IN" {
			continue
		}
		for _, n := range asns {
			for _, on := range o.asns {
				if on == n {
					return refuse(409, "asn-already-scheduled")
				}
			}
		}
	}
	a := &apptRec{
		id: f.w.id("appt"), site: site, dir: dir, door: door, carrier: carrier, scac: scac, seal: seal, state: "BOOKED",
		start: ws, end: we, asns: asns, completeLag: &lag{n: f.w.lagCompleteReads},
	}
	f.appts[a.id] = a
	// DockAppointmentBooked: yard-management and inbound-receiving copy the appointment after their lag.
	f.w.yard.apptCopy[a.id] = &apptCopy{asns: asns, lag: &lag{n: f.w.lagApptToYard}}
	f.w.recv.apptCopy[a.id] = &apptCopy{asns: asns, lag: &lag{n: f.w.lagApptToReceiving}}
	return created(f.apptJSON(a))
}

func str(v any) string { s, _ := v.(string); return s }
