package transport

// Fake of last-mile-delivery (apis/openapi.yaml, ADR 0002 A1-A31). Rules enforced:
//   - POST /waves: strict body, inductCutoffAt < windowStart < windowEnd (422 invalid-wave-times), a valid
//     service date, stopCapacity >= 0, stationNodeId a known DELIVERY_STATION (422 unknown-station); a wave
//     for today with a cutoff minutes ahead is accepted. Deferred packages of the station are inducted into
//     the new wave (InductDeferredOnSchedule).
//   - LoadArrived (published by the linehaul fake on a receipt) inducts each package into the EARLIEST OPEN
//     wave of the station whose cutoff is after the ARRIVAL time that has room (A5/A6/A7); with none the
//     package waits AT_STATION. The induction runs `lag` of service time after the arrival (the Kafka hop).
//   - OPEN -> INDUCTION_CLOSED (induction-closures) -> DISPATCHED (dispatches); anything else is 409
//     illegal-transition.
//   - POST /partners: ACTIVE partner, vanCapacity > 0 (422 invalid-van-capacity), known station. There is NO
//     list route for partners.
//   - POST /routes: wave INDUCTION_CLOSED, partner ACTIVE (partner-inactive) and of the wave's station
//     (partner-station-mismatch), stops within van capacity (route-over-capacity), stops numbered 1..n,
//     every package inducted in the wave (package-not-inducted) and on no other route (package-already-routed),
//     one stopKey per stop (stop-key-mismatch).
//   - POST /routes/{id}/starts: PLANNED and the wave DISPATCHED (else illegal-transition); emits an
//     OUT_FOR_DELIVERY scan stamped by the service clock.
//   - POST /routes/{id}/attempts: route OUT_FOR_DELIVERY (route-not-started), package on the route
//     (package-not-on-route), FAILED needs a failureReason and DELIVERED/COLLECTED forbid one, one attempt per
//     package per route (already-delivered after DELIVERED, attempt-already-recorded after FAILED); a FAILED
//     attempt re-inducts the package onto the next eligible wave by the attempt's `at` (the third failure
//     initiates a return instead); the attempt's `at` becomes the scan time in shipping-and-tracking.
//   - the induction cutoff is compared with the ARRIVAL time, never with the processing time.

import (
	"net/http"
	"sort"
	"strconv"
	"time"
)

type fwave struct {
	ID          string
	Station     string
	ServiceDate string
	Cutoff      time.Time
	WStart      time.Time
	WEnd        time.Time
	StopCap     int
	State       string
	Inducted    []string // tracking numbers in induction order
}

type fpartner struct {
	ID      string
	Station string
	Van     int
	State   string
}

type fattempt struct {
	TN      string
	Outcome string
	Detail  string
	At      time.Time
	Reason  string
}

type froute struct {
	ID       string
	WaveID   string
	DSP      string
	Station  string
	State    string
	Stops    [][]string
	Attempts []fattempt
}

type fpending struct {
	station string
	tns     []string
	at      time.Time
	due     time.Time
}

type lastMileFake struct {
	w        *world
	waves    []*fwave
	partners map[string]*fpartner
	routes   map[string]*froute
	failures map[string]int
	deferred map[string][]string
	pending  []fpending
	stopKeys map[string]string // tracking number -> stopKey (default "pkg:<tn>")
	returned map[string]bool
	// lag is how long after a LoadArrived (service time) the induction happens.
	lag                       time.Duration
	waveSeq, dspSeq, routeSeq int
}

func (l *lastMileFake) stopKey(tn string) string {
	if k, ok := l.stopKeys[tn]; ok {
		return k
	}
	return "pkg:" + tn
}

func (v *fwave) stops(l *lastMileFake) [][]string {
	var order []string
	byKey := map[string][]string{}
	for _, tn := range v.Inducted {
		k := l.stopKey(tn)
		if _, seen := byKey[k]; !seen {
			order = append(order, k)
		}
		byKey[k] = append(byKey[k], tn)
	}
	var out [][]string
	for _, k := range order {
		out = append(out, byKey[k])
	}
	return out
}

func (v *fwave) view(l *lastMileFake) map[string]any {
	assigned := len(v.stops(l))
	return map[string]any{"waveId": v.ID, "stationNodeId": v.Station, "serviceDate": v.ServiceDate, "inductCutoffAt": rfc(v.Cutoff),
		"windowStart": rfc(v.WStart), "windowEnd": rfc(v.WEnd), "stopCapacity": v.StopCap, "assignedStops": assigned,
		"remainingStops": v.StopCap - assigned, "state": v.State, "costPerStopCents": 0, "gCo2ePerStop": 0}
}

func (r *froute) view() map[string]any {
	stops := []map[string]any{}
	for i, s := range r.Stops {
		stops = append(stops, map[string]any{"stopNo": i + 1, "trackingNumbers": s})
	}
	atts := []map[string]any{}
	for _, a := range r.Attempts {
		att := map[string]any{"routeId": r.ID, "trackingNumber": a.TN, "outcome": a.Outcome, "at": rfc(a.At)}
		if a.Detail != "" {
			att["outcomeDetail"] = a.Detail
		}
		atts = append(atts, att)
	}
	return map[string]any{"routeId": r.ID, "waveId": r.WaveID, "dspId": r.DSP, "stationNodeId": r.Station, "state": r.State, "stopCount": len(r.Stops), "stops": stops, "attempts": atts}
}

func (l *lastMileFake) wave(id string) *fwave {
	for _, v := range l.waves {
		if v.ID == id {
			return v
		}
	}
	return nil
}

// eligible returns the OPEN waves of the station whose cutoff is after instant, earliest first.
func (l *lastMileFake) eligible(station string, instant time.Time) []*fwave {
	var out []*fwave
	for _, v := range l.waves {
		if v.Station == station && v.State == "OPEN" && v.Cutoff.After(instant) {
			out = append(out, v)
		}
	}
	sort.SliceStable(out, func(a, b int) bool {
		if !out[a].Cutoff.Equal(out[b].Cutoff) {
			return out[a].Cutoff.Before(out[b].Cutoff)
		}
		return out[a].ID < out[b].ID
	})
	return out
}

// induct places a package into the earliest eligible wave that accepts it, else leaves it deferred.
func (l *lastMileFake) induct(station, tn string, instant time.Time) {
	for _, v := range l.eligible(station, instant) {
		newStop := true
		for _, s := range v.stops(l) {
			if l.stopKey(s[0]) == l.stopKey(tn) {
				newStop = false
			}
		}
		if newStop && len(v.stops(l)) >= v.StopCap {
			continue // wave-full: the next wave
		}
		v.Inducted = append(v.Inducted, tn)
		return
	}
	l.deferred[station] = append(l.deferred[station], tn)
}

// onLoadArrived is the LoadArrived consumer: the induction happens `lag` after the arrival.
func (l *lastMileFake) onLoadArrived(station string, tns []string, at time.Time) {
	l.pending = append(l.pending, fpending{station: station, tns: tns, at: at, due: l.w.now().Add(l.lag)})
}

// process runs the induction of every arrival whose lag has passed; every handler calls it first.
func (l *lastMileFake) process() {
	var rest []fpending
	for _, p := range l.pending {
		if p.due.After(l.w.now()) {
			rest = append(rest, p)
			continue
		}
		for _, tn := range p.tns {
			l.induct(p.station, tn, p.at)
		}
	}
	l.pending = rest
}

func (l *lastMileFake) inWave(v *fwave, tn string) bool {
	for _, x := range v.Inducted {
		if x == tn {
			return true
		}
	}
	return false
}

func (w *world) addStation(id, tz string) {
	w.nodes[id] = fnode{ID: id, Type: "DELIVERY_STATION", Timezone: tz, Status: "ACTIVE"}
}

// addWave registers an OPEN wave directly (a wave that existed before the run).
func (w *world) addWave(station string, cutoff time.Time, stopCap int) *fwave {
	l := w.lastmile
	l.waveSeq++
	v := &fwave{ID: sprintf("wave-%04d", l.waveSeq), Station: station, ServiceDate: cutoff.In(chicago).Format("2006-01-02"), Cutoff: cutoff,
		WStart: cutoff.Add(time.Hour), WEnd: cutoff.Add(5 * time.Hour), StopCap: stopCap, State: "OPEN"}
	l.waves = append(l.waves, v)
	return v
}

// shareStop makes the packages share one stopKey (one address, one stop).
func (w *world) shareStop(key string, tns ...string) {
	for _, tn := range tns {
		w.lastmile.stopKeys[tn] = key
	}
}

func newLastMileFake(w *world) *lastMileFake {
	l := &lastMileFake{w: w, partners: map[string]*fpartner{}, routes: map[string]*froute{}, failures: map[string]int{},
		deferred: map[string][]string{}, stopKeys: map[string]string{}, returned: map[string]bool{}}
	r := func(method, path string, h func(r *http.Request, body []byte) reply) {
		w.route(method, SvcLastMile, path, func(rq *http.Request, body []byte) reply {
			l.process()
			return h(rq, body)
		})
	}

	r("POST", "/waves", func(_ *http.Request, body []byte) reply {
		var in struct {
			StationNodeID  string    `json:"stationNodeId"`
			ServiceDate    string    `json:"serviceDate"`
			InductCutoffAt time.Time `json:"inductCutoffAt"`
			WindowStart    time.Time `json:"windowStart"`
			WindowEnd      time.Time `json:"windowEnd"`
			StopCapacity   *int      `json:"stopCapacity"`
		}
		if bad := strict(body, &in); bad != nil {
			return *bad
		}
		if _, err := time.Parse("2006-01-02", in.ServiceDate); err != nil || in.StopCapacity == nil || *in.StopCapacity < 0 {
			return problem(422, "validation-failed", "serviceDate or stopCapacity")
		}
		if !in.InductCutoffAt.Before(in.WindowStart) || !in.WindowStart.Before(in.WindowEnd) {
			return problem(422, "invalid-wave-times", "inductCutoffAt < windowStart < windowEnd")
		}
		if n, known := w.nodes[in.StationNodeID]; !known || n.Type != "DELIVERY_STATION" {
			return problem(422, "unknown-station", in.StationNodeID)
		}
		l.waveSeq++
		v := &fwave{ID: sprintf("wave-%04d", l.waveSeq), Station: in.StationNodeID, ServiceDate: in.ServiceDate, Cutoff: in.InductCutoffAt.UTC(),
			WStart: in.WindowStart.UTC(), WEnd: in.WindowEnd.UTC(), StopCap: *in.StopCapacity, State: "OPEN"}
		l.waves = append(l.waves, v)
		// InductDeferredOnSchedule: packages waiting at the station join, oldest first (instant = now).
		waiting := l.deferred[v.Station]
		l.deferred[v.Station] = nil
		for _, tn := range waiting {
			l.induct(v.Station, tn, w.now())
		}
		return created(v.view(l))
	})
	r("GET", "/waves", func(rq *http.Request, _ []byte) reply {
		if bad := strictQuery(rq, "stationNodeId", "serviceDate", "limit", "cursor"); bad != nil {
			return *bad
		}
		q := rq.URL.Query()
		if q.Get("stationNodeId") == "" || q.Get("serviceDate") == "" {
			return problem(422, "validation-failed", "stationNodeId and serviceDate are required")
		}
		if _, err := time.Parse("2006-01-02", q.Get("serviceDate")); err != nil {
			return problem(422, "validation-failed", "serviceDate")
		}
		var match []*fwave
		for _, v := range l.waves {
			if v.Station == q.Get("stationNodeId") && v.ServiceDate == q.Get("serviceDate") {
				match = append(match, v)
			}
		}
		sort.SliceStable(match, func(a, b int) bool {
			if !match[a].Cutoff.Equal(match[b].Cutoff) {
				return match[a].Cutoff.Before(match[b].Cutoff)
			}
			return match[a].ID < match[b].ID
		})
		items := []map[string]any{}
		for _, v := range match {
			items = append(items, v.view(l))
		}
		return ok(map[string]any{"items": items})
	})
	r("GET", "/waves/{id}/stops", func(rq *http.Request, _ []byte) reply {
		if bad := strictQuery(rq, "limit", "cursor"); bad != nil {
			return *bad
		}
		v := l.wave(rq.PathValue("id"))
		if v == nil {
			return problem(404, "not-found", "unknown wave")
		}
		items := []map[string]any{}
		for _, s := range v.stops(l) {
			items = append(items, map[string]any{"stopKey": l.stopKey(s[0]), "trackingNumbers": s})
		}
		return ok(map[string]any{"items": items})
	})
	r("POST", "/waves/{id}/induction-closures", func(rq *http.Request, _ []byte) reply {
		v := l.wave(rq.PathValue("id"))
		if v == nil {
			return problem(404, "not-found", "unknown wave")
		}
		if v.State != "OPEN" {
			return problem(409, "illegal-transition", "wave is "+v.State)
		}
		v.State = "INDUCTION_CLOSED"
		return ok(v.view(l))
	})
	r("POST", "/waves/{id}/dispatches", func(rq *http.Request, _ []byte) reply {
		v := l.wave(rq.PathValue("id"))
		if v == nil {
			return problem(404, "not-found", "unknown wave")
		}
		if v.State != "INDUCTION_CLOSED" {
			return problem(409, "illegal-transition", "wave is "+v.State)
		}
		v.State = "DISPATCHED"
		return ok(v.view(l))
	})
	r("POST", "/partners", func(_ *http.Request, body []byte) reply {
		var in struct {
			StationNodeID string `json:"stationNodeId"`
			VanCapacity   int    `json:"vanCapacity"`
		}
		if bad := strict(body, &in); bad != nil {
			return *bad
		}
		if in.VanCapacity <= 0 {
			return problem(422, "invalid-van-capacity", "vanCapacity must be greater than zero")
		}
		if n, known := w.nodes[in.StationNodeID]; !known || n.Type != "DELIVERY_STATION" {
			return problem(422, "unknown-station", in.StationNodeID)
		}
		l.dspSeq++
		p := &fpartner{ID: sprintf("dsp-%04d", l.dspSeq), Station: in.StationNodeID, Van: in.VanCapacity, State: "ACTIVE"}
		l.partners[p.ID] = p
		return created(map[string]any{"dspId": p.ID, "stationNodeId": p.Station, "vanCapacity": p.Van, "state": p.State})
	})
	r("POST", "/routes", func(_ *http.Request, body []byte) reply {
		var in struct {
			WaveID string `json:"waveId"`
			DSPID  string `json:"dspId"`
			Stops  []struct {
				StopNo          int      `json:"stopNo"`
				TrackingNumbers []string `json:"trackingNumbers"`
			} `json:"stops"`
		}
		if bad := strict(body, &in); bad != nil {
			return *bad
		}
		v := l.wave(in.WaveID)
		if v == nil {
			return problem(404, "not-found", "unknown wave")
		}
		p := l.partners[in.DSPID]
		if p == nil {
			return problem(404, "not-found", "unknown partner")
		}
		if len(in.Stops) == 0 {
			return problem(422, "validation-failed", "at least one stop")
		}
		if v.State != "INDUCTION_CLOSED" {
			return problem(409, "illegal-transition", "wave is "+v.State)
		}
		if p.State != "ACTIVE" {
			return problem(409, "partner-inactive", p.ID)
		}
		if p.Station != v.Station {
			return problem(409, "partner-station-mismatch", p.ID+" works for "+p.Station+", the wave is at "+v.Station)
		}
		if len(in.Stops) > p.Van {
			return problem(409, "route-over-capacity", sprintf("%d stops, van capacity %d", len(in.Stops), p.Van))
		}
		seen := map[string]bool{}
		route := &froute{WaveID: v.ID, DSP: p.ID, Station: v.Station, State: "PLANNED"}
		for i, s := range in.Stops {
			if s.StopNo != i+1 || len(s.TrackingNumbers) == 0 {
				return problem(422, "validation-failed", "stops are numbered 1..n without gaps and carry packages")
			}
			key := ""
			for _, tn := range s.TrackingNumbers {
				if seen[tn] {
					return problem(422, "validation-failed", tn+" twice on the route")
				}
				seen[tn] = true
				if !l.inWave(v, tn) {
					return problem(409, "package-not-inducted", tn)
				}
				for _, other := range l.routes {
					if other.WaveID != v.ID {
						continue
					}
					for _, os := range other.Stops {
						for _, otn := range os {
							if otn == tn {
								return problem(409, "package-already-routed", tn+" is on "+other.ID)
							}
						}
					}
				}
				if key != "" && key != l.stopKey(tn) {
					return problem(409, "stop-key-mismatch", "stop "+strconv.Itoa(s.StopNo))
				}
				key = l.stopKey(tn)
			}
			route.Stops = append(route.Stops, s.TrackingNumbers)
		}
		l.routeSeq++
		route.ID = sprintf("route-%04d", l.routeSeq)
		l.routes[route.ID] = route
		return created(route.view())
	})
	r("GET", "/routes/{id}", func(rq *http.Request, _ []byte) reply {
		rt := l.routes[rq.PathValue("id")]
		if rt == nil {
			return problem(404, "not-found", "unknown route")
		}
		return ok(rt.view())
	})
	r("POST", "/routes/{id}/starts", func(rq *http.Request, _ []byte) reply {
		rt := l.routes[rq.PathValue("id")]
		if rt == nil {
			return problem(404, "not-found", "unknown route")
		}
		v := l.wave(rt.WaveID)
		if rt.State != "PLANNED" || v.State != "DISPATCHED" {
			return problem(409, "illegal-transition", "route is "+rt.State+", wave is "+v.State)
		}
		rt.State = "OUT_FOR_DELIVERY"
		at := w.now()
		for _, s := range rt.Stops {
			for _, tn := range s {
				w.scan(tn, "OUT_FOR_DELIVERY", rt.Station, at)
			}
		}
		return ok(rt.view())
	})
	r("POST", "/routes/{id}/attempts", func(rq *http.Request, body []byte) reply {
		rt := l.routes[rq.PathValue("id")]
		if rt == nil {
			return problem(404, "not-found", "unknown route")
		}
		var in struct {
			TrackingNumber string    `json:"trackingNumber"`
			Outcome        string    `json:"outcome"`
			At             time.Time `json:"at"`
			FailureReason  string    `json:"failureReason"`
			OutcomeDetail  string    `json:"outcomeDetail"`
		}
		if bad := strict(body, &in); bad != nil {
			return *bad
		}
		if in.At.IsZero() || (in.Outcome != "DELIVERED" && in.Outcome != "FAILED" && in.Outcome != "COLLECTED") {
			return problem(422, "validation-failed", "outcome and at are required")
		}
		if (in.Outcome == "FAILED") != (in.FailureReason != "") {
			return problem(422, "validation-failed", "failureReason is required when FAILED and forbidden otherwise")
		}
		okDetail := in.OutcomeDetail == "" ||
			(in.Outcome == "FAILED" && (in.OutcomeDetail == "NOT_HOME" || in.OutcomeDetail == "ACCESS_PROBLEM" || in.OutcomeDetail == "OTHER")) ||
			(in.Outcome == "DELIVERED" && in.OutcomeDetail == "SAFE_PLACE_USED")
		if !okDetail {
			return problem(422, "validation-failed", "outcomeDetail does not fit the outcome")
		}
		if rt.State != "OUT_FOR_DELIVERY" {
			return problem(409, "route-not-started", "route is "+rt.State)
		}
		onRoute := false
		for _, s := range rt.Stops {
			for _, tn := range s {
				onRoute = onRoute || tn == in.TrackingNumber
			}
		}
		if !onRoute {
			return problem(404, "package-not-on-route", in.TrackingNumber)
		}
		for _, a := range rt.Attempts {
			if a.TN == in.TrackingNumber {
				if a.Outcome == "DELIVERED" {
					return problem(409, "already-delivered", in.TrackingNumber)
				}
				return problem(409, "attempt-already-recorded", in.TrackingNumber)
			}
		}
		rt.Attempts = append(rt.Attempts, fattempt{TN: in.TrackingNumber, Outcome: in.Outcome, Detail: in.OutcomeDetail, At: in.At.UTC(), Reason: in.FailureReason})
		if in.Outcome != "COLLECTED" {
			// updateProfile runs in the same unit of work as RecordAttempt (ADR 0014, item 6).
			w.flags.attempt(l.stopKey(in.TrackingNumber), rt.ID, in.TrackingNumber, in.Outcome, in.OutcomeDetail, in.At.UTC())
		}
		switch in.Outcome {
		case "DELIVERED", "COLLECTED":
			w.scan(in.TrackingNumber, "DELIVERED", rt.Station, in.At)
		case "FAILED":
			w.scan(in.TrackingNumber, "DELIVERY_FAILED", rt.Station, in.At)
			l.failures[in.TrackingNumber]++
			if l.failures[in.TrackingNumber] >= 3 {
				l.returned[in.TrackingNumber] = true // PackageReturnInitiated, no re-induction
			} else {
				l.induct(rt.Station, in.TrackingNumber, in.At) // RetryNextWave, instant = the attempt's at
			}
		}
		total := 0
		for _, s := range rt.Stops {
			total += len(s)
		}
		if len(rt.Attempts) == total {
			rt.State = "COMPLETED"
		}
		out := map[string]any{"routeId": rt.ID, "trackingNumber": in.TrackingNumber, "outcome": in.Outcome, "at": rfc(in.At)}
		if in.OutcomeDetail != "" {
			out["outcomeDetail"] = in.OutcomeDetail
		}
		return created(out)
	})
	return l
}
