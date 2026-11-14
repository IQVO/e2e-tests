package transport

// Fake of linehaul-transportation (apis/openapi.yaml, ADR 0002). Rules enforced:
//   - GET /departures: filters origin/destination/from/to (cutoffAt, from inclusive, to exclusive),
//     ordered by cutoffAt then departureId, keyset-paged with limit 1-500 and an opaque cursor,
//     any other query parameter is 400 unknown-query-parameter;
//   - POST /loads needs a SCHEDULED departure, and a departure has at most one load (409 load-already-exists);
//   - a package goes on an OPEN load only (409 illegal-transition otherwise), once across OPEN and
//     CLOSED loads (409 package-already-loaded), within the capacity snapshot (409 load-over-capacity);
//   - OPEN -> CLOSED (closures), CLOSED -> DEPARTED (dispatches), DEPARTED -> ARRIVED (receipts); a repeat
//     of the same command answers 200 unchanged, any other order is 409 illegal-transition;
//   - NO time guard: nothing compares the clock with the departure's cutoff or departure time;
//   - dispatch and receipt are stamped by the SERVICE clock (neither takes a body), and publish
//     LoadDeparted / LoadArrived, which shipping-and-tracking turns into scans and last-mile-delivery
//     turns into an induction.

import (
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

type fdeparture struct {
	ID       string    `json:"departureId"`
	LaneID   string    `json:"laneId"`
	Kind     string    `json:"kind"`
	Origin   string    `json:"originNodeId"`
	Dest     string    `json:"destinationNodeId"`
	Cutoff   time.Time `json:"cutoffAt"`
	Departs  time.Time `json:"departsAt"`
	Arrives  time.Time `json:"arrivesAt"`
	Capacity int       `json:"capacityPackages"`
	Booked   int       `json:"bookedPackages"`
	State    string    `json:"state"`
}

type fload struct {
	ID          string
	DepartureID string
	Kind        string
	Origin      string
	Dest        string
	Capacity    int
	TNs         []string
	State       string
	DepartedAt  time.Time
	ArrivedAt   time.Time
}

type linehaulFake struct {
	w       *world
	deps    []*fdeparture
	loads   map[string]*fload
	byDep   map[string]string
	seq     int // departures
	loadSeq int
	// omitTimes drops departedAt/arrivedAt from load answers (both are optional in the schema).
	omitTimes bool
}

func (l *fload) view(omitTimes bool) map[string]any {
	tns := l.TNs
	if tns == nil {
		tns = []string{}
	}
	v := map[string]any{"loadId": l.ID, "departureId": l.DepartureID, "departureKind": l.Kind, "originNodeId": l.Origin, "destinationNodeId": l.Dest,
		"capacityPackages": l.Capacity, "packageCount": len(tns), "trackingNumbers": tns, "state": l.State}
	if !l.DepartedAt.IsZero() && !omitTimes {
		v["departedAt"] = rfc(l.DepartedAt)
	}
	if !l.ArrivedAt.IsZero() && !omitTimes {
		v["arrivedAt"] = rfc(l.ArrivedAt)
	}
	return v
}

func (d *fdeparture) view() map[string]any {
	return map[string]any{"departureId": d.ID, "laneId": d.LaneID, "kind": d.Kind, "originNodeId": d.Origin, "destinationNodeId": d.Dest,
		"cutoffAt": rfc(d.Cutoff), "departsAt": rfc(d.Departs), "arrivesAt": rfc(d.Arrives),
		"capacityPackages": d.Capacity, "bookedPackages": d.Booked, "remainingPackages": d.Capacity - d.Booked, "state": d.State}
}

// addDeparture registers a departure; departs = cutoff+30m, arrives = departs+2h.
func (w *world) addDeparture(kind, origin, dest string, cutoff time.Time, state string, capacity int) *fdeparture {
	l := w.linehaul
	l.seq++
	d := &fdeparture{ID: sprintf("dep-%04d", l.seq), LaneID: "lane-" + origin + "-" + dest, Kind: kind, Origin: origin, Dest: dest,
		Cutoff: cutoff, Departs: cutoff.Add(30 * time.Minute), Arrives: cutoff.Add(150 * time.Minute), Capacity: capacity, State: state}
	l.deps = append(l.deps, d)
	sort.SliceStable(l.deps, func(a, b int) bool {
		if !l.deps[a].Cutoff.Equal(l.deps[b].Cutoff) {
			return l.deps[a].Cutoff.Before(l.deps[b].Cutoff)
		}
		return l.deps[a].ID < l.deps[b].ID
	})
	return d
}

func (l *linehaulFake) departure(id string) *fdeparture {
	for _, d := range l.deps {
		if d.ID == id {
			return d
		}
	}
	return nil
}

func (l *linehaulFake) loadOf(r *http.Request) (*fload, *reply) {
	ld, found := l.loads[r.PathValue("id")]
	if !found {
		p := problem(404, "not-found", "unknown load")
		return nil, &p
	}
	return ld, nil
}

func newLinehaulFake(w *world) *linehaulFake {
	l := &linehaulFake{w: w, loads: map[string]*fload{}, byDep: map[string]string{}}
	w.route("GET", SvcLinehaul, "/departures", func(r *http.Request, _ []byte) reply {
		if bad := strictQuery(r, "origin", "destination", "from", "to", "limit", "cursor"); bad != nil {
			return *bad
		}
		q := r.URL.Query()
		limit := 100
		if s := q.Get("limit"); s != "" {
			n, err := strconv.Atoi(s)
			if err != nil || n < 1 || n > 500 {
				return problem(400, "invalid-limit", s)
			}
			limit = n
		}
		var from, to time.Time
		var err error
		if s := q.Get("from"); s != "" {
			if from, err = time.Parse(time.RFC3339, s); err != nil {
				return problem(422, "validation-failed", "from: "+err.Error())
			}
		}
		if s := q.Get("to"); s != "" {
			if to, err = time.Parse(time.RFC3339, s); err != nil {
				return problem(422, "validation-failed", "to: "+err.Error())
			}
		}
		var match []*fdeparture
		for _, d := range l.deps {
			if (q.Get("origin") != "" && d.Origin != q.Get("origin")) || (q.Get("destination") != "" && d.Dest != q.Get("destination")) {
				continue
			}
			if (!from.IsZero() && d.Cutoff.Before(from)) || (!to.IsZero() && !d.Cutoff.Before(to)) {
				continue
			}
			match = append(match, d)
		}
		start := 0
		if c := q.Get("cursor"); c != "" {
			n, err := strconv.Atoi(strings.TrimPrefix(c, "c"))
			if err != nil {
				return problem(400, "invalid-cursor", c)
			}
			start = n
		}
		end := min(start+limit, len(match))
		items := []map[string]any{}
		for _, d := range match[min(start, end):end] {
			items = append(items, d.view())
		}
		out := map[string]any{"items": items}
		if end < len(match) {
			out["nextCursor"] = "c" + strconv.Itoa(end)
		}
		return ok(out)
	})
	w.route("POST", SvcLinehaul, "/loads", func(_ *http.Request, body []byte) reply {
		var in struct {
			DepartureID string `json:"departureId"`
		}
		if bad := strict(body, &in); bad != nil {
			return *bad
		}
		d := l.departure(in.DepartureID)
		if d == nil {
			return problem(404, "not-found", "unknown departure")
		}
		if d.State != "SCHEDULED" {
			return problem(409, "illegal-transition", "departure is "+d.State)
		}
		if _, has := l.byDep[d.ID]; has {
			return problem(409, "load-already-exists", "the departure has a load")
		}
		l.loadSeq++
		ld := &fload{ID: sprintf("load-%04d", l.loadSeq), DepartureID: d.ID, Kind: d.Kind, Origin: d.Origin, Dest: d.Dest, Capacity: d.Capacity, State: "OPEN"}
		l.loads[ld.ID] = ld
		l.byDep[d.ID] = ld.ID
		return created(ld.view(l.omitTimes))
	})
	w.route("POST", SvcLinehaul, "/loads/{id}/packages", func(r *http.Request, body []byte) reply {
		ld, bad := l.loadOf(r)
		if bad != nil {
			return *bad
		}
		var in struct {
			TrackingNumber string `json:"trackingNumber"`
		}
		if b := strict(body, &in); b != nil {
			return *b
		}
		if tn := strings.TrimSpace(in.TrackingNumber); tn == "" || len(tn) > 64 {
			return problem(422, "validation-failed", "trackingNumber must be 1 to 64 characters")
		}
		for _, t := range ld.TNs {
			if t == in.TrackingNumber {
				return ok(ld.view(l.omitTimes))
			}
		}
		if ld.State != "OPEN" {
			return problem(409, "illegal-transition", "load is "+ld.State)
		}
		for _, other := range l.loads {
			if other.State != "OPEN" && other.State != "CLOSED" {
				continue
			}
			for _, t := range other.TNs {
				if t == in.TrackingNumber {
					return problem(409, "package-already-loaded", in.TrackingNumber+" is on "+other.ID)
				}
			}
		}
		if len(ld.TNs)+1 > ld.Capacity {
			return problem(409, "load-over-capacity", "capacity snapshot reached")
		}
		ld.TNs = append(ld.TNs, in.TrackingNumber)
		return ok(ld.view(l.omitTimes))
	})
	w.route("POST", SvcLinehaul, "/loads/{id}/closures", func(r *http.Request, _ []byte) reply {
		ld, bad := l.loadOf(r)
		if bad != nil {
			return *bad
		}
		switch ld.State {
		case "CLOSED":
			return ok(ld.view(l.omitTimes))
		case "OPEN":
			ld.State = "CLOSED"
			l.departure(ld.DepartureID).State = "CLOSED"
			return ok(ld.view(l.omitTimes))
		}
		return problem(409, "illegal-transition", "load is "+ld.State)
	})
	w.route("POST", SvcLinehaul, "/loads/{id}/dispatches", func(r *http.Request, _ []byte) reply {
		ld, bad := l.loadOf(r)
		if bad != nil {
			return *bad
		}
		switch ld.State {
		case "DEPARTED":
			return ok(ld.view(l.omitTimes))
		case "CLOSED":
			ld.State = "DEPARTED"
			ld.DepartedAt = w.now()
			l.departure(ld.DepartureID).State = "DEPARTED"
			typ := "PICKUP_DEPARTURE"
			if ld.Kind != "PICKUP" {
				typ = "DEPARTURE"
			}
			for _, tn := range ld.TNs {
				w.scan(tn, typ, ld.Origin, ld.DepartedAt)
			}
			return ok(ld.view(l.omitTimes))
		}
		return problem(409, "illegal-transition", "load is "+ld.State)
	})
	w.route("POST", SvcLinehaul, "/loads/{id}/receipts", func(r *http.Request, _ []byte) reply {
		ld, bad := l.loadOf(r)
		if bad != nil {
			return *bad
		}
		switch ld.State {
		case "ARRIVED":
			return ok(ld.view(l.omitTimes))
		case "DEPARTED":
			ld.State = "ARRIVED"
			ld.ArrivedAt = w.now()
			l.departure(ld.DepartureID).State = "ARRIVED"
			for _, tn := range ld.TNs {
				w.scan(tn, "STATION_ARRIVAL", ld.Dest, ld.ArrivedAt)
			}
			w.lastmile.onLoadArrived(ld.Dest, ld.TNs, ld.ArrivedAt)
			return ok(ld.view(l.omitTimes))
		}
		return problem(409, "illegal-transition", "load is "+ld.State)
	})
	return l
}
