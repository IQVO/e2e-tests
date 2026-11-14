package transport

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"sync"
	"time"
	_ "time/tzdata" // a station's IANA timezone must resolve wherever the simulator runs

	"github.com/claudioed/warehouse-e2e/internal/sim"
)

// Station is the delivery-station associate (last-mile-delivery): it makes sure
// a wave will take the arriving packages, waits for their induction, closes the
// induction, dispatches the wave, makes sure a delivery service partner exists
// and plans the route.
//
// Contract details it works within (assumptions are listed in the PR):
//   - POST /waves validates only inductCutoffAt < windowStart < windowEnd, a
//     valid service date, the stop capacity and a known DELIVERY_STATION; a wave
//     for today with a cutoff minutes ahead is accepted.
//   - LoadArrived inducts a package into the EARLIEST OPEN wave whose cutoff is
//     after the ARRIVAL time (not after the moment the consumer ran); with no
//     such wave the package waits and joins the next wave that is scheduled.
//   - A route needs the wave INDUCTION_CLOSED, an ACTIVE partner of the same
//     station and stops within the van capacity.
//   - There is NO route that lists partners: "ensure" is made idempotent with a
//     deterministic Idempotency-Key (the service replays the first answer for
//     24 h), not by listing first.
type Station struct {
	C        *sim.Client
	Clock    Clock
	Log      sim.LogFunc
	Settings Settings
	Timeline *Timeline

	mu       sync.Mutex
	partners map[string]Partner
}

// NewStation builds a station associate. tl may be nil.
func NewStation(c *sim.Client, clock Clock, log sim.LogFunc, s Settings, tl *Timeline) *Station {
	if tl == nil {
		tl = NewTimeline()
	}
	return &Station{C: c, Clock: clock, Log: log, Settings: s.withDefaults(), Timeline: tl, partners: map[string]Partner{}}
}

func (s *Station) logf(format string, a ...any) {
	if s.Log != nil {
		s.Log("station", format, a...)
	}
}

// Node is a node-topology node (only what the associate needs).
type Node struct {
	NodeID   string `json:"nodeId"`
	NodeType string `json:"nodeType"`
	Timezone string `json:"timezone"`
	Status   string `json:"status"`
}

// Wave is a delivery wave of a station.
type Wave struct {
	WaveID         string    `json:"waveId"`
	StationNodeID  string    `json:"stationNodeId"`
	ServiceDate    string    `json:"serviceDate"`
	InductCutoffAt time.Time `json:"inductCutoffAt"`
	WindowStart    time.Time `json:"windowStart"`
	WindowEnd      time.Time `json:"windowEnd"`
	StopCapacity   int       `json:"stopCapacity"`
	AssignedStops  int       `json:"assignedStops"`
	RemainingStops int       `json:"remainingStops"`
	State          string    `json:"state"`
}

// WaveStop is one stop of a wave: packages grouped by the opaque stopKey.
type WaveStop struct {
	StopKey         string   `json:"stopKey"`
	TrackingNumbers []string `json:"trackingNumbers"`
}

// Partner is a delivery service partner.
type Partner struct {
	DSPID         string `json:"dspId"`
	StationNodeID string `json:"stationNodeId"`
	VanCapacity   int    `json:"vanCapacity"`
	State         string `json:"state"`
}

// RouteStop is one numbered stop of a route.
type RouteStop struct {
	StopNo          int      `json:"stopNo"`
	TrackingNumbers []string `json:"trackingNumbers"`
}

// Route is a planned DSP van route.
type Route struct {
	RouteID       string      `json:"routeId"`
	WaveID        string      `json:"waveId"`
	DSPID         string      `json:"dspId"`
	StationNodeID string      `json:"stationNodeId"`
	State         string      `json:"state"`
	StopCount     int         `json:"stopCount"`
	Stops         []RouteStop `json:"stops"`
}

// Node reads a node (node-topology GET /nodes/{nodeId}).
func (s *Station) Node(ctx context.Context, nodeID string) (Node, error) {
	r := s.C.Call(ctx, SvcNodes, http.MethodGet, "GET /nodes/{nodeId}", "/nodes/"+pathEscape(nodeID), nil)
	var n Node
	if err := decode("node-topology GET /nodes/{nodeId}", r, &n); err != nil {
		return Node{}, err
	}
	return n, nil
}

func (s *Station) location(n Node) *time.Location {
	loc, err := time.LoadLocation(n.Timezone)
	if err != nil || n.Timezone == "" {
		s.logf("node %s has timezone %q which does not resolve (%v): using UTC for the service date", n.NodeID, n.Timezone, err)
		return time.UTC
	}
	return loc
}

// ListWaves lists the station's waves of one service date (the station's local date).
func (s *Station) ListWaves(ctx context.Context, stationID, serviceDate string) ([]Wave, error) {
	path := "/waves?stationNodeId=" + url.QueryEscape(stationID) + "&serviceDate=" + url.QueryEscape(serviceDate)
	waves, r := sim.ListAll[Wave](ctx, s.C, SvcLastMile, "GET /waves", path)
	if !r.OK() {
		return waves, stepErr("last-mile GET /waves", r)
	}
	return waves, nil
}

// dates are the station-local service dates a wave eligible for an arrival can have:
// the arrival's own date and the next one (a window that opens after midnight).
func (s *Station) dates(n Node, arrival time.Time) []string {
	loc := s.location(n)
	a := arrival.In(loc).Format("2006-01-02")
	b := arrival.Add(24 * time.Hour).In(loc).Format("2006-01-02")
	if a == b {
		return []string{a}
	}
	return []string{a, b}
}

// OpenWaves lists the station's OPEN waves whose induct cutoff is after the
// arrival, earliest cutoff first. needRoom drops the full ones.
func (s *Station) OpenWaves(ctx context.Context, n Node, arrival time.Time, needRoom bool) ([]Wave, error) {
	var out []Wave
	for _, d := range s.dates(n, arrival) {
		waves, err := s.ListWaves(ctx, n.NodeID, d)
		if err != nil {
			return nil, err
		}
		for _, w := range waves {
			if w.State == "OPEN" && w.InductCutoffAt.After(arrival) && (!needRoom || w.RemainingStops > 0) {
				out = append(out, w)
			}
		}
	}
	sort.SliceStable(out, func(a, b int) bool {
		if !out[a].InductCutoffAt.Equal(out[b].InductCutoffAt) {
			return out[a].InductCutoffAt.Before(out[b].InductCutoffAt)
		}
		return out[a].WaveID < out[b].WaveID
	})
	return out, nil
}

// ScheduleWave creates an OPEN wave at the station whose induct cutoff is
// `cutoff`, with the configured delivery window after it.
func (s *Station) ScheduleWave(ctx context.Context, n Node, cutoff time.Time) (Wave, error) {
	start := cutoff.Add(s.Settings.WaveWindowDelay)
	end := start.Add(s.Settings.WaveWindowLength)
	body := map[string]any{
		"stationNodeId":  n.NodeID,
		"serviceDate":    start.In(s.location(n)).Format("2006-01-02"),
		"inductCutoffAt": cutoff.UTC(),
		"windowStart":    start.UTC(),
		"windowEnd":      end.UTC(),
		"stopCapacity":   s.Settings.StopCapacity,
	}
	r := s.C.Call(ctx, SvcLastMile, http.MethodPost, "POST /waves", "/waves", body)
	var w Wave
	if err := decode("last-mile POST /waves", r, &w); err != nil {
		return Wave{}, err
	}
	return w, nil
}

// EnsureWave returns the earliest OPEN wave of the station with room whose
// induct cutoff is after the arrival, or creates one with the cutoff
// Settings.InductLead after the arrival. created tells which.
func (s *Station) EnsureWave(ctx context.Context, n Node, arrival time.Time) (Wave, bool, error) {
	open, err := s.OpenWaves(ctx, n, arrival, true)
	if err != nil {
		return Wave{}, false, err
	}
	if len(open) > 0 {
		s.logf("reusing OPEN wave %s (induct cutoff %s)", open[0].WaveID, open[0].InductCutoffAt.Format(time.RFC3339))
		return open[0], false, nil
	}
	w, err := s.ScheduleWave(ctx, n, arrival.Add(s.Settings.InductLead))
	if err != nil {
		return Wave{}, false, err
	}
	s.logf("wave %s scheduled (induct cutoff %s)", w.WaveID, w.InductCutoffAt.Format(time.RFC3339))
	return w, true, nil
}

// Induction is where the packages were inducted.
type Induction struct {
	WaveID string
	// Inducted are the tracking numbers found in WaveID's stops, in the order asked.
	Inducted []string
	// Missing are the ones not in WaveID: inducted in another wave or not at all.
	Missing []string
}

// AwaitInduction waits, bounded by Settings.InductionTimeout, until every
// tracking number is inducted in some OPEN wave of the station whose cutoff is
// after the arrival. Which wave took a package is read from the waves' stops
// (the service picks the EARLIEST eligible wave that has room, which may not be
// the one the actor ensured). It returns the wave holding most of them; when
// only some were inducted by the deadline it returns that wave with the rest in
// Missing and no error; when none was, a timeout error that names why.
func (s *Station) AwaitInduction(ctx context.Context, n Node, arrival time.Time, tns []string) (Induction, error) {
	var best Induction
	err := await(ctx, s.Clock, s.Settings.InductionTimeout, s.Settings.PollInterval, func(ctx context.Context) (bool, string, error) {
		waves, err := s.OpenWaves(ctx, n, arrival, false)
		if err != nil {
			if transient(err) {
				return false, err.Error(), nil
			}
			return false, "", err
		}
		found := map[string]bool{}
		best = Induction{}
		bestCount := 0
		for _, w := range waves {
			stops, err := s.Stops(ctx, w.WaveID)
			if err != nil {
				if transient(err) {
					return false, err.Error(), nil
				}
				return false, "", err
			}
			in := map[string]bool{}
			for _, st := range stops {
				for _, tn := range st.TrackingNumbers {
					in[tn] = true
				}
			}
			var here []string
			for _, tn := range tns {
				if in[tn] {
					here = append(here, tn)
					found[tn] = true
				}
			}
			if len(here) > bestCount {
				bestCount = len(here)
				best = Induction{WaveID: w.WaveID, Inducted: here}
			}
		}
		for _, tn := range tns {
			if best.WaveID == "" || !contains(best.Inducted, tn) {
				best.Missing = append(best.Missing, tn)
			}
		}
		if len(found) == len(tns) {
			return true, "", nil
		}
		return false, fmt.Sprintf("%d of %d packages inducted; %d OPEN wave(s) of %s have an induct cutoff after the arrival %s",
			len(found), len(tns), len(waves), n.NodeID, arrival.UTC().Format(time.RFC3339)), nil
	})
	if err == nil {
		return best, nil
	}
	if IsTimeout(err) && len(best.Inducted) > 0 {
		return best, nil
	}
	return best, fmt.Errorf("induction of %v at %s: %w", tns, n.NodeID, err)
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

// Stops lists the stops of a wave (packages grouped by stopKey).
func (s *Station) Stops(ctx context.Context, waveID string) ([]WaveStop, error) {
	stops, r := sim.ListAll[WaveStop](ctx, s.C, SvcLastMile, "GET /waves/{waveId}/stops", "/waves/"+pathEscape(waveID)+"/stops")
	if !r.OK() {
		return stops, stepErr("last-mile GET /waves/{id}/stops", r)
	}
	return stops, nil
}

// CloseInduction moves an OPEN wave to INDUCTION_CLOSED.
func (s *Station) CloseInduction(ctx context.Context, waveID string) (Wave, error) {
	return s.waveCommand(ctx, "last-mile POST /waves/{id}/induction-closures", "POST /waves/{waveId}/induction-closures", waveID, "induction-closures")
}

// DispatchWave moves an INDUCTION_CLOSED wave to DISPATCHED.
func (s *Station) DispatchWave(ctx context.Context, waveID string) (Wave, error) {
	return s.waveCommand(ctx, "last-mile POST /waves/{id}/dispatches", "POST /waves/{waveId}/dispatches", waveID, "dispatches")
}

func (s *Station) waveCommand(ctx context.Context, step, route, waveID, leaf string) (Wave, error) {
	r := s.C.Call(ctx, SvcLastMile, http.MethodPost, route, "/waves/"+pathEscape(waveID)+"/"+leaf, nil)
	var w Wave
	if err := decode(step, r, &w); err != nil {
		return Wave{}, err
	}
	return w, nil
}

// EnsurePartner returns an ACTIVE partner of the station whose van takes at
// least minCapacity stops (Settings.VanCapacity when larger). The contract has
// no partner list route, so the ensure is a POST /partners with a key derived
// from (station, capacity): within the key's 24 h the service replays the same
// partner instead of contracting another, and this actor remembers it for the
// rest of its life. A partner deactivated meanwhile shows up as a refused route
// (partner-inactive), a finding.
func (s *Station) EnsurePartner(ctx context.Context, stationID string, minCapacity int) (Partner, error) {
	van := max(minCapacity, s.Settings.VanCapacity)
	cache := fmt.Sprintf("%s|%d", stationID, van)
	s.mu.Lock()
	p, ok := s.partners[cache]
	s.mu.Unlock()
	if ok {
		return p, nil
	}
	key := fmt.Sprintf("e2e-transport-partner:%s:%d", stationID, van)
	r := s.C.CallWith(ctx, SvcLastMile, http.MethodPost, "POST /partners", "/partners",
		map[string]any{"stationNodeId": stationID, "vanCapacity": van}, map[string]string{"Idempotency-Key": key})
	if err := decode("last-mile POST /partners", r, &p); err != nil {
		return Partner{}, err
	}
	s.mu.Lock()
	s.partners[cache] = p
	s.mu.Unlock()
	return p, nil
}

// PlanRoute plans a route in an INDUCTION_CLOSED wave for the given packages:
// the stops are the wave's own stops (packages grouped by stopKey, as the
// service requires) restricted to OUR packages, so a package of another run
// that shares an address is never put on our route. A package that is not among
// the wave's stops is reported before anything is posted.
func (s *Station) PlanRoute(ctx context.Context, waveID, dspID string, tns []string) (Route, error) {
	stops, err := s.Stops(ctx, waveID)
	if err != nil {
		return Route{}, err
	}
	want := map[string]bool{}
	for _, tn := range tns {
		want[tn] = true
	}
	var inputs []RouteStop
	got := map[string]bool{}
	for _, st := range stops {
		var ours []string
		for _, tn := range st.TrackingNumbers {
			if want[tn] {
				ours = append(ours, tn)
				got[tn] = true
			}
		}
		if len(ours) > 0 {
			inputs = append(inputs, RouteStop{StopNo: len(inputs) + 1, TrackingNumbers: ours})
		}
	}
	var missing []string
	for _, tn := range tns {
		if !got[tn] {
			missing = append(missing, tn)
		}
	}
	if len(missing) > 0 {
		return Route{}, fmt.Errorf("last-mile plan route: %v not among the stops of wave %s", missing, waveID)
	}
	r := s.C.Call(ctx, SvcLastMile, http.MethodPost, "POST /routes", "/routes", map[string]any{"waveId": waveID, "dspId": dspID, "stops": inputs})
	var rt Route
	if err := decode("last-mile POST /routes", r, &rt); err != nil {
		return Route{}, err
	}
	return rt, nil
}
