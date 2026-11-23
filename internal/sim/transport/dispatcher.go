package transport

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/claudioed/warehouse-e2e/internal/paging"
	"github.com/claudioed/warehouse-e2e/internal/sim"
)

// Dispatcher is the linehaul dispatcher (linehaul-transportation): it opens the
// load of a scheduled pickup departure, puts the shipped packages on it by
// tracking number, closes it, dispatches it and receives it at the destination.
//
// The contract has NO time guard on close, dispatch or receipt, so a load may
// leave before the departure's scheduled time; it only needs a SCHEDULED
// departure. dispatch and receipt take no body: the SERVICE stamps
// departedAt/arrivedAt, so the dispatcher cannot send a wrong time; it OBSERVES
// them (Timeline) so that every time it sends later is after them.
type Dispatcher struct {
	C        *sim.Client
	Clock    Clock
	Log      sim.LogFunc
	Settings Settings
	Timeline *Timeline
}

// NewDispatcher builds a dispatcher. tl may be nil.
func NewDispatcher(c *sim.Client, clock Clock, log sim.LogFunc, s Settings, tl *Timeline) *Dispatcher {
	if tl == nil {
		tl = NewTimeline()
	}
	return &Dispatcher{C: c, Clock: clock, Log: log, Settings: s.withDefaults(), Timeline: tl}
}

func (d *Dispatcher) logf(format string, a ...any) {
	if d.Log != nil {
		d.Log("linehaul", format, a...)
	}
}

// Departure is a scheduled trip of a lane.
type Departure struct {
	DepartureID       string    `json:"departureId"`
	LaneID            string    `json:"laneId"`
	Kind              string    `json:"kind"`
	OriginNodeID      string    `json:"originNodeId"`
	DestinationNodeID string    `json:"destinationNodeId"`
	CutoffAt          time.Time `json:"cutoffAt"`
	DepartsAt         time.Time `json:"departsAt"`
	ArrivesAt         time.Time `json:"arrivesAt"`
	CapacityPackages  int       `json:"capacityPackages"`
	BookedPackages    int       `json:"bookedPackages"`
	State             string    `json:"state"`
}

// Load is the packages consolidated onto one departure.
type Load struct {
	LoadID            string    `json:"loadId"`
	DepartureID       string    `json:"departureId"`
	DepartureKind     string    `json:"departureKind"`
	OriginNodeID      string    `json:"originNodeId"`
	DestinationNodeID string    `json:"destinationNodeId"`
	CapacityPackages  int       `json:"capacityPackages"`
	PackageCount      int       `json:"packageCount"`
	TrackingNumbers   []string  `json:"trackingNumbers"`
	State             string    `json:"state"`
	DepartedAt        time.Time `json:"departedAt"`
	ArrivedAt         time.Time `json:"arrivedAt"`
}

// PickupDepartures returns up to max SCHEDULED PICKUP departures of the origin
// node (to the destination node when given) whose cutoff is not past, earliest
// cutoff first, that can hold `packages` packages. It walks the keyset pages
// (filters repeated on every page) and stops as soon as it has max.
func (d *Dispatcher) PickupDepartures(ctx context.Context, origin, dest string, packages, max int) ([]Departure, error) {
	now := d.Clock.Now().UTC()
	q := url.Values{}
	q.Set("origin", origin)
	if dest != "" {
		q.Set("destination", dest)
	}
	q.Set("from", now.Format(time.RFC3339))
	q.Set("to", now.Add(d.Settings.DepartureLookahead).Format(time.RFC3339))
	q.Set("limit", "100")
	base := "/departures?" + q.Encode()

	var out []Departure
	cursor := ""
	for pages := 0; pages < 1000; pages++ {
		path := base
		if cursor != "" {
			path += "&cursor=" + url.QueryEscape(cursor)
		}
		r := d.C.Call(ctx, SvcLinehaul, http.MethodGet, "GET /departures", path, nil)
		if !r.OK() {
			return out, stepErr("linehaul GET /departures", r)
		}
		items, next, err := paging.Decode[Departure](r.Body)
		if err != nil {
			return out, fmt.Errorf("linehaul GET /departures: %w", err)
		}
		for _, dep := range items {
			if dep.Kind != "PICKUP" || dep.State != "SCHEDULED" || dep.CapacityPackages < packages {
				continue
			}
			out = append(out, dep)
			if len(out) >= max {
				return out, nil
			}
		}
		if next == "" || next == cursor {
			return out, nil
		}
		cursor = next
	}
	return out, errors.New("linehaul GET /departures: paging did not end")
}

// NextPickupDeparture is the earliest SCHEDULED pickup departure that fits the packages.
func (d *Dispatcher) NextPickupDeparture(ctx context.Context, origin, dest string, packages int) (Departure, error) {
	deps, err := d.PickupDepartures(ctx, origin, dest, packages, 1)
	if err != nil {
		return Departure{}, err
	}
	if len(deps) == 0 {
		return Departure{}, noDeparture(origin, dest, d.Settings.DepartureLookahead)
	}
	return deps[0], nil
}

func noDeparture(origin, dest string, look time.Duration) error {
	to := dest
	if to == "" {
		to = "any station"
	}
	return fmt.Errorf("no SCHEDULED PICKUP departure from %s to %s within %s (profile cutoffs generate them; a lane leaving a vendor node yields none)", origin, to, look)
}

// OpenLoad opens the load of a departure (POST /loads).
func (d *Dispatcher) OpenLoad(ctx context.Context, departureID string) (Load, error) {
	return d.load(ctx, "linehaul POST /loads", "POST /loads", "/loads", map[string]string{"departureId": departureID})
}

// AddPackage puts a package on an OPEN load by tracking number.
func (d *Dispatcher) AddPackage(ctx context.Context, loadID, tn string) (Load, error) {
	return d.load(ctx, "linehaul POST /loads/{id}/packages", "POST /loads/{loadId}/packages", "/loads/"+pathEscape(loadID)+"/packages", map[string]string{"trackingNumber": tn})
}

// CloseLoad moves an OPEN load to CLOSED.
func (d *Dispatcher) CloseLoad(ctx context.Context, loadID string) (Load, error) {
	return d.load(ctx, "linehaul POST /loads/{id}/closures", "POST /loads/{loadId}/closures", "/loads/"+pathEscape(loadID)+"/closures", nil)
}

// Dispatch moves a CLOSED load to DEPARTED; the service stamps departedAt.
func (d *Dispatcher) Dispatch(ctx context.Context, loadID string) (Load, error) {
	return d.load(ctx, "linehaul POST /loads/{id}/dispatches", "POST /loads/{loadId}/dispatches", "/loads/"+pathEscape(loadID)+"/dispatches", nil)
}

// Receive moves a DEPARTED load to ARRIVED at its destination; the service
// stamps arrivedAt and publishes LoadArrived.
func (d *Dispatcher) Receive(ctx context.Context, loadID string) (Load, error) {
	return d.load(ctx, "linehaul POST /loads/{id}/receipts", "POST /loads/{loadId}/receipts", "/loads/"+pathEscape(loadID)+"/receipts", nil)
}

func (d *Dispatcher) load(ctx context.Context, step, route, path string, body any) (Load, error) {
	r := d.C.Call(ctx, SvcLinehaul, http.MethodPost, route, path, body)
	var l Load
	if err := decode(step, r, &l); err != nil {
		return Load{}, err
	}
	return l, nil
}

// LoadResult is what one Run did.
type LoadResult struct {
	Departure  Departure
	Load       Load // the load as of the receipt
	Packed     []string
	DepartedAt time.Time
	ArrivedAt  time.Time
	// Problems are findings that did not stop the run (a package the service refused).
	Problems []string
}

// Run moves the parcel's packages from the origin node to the destination
// station on the next scheduled pickup departure: open the load (moving on to
// the next departure when one already has a load), pack every tracking number,
// close, dispatch, pause, receive. A package the service refuses is a finding
// and the rest go on; any other refusal ends the run with an error. Whatever
// was done is in the result either way.
func (d *Dispatcher) Run(ctx context.Context, p Parcel) (LoadResult, error) {
	res, err := d.openLoad(ctx, p)
	if err != nil {
		return res, err
	}
	for _, tn := range p.TrackingNumbers {
		ld, err := d.AddPackage(ctx, res.Load.LoadID, tn)
		if err != nil {
			res.Problems = append(res.Problems, fmt.Sprintf("%s (package %s)", err, tn))
			continue
		}
		res.Load = ld
		res.Packed = append(res.Packed, tn)
	}
	if len(res.Packed) == 0 {
		return res, fmt.Errorf("load %s: no package could be added", res.Load.LoadID)
	}

	if ld, err := d.CloseLoad(ctx, res.Load.LoadID); err != nil {
		return res, err
	} else {
		res.Load = ld
	}
	ld, err := d.Dispatch(ctx, res.Load.LoadID)
	if err != nil {
		return res, err
	}
	res.Load = ld
	res.DepartedAt = d.stamped(ld.DepartedAt, res.Packed)
	d.logf("load %s dispatched at %s with %d packages", ld.LoadID, res.DepartedAt.Format(time.RFC3339), len(res.Packed))

	if err := d.receiveLoad(ctx, &res); err != nil {
		return res, err
	}
	return res, nil
}

// openLoad finds the earliest pickup departure that still has a free load and
// opens it. A departure whose load already exists (another run took it) is
// skipped, not failed.
func (d *Dispatcher) openLoad(ctx context.Context, p Parcel) (LoadResult, error) {
	var res LoadResult
	if len(p.TrackingNumbers) == 0 {
		return res, errors.New("linehaul: no tracking numbers to load")
	}
	deps, err := d.PickupDepartures(ctx, p.OriginNodeID, p.DestStationNodeID, len(p.TrackingNumbers), d.Settings.MaxDepartureTries)
	if err != nil {
		return res, err
	}
	if len(deps) == 0 {
		return res, noDeparture(p.OriginNodeID, p.DestStationNodeID, d.Settings.DepartureLookahead)
	}
	var ld Load
	var openErr error
	for _, dep := range deps {
		ld, openErr = d.OpenLoad(ctx, dep.DepartureID)
		if openErr == nil {
			res.Departure = dep
			break
		}
		if !hasSlug(openErr, "load-already-exists") {
			return res, openErr
		}
		d.logf("departure %s already has a load, trying the next", dep.DepartureID)
	}
	if openErr != nil {
		return res, fmt.Errorf("no pickup departure with a free load in %d tries: %w", len(deps), openErr)
	}
	res.Load = ld
	d.logf("load %s opened on departure %s (cutoff %s)", ld.LoadID, res.Departure.DepartureID, res.Departure.CutoffAt.Format(time.RFC3339))
	return res, nil
}

// receiveLoad receives the dispatched load at its destination, after the
// transit pause, and stamps the arrival on the timeline.
func (d *Dispatcher) receiveLoad(ctx context.Context, res *LoadResult) error {
	// The service stamps the arrival with its own clock; leave it a gap so that
	// the arrival scan can never share the departure scan's instant.
	if d.Settings.TransitPause > 0 {
		if err := d.Clock.Sleep(ctx, d.Settings.TransitPause); err != nil {
			return err
		}
	}
	ld, err := d.Receive(ctx, res.Load.LoadID)
	if err != nil {
		return err
	}
	res.Load = ld
	res.ArrivedAt = d.stamped(ld.ArrivedAt, res.Packed)
	if !res.ArrivedAt.After(res.DepartedAt) {
		res.Problems = append(res.Problems, fmt.Sprintf("load %s arrived at %s, not after its departure at %s (service clock went backwards?)",
			ld.LoadID, res.ArrivedAt.Format(time.RFC3339Nano), res.DepartedAt.Format(time.RFC3339Nano)))
	}
	d.logf("load %s received at %s", ld.LoadID, res.ArrivedAt.Format(time.RFC3339))
	return nil
}

// stamped returns the service's time (our own clock when the answer omitted it)
// and makes the timeline know it for every package of the load.
func (d *Dispatcher) stamped(serviceTime time.Time, tns []string) time.Time {
	at := serviceTime
	if at.IsZero() {
		at = d.Clock.Now()
	}
	for _, tn := range tns {
		d.Timeline.Observe(tn, at)
	}
	return at
}
