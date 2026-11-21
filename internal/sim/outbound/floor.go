package outbound

import (
	"context"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/claudioed/warehouse-e2e/internal/sim"
)

// Carrier is the carrier the simulated trucks belong to.
type Carrier struct {
	Name string // booking and gate name, 1..100 characters
	SCAC string // 2..4 upper-case letters; empty sends none
}

// Floor is the outbound crew of one building: dock planner, gate and yard
// clerk, stager, loader and load supervisor. Every method is one role doing one
// thing over REST and returns an error naming the call and the answer when the
// service refuses; none panics. It is safe for concurrent use.
type Floor struct {
	C       *sim.Client
	Site    Site
	RunID   string  // namespaces plates, unit numbers and seals: short, upper-case, run-unique
	Carrier Carrier // the trucks' carrier
	Wait    Waits   // bound of every eventually-style wait
	Log     sim.LogFunc
	// GridMinutes is the dock calendar's slot granularity (Report.Calendar.SlotGranularityMinutes);
	// appointment windows start and end on it. Default 5, what Provision declares.
	GridMinutes int
	// SealState is what the dock operator records on the closed trailer: INTACT (the shape the
	// yard journey feature uses) or APPLIED. Default INTACT.
	SealState string
	// Hostler is the ref the clerk claims yard moves under. Default RunID + "-HOSTLER".
	Hostler string
	// Now is the clock appointment windows are cut from. Default time.Now.
	Now func() time.Time

	n atomic.Int64
}

func (f *Floor) logf(who, format string, args ...any) {
	if f.Log != nil {
		f.Log(who, format, args...)
	}
}

func (f *Floor) now() time.Time {
	if f.Now != nil {
		return f.Now()
	}
	return time.Now()
}

func (f *Floor) grid() int {
	if f.GridMinutes > 0 {
		return f.GridMinutes
	}
	return 5
}

func (f *Floor) next() int64 { return f.n.Add(1) }

func (f *Floor) hostler() string {
	if f.Hostler != "" {
		return f.Hostler
	}
	return f.RunID + "-HOSTLER"
}

// NextWindow is the appointment window of the given length starting on the
// next slot boundary at least startIn from now.
func (f *Floor) NextWindow(startIn, length time.Duration) Window {
	grid := time.Duration(f.grid()) * time.Minute
	start := f.now().Add(startIn).UTC().Truncate(grid)
	if start.Before(f.now().Add(startIn)) {
		start = start.Add(grid)
	}
	return Window{Start: start, End: start.Add(length)}
}

func (f *Floor) call(ctx context.Context, svc, method, route, path string, body any) sim.Resp {
	return f.C.Call(ctx, svc, method, route, path, body)
}

// ---------------------------------------------------------------------
// dock planner (dock-scheduling)
// ---------------------------------------------------------------------

// BookDoor books an OUTBOUND appointment for the cutoff on one of the site's
// doors: the first that is free in the window. outbound-shipping gives the
// load of that cutoff the door of the booking, so book AFTER the load is
// planned (outbound-shipping ignores an appointment that precedes its load).
// 422 unknown-door and missing-outbound-ref are the consumer lag of
// facility-layout's slot and of the CPT schedule and are retried in a bounded
// loop; any other refusal fails at once.
func (f *Floor) BookDoor(ctx context.Context, cptID string, w Window) (Appointment, error) {
	if len(f.Site.Doors) == 0 {
		return Appointment{}, fmt.Errorf("book a door for cutoff %s: site %s has no OUTBOUND door configured", cptID, f.Site.Code)
	}
	var busy []string
	for _, door := range f.Site.Doors {
		body := map[string]any{
			"direction": "OUTBOUND", "doorCode": door, "carrier": f.Carrier.Name,
			"windowStart": w.Start.UTC().Format(time.RFC3339), "windowEnd": w.End.UTC().Format(time.RFC3339), "cptId": cptID,
		}
		if f.Carrier.SCAC != "" {
			body["carrierScac"] = f.Carrier.SCAC
		}
		what := fmt.Sprintf("book door %s for cutoff %s", door, cptID)
		r, err := callTransient(ctx, f.Wait, what, []Transient{unknownDoor, missingOutboundRef}, func() sim.Resp {
			return f.call(ctx, SvcDock, http.MethodPost, "/appointments", "/appointments", body)
		})
		if err != nil {
			if r.Status == http.StatusConflict && Slug(r) == "door-double-booked" {
				busy = append(busy, door)
				continue
			}
			return Appointment{}, err
		}
		var got struct {
			AppointmentID string `json:"appointmentId"`
			SiteID        string `json:"siteId"`
			Direction     string `json:"direction"`
			DoorCode      string `json:"doorCode"`
			CptID         string `json:"cptId"`
			State         string `json:"state"`
		}
		if err := r.Decode(&got); err != nil {
			return Appointment{}, fmt.Errorf("%s: %w", what, err)
		}
		if r.Status != http.StatusCreated || got.AppointmentID == "" || got.State != "BOOKED" || got.DoorCode != door || got.Direction != "OUTBOUND" || got.CptID != cptID {
			return Appointment{}, fmt.Errorf("%s: status %d state %q door %q direction %q cpt %q, want 201 BOOKED OUTBOUND on %s for %s (body: %s)",
				what, r.Status, got.State, got.DoorCode, got.Direction, got.CptID, door, cptID, sim.Truncate(string(r.Body), 200))
		}
		f.logf("dock-planner", "booked door %s for cutoff %s: %s", door, cptID, got.AppointmentID)
		return Appointment{ID: got.AppointmentID, SiteID: got.SiteID, Door: door, CptID: cptID, Window: w, State: got.State}, nil
	}
	return Appointment{}, fmt.Errorf("book a door for cutoff %s: every door of %s is double-booked in the window (%v)", cptID, f.Site.Code, busy)
}

// ---------------------------------------------------------------------
// gate and yard clerk (yard-management)
// ---------------------------------------------------------------------

type moveDTO struct {
	MoveID    string `json:"moveId"`
	TrailerID string `json:"trailerId"`
	Performer string `json:"performer"`
	Reason    string `json:"reason"`
	State     string `json:"state"`
	To        struct {
		Kind string `json:"kind"`
		Code string `json:"code"`
	} `json:"to"`
}

type trailerDTO struct {
	TrailerID string `json:"trailerId"`
	State     string `json:"state"`
	DoorWork  string `json:"doorWork"`
	LoadState string `json:"loadState"`
	VisitID   string `json:"visitId"`
	Location  *struct {
		Kind string `json:"kind"`
		Code string `json:"code"`
	} `json:"location"`
}

// GateIn is the carrier's arrival: the clerk checks the visit in against the
// appointment, the trailer is moved to the appointment's door (the visit's own
// TO_DOOR move, or, when the gate parked it early or the door was busy, a
// hostler move the clerk requests, claims and completes) and door work starts.
// Completing the move at the door raises TrailerDocked, which is what starts the
// load's LOADING in outbound-shipping (there is no REST route for it).
//
// 422 unknown-appointment is yard-management's consumer lag of
// DockAppointmentBooked and is retried; every other refusal fails at once.
func (f *Floor) GateIn(ctx context.Context, a Appointment) (Visit, error) {
	n := f.next()
	v := Visit{SiteID: f.Site.Code, AppointmentID: a.ID, Door: a.Door,
		Plate: fmt.Sprintf("%s-TRK-%03d", f.RunID, n), Unit: fmt.Sprintf("%s-TRL-%03d", f.RunID, n)}
	body := map[string]any{
		"siteId": f.Site.Code, "carrier": f.Carrier.Name, "tractorPlate": v.Plate, "driverRef": fmt.Sprintf("%s-DRV-%03d", f.RunID, n),
		"purpose": "LIVE_LOAD", "appointmentId": a.ID,
		"trailers": []map[string]any{{"unitNumber": v.Unit, "kind": "DRY_VAN", "loadState": "EMPTY"}},
	}
	if f.Carrier.SCAC != "" {
		body["carrierScac"] = f.Carrier.SCAC
	}
	what := fmt.Sprintf("check in %s on appointment %s", v.Plate, a.ID)
	r, err := callTransient(ctx, f.Wait, what, []Transient{unknownAppointment}, func() sim.Resp {
		return f.call(ctx, SvcYard, http.MethodPost, "/visits", "/visits", body)
	})
	if err != nil {
		return v, err
	}
	var in struct {
		Visit struct {
			VisitID string `json:"visitId"`
			State   string `json:"state"`
		} `json:"visit"`
		Trailers []trailerDTO `json:"trailers"`
		Moves    []moveDTO    `json:"moves"`
	}
	if err := r.Decode(&in); err != nil {
		return v, fmt.Errorf("%s: %w", what, err)
	}
	if r.Status != http.StatusCreated || in.Visit.VisitID == "" || in.Visit.State != "CHECKED_IN" || len(in.Trailers) != 1 || len(in.Moves) != 1 {
		return v, fmt.Errorf("%s: status %d visit %q state %q with %d trailers and %d moves, want 201 CHECKED_IN with 1 and 1 (body: %s)",
			what, r.Status, in.Visit.VisitID, in.Visit.State, len(in.Trailers), len(in.Moves), sim.Truncate(string(r.Body), 300))
	}
	v.ID, v.TrailerID, v.MoveID = in.Visit.VisitID, in.Trailers[0].TrailerID, in.Moves[0].MoveID
	f.logf("gate", "%s checked in as %s with trailer %s: move %s (%s) to %s %s", v.Plate, v.ID, v.TrailerID, in.Moves[0].MoveID, in.Moves[0].Reason, in.Moves[0].To.Kind, in.Moves[0].To.Code)

	mv := in.Moves[0]
	for hops := 0; ; hops++ {
		if hops > 3 {
			return v, fmt.Errorf("%s: the trailer did not reach door %s after %d moves", what, a.Door, hops)
		}
		done, err := f.completeMove(ctx, mv)
		if err != nil {
			return v, err
		}
		if done.To.Kind == "DOOR" && done.To.Code == a.Door {
			break
		}
		// Parked at a spot (early arrival, busy door): a hostler brings it to the door.
		mv, err = f.hostlerToDoor(ctx, v, a.Door)
		if err != nil {
			return v, err
		}
	}
	v.MoveID = mv.MoveID

	tr, err := f.startDoorWork(ctx, v)
	if err != nil {
		return v, err
	}
	if tr.State != "DOCKED" || tr.DoorWork != "IN_PROGRESS" {
		return v, fmt.Errorf("start door work on %s: trailer is %s with door work %s, want DOCKED and IN_PROGRESS", v.TrailerID, tr.State, tr.DoorWork)
	}
	return v, nil
}

// completeMove completes a move as its performer: the visiting driver's move by
// the clerk without a hostler ref, a hostler move under the clerk's hostler ref.
func (f *Floor) completeMove(ctx context.Context, mv moveDTO) (moveDTO, error) {
	body := map[string]any{}
	if mv.Performer == "HOSTLER" {
		body["hostlerRef"] = f.hostler()
	}
	what := fmt.Sprintf("complete move %s (%s to %s %s)", mv.MoveID, mv.Reason, mv.To.Kind, mv.To.Code)
	r := f.call(ctx, SvcYard, http.MethodPost, "/moves/{id}/completions", "/moves/"+mv.MoveID+"/completions", body)
	if !r.OK() {
		return mv, refusal(what, r)
	}
	var got moveDTO
	if err := r.Decode(&got); err != nil {
		return mv, fmt.Errorf("%s: %w", what, err)
	}
	if got.State != "COMPLETED" {
		return mv, fmt.Errorf("%s: move is %s, want COMPLETED (body: %s)", what, got.State, sim.Truncate(string(r.Body), 200))
	}
	return got, nil
}

// hostlerToDoor requests a TO_DOOR hostler move for the visit's trailer and claims it.
// The claim is pull dispatch: the yard hands out the best waiting move of the SITE, which
// can be another visit's. The clerk never completes a move that is not its own trailer's:
// it reports the foreign claim and stops.
func (f *Floor) hostlerToDoor(ctx context.Context, v Visit, door string) (moveDTO, error) {
	what := fmt.Sprintf("move trailer %s to door %s with a hostler", v.TrailerID, door)
	r := f.call(ctx, SvcYard, http.MethodPost, "/moves", "/moves", map[string]any{
		"trailerId": v.TrailerID, "to": map[string]any{"kind": "DOOR", "code": door}, "reason": "TO_DOOR",
	})
	if !r.OK() {
		return moveDTO{}, refusal("request "+what, r)
	}
	r = f.call(ctx, SvcYard, http.MethodPost, "/moves/claims", "/moves/claims", map[string]any{"siteId": f.Site.Code, "hostlerRef": f.hostler()})
	if !r.OK() {
		return moveDTO{}, refusal("claim "+what, r)
	}
	var mv moveDTO
	if err := r.Decode(&mv); err != nil {
		return moveDTO{}, fmt.Errorf("claim %s: %w", what, err)
	}
	if mv.TrailerID != v.TrailerID {
		return moveDTO{}, fmt.Errorf("claim %s: the yard dispatched move %s of trailer %s, not ours; left CLAIMED under %s (its lease expires on its own), not completed",
			what, mv.MoveID, mv.TrailerID, f.hostler())
	}
	return mv, nil
}

func (f *Floor) startDoorWork(ctx context.Context, v Visit) (trailerDTO, error) {
	what := fmt.Sprintf("start door work on trailer %s", v.TrailerID)
	r := f.call(ctx, SvcYard, http.MethodPost, "/trailers/{id}/door-work-starts", "/trailers/"+v.TrailerID+"/door-work-starts", nil)
	if !r.OK() {
		return trailerDTO{}, refusal(what, r)
	}
	var tr trailerDTO
	if err := r.Decode(&tr); err != nil {
		return tr, fmt.Errorf("%s: %w", what, err)
	}
	return tr, nil
}

// SealAndDepart is the end of the visit: the dock operator completes the door
// work as LOADED, records the seal on the trailer and the clerk checks the visit
// out with the trailer. The yard refuses the check-out of a trailer that is not
// released (409 trailer-not-released: an open move or door work not DONE) and of a
// LOADED live-load trailer without a seal (409 seal-required); neither is retried.
//
// It does not look at outbound-shipping: a truck that leaves before its load was
// closed is a real thing a yard can do, and outbound-shipping answers it with
// PackageMissedLoad instead of ShipmentConfirmed.
func (f *Floor) SealAndDepart(ctx context.Context, v Visit, seal string) error {
	what := fmt.Sprintf("complete door work on trailer %s", v.TrailerID)
	r := f.call(ctx, SvcYard, http.MethodPost, "/trailers/{id}/door-work-completions", "/trailers/"+v.TrailerID+"/door-work-completions", map[string]any{"loadState": "LOADED"})
	if !r.OK() {
		return refusal(what, r)
	}
	var tr trailerDTO
	if err := r.Decode(&tr); err != nil {
		return fmt.Errorf("%s: %w", what, err)
	}
	if tr.DoorWork != "DONE" || tr.LoadState != "LOADED" {
		return fmt.Errorf("%s: trailer has door work %q and load %q, want DONE and LOADED", what, tr.DoorWork, tr.LoadState)
	}

	what = fmt.Sprintf("record seal %s on trailer %s", seal, v.TrailerID)
	state := f.SealState
	if state == "" {
		state = "INTACT"
	}
	if r = f.call(ctx, SvcYard, http.MethodPost, "/trailers/{id}/seals", "/trailers/"+v.TrailerID+"/seals", map[string]any{"state": state, "sealNumber": seal}); !r.OK() {
		return refusal(what, r)
	}

	what = fmt.Sprintf("check out visit %s", v.ID)
	r = f.call(ctx, SvcYard, http.MethodPost, "/visits/{id}/check-outs", "/visits/"+v.ID+"/check-outs", map[string]any{"trailersOut": []map[string]any{{"trailerId": v.TrailerID}}})
	if !r.OK() {
		return refusal(what, r)
	}
	var out struct {
		State       string   `json:"state"`
		TrailersOut []string `json:"trailersOut"`
	}
	if err := r.Decode(&out); err != nil {
		return fmt.Errorf("%s: %w", what, err)
	}
	if out.State != "CHECKED_OUT" || len(out.TrailersOut) != 1 {
		return fmt.Errorf("%s: visit is %s with %d trailers out, want CHECKED_OUT with 1 (body: %s)", what, out.State, len(out.TrailersOut), sim.Truncate(string(r.Body), 200))
	}
	f.logf("gate", "visit %s left with trailer %s sealed %s", v.ID, v.TrailerID, seal)
	return nil
}

// visitOf reads the visit that brought a trailer (a load already docked by someone else).
func (f *Floor) visitOf(ctx context.Context, siteDoor, trailerID, appointmentID string) (Visit, error) {
	what := fmt.Sprintf("read trailer %s", trailerID)
	r := f.call(ctx, SvcYard, http.MethodGet, "/trailers/{id}", "/trailers/"+trailerID, nil)
	if !r.OK() {
		return Visit{}, refusal(what, r)
	}
	var tr trailerDTO
	if err := r.Decode(&tr); err != nil {
		return Visit{}, fmt.Errorf("%s: %w", what, err)
	}
	if tr.VisitID == "" {
		return Visit{}, fmt.Errorf("%s: no visit on the trailer (body: %s)", what, sim.Truncate(string(r.Body), 200))
	}
	return Visit{ID: tr.VisitID, SiteID: f.Site.Code, TrailerID: trailerID, AppointmentID: appointmentID, Door: siteDoor}, nil
}

// ---------------------------------------------------------------------
// stager and loader (outbound-shipping packages)
// ---------------------------------------------------------------------

// Stage is the stager scanning a package into a staging location of the site.
// 404 package-not-found is outbound-shipping's consumer lag of PackageManifested
// (it also answers it for a manifest it ignored: no CPT schedule for the site, or a
// cpt that is no cutoff) and is retried in a bounded loop; a refusal such as 422
// not-staging-location fails at once. It returns the location used.
func (f *Floor) Stage(ctx context.Context, p Package) (string, error) {
	if len(f.Site.StagingLocations) == 0 {
		return "", fmt.Errorf("stage package %s: site %s has no Staging location configured", p.ID, f.Site.Code)
	}
	loc := f.Site.StagingLocations[int(f.next()-1)%len(f.Site.StagingLocations)]
	what := fmt.Sprintf("stage package %s at %s", p.ID, loc)
	r, err := callTransient(ctx, f.Wait, what, []Transient{packageNotFound}, func() sim.Resp {
		return f.call(ctx, SvcOutbound, http.MethodPost, "/packages/{id}/stagings", "/packages/"+p.ID+"/stagings", map[string]any{"staging_location": loc})
	})
	if err != nil {
		return "", err
	}
	var got packageDTO
	if err := r.Decode(&got); err != nil {
		return "", fmt.Errorf("%s: %w", what, err)
	}
	if got.State != PackageStaged {
		return "", fmt.Errorf("%s: package is %s, want %s (body: %s)", what, got.State, PackageStaged, sim.Truncate(string(r.Body), 200))
	}
	return loc, nil
}

// Load is the loader scanning a package onto its load through the load's door.
// outbound-shipping refuses another load or another door, and a load that has no
// door yet, with 409 wrong-load: wait for the door (Ship does) instead of retrying
// the scan. 404 package-not-found is retried like in Stage.
func (f *Floor) Load(ctx context.Context, p Package, loadID, door string) error {
	what := fmt.Sprintf("load package %s onto %s through door %s", p.ID, loadID, door)
	r, err := callTransient(ctx, f.Wait, what, []Transient{packageNotFound}, func() sim.Resp {
		return f.call(ctx, SvcOutbound, http.MethodPost, "/packages/{id}/loadings", "/packages/"+p.ID+"/loadings", map[string]any{"load_id": loadID, "door_code": door})
	})
	if err != nil {
		return err
	}
	var got packageDTO
	if err := r.Decode(&got); err != nil {
		return fmt.Errorf("%s: %w", what, err)
	}
	if got.State != PackageLoaded {
		return fmt.Errorf("%s: package is %s, want %s (body: %s)", what, got.State, PackageLoaded, sim.Truncate(string(r.Body), 200))
	}
	return nil
}

// ---------------------------------------------------------------------
// load supervisor (outbound-shipping loads)
// ---------------------------------------------------------------------

// CloseLoad is the load supervisor closing a LOADING load. outbound-shipping
// refuses a load that is not LOADING (409 load-state) and one with a package
// neither loaded nor short-shipped (409 packages-outstanding); both are findings
// returned as errors, not retried: no consumer sits between the loader's scans and
// the close, and the trailer's arrival is awaited by reading the load (Ship does).
func (f *Floor) CloseLoad(ctx context.Context, loadID string) error {
	what := fmt.Sprintf("close load %s", loadID)
	r := f.call(ctx, SvcOutbound, http.MethodPost, "/loads/{id}/closures", "/loads/"+loadID+"/closures", map[string]any{})
	if !r.OK() {
		return refusal(what, r)
	}
	var got Load
	if err := r.Decode(&got); err != nil {
		return fmt.Errorf("%s: %w", what, err)
	}
	if got.State != LoadClosed {
		return fmt.Errorf("%s: load is %s, want %s (body: %s)", what, got.State, LoadClosed, sim.Truncate(string(r.Body), 200))
	}
	return nil
}

// ReadLoad reads one load.
func (f *Floor) ReadLoad(ctx context.Context, loadID string) (Load, error) {
	what := fmt.Sprintf("read load %s", loadID)
	r := f.call(ctx, SvcOutbound, http.MethodGet, "/loads/{id}", "/loads/"+loadID, nil)
	if !r.OK() {
		return Load{}, refusal(what, r)
	}
	var l Load
	if err := r.Decode(&l); err != nil {
		return Load{}, fmt.Errorf("%s: %w", what, err)
	}
	return l, nil
}
