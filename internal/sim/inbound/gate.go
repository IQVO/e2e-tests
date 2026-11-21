package inbound

import (
	"context"
	"fmt"
	"net/http"

	"github.com/claudioed/warehouse-e2e/internal/sim"
	"github.com/claudioed/warehouse-e2e/internal/sim/outbound"
)

// ---------------------------------------------------------------------
// gate and yard clerk (yard-management)
// ---------------------------------------------------------------------

// Yard states the clerk branches on.
const (
	trailerDocked   = "DOCKED"
	doorWorkRunning = "IN_PROGRESS"
	doorWorkDone    = "DONE"
	loadEmpty       = "EMPTY"
)

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
}

// GateIn is the carrier's arrival: the clerk checks the visit in against the appointment (one LOADED trailer
// carrying the seal the booking declared), the trailer is moved to the appointment's door (the visit's own TO_DOOR
// move, or, when the gate parked it early or the door was busy, a hostler move the clerk requests, claims and
// completes) and door work starts, which is what lets the receipt's close release the unload.
//
// 422 unknown-appointment is yard-management's consumer lag of DockAppointmentBooked and 422 unknown-location its
// lag of facility-layout's door slot; both are retried, bounded. A visit the gate turns away (state REJECTED, for
// instance SEAL_MISMATCH) is a finding, and so is every other refusal.
func (c *Crew) GateIn(ctx context.Context, a Appointment) (Visit, error) {
	n := c.next()
	site := a.SiteID
	if site == "" {
		site = c.Site.Code
	}
	v := Visit{AppointmentID: a.ID, Door: a.Door,
		Plate: fmt.Sprintf("%s-IB-TRK-%03d", c.RunID, n), Unit: fmt.Sprintf("%s-IB-TRL-%03d", c.RunID, n)}
	trailer := map[string]any{"unitNumber": v.Unit, "kind": "DRY_VAN", "loadState": "LOADED"}
	if a.Seal != "" {
		trailer["sealNumber"] = a.Seal
	}
	body := map[string]any{
		"siteId": site, "carrier": c.Carrier.Name, "tractorPlate": v.Plate, "driverRef": fmt.Sprintf("%s-IB-DRV-%03d", c.RunID, n),
		"purpose": "LIVE_UNLOAD", "appointmentId": a.ID, "trailers": []map[string]any{trailer},
	}
	if c.Carrier.SCAC != "" {
		body["carrierScac"] = c.Carrier.SCAC
	}
	what := fmt.Sprintf("check in %s on appointment %s", v.Plate, a.ID)
	r, err := c.callTransient(ctx, what, []outbound.Transient{yardUnknownAppointment, yardUnknownLocation}, func() sim.Resp {
		return c.call(ctx, SvcYard, http.MethodPost, "/visits", "/visits", body)
	})
	if err != nil {
		return v, err
	}
	var in struct {
		Visit struct {
			VisitID         string `json:"visitId"`
			State           string `json:"state"`
			RejectionReason string `json:"rejectionReason"`
		} `json:"visit"`
		Trailers []trailerDTO `json:"trailers"`
		Moves    []moveDTO    `json:"moves"`
	}
	if err := r.Decode(&in); err != nil {
		return v, fmt.Errorf("%s: %w", what, err)
	}
	v.ID = in.Visit.VisitID
	if in.Visit.State == "REJECTED" {
		return v, fmt.Errorf("%s: the gate turned the carrier away: visit %s is REJECTED (%s)", what, in.Visit.VisitID, in.Visit.RejectionReason)
	}
	if r.Status != http.StatusCreated || in.Visit.VisitID == "" || in.Visit.State != "CHECKED_IN" || len(in.Trailers) != 1 || len(in.Moves) != 1 {
		return v, fmt.Errorf("%s: status %d visit %q state %q with %d trailers and %d moves, want 201 CHECKED_IN with 1 and 1 (body: %s)",
			what, r.Status, in.Visit.VisitID, in.Visit.State, len(in.Trailers), len(in.Moves), sim.Truncate(string(r.Body), 300))
	}
	v.TrailerID, v.MoveID = in.Trailers[0].TrailerID, in.Moves[0].MoveID
	c.logf("gate", "%s checked in as %s with trailer %s: move %s (%s) to %s %s", v.Plate, v.ID, v.TrailerID, in.Moves[0].MoveID, in.Moves[0].Reason, in.Moves[0].To.Kind, in.Moves[0].To.Code)

	mv := in.Moves[0]
	for hops := 0; ; hops++ {
		if hops > 3 {
			return v, fmt.Errorf("%s: the trailer did not reach door %s after %d moves", what, a.Door, hops)
		}
		done, err := c.completeMove(ctx, mv)
		if err != nil {
			return v, err
		}
		if done.To.Kind == "DOOR" && done.To.Code == a.Door {
			break
		}
		// Parked at a spot (early arrival, busy door): a hostler brings it to the door.
		if mv, err = c.hostlerToDoor(ctx, v, a.Door); err != nil {
			return v, err
		}
	}
	v.MoveID = mv.MoveID

	tr, err := c.startDoorWork(ctx, v)
	if err != nil {
		return v, err
	}
	if tr.State != trailerDocked || tr.DoorWork != doorWorkRunning {
		return v, fmt.Errorf("start door work on %s: trailer is %s with door work %s, want %s and %s", v.TrailerID, tr.State, tr.DoorWork, trailerDocked, doorWorkRunning)
	}
	return v, nil
}

// completeMove completes a move as its performer: the visiting driver's move by the clerk without a hostler
// ref, a hostler move under the clerk's hostler ref.
func (c *Crew) completeMove(ctx context.Context, mv moveDTO) (moveDTO, error) {
	body := map[string]any{}
	if mv.Performer == "HOSTLER" {
		body["hostlerRef"] = c.hostler()
	}
	what := fmt.Sprintf("complete move %s (%s to %s %s)", mv.MoveID, mv.Reason, mv.To.Kind, mv.To.Code)
	r := c.call(ctx, SvcYard, http.MethodPost, "/moves/{id}/completions", "/moves/"+mv.MoveID+"/completions", body)
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

// hostlerToDoor requests a TO_DOOR hostler move for the visit's trailer and claims it. The claim is pull
// dispatch: the yard hands out the best waiting move of the SITE, which can be another visit's. The clerk never
// completes a move that is not its own trailer's: it reports the foreign claim and stops.
func (c *Crew) hostlerToDoor(ctx context.Context, v Visit, door string) (moveDTO, error) {
	what := fmt.Sprintf("move trailer %s to door %s with a hostler", v.TrailerID, door)
	r := c.call(ctx, SvcYard, http.MethodPost, "/moves", "/moves", map[string]any{
		"trailerId": v.TrailerID, "to": map[string]any{"kind": "DOOR", "code": door}, "reason": "TO_DOOR",
	})
	if !r.OK() {
		return moveDTO{}, refusal("request "+what, r)
	}
	r = c.call(ctx, SvcYard, http.MethodPost, "/moves/claims", "/moves/claims", map[string]any{"siteId": c.Site.Code, "hostlerRef": c.hostler()})
	if !r.OK() {
		return moveDTO{}, refusal("claim "+what, r)
	}
	var mv moveDTO
	if err := r.Decode(&mv); err != nil {
		return moveDTO{}, fmt.Errorf("claim %s: %w", what, err)
	}
	if mv.TrailerID != v.TrailerID {
		return moveDTO{}, fmt.Errorf("claim %s: the yard dispatched move %s of trailer %s, not ours; left CLAIMED under %s (its lease expires on its own), not completed",
			what, mv.MoveID, mv.TrailerID, c.hostler())
	}
	return mv, nil
}

func (c *Crew) startDoorWork(ctx context.Context, v Visit) (trailerDTO, error) {
	what := fmt.Sprintf("start door work on trailer %s", v.TrailerID)
	r := c.call(ctx, SvcYard, http.MethodPost, "/trailers/{id}/door-work-starts", "/trailers/"+v.TrailerID+"/door-work-starts", nil)
	if !r.OK() {
		return trailerDTO{}, refusal(what, r)
	}
	var tr trailerDTO
	if err := r.Decode(&tr); err != nil {
		return tr, fmt.Errorf("%s: %w", what, err)
	}
	return tr, nil
}
