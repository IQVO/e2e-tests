package floor

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
)

// Crew is how many people work the floor. Rebin and SLAM have no station (their routes take none):
// the rebin associate and the SLAM operator are roles of whoever finishes the pick and the seal.
type Crew struct{ Pickers, Packers int }

type stationDTO struct {
	ID       string `json:"id"`
	Occupied bool   `json:"occupied"`
}

// Provision installs one station per associate (POST /stations upserts by id, so installing the same
// run's stations again is harmless) and checks the associate in. A station that is still occupied by
// an earlier run of the same run id is checked out and in again. It must finish before Process starts
// and is not safe to call concurrently with it. Any refusal is returned and ends the provisioning.
func (f *Floor) Provision(ctx context.Context, crew Crew) error {
	if crew.Pickers < 1 || crew.Packers < 1 {
		return fmt.Errorf("provision: a crew of %d pickers and %d packers cannot take an order from pick to pack: both need at least one", crew.Pickers, crew.Packers)
	}
	var pickers, packers []*station
	slot := 0
	install := func(kind, capability string, n int) ([]*station, error) {
		var out []*station
		for i := 1; i <= n; i++ {
			s := &station{ID: fmt.Sprintf("%s-%s-%02d", f.runID, kind, i), Occupant: fmt.Sprintf("%s-%sER-%02d", f.runID, kind, i), Capability: capability}
			body := map[string]any{"stationId": s.ID, "capabilities": []string{capability}}
			if len(f.set.WorkCentres) > 0 {
				body["locationCode"] = f.set.WorkCentres[slot%len(f.set.WorkCentres)]
				slot++
			}
			if r := f.call(ctx, SvcExecution, http.MethodPost, "/stations", "/stations", body); r.Status != http.StatusCreated {
				return nil, refusal("install station "+s.ID, r)
			}
			if err := f.checkIn(ctx, s); err != nil {
				return nil, err
			}
			f.logf("station", "%s installed for %s", s.ID, s.Occupant)
			out = append(out, s)
		}
		return out, nil
	}
	var err error
	if pickers, err = install("PICK", f.set.PickCapability, crew.Pickers); err != nil {
		return err
	}
	if packers, err = install("PACK", f.set.PackCapability, crew.Packers); err != nil {
		return err
	}
	f.mu.Lock()
	f.pickers, f.packers = pickers, packers
	f.mu.Unlock()
	return nil
}

func (f *Floor) checkIn(ctx context.Context, s *station) error {
	what := fmt.Sprintf("check %s in at %s", s.Occupant, s.ID)
	path := "/stations/" + url.PathEscape(s.ID)
	r := f.call(ctx, SvcExecution, http.MethodPost, "/stations/{id}/check-ins", path+"/check-ins", map[string]any{"occupantId": s.Occupant})
	if r.Status == http.StatusConflict && Slug(r) == "station-occupied" {
		// an earlier run of this run id left somebody there: they go home, ours starts
		if o := f.call(ctx, SvcExecution, http.MethodPost, "/stations/{id}/check-outs", path+"/check-outs", nil); !o.OK() {
			return refusal("check out the previous occupant of "+s.ID, o)
		}
		r = f.call(ctx, SvcExecution, http.MethodPost, "/stations/{id}/check-ins", path+"/check-ins", map[string]any{"occupantId": s.Occupant})
	}
	if !r.OK() {
		return refusal(what, r)
	}
	var got stationDTO
	if err := r.Decode(&got); err != nil {
		return fmt.Errorf("%s: %w", what, err)
	}
	if !got.Occupied {
		return fmt.Errorf("%s: the station answered unoccupied", what)
	}
	return nil
}

// Shutdown checks every associate out. A station that already has nobody is not a failure; any other
// refusal is reported with the station's name, and the remaining stations are still checked out.
func (f *Floor) Shutdown(ctx context.Context) error {
	f.mu.Lock()
	all := append(append([]*station(nil), f.pickers...), f.packers...)
	f.mu.Unlock()
	var errs []error
	for _, s := range all {
		r := f.call(ctx, SvcExecution, http.MethodPost, "/stations/{id}/check-outs", "/stations/"+url.PathEscape(s.ID)+"/check-outs", nil)
		if r.OK() || (r.Status == http.StatusConflict && Slug(r) == "station-not-occupied") {
			continue
		}
		errs = append(errs, refusal("check out "+s.ID, r))
	}
	return errors.Join(errs...)
}

// acquire hands out the first station of the pool that is not busy, or nil: a person works one task
// at a time. It never blocks: the caller sleeps on the injected clock and looks again.
func (f *Floor) acquire(pool *[]*station) *station {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, s := range *pool {
		if !s.busy {
			s.busy = true
			return s
		}
	}
	return nil
}

func (f *Floor) free(s *station) {
	f.mu.Lock()
	s.busy = false
	f.mu.Unlock()
}

func (f *Floor) crewSize() (pickers, packers int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.pickers), len(f.packers)
}
