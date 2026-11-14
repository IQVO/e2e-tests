package floor

import (
	"context"
	"strings"
	"testing"
)

func newTestFloor(w *world, s Settings) *Floor {
	return New(w.client(), w.clock, nil, "RD01", s)
}

func TestProvisionInstallsAndChecksInOneStationPerAssociate(t *testing.T) {
	w := newWorld(t)
	f := newTestFloor(w, Settings{WorkCentres: []string{"AUS3-WC-01", "AUS3-WC-02", "AUS3-WC-03"}})
	if err := f.Provision(context.Background(), Crew{Pickers: 2, Packers: 1}); err != nil {
		t.Fatal(err)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.fe.stations) != 3 {
		t.Fatalf("%d stations installed, want 3", len(w.fe.stations))
	}
	for id, want := range map[string][]string{"RD01-PICK-01": {"pick"}, "RD01-PICK-02": {"pick"}, "RD01-PACK-01": {"pack"}} {
		s := w.fe.stations[id]
		if s == nil || strings.Join(s.Caps, ",") != strings.Join(want, ",") {
			t.Fatalf("station %s = %+v, want capabilities %v", id, s, want)
		}
		if s.Occupant == "" || s.Loc == "" {
			t.Fatalf("station %s has occupant %q and location %q: both are required", id, s.Occupant, s.Loc)
		}
	}
	if w.fe.stations["RD01-PICK-01"].Occupant == w.fe.stations["RD01-PICK-02"].Occupant {
		t.Fatal("two stations share an occupant")
	}
}

func TestProvisionWithoutWorkCentresSendsNoLocation(t *testing.T) {
	w := newWorld(t)
	f := newTestFloor(w, Settings{})
	if err := f.Provision(context.Background(), Crew{Pickers: 1, Packers: 1}); err != nil {
		t.Fatal(err)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	for id, s := range w.fe.stations {
		if s.Loc != "" {
			t.Fatalf("station %s was given location %q though none was configured", id, s.Loc)
		}
	}
}

func TestProvisionAgainTakesOverStationsStillOccupiedByAnEarlierRun(t *testing.T) {
	w := newWorld(t)
	first := newTestFloor(w, Settings{})
	if err := first.Provision(context.Background(), Crew{Pickers: 1, Packers: 1}); err != nil {
		t.Fatal(err)
	}
	// the same run id again (a restarted simulator): the stations are occupied, 409 station-occupied
	second := newTestFloor(w, Settings{})
	if err := second.Provision(context.Background(), Crew{Pickers: 1, Packers: 1}); err != nil {
		t.Fatalf("re-provision: %v", err)
	}
	if got := w.callsTo("POST /fulfillment-execution/stations/{id}/check-outs"); got != 2 {
		t.Fatalf("%d check-outs, want 2: one per occupied station, nothing more", got)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	for id, s := range w.fe.stations {
		if s.Occupant == "" {
			t.Fatalf("station %s is empty after the second provision", id)
		}
	}
}

func TestProvisionRefusedIsAnErrorAndNotAPanic(t *testing.T) {
	w := newWorld(t)
	w.fe.rejectLocations = true
	f := newTestFloor(w, Settings{WorkCentres: []string{"AUS3-DOCK-01"}})
	err := f.Provision(context.Background(), Crew{Pickers: 1, Packers: 1})
	if err == nil || !strings.Contains(err.Error(), "station-location-not-workcenter") {
		t.Fatalf("err = %v, want the refusal named", err)
	}
	if got := w.callsTo("POST /fulfillment-execution/stations/{id}/check-ins"); got != 0 {
		t.Fatalf("%d check-ins after a refused installation, want none", got)
	}
}

func TestProvisionRejectsAnEmptyCrew(t *testing.T) {
	w := newWorld(t)
	f := newTestFloor(w, Settings{})
	for _, c := range []Crew{{}, {Pickers: 1}, {Packers: 1}} {
		if err := f.Provision(context.Background(), c); err == nil {
			t.Fatalf("crew %+v was accepted: an order cannot go from pick to pack without both", c)
		}
	}
	if w.writes() != 0 {
		t.Fatalf("%d writes for an invalid crew", w.writes())
	}
}

func TestShutdownChecksEveryoneOutAndReportsWhoCouldNot(t *testing.T) {
	w := newWorld(t)
	f := newTestFloor(w, Settings{})
	if err := f.Provision(context.Background(), Crew{Pickers: 2, Packers: 1}); err != nil {
		t.Fatal(err)
	}
	if err := f.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	w.mu.Lock()
	for id, s := range w.fe.stations {
		if s.Occupant != "" {
			t.Fatalf("station %s still occupied", id)
		}
	}
	w.mu.Unlock()
	// a second shutdown finds nobody to check out: already gone is not a failure
	if err := f.Shutdown(context.Background()); err != nil {
		t.Fatalf("second shutdown: %v", err)
	}
	// a station that no longer exists is reported by name, and the others are still checked out
	w.mu.Lock()
	for _, s := range w.fe.stations {
		s.Occupant = "someone"
	}
	delete(w.fe.stations, "RD01-PICK-01")
	w.mu.Unlock()
	err := f.Shutdown(context.Background())
	if err == nil || !strings.Contains(err.Error(), "RD01-PICK-01") || !strings.Contains(err.Error(), "station-not-found") {
		t.Fatalf("err = %v, want the missing station named", err)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.fe.stations["RD01-PICK-02"].Occupant != "" || w.fe.stations["RD01-PACK-01"].Occupant != "" {
		t.Fatal("one failed check-out stopped the others")
	}
}

func TestAStationWorksOneTaskAtATime(t *testing.T) {
	w := newWorld(t)
	f := newTestFloor(w, Settings{})
	if err := f.Provision(context.Background(), Crew{Pickers: 2, Packers: 1}); err != nil {
		t.Fatal(err)
	}
	a, b := f.acquire(&f.pickers), f.acquire(&f.pickers)
	if a == nil || b == nil || a == b {
		t.Fatalf("two pickers must get two different stations, got %v and %v", a, b)
	}
	if c := f.acquire(&f.pickers); c != nil {
		t.Fatalf("a third task was handed to %s while both pickers were busy", c.ID)
	}
	if p := f.acquire(&f.packers); p == nil || p.Capability != "pack" {
		t.Fatalf("the packer pool is separate from the pickers': %+v", p)
	}
	f.free(a)
	if c := f.acquire(&f.pickers); c != a {
		t.Fatalf("a freed station must be available again, got %v", c)
	}
}
