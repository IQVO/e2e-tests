package inbound

import (
	"context"
	"strings"
	"testing"

	"github.com/claudioed/warehouse-e2e/internal/sim"
	"github.com/claudioed/warehouse-e2e/internal/sim/outbound"
)

func check(t *testing.T, r Readiness, name string) outbound.Check {
	t.Helper()
	for _, c := range r.Checks {
		if strings.Contains(c.Name, name) {
			return c
		}
	}
	t.Fatalf("no check named like %q in:\n%s", name, r)
	return outbound.Check{}
}

func TestReadyOnAFullyProvisionedSiteWritesNothing(t *testing.T) {
	w := newWorld(t)
	site := w.seedSite()
	w.with(func() { w.recv.doors = append(w.recv.doors, "OTHER9-IB-DR-A01-01-01-A") }) // another building's door is not ours
	before := w.writes()
	r := Ready(context.Background(), w.client, site, ReadyOptions{})
	if !r.Ready() {
		t.Fatalf("not ready:\n%s", r)
	}
	if r.Writes != 0 || w.writes() != before {
		t.Errorf("writes = %d (%d on the wire), want 0", r.Writes, w.writes()-before)
	}
	if got := strings.Join(r.Doors, ","); got != inDoorA+","+inDoorB {
		t.Errorf("doors = %s, want the site's two inbound doors only", got)
	}
	if got := strings.Join(r.Bins, ","); got != binA+","+binB {
		t.Errorf("bins = %s", got)
	}
	if r.Calendar.SlotGranularityMinutes != 5 {
		t.Errorf("calendar = %+v, want the declared 5 minute grid", r.Calendar)
	}
	if !strings.Contains(r.String(), "inbound readiness of AUS3: READY") {
		t.Errorf("report text = %q", r.String())
	}
}

func TestReadyAppliesWhatItFoundToASiteThatNamedNothing(t *testing.T) {
	w := newWorld(t)
	w.seedSite()
	r := Ready(context.Background(), w.client, Site{Code: siteCode}, ReadyOptions{})
	if r.Ready() {
		t.Fatalf("a site with no configured bin must not be ready:\n%s", r)
	}
	got := r.Apply(Site{Code: siteCode, Bins: []string{binA}})
	if len(got.Doors) != 2 || got.Doors[0] != inDoorA || len(got.Bins) != 1 || got.Bins[0] != binA {
		t.Errorf("Apply = %+v, want the found doors filled in and the configured bin kept", got)
	}
}

func TestReadyNamesWhatIsMissing(t *testing.T) {
	for _, tc := range []struct {
		name   string
		break_ func(w *world, s *Site)
		check  string
		status outbound.Status
		detail string
	}{
		{"no inbound door known to inbound-receiving", func(w *world, s *Site) { w.recv.doors = nil; s.Doors = nil }, "inbound doors", outbound.StatusMissing, "no inbound door"},
		{"a configured door inbound-receiving does not list", func(w *world, s *Site) { s.Doors = []string{inDoorA, "AUS3-IB-DR-Z99-01-01-A"} }, "door AUS3-IB-DR-Z99", outbound.StatusMissing, "not an inbound door"},
		{"calendar never declared", func(w *world, s *Site) { delete(w.dock.calendar, siteCode) }, "dock calendar", outbound.StatusMissing, "never declared"},
		{"yard board without the doors", func(w *world, s *Site) { w.yard.doors = map[string]string{} }, "yard board", outbound.StatusMissing, "not on the board"},
		{"a bin inventory-storage does not know", func(w *world, s *Site) { s.Bins = []string{binA, "AUS3-POD-BIN-Z99-01-01-A"} }, "bin AUS3-POD-BIN-Z99", outbound.StatusMissing, "not registered"},
		{"a full bin", func(w *world, s *Site) { w.inv.bins[binB].capacity = w.inv.bins[binB].occupied }, "bin " + binB, outbound.StatusMismatch, "full"},
		{"no bin configured", func(w *world, s *Site) { s.Bins = nil }, "stow bins", outbound.StatusMissing, "no bin configured"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newWorld(t)
			site := w.seedSite()
			w.with(func() { tc.break_(w, &site) })
			r := Ready(context.Background(), w.client, site, ReadyOptions{})
			if r.Ready() {
				t.Fatalf("ready although %s:\n%s", tc.name, r)
			}
			c := check(t, r, tc.check)
			if c.Status != tc.status || !strings.Contains(c.Detail, tc.detail) {
				t.Errorf("check %q = %s %q, want %s mentioning %q", c.Name, c.Status, c.Detail, tc.status, tc.detail)
			}
		})
	}
}

func TestReadyRegistersAMissingBinOnlyWhenAskedAndOnlyOnce(t *testing.T) {
	w := newWorld(t)
	site := w.seedSite()
	site.Bins = []string{binA, "AUS3-POD-BIN-Z99-01-01-A"}

	if r := Ready(context.Background(), w.client, site, ReadyOptions{}); r.Ready() || r.Writes != 0 {
		t.Fatalf("a read-only run must report the bin missing and write nothing:\n%s", r)
	}
	before := w.writes()
	r := Ready(context.Background(), w.client, site, ReadyOptions{BinCapacity: 500})
	if !r.Ready() || r.Writes != 1 || w.writes()-before != 1 {
		t.Fatalf("first run: writes %d (%d on the wire), ready %v:\n%s", r.Writes, w.writes()-before, r.Ready(), r)
	}
	if c := check(t, r, "bin AUS3-POD-BIN-Z99"); c.Status != outbound.StatusCreated {
		t.Errorf("check = %s, want created", c.Status)
	}
	again := w.writes()
	r = Ready(context.Background(), w.client, site, ReadyOptions{BinCapacity: 500})
	if !r.Ready() || r.Writes != 0 || w.writes() != again {
		t.Errorf("second run added %d POST/PUT, want 0:\n%s", w.writes()-again, r)
	}
}

func TestReadyWithServicesThatAreNotThereIsAnErrorPerCheckNotAPanic(t *testing.T) {
	c := sim.NewClient(map[string]string{}, sim.NewCallStats())
	r := Ready(context.Background(), c, Site{Code: siteCode, Doors: []string{inDoorA}, Bins: []string{binA}}, ReadyOptions{BinCapacity: 100})
	if r.Ready() || r.Writes != 0 {
		t.Fatalf("ready with nothing there, or wrote:\n%s", r)
	}
	for _, name := range []string{"inbound doors", "dock calendar", "yard board", "bin " + binA} {
		if c := check(t, r, name); c.Status != outbound.StatusError {
			t.Errorf("check %q = %s, want error", c.Name, c.Status)
		}
	}
}
