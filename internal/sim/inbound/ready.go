package inbound

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"

	"github.com/claudioed/warehouse-e2e/internal/sim"
	"github.com/claudioed/warehouse-e2e/internal/sim/outbound"
)

// ReadyOptions steer Ready.
type ReadyOptions struct {
	// BinCapacity, when positive, lets Ready register a configured bin inventory-storage does not know, through the
	// route that owns it (PUT /bins/{binId}, declarative and idempotent: a bin that exists is left untouched). Zero
	// writes nothing: every check is then a read.
	BinCapacity int
	Log         sim.LogFunc
}

// Readiness is the outcome of one readiness check: outbound's Report (checks, writes, doors, calendar), plus the
// bins found usable. It is not a boolean: the checks say what is missing and why.
type Readiness struct {
	outbound.Report
	Bins []string // configured bins that exist in inventory-storage and have room
}

func (r Readiness) String() string {
	var b strings.Builder
	state := "READY"
	if !r.Ready() {
		state = "NOT READY"
	}
	fmt.Fprintf(&b, "inbound readiness of %s: %s (%d writes)\n", r.Site, state, r.Writes)
	for _, c := range r.Checks {
		fmt.Fprintf(&b, "  %-8s %-24s %-40s %s\n", c.Status, c.Area, c.Name, c.Detail)
	}
	return b.String()
}

// Apply fills the doors and bins the site left empty from what Ready found usable.
func (r Readiness) Apply(s Site) Site {
	if len(s.Doors) == 0 {
		s.Doors = append([]string(nil), r.Doors...)
	}
	if len(s.Bins) == 0 {
		s.Bins = append([]string(nil), r.Bins...)
	}
	return s
}

// Configure applies what Ready found to the crew: the doors and bins its site left empty (Apply) and the dock
// calendar's slot granularity, so the appointment windows start and end on the grid the site declared. A calendar
// that was not read (zero grid) leaves the crew's grid as it was.
func (r Readiness) Configure(c *Crew) {
	c.Site = r.Apply(c.Site)
	if g := r.Calendar.SlotGranularityMinutes; g > 0 {
		c.GridMinutes = g
	}
}

type ready struct {
	c    *sim.Client
	site Site
	opt  ReadyOptions
	rep  *Readiness
}

// Ready checks that the building can take a delivery and creates ONLY what is missing, each through the route of the
// service that owns it (today only a bin, and only with ReadyOptions.BinCapacity):
//
//   - inbound-receiving: the site's inbound dock doors, as inbound-receiving knows them from facility-layout's events
//     (GET /docks lists only Inbound and Both doors, of every site: this keeps the site's). Every configured door must
//     be listed; with none configured it reports the ones it found instead of inventing codes.
//   - dock-scheduling: the site declared its dock calendar settings (a never-declared site answers version 0). Never
//     written here: the outbound readiness declares it for the same site, and one declaration serves both directions.
//   - yard-management (read only): its yard board knows the doors. It learns them from facility-layout's events, not
//     from a route, so a door created a moment ago may still be missing: run Ready again.
//   - inventory-storage: every configured stow bin exists and has room.
//
// It is idempotent: a second run against the state it left sends no POST or PUT. It never panics and does not return
// an error: every failure is a Check.
func Ready(ctx context.Context, c *sim.Client, site Site, opt ReadyOptions) Readiness {
	p := &ready{c: c, site: site, opt: opt, rep: &Readiness{Report: outbound.Report{Site: site.Code}}}
	p.doors(ctx)
	p.calendar(ctx)
	p.yardBoard(ctx)
	p.bins(ctx)
	if opt.Log != nil {
		for _, line := range strings.Split(strings.TrimRight(p.rep.String(), "\n"), "\n") {
			opt.Log("ready", "%s", line)
		}
	}
	return *p.rep
}

func (p *ready) add(area, name string, st outbound.Status, format string, args ...any) {
	p.rep.Checks = append(p.rep.Checks, outbound.Check{Area: area, Name: name, Status: st, Detail: fmt.Sprintf(format, args...)})
}

// ---------------------------------------------------------------------
// inbound-receiving: the inbound doors
// ---------------------------------------------------------------------

func (p *ready) doors(ctx context.Context) {
	const area = SvcInbound
	type door struct {
		DoorCode string `json:"doorCode"`
		DockFlow string `json:"dockFlow"`
	}
	all, r := sim.ListAll[door](ctx, p.c, SvcInbound, "/docks", "/docks")
	if !r.OK() {
		p.add(area, "inbound doors", outbound.StatusError, "%v", refusal("list the inbound doors", r))
		return
	}
	known := map[string]bool{}
	var mine []string
	for _, d := range all {
		known[d.DoorCode] = true
		if strings.HasPrefix(d.DoorCode, p.site.Code+"-") {
			mine = append(mine, d.DoorCode)
		}
	}
	sort.Strings(mine)

	if len(p.site.Doors) == 0 {
		if len(mine) == 0 {
			p.add(area, "inbound doors", outbound.StatusMissing, "none configured and no inbound door of %s known to inbound-receiving (it learns doors from facility-layout's events; only Inbound and Both doors are listed)", p.site.Code)
			return
		}
		p.add(area, "inbound doors", outbound.StatusOK, "none configured; found %s", strings.Join(mine, ","))
		p.rep.Doors = mine
		return
	}
	var ok []string
	for _, d := range p.site.Doors {
		if known[d] {
			p.add(area, "door "+d, outbound.StatusOK, "listed as an inbound door")
			ok = append(ok, d)
		} else {
			p.add(area, "door "+d, outbound.StatusMissing, "not an inbound door known to inbound-receiving (an outbound-only door is never listed; a new one is learned from facility-layout's events: retry shortly)")
		}
	}
	sort.Strings(ok)
	p.rep.Doors = ok
}

// ---------------------------------------------------------------------
// dock-scheduling: the calendar settings
// ---------------------------------------------------------------------

func (p *ready) calendar(ctx context.Context) {
	const area, name = SvcDock, "dock calendar settings"
	path := "/sites/" + url.PathEscape(p.site.Code) + "/dock-calendar"
	r := p.c.Call(ctx, SvcDock, http.MethodGet, "/sites/{id}/dock-calendar", path, nil)
	if !r.OK() {
		p.add(area, name, outbound.StatusError, "%v", refusal("read "+path, r))
		return
	}
	var cal struct {
		Version  int                       `json:"version"`
		Settings outbound.CalendarSettings `json:"settings"`
	}
	if err := r.Decode(&cal); err != nil {
		p.add(area, name, outbound.StatusError, "%v", err)
		return
	}
	p.rep.Calendar = cal.Settings
	if cal.Version > 0 {
		p.add(area, name, outbound.StatusOK, "declared (version %d): grid %d min, arrival tolerance %d min", cal.Version, cal.Settings.SlotGranularityMinutes, cal.Settings.ArrivalToleranceMinutes)
		return
	}
	p.add(area, name, outbound.StatusMissing, "never declared (defaults answer at version 0); not written here: declare it with outbound.Provision, or PUT %s/settings", path)
}

// ---------------------------------------------------------------------
// yard-management: what it learned (read only)
// ---------------------------------------------------------------------

func (p *ready) yardBoard(ctx context.Context) {
	const area, name = SvcYard, "yard board knows the doors"
	path := "/sites/" + url.PathEscape(p.site.Code) + "/yard-board"
	r := p.c.Call(ctx, SvcYard, http.MethodGet, "/sites/{id}/yard-board", path, nil)
	if r.Status == http.StatusNotFound {
		p.add(area, name, outbound.StatusMissing, "no locations known for the site yet: yard-management learns them from facility-layout's events (no route to write them); retry shortly")
		return
	}
	if !r.OK() {
		p.add(area, name, outbound.StatusError, "%v", refusal("read "+path, r))
		return
	}
	var board struct {
		Spots []struct {
			Code string `json:"code"`
			Kind string `json:"kind"`
		} `json:"spots"`
	}
	if err := r.Decode(&board); err != nil {
		p.add(area, name, outbound.StatusError, "%v", err)
		return
	}
	onBoard := map[string]bool{}
	for _, s := range board.Spots {
		if s.Kind == "DOOR" {
			onBoard[s.Code] = true
		}
	}
	if len(p.rep.Doors) == 0 {
		p.add(area, name, outbound.StatusMissing, "no usable door to look for")
		return
	}
	var missing []string
	for _, d := range p.rep.Doors {
		if !onBoard[d] {
			missing = append(missing, d)
		}
	}
	if len(missing) > 0 {
		p.add(area, name, outbound.StatusMissing, "doors %s are not on the board yet: the facility events are still being consumed; retry shortly", strings.Join(missing, ","))
		return
	}
	p.add(area, name, outbound.StatusOK, "%d door(s) on the board", len(p.rep.Doors))
}

// ---------------------------------------------------------------------
// inventory-storage: the stow bins
// ---------------------------------------------------------------------

func (p *ready) bins(ctx context.Context) {
	const area = SvcInventory
	if len(p.site.Bins) == 0 {
		p.add(area, "stow bins", outbound.StatusMissing, "no bin configured: name the inventory-storage bins stock may be stowed into (Site.Bins)")
		return
	}
	var ok []string
	for _, b := range p.site.Bins {
		name := "bin " + b
		path := "/bins/" + url.PathEscape(b)
		r := p.c.Call(ctx, SvcInventory, http.MethodGet, "/bins/{binId}", path, nil)
		switch {
		case r.Status == http.StatusNotFound && outbound.Slug(r) == "bin-not-found":
			if p.opt.BinCapacity <= 0 {
				p.add(area, name, outbound.StatusMissing, "not registered in inventory-storage; not written: no BinCapacity given")
				continue
			}
			p.rep.Writes++
			w := p.c.Call(ctx, SvcInventory, http.MethodPut, "/bins/{binId}", path, map[string]any{"capacity": p.opt.BinCapacity})
			if !w.OK() {
				p.add(area, name, outbound.StatusRefused, "%v", refusal("PUT "+path, w))
				continue
			}
			p.add(area, name, outbound.StatusCreated, "registered with capacity %d", p.opt.BinCapacity)
			ok = append(ok, b)
		case !r.OK():
			p.add(area, name, outbound.StatusError, "%v", refusal("read "+path, r))
		default:
			var got struct {
				Capacity  int `json:"capacity"`
				Available int `json:"available"`
			}
			if err := r.Decode(&got); err != nil {
				p.add(area, name, outbound.StatusError, "%v", err)
				continue
			}
			if got.Available <= 0 {
				p.add(area, name, outbound.StatusMismatch, "bin is full (capacity %d): nothing can be stowed into it", got.Capacity)
				continue
			}
			p.add(area, name, outbound.StatusOK, "room for %d of %d units", got.Available, got.Capacity)
			ok = append(ok, b)
		}
	}
	p.rep.Bins = ok
}
