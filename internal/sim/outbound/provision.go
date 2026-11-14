package outbound

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"

	"github.com/claudioed/warehouse-e2e/internal/sim"
)

// CalendarSettings are dock-scheduling's per-site booking rules.
type CalendarSettings struct {
	MaxWindowMinutes        int `json:"maxWindowMinutes"`
	SlotGranularityMinutes  int `json:"slotGranularityMinutes"`
	ArrivalToleranceMinutes int `json:"arrivalToleranceMinutes"`
	NoShowGraceMinutes      int `json:"noShowGraceMinutes"`
}

// DefaultCalendar is what the yard journey declares: a 5-minute slot grid, so a booking can start a few
// minutes from now, and a 30-minute arrival tolerance, so a carrier that arrives that early is sent
// straight to the door instead of to a holding spot.
var DefaultCalendar = CalendarSettings{MaxWindowMinutes: 240, SlotGranularityMinutes: 5, ArrivalToleranceMinutes: 30, NoShowGraceMinutes: 60}

// ProvisionOptions steer Provision.
type ProvisionOptions struct {
	// Cutoffs is the CPT schedule the site must have: every cutoff listed must be in it with the same
	// local time, days and ship method (cutoffs the site has besides are left alone). Empty means "any
	// schedule with at least one cutoff will do".
	Cutoffs []Cutoff
	// AllowWriteSchedule lets Provision PUT the site's CPT schedule when it is missing or differs. It is
	// false by default and must stay false for the storefront building: that schedule is owned by the
	// storefront seeder, and a PUT replaces it as a whole.
	AllowWriteSchedule bool
	// Calendar is declared only when the site never declared dock-calendar settings (version 0); zero
	// means DefaultCalendar. A site that declared its own keeps them.
	Calendar CalendarSettings
	// DoorLocationType and StagingLocationType name the facility-layout location type created for a
	// missing slot when no location type of that role exists yet (one that exists is used as it is).
	DoorLocationType    string // default "OutboundDoor"
	StagingLocationType string // default "StagingLane"
	// ReadOnly reports what is missing and writes nothing.
	ReadOnly bool
	Log      sim.LogFunc
}

// Status is the outcome of one readiness check.
type Status string

const (
	StatusOK       Status = "ok"       // there, as required
	StatusCreated  Status = "created"  // was missing; written through the owning route
	StatusMissing  Status = "missing"  // absent and not written (read-only, not allowed, or not creatable over REST)
	StatusMismatch Status = "mismatch" // present but not as required, and not fixable from here
	StatusRefused  Status = "refused"  // a write was attempted and the service refused it
	StatusError    Status = "error"    // a read failed
)

// Check is one thing the outbound chain needs.
type Check struct {
	Area   string // owning service
	Name   string
	Status Status
	Detail string
}

// Report is what Provision found and did. It is not a boolean: the checks say what is missing and why.
type Report struct {
	Site   string
	Checks []Check
	Writes int // POST/PUT requests sent
	// What was found usable, to configure the Floor with.
	Doors            []string // OUTBOUND/Both-flow Active dock slots of the site
	StagingLocations []string // Active Staging slots of the site
	Calendar         CalendarSettings
	Timezone         string // of the CPT schedule as read
	Cutoffs          []Cutoff
}

// Ready is true when no check is missing, mismatched, refused or unreadable.
func (r Report) Ready() bool { return len(r.NotReady()) == 0 }

// NotReady lists the checks that are not OK or Created.
func (r Report) NotReady() []Check {
	var out []Check
	for _, c := range r.Checks {
		if c.Status != StatusOK && c.Status != StatusCreated {
			out = append(out, c)
		}
	}
	return out
}

// Apply fills the doors and staging locations the site left empty from what Provision found usable.
func (r Report) Apply(s Site) Site {
	if len(s.Doors) == 0 {
		s.Doors = append([]string(nil), r.Doors...)
	}
	if len(s.StagingLocations) == 0 {
		s.StagingLocations = append([]string(nil), r.StagingLocations...)
	}
	if s.Timezone == "" {
		s.Timezone = r.Timezone
	}
	return s
}

func (r Report) String() string {
	var b strings.Builder
	state := "READY"
	if !r.Ready() {
		state = "NOT READY"
	}
	fmt.Fprintf(&b, "outbound readiness of %s: %s (%d writes)\n", r.Site, state, r.Writes)
	for _, c := range r.Checks {
		fmt.Fprintf(&b, "  %-8s %-24s %-40s %s\n", c.Status, c.Area, c.Name, c.Detail)
	}
	return b.String()
}

type prov struct {
	c    *sim.Client
	site Site
	opt  ProvisionOptions
	rep  *Report

	siteKnown    bool
	zones        map[string]bool
	zonesRead    bool
	aisles       map[string]bool
	locationType map[string]string // role -> name found or created
	types        map[string]string // name -> role, once read
	typesRead    bool
}

// Provision checks that the building can ship a package and creates ONLY what is missing, each through the
// route of the service that owns it:
//
//   - process-path-management: the site's CPT schedule exists and has the wanted cutoffs. Written (PUT, a
//     whole-schedule replacement) only with opt.AllowWriteSchedule: the storefront building's schedule belongs
//     to its seeder.
//   - facility-layout: the site, and an Active OUTBOUND-flow dock slot for every door and an Active Staging slot
//     for every staging location of Site (zone, aisle and location type created only when absent). With Site.Doors
//     or Site.StagingLocations empty it reports the usable ones it found instead of inventing codes.
//   - dock-scheduling: the site declared its dock calendar settings (a never-declared site answers version 0).
//   - yard-management (read only): its yard board knows the site's doors. It learns them from facility-layout's
//     events, not from a route, so a freshly created door may still be missing here: run Provision again.
//
// It is idempotent: a second run against the state it left sends no POST or PUT. It never panics and does not
// return an error: every failure is a Check.
func Provision(ctx context.Context, c *sim.Client, site Site, opt ProvisionOptions) Report {
	p := &prov{c: c, site: site, opt: opt, rep: &Report{Site: site.Code}, zones: map[string]bool{}, aisles: map[string]bool{}, locationType: map[string]string{}, types: map[string]string{}}
	p.schedule(ctx)
	p.facility(ctx)
	p.calendar(ctx)
	p.yardBoard(ctx)
	if opt.Log != nil {
		for _, line := range strings.Split(strings.TrimRight(p.rep.String(), "\n"), "\n") {
			opt.Log("provision", "%s", line)
		}
	}
	return *p.rep
}

func (p *prov) add(area, name string, st Status, format string, args ...any) {
	p.rep.Checks = append(p.rep.Checks, Check{Area: area, Name: name, Status: st, Detail: fmt.Sprintf(format, args...)})
}

// write sends one POST or PUT (never in read-only mode) and counts it.
func (p *prov) write(ctx context.Context, svc, method, route, path string, body any) sim.Resp {
	p.rep.Writes++
	return p.c.Call(ctx, svc, method, route, path, body)
}

// ---------------------------------------------------------------------
// process-path-management: the CPT schedule
// ---------------------------------------------------------------------

type scheduleDTO struct {
	Timezone string   `json:"timezone"`
	Cutoffs  []Cutoff `json:"cutoffs"`
}

func sameSet(a, b []string) bool {
	x, y := append([]string(nil), a...), append([]string(nil), b...)
	sort.Strings(x)
	sort.Strings(y)
	return strings.Join(x, ",") == strings.Join(y, ",")
}

func hasAll(have, want []string) bool {
	set := map[string]bool{}
	for _, h := range have {
		set[h] = true
	}
	for _, w := range want {
		if !set[w] {
			return false
		}
	}
	return true
}

// scheduleDiff says how the schedule read differs from what is wanted ("" = it matches).
func scheduleDiff(have scheduleDTO, tz string, want []Cutoff) []string {
	var diffs []string
	if tz != "" && have.Timezone != tz {
		diffs = append(diffs, fmt.Sprintf("timezone %q, want %q", have.Timezone, tz))
	}
	if len(want) == 0 {
		if len(have.Cutoffs) == 0 {
			diffs = append(diffs, "no cutoff")
		}
		return diffs
	}
	byID := map[string]Cutoff{}
	for _, c := range have.Cutoffs {
		byID[c.CptID] = c
	}
	for _, w := range want {
		h, ok := byID[w.CptID]
		switch {
		case !ok:
			diffs = append(diffs, fmt.Sprintf("cutoff %s absent", w.CptID))
		case h.LocalTime != w.LocalTime || !sameSet(h.DaysOfWeek, w.DaysOfWeek) || h.ShipMethod != w.ShipMethod || !hasAll(h.EligiblePathIDs, w.EligiblePathIDs):
			diffs = append(diffs, fmt.Sprintf("cutoff %s is %s %v %s paths %v, want %s %v %s paths %v", w.CptID,
				h.LocalTime, h.DaysOfWeek, h.ShipMethod, h.EligiblePathIDs, w.LocalTime, w.DaysOfWeek, w.ShipMethod, w.EligiblePathIDs))
		}
	}
	return diffs
}

func (p *prov) schedule(ctx context.Context) {
	const area, name = SvcProcessPath, "cpt schedule"
	path := "/sites/" + url.PathEscape(p.site.Code) + "/cpt-schedule"
	r := p.c.Call(ctx, SvcProcessPath, http.MethodGet, "/sites/{id}/cpt-schedule", path, nil)
	var have scheduleDTO
	var diffs []string
	switch {
	case r.Status == http.StatusNotFound:
		diffs = []string{"no schedule defined for the site"}
	case !r.OK():
		p.add(area, name, StatusError, "%v", refusal("read "+path, r))
		return
	default:
		if err := r.Decode(&have); err != nil {
			p.add(area, name, StatusError, "%v", err)
			return
		}
		diffs = scheduleDiff(have, p.site.Timezone, p.opt.Cutoffs)
		p.rep.Timezone, p.rep.Cutoffs = have.Timezone, have.Cutoffs
	}
	if len(diffs) == 0 {
		p.add(area, name, StatusOK, "timezone %s, %d cutoffs", have.Timezone, len(have.Cutoffs))
		return
	}
	state := StatusMissing
	if r.OK() {
		state = StatusMismatch
	}
	why := strings.Join(diffs, "; ")
	switch {
	case !p.opt.AllowWriteSchedule:
		p.add(area, name, state, "%s; not written: AllowWriteSchedule is false (a site's schedule belongs to its owner)", why)
		return
	case p.opt.ReadOnly:
		p.add(area, name, state, "%s; not written: read-only run", why)
		return
	case len(p.opt.Cutoffs) == 0 || p.site.Timezone == "":
		p.add(area, name, state, "%s; cannot write: the wanted cutoffs and the site's timezone are needed", why)
		return
	}
	// A PUT replaces the whole schedule: keep the cutoffs the site has besides the wanted ones.
	out := append([]Cutoff(nil), p.opt.Cutoffs...)
	wanted := map[string]bool{}
	for _, w := range p.opt.Cutoffs {
		wanted[w.CptID] = true
		if len(w.EligiblePathIDs) == 0 {
			p.add(area, name, StatusRefused, "cutoff %s has no EligiblePathIDs: process-path-management requires at least one Active process path", w.CptID)
			return
		}
	}
	for _, h := range have.Cutoffs {
		if !wanted[h.CptID] {
			out = append(out, h)
		}
	}
	w := p.write(ctx, SvcProcessPath, http.MethodPut, "/sites/{id}/cpt-schedule", path, map[string]any{"timezone": p.site.Timezone, "cutoffs": out})
	if !w.OK() {
		p.add(area, name, StatusRefused, "%v", refusal("PUT "+path, w))
		return
	}
	p.rep.Timezone, p.rep.Cutoffs = p.site.Timezone, out
	p.add(area, name, StatusCreated, "PUT %s: %s", path, why)
}

// ---------------------------------------------------------------------
// facility-layout: the site, its OUTBOUND doors and its Staging lanes
// ---------------------------------------------------------------------

type slotDTO struct {
	LocationCode string `json:"locationCode"`
	LocationType string `json:"locationType"`
	Role         string `json:"role"`
	DockFlow     string `json:"dockFlow"`
	Status       string `json:"status"`
}

func (p *prov) facility(ctx context.Context) {
	const area = SvcFacility
	site := url.PathEscape(p.site.Code)
	r := p.c.Call(ctx, SvcFacility, http.MethodGet, "/sites/{code}", "/sites/"+site, nil)
	switch {
	case r.OK():
		p.siteKnown = true
		p.add(area, "site "+p.site.Code, StatusOK, "registered")
	case r.Status != http.StatusNotFound:
		p.add(area, "site "+p.site.Code, StatusError, "%v", refusal("read site "+p.site.Code, r))
		return
	case p.opt.ReadOnly:
		p.add(area, "site "+p.site.Code, StatusMissing, "not registered; not written: read-only run")
	default:
		w := p.write(ctx, SvcFacility, http.MethodPost, "/sites", "/sites", map[string]any{"siteCode": p.site.Code, "name": "Outbound " + p.site.Code})
		if w.OK() || w.Status == http.StatusConflict {
			p.siteKnown = true
			p.add(area, "site "+p.site.Code, StatusCreated, "registered through POST /sites")
		} else {
			p.add(area, "site "+p.site.Code, StatusRefused, "%v", refusal("register site "+p.site.Code, w))
		}
	}
	p.slots(ctx, "Dock", "dock door", p.site.Doors, true)
	p.slots(ctx, "Staging", "staging slot", p.site.StagingLocations, false)
}

func (p *prov) slots(ctx context.Context, role, label string, want []string, outbound bool) {
	const area = SvcFacility
	var have []slotDTO
	if p.siteKnown {
		var r sim.Resp
		have, r = listAll[slotDTO](ctx, p.c, SvcFacility, "/sites/{code}/locations?role", "/sites/"+url.PathEscape(p.site.Code)+"/locations?role="+role)
		if !r.OK() {
			p.add(area, label+"s", StatusError, "%v", refusal("list "+role+" slots of "+p.site.Code, r))
			return
		}
	}
	byCode := map[string]slotDTO{}
	var usable []string
	for _, s := range have {
		byCode[s.LocationCode] = s
		if s.Status == "Active" && (!outbound || s.DockFlow == "Outbound" || s.DockFlow == "Both") {
			usable = append(usable, s.LocationCode)
		}
	}
	sort.Strings(usable)

	if len(want) == 0 {
		if len(usable) == 0 {
			what := role
			if outbound {
				what = "OUTBOUND-flow dock"
			}
			p.add(area, label+"s", StatusMissing, "none configured and no Active %s slot at %s: configure the location codes to create", what, p.site.Code)
			return
		}
		p.add(area, label+"s", StatusOK, "none configured; found %s", strings.Join(usable, ","))
		p.setUsable(role, usable)
		return
	}
	var ok []string
	for _, code := range want {
		name := label + " " + code
		s, exists := byCode[code]
		switch {
		case exists && s.Status != "Active":
			p.add(area, name, StatusMismatch, "slot is %s, want Active: a retired code is never re-registered", s.Status)
		case exists && outbound && s.DockFlow != "Outbound" && s.DockFlow != "Both":
			p.add(area, name, StatusMismatch, "dock flow %q, want Outbound or Both (it cannot be changed after registration)", s.DockFlow)
		case exists:
			p.add(area, name, StatusOK, "type %s", s.LocationType)
			ok = append(ok, code)
		case !p.siteKnown || p.opt.ReadOnly:
			why := "read-only run"
			if !p.siteKnown {
				why = "the site is not registered"
			}
			p.add(area, name, StatusMissing, "no %s slot with this code; not written: %s", role, why)
		default:
			if err := p.createSlot(ctx, code, role, outbound); err != nil {
				st := StatusRefused
				if strings.Contains(err.Error(), "already registered") {
					st = StatusMismatch
				}
				p.add(area, name, st, "%v", err)
				continue
			}
			p.add(area, name, StatusCreated, "registered as a %s slot", role)
			ok = append(ok, code)
		}
	}
	p.setUsable(role, ok)
}

func (p *prov) setUsable(role string, codes []string) {
	sort.Strings(codes)
	if role == "Dock" {
		p.rep.Doors = codes
	} else {
		p.rep.StagingLocations = codes
	}
}

// createSlot registers one slot through facility-layout's chain of custody: location type, zone, aisle, slot,
// creating only the links that are absent. A 409 on a parent means another run created it first.
func (p *prov) createSlot(ctx context.Context, code, role string, outbound bool) error {
	segs := strings.Split(code, "-")
	if len(segs) != 7 || segs[0] != p.site.Code {
		return fmt.Errorf("location code %q is not SITE-AREA-ZONE-AISLE-BAY-LEVEL-POSITION of site %s", code, p.site.Code)
	}
	zone, aisle := strings.Join(segs[:3], "-"), strings.Join(segs[:4], "-")

	typ, err := p.ensureType(ctx, role)
	if err != nil {
		return err
	}
	if err := p.ensureZone(ctx, zone, segs[1], segs[2]); err != nil {
		return err
	}
	if err := p.ensureAisle(ctx, zone, aisle, segs[3]); err != nil {
		return err
	}
	body := map[string]any{"locationCode": code, "locationType": typ}
	if outbound {
		body["dockFlow"] = "Outbound"
	}
	w := p.write(ctx, SvcFacility, http.MethodPost, "/locations", "/locations", body)
	switch {
	case w.OK():
		return nil
	case w.Status == http.StatusConflict:
		return fmt.Errorf("location %s is already registered, but not as an Active %s slot of this site: it cannot be re-registered (%s)", code, role, Slug(w))
	}
	return refusal("register slot "+code, w)
}

func (p *prov) readTypes(ctx context.Context) error {
	if p.typesRead {
		return nil
	}
	type typeDTO struct {
		Name string `json:"name"`
		Role string `json:"role"`
	}
	types, r := listAll[typeDTO](ctx, p.c, SvcFacility, "/location-types", "/location-types")
	if !r.OK() {
		return refusal("list location types", r)
	}
	for _, t := range types {
		p.types[t.Name] = t.Role
	}
	p.typesRead = true
	return nil
}

// ensureType returns the name of a location type of the role: the configured one if it exists, else any that
// does, else a new one.
func (p *prov) ensureType(ctx context.Context, role string) (string, error) {
	if n, ok := p.locationType[role]; ok {
		return n, nil
	}
	if err := p.readTypes(ctx); err != nil {
		return "", err
	}
	configured, def := p.opt.DoorLocationType, "OutboundDoor"
	if role == "Staging" {
		configured, def = p.opt.StagingLocationType, "StagingLane"
	}
	if configured != "" && p.types[configured] == role {
		p.locationType[role] = configured
		return configured, nil
	}
	var names []string
	for n, r := range p.types {
		if r == role {
			names = append(names, n)
		}
	}
	sort.Strings(names)
	if len(names) > 0 {
		p.locationType[role] = names[0]
		return names[0], nil
	}
	name := firstNonEmpty(configured, def)
	body := map[string]any{"name": name, "role": role}
	if role == "Staging" {
		body["defaultCapacity"] = map[string]any{"maxWeightKg": 1000, "maxVolumeM3": 2}
	}
	w := p.write(ctx, SvcFacility, http.MethodPost, "/location-types", "/location-types", body)
	if !w.OK() && w.Status != http.StatusConflict {
		return "", refusal("register location type "+name, w)
	}
	p.types[name] = role
	p.locationType[role] = name
	return name, nil
}

func (p *prov) ensureZone(ctx context.Context, zone, areaCode, zoneCode string) error {
	if !p.zonesRead {
		type zoneDTO struct {
			ZoneID string `json:"zoneId"`
		}
		zs, r := listAll[zoneDTO](ctx, p.c, SvcFacility, "/sites/{code}/zones", "/sites/"+url.PathEscape(p.site.Code)+"/zones")
		if !r.OK() {
			return refusal("list zones of "+p.site.Code, r)
		}
		for _, z := range zs {
			p.zones[z.ZoneID] = true
		}
		p.zonesRead = true
	}
	if p.zones[zone] {
		return nil
	}
	w := p.write(ctx, SvcFacility, http.MethodPost, "/sites/{code}/zones", "/sites/"+url.PathEscape(p.site.Code)+"/zones",
		map[string]any{"areaCode": areaCode, "zoneCode": zoneCode, "temperatureClass": "Ambient"})
	if !w.OK() && w.Status != http.StatusConflict {
		return refusal("register zone "+zone, w)
	}
	p.zones[zone] = true
	return nil
}

func (p *prov) ensureAisle(ctx context.Context, zone, aisle, aisleCode string) error {
	if !p.aisles["read:"+zone] {
		type aisleDTO struct {
			AisleID string `json:"aisleId"`
		}
		as, r := listAll[aisleDTO](ctx, p.c, SvcFacility, "/zones/{id}/aisles", "/zones/"+url.PathEscape(zone)+"/aisles")
		switch {
		case r.OK():
			for _, a := range as {
				p.aisles[a.AisleID] = true
			}
		case r.Status != http.StatusNotFound: // 404: the zone was just created and has no aisle yet
			return refusal("list aisles of "+zone, r)
		}
		p.aisles["read:"+zone] = true
	}
	if p.aisles[aisle] {
		return nil
	}
	w := p.write(ctx, SvcFacility, http.MethodPost, "/zones/{id}/aisles", "/zones/"+url.PathEscape(zone)+"/aisles",
		map[string]any{"aisleCode": aisleCode, "sequenceHint": 1, "direction": "TwoWay"})
	if !w.OK() && w.Status != http.StatusConflict {
		return refusal("register aisle "+aisle, w)
	}
	p.aisles[aisle] = true
	return nil
}

// ---------------------------------------------------------------------
// dock-scheduling: the calendar settings
// ---------------------------------------------------------------------

func (p *prov) calendar(ctx context.Context) {
	const area, name = SvcDock, "dock calendar settings"
	path := "/sites/" + url.PathEscape(p.site.Code) + "/dock-calendar"
	r := p.c.Call(ctx, SvcDock, http.MethodGet, "/sites/{id}/dock-calendar", path, nil)
	if !r.OK() {
		p.add(area, name, StatusError, "%v", refusal("read "+path, r))
		return
	}
	var cal struct {
		Version  int              `json:"version"`
		Settings CalendarSettings `json:"settings"`
	}
	if err := r.Decode(&cal); err != nil {
		p.add(area, name, StatusError, "%v", err)
		return
	}
	p.rep.Calendar = cal.Settings
	if cal.Version > 0 {
		p.add(area, name, StatusOK, "declared (version %d): grid %d min, arrival tolerance %d min", cal.Version, cal.Settings.SlotGranularityMinutes, cal.Settings.ArrivalToleranceMinutes)
		return
	}
	if p.opt.ReadOnly {
		p.add(area, name, StatusMissing, "never declared (defaults answer at version 0); not written: read-only run")
		return
	}
	want := p.opt.Calendar
	if want == (CalendarSettings{}) {
		want = DefaultCalendar
	}
	w := p.write(ctx, SvcDock, http.MethodPut, "/sites/{id}/dock-calendar/settings", path+"/settings", want)
	if !w.OK() {
		p.add(area, name, StatusRefused, "%v", refusal("PUT "+path+"/settings", w))
		return
	}
	p.rep.Calendar = want
	p.add(area, name, StatusCreated, "declared: grid %d min, arrival tolerance %d min", want.SlotGranularityMinutes, want.ArrivalToleranceMinutes)
}

// ---------------------------------------------------------------------
// yard-management: what it learned (read only)
// ---------------------------------------------------------------------

func (p *prov) yardBoard(ctx context.Context) {
	const area, name = SvcYard, "yard board knows the doors"
	path := "/sites/" + url.PathEscape(p.site.Code) + "/yard-board"
	r := p.c.Call(ctx, SvcYard, http.MethodGet, "/sites/{id}/yard-board", path, nil)
	if r.Status == http.StatusNotFound {
		p.add(area, name, StatusMissing, "no locations known for the site yet: yard-management learns them from facility-layout's events (no route to write them); retry shortly")
		return
	}
	if !r.OK() {
		p.add(area, name, StatusError, "%v", refusal("read "+path, r))
		return
	}
	var board struct {
		Spots []struct {
			Code string `json:"code"`
			Kind string `json:"kind"`
		} `json:"spots"`
	}
	if err := r.Decode(&board); err != nil {
		p.add(area, name, StatusError, "%v", err)
		return
	}
	known := map[string]bool{}
	for _, s := range board.Spots {
		if s.Kind == "DOOR" {
			known[s.Code] = true
		}
	}
	doors := p.rep.Doors
	if len(doors) == 0 {
		p.add(area, name, StatusMissing, "no usable door to look for")
		return
	}
	var missing []string
	for _, d := range doors {
		if !known[d] {
			missing = append(missing, d)
		}
	}
	if len(missing) > 0 {
		p.add(area, name, StatusMissing, "doors %s are not on the board yet: the facility events are still being consumed; retry shortly", strings.Join(missing, ","))
		return
	}
	p.add(area, name, StatusOK, "%d door(s) on the board", len(doors))
}
