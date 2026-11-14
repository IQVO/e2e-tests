package outbound

// Contract fakes of the five services the outbound actors talk to. Each one
// enforces the rules its published contract states (and the ones its docs state
// for the refusals the actors must survive), so a test fails when an actor
// breaks an ordering rule, sends no Idempotency-Key, or retries what it must
// not. They are deliberately small state machines, not mocks: the chain
// (appointment -> door of the load -> TrailerDocked -> LOADING -> close ->
// TrailerDeparted -> SHIP_CONFIRMED / PackageMissedLoad) runs through them
// exactly as the real events would, with a "consumer lag" knob in front of each
// event hop.

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/claudioed/warehouse-e2e/internal/sim"
)

// ---------------------------------------------------------------------
// a tiny contract-fake framework
// ---------------------------------------------------------------------

type reply struct {
	status int
	body   any
	slug   string
	header map[string]string
}

func ok(v any) reply      { return reply{status: http.StatusOK, body: v} }
func created(v any) reply { return reply{status: http.StatusCreated, body: v} }
func refuse(status int, slug string) reply {
	return reply{status: status, slug: slug}
}

type fctx struct {
	r      *http.Request
	body   []byte
	params map[string]string
}

func (c *fctx) q(name string) string { return c.r.URL.Query().Get(name) }

// obj decodes the JSON body into a map; a body that is not an object is a 400.
func (c *fctx) obj() (map[string]any, *reply) {
	m := map[string]any{}
	if len(c.body) > 0 {
		if err := json.Unmarshal(c.body, &m); err != nil {
			r := refuse(http.StatusBadRequest, "malformed-request-body")
			return nil, &r
		}
	}
	return m, nil
}

type route struct {
	method   string
	template string
	parts    []string
	query    []string // allowed query parameters; nil = none checked
	checkQ   bool
	fn       func(c *fctx) reply
}

type stored struct {
	fingerprint string
	status      int
	body        []byte
}

type fake struct {
	w      *world
	name   string
	routes []*route
	idem   map[string]stored
	srv    *httptest.Server
	// codeInProblem adds the fleet `code` next to `type` (outbound-shipping's Problem).
	codeInProblem bool
	before        func() // runs under the world lock before every request (consumer lag ticks)
}

func (f *fake) handle(method, template string, fn func(c *fctx) reply) *route {
	rt := &route{method: method, template: template, parts: strings.Split(strings.Trim(template, "/"), "/"), fn: fn}
	f.routes = append(f.routes, rt)
	return rt
}

// allow declares the query parameters a GET route accepts; any other is a 400
// unknown-query-parameter, as the fleet's collections answer.
func (rt *route) allow(names ...string) *route { rt.query, rt.checkQ = names, true; return rt }

func (f *fake) match(r *http.Request) (*route, map[string]string) {
	segs := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	for _, rt := range f.routes {
		if rt.method != r.Method || len(rt.parts) != len(segs) {
			continue
		}
		params := map[string]string{}
		okm := true
		for i, p := range rt.parts {
			if strings.HasPrefix(p, "{") {
				params[p[1:len(p)-1]] = segs[i]
			} else if p != segs[i] {
				okm = false
				break
			}
		}
		if okm {
			return rt, params
		}
	}
	return nil, nil
}

func (f *fake) problemBody(status int, slug string) []byte {
	m := map[string]any{
		"type": "https://errors." + f.name + ".warehouse-systems.dev/" + slug, "title": slug, "status": status,
		"detail": slug, "instance": "/",
	}
	if f.codeInProblem {
		m["code"] = slug
	}
	b, _ := json.Marshal(m)
	return b
}

func (f *fake) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	f.w.mu.Lock()
	defer f.w.mu.Unlock()
	if f.before != nil {
		f.before()
	}
	body, _ := io.ReadAll(r.Body)
	rt, params := f.match(r)
	write := func(status int, b []byte, ct string, extra map[string]string) {
		rw.Header().Set("Content-Type", ct)
		for k, v := range extra {
			rw.Header().Set(k, v)
		}
		rw.WriteHeader(status)
		_, _ = rw.Write(b)
	}
	if rt == nil {
		write(http.StatusNotFound, f.problemBody(http.StatusNotFound, "not-found"), "application/problem+json", nil)
		return
	}
	f.w.count(f.name, r.Method, rt.template)
	if rt.checkQ {
		allowed := map[string]bool{}
		for _, a := range rt.query {
			allowed[a] = true
		}
		for k := range r.URL.Query() {
			if !allowed[k] {
				write(http.StatusBadRequest, f.problemBody(http.StatusBadRequest, "unknown-query-parameter"), "application/problem+json", nil)
				return
			}
		}
	}
	c := &fctx{r: r, body: body, params: params}
	var rep reply
	switch r.Method {
	case http.MethodPost:
		key := r.Header.Get("Idempotency-Key")
		f.w.posts = append(f.w.posts, post{svc: f.name, path: r.URL.Path, key: key})
		fp := r.URL.Path + "|" + string(body)
		switch st, seen := f.idem[key]; {
		case key == "":
			rep = refuse(http.StatusBadRequest, "idempotency-key-required")
		case seen && st.fingerprint != fp:
			rep = refuse(http.StatusConflict, "idempotency-key-reused")
		case seen:
			// A replay answers the stored response, a refusal included: the real services store a 4xx too.
			write(st.status, st.body, "application/json", map[string]string{"Idempotency-Replayed": "true"})
			return
		default:
			rep = rt.fn(c)
		}
	default:
		rep = rt.fn(c)
	}
	var out []byte
	ct := "application/json"
	if rep.slug != "" {
		out, ct = f.problemBody(rep.status, rep.slug), "application/problem+json"
	} else if rep.body != nil {
		out, _ = json.Marshal(rep.body)
	}
	if r.Method == http.MethodPost && r.Header.Get("Idempotency-Key") != "" && rep.status < 500 && rep.slug != "idempotency-key-reused" {
		f.idem[r.Header.Get("Idempotency-Key")] = stored{fingerprint: r.URL.Path + "|" + string(body), status: rep.status, body: out}
	}
	write(rep.status, out, ct, rep.header)
}

// page serves one keyset page of items (the cursor is the next offset).
func page(c *fctx, max int, items []any) reply {
	limit := 50
	if s := c.q("limit"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n < 1 || n > max {
			return refuse(http.StatusBadRequest, "invalid-parameter")
		}
		limit = n
	}
	start := 0
	if s := c.q("cursor"); s != "" {
		start, _ = strconv.Atoi(s)
	}
	end := min(start+limit, len(items))
	if start > len(items) {
		start = len(items)
	}
	out := map[string]any{"items": append([]any{}, items[start:end]...), "nextCursor": nil}
	if end < len(items) {
		out["nextCursor"] = strconv.Itoa(end)
	}
	return ok(out)
}

// ---------------------------------------------------------------------
// the world: five fakes behind one lock, wired like the event topics
// ---------------------------------------------------------------------

type post struct{ svc, path, key string }

type world struct {
	t   *testing.T
	mu  sync.Mutex
	seq int

	counts map[string]int // "svc METHOD template" -> requests served
	posts  []post

	facility *fakeFacility
	ppm      *fakePPM
	dock     *fakeDock
	yard     *fakeYard
	out      *fakeOutbound

	client *sim.Client
}

func (w *world) count(svc, method, template string) { w.counts[svc+" "+method+" "+template]++ }

func (w *world) id(prefix string) string {
	w.seq++
	return fmt.Sprintf("%s-%04d", prefix, w.seq)
}

// writes is how many POST/PUT requests the fakes served, per service (any answer).
func (w *world) writes() map[string]int {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := map[string]int{}
	for k, n := range w.counts {
		parts := strings.SplitN(k, " ", 3)
		if parts[1] == http.MethodPost || parts[1] == http.MethodPut {
			out[parts[0]] += n
		}
	}
	return out
}

func (w *world) totalWrites() int {
	n := 0
	for _, v := range w.writes() {
		n += v
	}
	return n
}

func newWorld(t *testing.T) *world {
	t.Helper()
	w := &world{t: t, counts: map[string]int{}}
	w.facility = newFakeFacility(w)
	w.ppm = newFakePPM(w)
	w.dock = newFakeDock(w)
	w.yard = newFakeYard(w)
	w.out = newFakeOutbound(w)
	base := map[string]string{}
	for svc, f := range map[string]*fake{
		SvcFacility: w.facility.fake, SvcProcessPath: w.ppm.fake, SvcDock: w.dock.fake, SvcYard: w.yard.fake, SvcOutbound: w.out.fake,
	} {
		f.srv = httptest.NewServer(f)
		t.Cleanup(f.srv.Close)
		base[svc] = f.srv.URL
	}
	w.client = sim.NewClient(base, sim.NewCallStats())
	// A private pool: the polls of a bounded wait are many small requests, and the default transport keeps only
	// two idle connections per host, so a repeated run would otherwise churn through the ephemeral ports.
	tr := &http.Transport{MaxIdleConnsPerHost: 8}
	t.Cleanup(tr.CloseIdleConnections)
	w.client.HTTP = &http.Client{Transport: tr, Timeout: 5 * time.Second}
	return w
}

// assertEveryPostKeyedOnce fails when a POST arrived without an Idempotency-Key or when one key was
// sent twice to the same service (a retry must be a NEW logical request here: the refusals are stored).
func (w *world) assertEveryPostKeyedOnce() {
	w.t.Helper()
	w.mu.Lock()
	defer w.mu.Unlock()
	seen := map[string]string{}
	for _, p := range w.posts {
		if p.key == "" {
			w.t.Errorf("POST %s %s carried no Idempotency-Key", p.svc, p.path)
			continue
		}
		k := p.svc + "|" + p.key
		if prev, dup := seen[k]; dup {
			w.t.Errorf("Idempotency-Key %s was sent twice to %s (%s then %s)", p.key, p.svc, prev, p.path)
		}
		seen[k] = p.path
	}
}

// seedSite registers what a site needs in facility-layout (site, types, zones, one
// OUTBOUND dock door, one Staging lane, two yard spots), exactly as the real routes would, and
// returns the Site the floor works in.
func (w *world) seedSite(code string) Site {
	w.mu.Lock()
	defer w.mu.Unlock()
	f := w.facility
	f.sites[code] = "site " + code
	f.locTypes["OutboundDoor"] = "Dock"
	f.locTypes["StagingLane"] = "Staging"
	f.locTypes["YardSpot"] = "Yard"
	door := code + "-DOCK-DR-A01-01-01-A"
	lane := code + "-STAG-LN-A01-01-01-A"
	for _, z := range []string{code + "-DOCK-DR", code + "-STAG-LN", code + "-YARD-PRK"} {
		f.zones[z] = code
		f.aisles[z+"-A01"] = true
	}
	f.slots[door] = &slot{Code: door, Type: "OutboundDoor", Role: "Dock", DockFlow: "Outbound", Status: "Active", Site: code}
	f.slots[lane] = &slot{Code: lane, Type: "StagingLane", Role: "Staging", Status: "Active", Site: code}
	for _, bay := range []string{"01", "02"} {
		c := code + "-YARD-PRK-A01-" + bay + "-01-A"
		f.slots[c] = &slot{Code: c, Type: "YardSpot", Role: "Yard", Status: "Active", Site: code}
	}
	return Site{Code: code, Timezone: "America/Chicago", Doors: []string{door}, StagingLocations: []string{lane}}
}

var everyDay = []string{"Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"}

// seedSchedule declares the site's CPT schedule the way process-path-management would publish it.
func (w *world) seedSchedule(site, tz string, cutoffs ...Cutoff) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.ppm.set(site, tz, cutoffs)
}

func (w *world) floor(site Site) *Floor {
	return &Floor{
		C: w.client, Site: site, RunID: "T1", Carrier: Carrier{Name: "ACME Freight", SCAC: "ACME"},
		Wait: Waits{Timeout: 3 * time.Second, Interval: time.Millisecond},
	}
}

// ---------------------------------------------------------------------
// facility-layout
// ---------------------------------------------------------------------

type slot struct {
	Code, Type, Role, DockFlow, Status, Site string
}

type fakeFacility struct {
	*fake
	sites    map[string]string
	locTypes map[string]string // name -> role
	zones    map[string]string // zoneId -> site
	aisles   map[string]bool   // aisleId
	slots    map[string]*slot
	// failSlots makes POST /locations answer a placement-rule 422.
	failSlots bool
}

func newFakeFacility(w *world) *fakeFacility {
	f := &fakeFacility{
		fake:     &fake{w: w, name: SvcFacility, idem: map[string]stored{}},
		sites:    map[string]string{},
		locTypes: map[string]string{},
		zones:    map[string]string{},
		aisles:   map[string]bool{},
		slots:    map[string]*slot{},
	}
	f.handle("GET", "/sites/{code}", func(c *fctx) reply {
		name, found := f.sites[c.params["code"]]
		if !found {
			return refuse(404, "site-not-found")
		}
		return ok(map[string]any{"siteCode": c.params["code"], "name": name, "status": "Active", "siteRole": "UNSPECIFIED"})
	})
	f.handle("POST", "/sites", func(c *fctx) reply {
		m, bad := c.obj()
		if bad != nil {
			return *bad
		}
		code, _ := m["siteCode"].(string)
		if code == "" || code != strings.ToUpper(code) {
			return refuse(400, "bad-request")
		}
		if _, dup := f.sites[code]; dup {
			return refuse(409, "site-already-registered")
		}
		f.sites[code] = fmt.Sprint(m["name"])
		return created(map[string]any{"siteCode": code, "name": m["name"], "status": "Active", "siteRole": "UNSPECIFIED"})
	})
	f.handle("GET", "/location-types", func(c *fctx) reply {
		names := make([]string, 0, len(f.locTypes))
		for n := range f.locTypes {
			names = append(names, n)
		}
		sort.Strings(names)
		var items []any
		for _, n := range names {
			items = append(items, map[string]any{"name": n, "role": f.locTypes[n], "defaultCapacity": map[string]any{"maxWeightKg": 1000, "maxVolumeM3": 2}})
		}
		return page(c, 500, items)
	}).allow("limit", "cursor")
	f.handle("POST", "/location-types", func(c *fctx) reply {
		m, bad := c.obj()
		if bad != nil {
			return *bad
		}
		name, _ := m["name"].(string)
		role, _ := m["role"].(string)
		if name == "" {
			return refuse(400, "bad-request")
		}
		if _, dup := f.locTypes[name]; dup {
			return refuse(409, "location-type-already-registered")
		}
		f.locTypes[name] = role
		return created(map[string]any{"name": name, "role": role, "defaultCapacity": map[string]any{"maxWeightKg": 1000, "maxVolumeM3": 2}})
	})
	f.handle("GET", "/sites/{code}/zones", func(c *fctx) reply {
		if _, found := f.sites[c.params["code"]]; !found {
			return refuse(404, "site-not-found")
		}
		var ids []string
		for z, s := range f.zones {
			if s == c.params["code"] {
				ids = append(ids, z)
			}
		}
		sort.Strings(ids)
		var items []any
		for _, z := range ids {
			items = append(items, map[string]any{"zoneId": z, "siteCode": c.params["code"], "status": "Active"})
		}
		return page(c, 500, items)
	}).allow("limit", "cursor")
	f.handle("POST", "/sites/{code}/zones", func(c *fctx) reply {
		m, bad := c.obj()
		if bad != nil {
			return *bad
		}
		site := c.params["code"]
		if _, found := f.sites[site]; !found {
			return refuse(404, "site-not-found")
		}
		id := site + "-" + fmt.Sprint(m["areaCode"]) + "-" + fmt.Sprint(m["zoneCode"])
		if _, dup := f.zones[id]; dup {
			return refuse(409, "zone-already-registered")
		}
		f.zones[id] = site
		return created(map[string]any{"zoneId": id, "siteCode": site, "status": "Active"})
	})
	f.handle("GET", "/zones/{zone}/aisles", func(c *fctx) reply {
		if _, found := f.zones[c.params["zone"]]; !found {
			return refuse(404, "zone-not-found")
		}
		var ids []string
		for a := range f.aisles {
			if strings.HasPrefix(a, c.params["zone"]+"-") && strings.Count(a, "-") == strings.Count(c.params["zone"], "-")+1 {
				ids = append(ids, a)
			}
		}
		sort.Strings(ids)
		var items []any
		for _, a := range ids {
			items = append(items, map[string]any{"aisleId": a, "zoneId": c.params["zone"], "aisleCode": a[strings.LastIndex(a, "-")+1:], "status": "Active"})
		}
		return page(c, 500, items)
	}).allow("limit", "cursor")
	f.handle("POST", "/zones/{zone}/aisles", func(c *fctx) reply {
		m, bad := c.obj()
		if bad != nil {
			return *bad
		}
		if _, found := f.zones[c.params["zone"]]; !found {
			return refuse(404, "zone-not-found")
		}
		id := c.params["zone"] + "-" + fmt.Sprint(m["aisleCode"])
		if f.aisles[id] {
			return refuse(409, "aisle-already-registered")
		}
		f.aisles[id] = true
		return created(map[string]any{"aisleId": id, "zoneId": c.params["zone"], "aisleCode": m["aisleCode"], "status": "Active"})
	})
	f.handle("GET", "/sites/{code}/locations", func(c *fctx) reply {
		if _, found := f.sites[c.params["code"]]; !found {
			return refuse(404, "site-not-found")
		}
		role := c.q("role")
		if role == "" {
			return refuse(400, "bad-request")
		}
		var codes []string
		for code, s := range f.slots {
			if s.Site == c.params["code"] && s.Role == role {
				codes = append(codes, code)
			}
		}
		sort.Strings(codes)
		var items []any
		for _, code := range codes {
			s := f.slots[code]
			it := map[string]any{"locationCode": code, "locationType": s.Type, "role": s.Role, "status": s.Status}
			if s.DockFlow != "" {
				it["dockFlow"] = s.DockFlow
			}
			items = append(items, it)
		}
		return page(c, 500, items)
	}).allow("role", "limit", "cursor")
	f.handle("POST", "/locations", func(c *fctx) reply {
		m, bad := c.obj()
		if bad != nil {
			return *bad
		}
		code, _ := m["locationCode"].(string)
		segs := strings.Split(code, "-")
		if len(segs) != 7 {
			return refuse(400, "bad-request")
		}
		site, zone, aisle := segs[0], strings.Join(segs[:3], "-"), strings.Join(segs[:4], "-")
		if _, found := f.sites[site]; !found {
			return refuse(404, "site-not-found")
		}
		if _, found := f.zones[zone]; !found {
			return refuse(404, "zone-not-found")
		}
		if !f.aisles[aisle] {
			return refuse(404, "aisle-not-found")
		}
		typ, _ := m["locationType"].(string)
		role, known := f.locTypes[typ]
		if !known {
			return refuse(404, "location-type-not-found")
		}
		if _, dup := f.slots[code]; dup {
			return refuse(409, "location-already-registered")
		}
		if f.failSlots {
			return refuse(422, "placement-rule-violated")
		}
		flow, _ := m["dockFlow"].(string)
		if role != "Dock" && flow != "" {
			return refuse(422, "validation")
		}
		f.slots[code] = &slot{Code: code, Type: typ, Role: role, DockFlow: flow, Status: "Active", Site: site}
		return created(map[string]any{"locationCode": code, "locationType": typ, "role": role, "status": "Active"})
	})
	return f
}

// ---------------------------------------------------------------------
// process-path-management
// ---------------------------------------------------------------------

type fakePPM struct {
	*fake
	schedules map[string]schedule
}

type schedule struct {
	Timezone string
	Cutoffs  []Cutoff
}

func newFakePPM(w *world) *fakePPM {
	p := &fakePPM{fake: &fake{w: w, name: SvcProcessPath, idem: map[string]stored{}}, schedules: map[string]schedule{}}
	p.handle("GET", "/sites/{site}/cpt-schedule", func(c *fctx) reply {
		s, found := p.schedules[c.params["site"]]
		if !found {
			return refuse(404, "cpt-schedule-not-found")
		}
		return ok(scheduleJSON(c.params["site"], s))
	})
	p.handle("PUT", "/sites/{site}/cpt-schedule", func(c *fctx) reply {
		m, bad := c.obj()
		if bad != nil {
			return *bad
		}
		tz, _ := m["timezone"].(string)
		raw, _ := m["cutoffs"].([]any)
		if tz == "" || len(raw) == 0 {
			return refuse(422, "validation")
		}
		var cutoffs []Cutoff
		for _, r := range raw {
			cm, _ := r.(map[string]any)
			var days []string
			for _, d := range cm["daysOfWeek"].([]any) {
				days = append(days, fmt.Sprint(d))
			}
			var paths []string
			for _, d := range cm["eligiblePathIds"].([]any) {
				paths = append(paths, fmt.Sprint(d))
			}
			if len(paths) == 0 || len(days) == 0 {
				return refuse(422, "validation")
			}
			cutoffs = append(cutoffs, Cutoff{CptID: fmt.Sprint(cm["cptId"]), LocalTime: fmt.Sprint(cm["localTime"]), DaysOfWeek: days, ShipMethod: fmt.Sprint(cm["shipMethod"]), EligiblePathIDs: paths})
		}
		p.set(c.params["site"], tz, cutoffs)
		return ok(scheduleJSON(c.params["site"], p.schedules[c.params["site"]]))
	})
	return p
}

// set stores the schedule and publishes CPTScheduleChanged to outbound-shipping (caller holds the lock).
func (p *fakePPM) set(site, tz string, cutoffs []Cutoff) {
	p.schedules[site] = schedule{Timezone: tz, Cutoffs: cutoffs}
	p.w.out.deliver(0, func() { p.w.out.schedules[site] = p.schedules[site] })
}

func scheduleJSON(site string, s schedule) map[string]any {
	var cs []any
	for _, c := range s.Cutoffs {
		cs = append(cs, map[string]any{"cptId": c.CptID, "localTime": c.LocalTime, "daysOfWeek": c.DaysOfWeek, "shipMethod": c.ShipMethod, "eligiblePathIds": c.EligiblePathIDs})
	}
	return map[string]any{"siteId": site, "timezone": s.Timezone, "cutoffs": cs, "createdAt": "2026-11-14T00:00:00Z", "updatedAt": "2026-11-14T00:00:00Z"}
}

// ---------------------------------------------------------------------
// dock-scheduling
// ---------------------------------------------------------------------

type appt struct {
	ID, Site, Direction, Door, Carrier, SCAC, CptID, State string
	Start, End                                             time.Time
}

type fakeDock struct {
	*fake
	settings map[string]map[string]int // site -> settings (absent = never declared, version 0)
	version  map[string]int
	appts    map[string]*appt
	order    []string
	// consumer-lag knobs: the first N bookings answer the transient refusal.
	doorLag   int
	cutoffLag int
}

func newFakeDock(w *world) *fakeDock {
	d := &fakeDock{fake: &fake{w: w, name: SvcDock, idem: map[string]stored{}}, settings: map[string]map[string]int{}, version: map[string]int{}, appts: map[string]*appt{}}
	calendar := func(site string) map[string]any {
		s, declared := d.settings[site]
		if !declared {
			s = map[string]int{"maxWindowMinutes": 240, "slotGranularityMinutes": 15, "arrivalToleranceMinutes": 15, "noShowGraceMinutes": 60}
		}
		return map[string]any{"siteId": site, "version": d.version[site], "settings": s, "operatingHours": []any{}, "closures": []any{}}
	}
	d.handle("GET", "/sites/{site}/dock-calendar", func(c *fctx) reply { return ok(calendar(c.params["site"])) })
	d.handle("PUT", "/sites/{site}/dock-calendar/settings", func(c *fctx) reply {
		m, bad := c.obj()
		if bad != nil {
			return *bad
		}
		s := map[string]int{}
		for _, k := range []string{"maxWindowMinutes", "slotGranularityMinutes", "arrivalToleranceMinutes", "noShowGraceMinutes"} {
			f, isNum := m[k].(float64)
			if !isNum {
				return refuse(400, "malformed-request-body")
			}
			s[k] = int(f)
		}
		if s["maxWindowMinutes"] < 1 || s["maxWindowMinutes"] > 240 || s["slotGranularityMinutes"] < 1 || s["slotGranularityMinutes"] > 240 {
			return refuse(422, "invalid-settings")
		}
		d.settings[c.params["site"]] = s
		d.version[c.params["site"]]++
		return ok(calendar(c.params["site"]))
	})
	d.handle("GET", "/appointments/{id}", func(c *fctx) reply {
		a, found := d.appts[c.params["id"]]
		if !found {
			return refuse(404, "appointment-not-found")
		}
		return ok(a.json())
	})
	d.handle("POST", "/appointments", func(c *fctx) reply {
		m, bad := c.obj()
		if bad != nil {
			return *bad
		}
		str := func(k string) string { s, _ := m[k].(string); return s }
		door := str("doorCode")
		if d.doorLag > 0 {
			d.doorLag--
			return refuse(422, "unknown-door")
		}
		sl, known := d.w.facility.slots[door]
		if !known || sl.Role != "Dock" || sl.Status != "Active" {
			return refuse(422, "unknown-door")
		}
		dir := str("direction")
		if dir != "INBOUND" && dir != "OUTBOUND" {
			return refuse(400, "malformed-request-body")
		}
		if (dir == "OUTBOUND" && sl.DockFlow == "Inbound") || (dir == "INBOUND" && sl.DockFlow == "Outbound") {
			return refuse(422, "door-flow-mismatch")
		}
		if str("carrier") == "" {
			return refuse(422, "invalid-carrier")
		}
		start, e1 := time.Parse(time.RFC3339, str("windowStart"))
		end, e2 := time.Parse(time.RFC3339, str("windowEnd"))
		if e1 != nil || e2 != nil || !end.After(start) {
			return refuse(422, "invalid-window")
		}
		st := d.calendarSettings(sl.Site)
		if end.Sub(start) > time.Duration(st["maxWindowMinutes"])*time.Minute {
			return refuse(422, "invalid-window")
		}
		grid := int64(st["slotGranularityMinutes"]) * 60
		if start.Unix()%grid != 0 || end.Unix()%grid != 0 {
			return refuse(422, "invalid-window")
		}
		if start.Before(time.Now().Add(-time.Second)) {
			return refuse(422, "window-in-past")
		}
		cpt := str("cptId")
		if dir == "OUTBOUND" {
			if d.cutoffLag > 0 {
				d.cutoffLag--
				return refuse(422, "missing-outbound-ref")
			}
			if !d.w.ppm.knowsCutoff(sl.Site, cpt) && len(asStrings(m["shipmentRefs"])) == 0 {
				return refuse(422, "missing-outbound-ref")
			}
		}
		for _, id := range d.order {
			o := d.appts[id]
			if o.Door == door && (o.State == "BOOKED" || o.State == "CHECKED_IN") && start.Before(o.End) && end.After(o.Start) {
				return refuse(409, "door-double-booked")
			}
		}
		a := &appt{ID: "appt-" + d.w.id("a"), Site: sl.Site, Direction: dir, Door: door, Carrier: str("carrier"), SCAC: str("carrierScac"), CptID: cpt, State: "BOOKED", Start: start, End: end}
		d.appts[a.ID] = a
		d.order = append(d.order, a.ID)
		// DockAppointmentBooked reaches outbound-shipping and yard-management.
		d.w.out.deliver(d.w.out.lagAppt, func() { d.w.out.onAppointmentBooked(a) })
		return created(a.json())
	})
	return d
}

func (d *fakeDock) calendarSettings(site string) map[string]int {
	if s, declared := d.settings[site]; declared {
		return s
	}
	return map[string]int{"maxWindowMinutes": 240, "slotGranularityMinutes": 15, "arrivalToleranceMinutes": 15, "noShowGraceMinutes": 60}
}

func (a *appt) json() map[string]any {
	return map[string]any{
		"appointmentId": a.ID, "siteId": a.Site, "direction": a.Direction, "doorCode": a.Door, "carrier": a.Carrier, "carrierScac": a.SCAC,
		"windowStart": a.Start.UTC().Format(time.RFC3339), "windowEnd": a.End.UTC().Format(time.RFC3339), "cptId": a.CptID,
		"state": a.State, "provenance": "native", "version": 1, "atRisk": false,
	}
}

func asStrings(v any) []string {
	var out []string
	l, _ := v.([]any)
	for _, x := range l {
		out = append(out, fmt.Sprint(x))
	}
	return out
}

func (p *fakePPM) knowsCutoff(site, cpt string) bool {
	for _, c := range p.schedules[site].Cutoffs {
		if c.CptID == cpt && cpt != "" {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------
// yard-management
// ---------------------------------------------------------------------

type yloc struct{ Kind, Code string }

type ytrailer struct {
	ID, Site, Unit, Kind, LoadState, State, DoorWork, VisitID, ApptID, SealNumber, SealState string
	Loc                                                                                      *yloc
}

type yvisit struct {
	ID, Site, Plate, Purpose, Appt, State string
	TrailersIn, TrailersOut               []string
}

type ymove struct {
	ID, Site, Trailer, Performer, Reason, State, Hostler, Appt string
	To                                                         yloc
	Band, Seq                                                  int
}

type fakeYard struct {
	*fake
	trailers map[string]*ytrailer
	visits   map[string]*yvisit
	moves    map[string]*ymove
	// apptLag: the first N check-ins naming a booked appointment answer 422 unknown-appointment.
	apptLag int
}

func (y *fakeYard) trailerJSON(t *ytrailer) map[string]any {
	m := map[string]any{
		"trailerId": t.ID, "siteId": t.Site, "unitNumber": t.Unit, "kind": t.Kind, "loadState": t.LoadState, "state": t.State,
		"visitId": t.VisitID, "appointmentId": t.ApptID, "seal": map[string]any{"number": t.SealNumber, "state": firstNonEmpty(t.SealState, "NONE")},
	}
	if t.DoorWork != "" {
		m["doorWork"] = t.DoorWork
	}
	if t.Loc != nil {
		m["location"] = map[string]any{"kind": t.Loc.Kind, "code": t.Loc.Code}
	}
	return m
}

func (y *fakeYard) moveJSON(m *ymove) map[string]any {
	out := map[string]any{
		"moveId": m.ID, "siteId": m.Site, "trailerId": m.Trailer, "performer": m.Performer, "reason": m.Reason, "state": m.State,
		"to": map[string]any{"kind": m.To.Kind, "code": m.To.Code}, "priority": map[string]any{"band": m.Band}, "requestedAt": "2026-11-14T00:00:00Z", "version": 1,
	}
	if m.Appt != "" {
		out["appointmentId"] = m.Appt
	}
	if m.Hostler != "" {
		out["hostlerRef"] = m.Hostler
	}
	return out
}

func (y *fakeYard) visitJSON(v *yvisit) map[string]any {
	return map[string]any{
		"visitId": v.ID, "siteId": v.Site, "tractorPlate": v.Plate, "purpose": v.Purpose, "appointmentId": v.Appt, "state": v.State,
		"trailersIn": v.TrailersIn, "trailersOut": v.TrailersOut, "arrivedAt": "2026-11-14T00:00:00Z", "version": 1,
	}
}

func (y *fakeYard) occupant(site string, l yloc) *ytrailer {
	for _, t := range y.trailers {
		if t.Site == site && t.Loc != nil && *t.Loc == l && t.State != "DEPARTED" {
			return t
		}
	}
	return nil
}

func (y *fakeYard) openMove(trailer string) *ymove {
	for _, m := range y.moves {
		if m.Trailer == trailer && (m.State == "REQUESTED" || m.State == "CLAIMED") {
			return m
		}
	}
	return nil
}

func newFakeYard(w *world) *fakeYard {
	y := &fakeYard{fake: &fake{w: w, name: SvcYard, idem: map[string]stored{}}, trailers: map[string]*ytrailer{}, visits: map[string]*yvisit{}, moves: map[string]*ymove{}}

	y.handle("POST", "/visits", func(c *fctx) reply {
		m, bad := c.obj()
		if bad != nil {
			return *bad
		}
		str := func(k string) string { s, _ := m[k].(string); return s }
		site := str("siteId")
		if site == "" || str("carrier") == "" || str("tractorPlate") == "" || str("purpose") == "" {
			return refuse(400, "malformed-request-body")
		}
		trs, _ := m["trailers"].([]any)
		var door string
		var window time.Time
		if id := str("appointmentId"); id != "" {
			a, booked := y.w.dock.appts[id]
			if !booked || len(trs) != 1 {
				return refuse(422, "unknown-appointment")
			}
			if y.apptLag > 0 {
				y.apptLag--
				return refuse(422, "unknown-appointment")
			}
			door, window = a.Door, a.Start
		}
		visit := &yvisit{ID: y.w.id("visit"), Site: site, Plate: str("tractorPlate"), Purpose: str("purpose"), Appt: str("appointmentId"), State: "CHECKED_IN"}
		var outTr, outMv []any
		for _, raw := range trs {
			tm, _ := raw.(map[string]any)
			unit, _ := tm["unitNumber"].(string)
			if unit == "" || tm["kind"] == nil || tm["loadState"] == nil {
				return refuse(400, "malformed-request-body")
			}
			for _, o := range y.trailers {
				if o.Site == site && o.Unit == unit && o.State != "DEPARTED" {
					return refuse(409, "trailer-already-on-site")
				}
			}
			t := &ytrailer{ID: y.w.id("trl"), Site: site, Unit: unit, Kind: fmt.Sprint(tm["kind"]), LoadState: fmt.Sprint(tm["loadState"]), State: "IN_MOVE", VisitID: visit.ID, ApptID: visit.Appt}
			y.trailers[t.ID] = t
			visit.TrailersIn = append(visit.TrailersIn, t.ID)
			mv := &ymove{ID: y.w.id("move"), Site: site, Trailer: t.ID, Performer: "VISITING_DRIVER", State: "CLAIMED", Appt: visit.Appt, Band: 5, Seq: y.w.seq}
			dest := yloc{"DOOR", door}
			mv.Reason = "TO_DOOR"
			tolerance := time.Duration(y.w.dock.calendarSettings(site)["arrivalToleranceMinutes"]) * time.Minute
			switch {
			case door == "":
				mv.Reason, dest = "TO_HOLDING", y.freeSpot(site)
			case time.Now().Before(window.Add(-tolerance)):
				mv.Reason, dest = "TO_HOLDING", y.freeSpot(site)
			case y.occupant(site, dest) != nil:
				mv.Reason, dest = "TO_SPOT", y.freeSpot(site)
			}
			mv.To = dest
			y.moves[mv.ID] = mv
			outTr = append(outTr, y.trailerJSON(t))
			outMv = append(outMv, y.moveJSON(mv))
		}
		y.visits[visit.ID] = visit
		return created(map[string]any{"visit": y.visitJSON(visit), "trailers": outTr, "moves": outMv})
	})

	y.handle("POST", "/moves", func(c *fctx) reply {
		m, bad := c.obj()
		if bad != nil {
			return *bad
		}
		t, found := y.trailers[fmt.Sprint(m["trailerId"])]
		if !found {
			return refuse(404, "trailer-not-found")
		}
		to, _ := m["to"].(map[string]any)
		dest := yloc{fmt.Sprint(to["kind"]), fmt.Sprint(to["code"])}
		if y.openMove(t.ID) != nil {
			return refuse(409, "trailer-has-open-move")
		}
		for _, o := range y.moves {
			if (o.State == "REQUESTED" || o.State == "CLAIMED") && o.To == dest {
				return refuse(409, "destination-reserved")
			}
		}
		if y.occupant(t.Site, dest) != nil {
			return refuse(409, "location-occupied")
		}
		reason, _ := m["reason"].(string)
		mv := &ymove{ID: y.w.id("move"), Site: t.Site, Trailer: t.ID, Performer: "HOSTLER", Reason: firstNonEmpty(reason, "RELOCATE"), State: "REQUESTED", To: dest, Band: 5, Seq: y.w.seq, Appt: t.ApptID}
		y.moves[mv.ID] = mv
		return created(y.moveJSON(mv))
	})

	y.handle("POST", "/moves/claims", func(c *fctx) reply {
		m, bad := c.obj()
		if bad != nil {
			return *bad
		}
		site, hostler := fmt.Sprint(m["siteId"]), fmt.Sprint(m["hostlerRef"])
		for _, o := range y.moves {
			if o.State == "CLAIMED" && o.Hostler == hostler {
				return refuse(409, "hostler-busy")
			}
		}
		var best *ymove
		for _, o := range y.moves {
			if o.Site == site && o.State == "REQUESTED" && o.Performer == "HOSTLER" {
				if best == nil || o.Band < best.Band || (o.Band == best.Band && o.Seq < best.Seq) {
					best = o
				}
			}
		}
		if best == nil {
			return refuse(404, "no-claimable-move")
		}
		best.State, best.Hostler = "CLAIMED", hostler
		if t := y.trailers[best.Trailer]; t != nil {
			t.State, t.Loc = "IN_MOVE", nil
		}
		return created(y.moveJSON(best))
	})

	y.handle("POST", "/moves/{id}/completions", func(c *fctx) reply {
		mv, found := y.moves[c.params["id"]]
		if !found {
			return refuse(404, "move-not-found")
		}
		m, bad := c.obj()
		if bad != nil {
			return *bad
		}
		if mv.State != "CLAIMED" {
			return refuse(409, "illegal-transition")
		}
		if mv.Performer == "HOSTLER" && fmt.Sprint(m["hostlerRef"]) != mv.Hostler {
			return refuse(409, "not-lease-holder")
		}
		if o := y.occupant(mv.Site, mv.To); o != nil && o.ID != mv.Trailer {
			return refuse(409, "location-occupied")
		}
		t := y.trailers[mv.Trailer]
		dest := mv.To
		t.Loc = &dest
		mv.State = "COMPLETED"
		if dest.Kind == "DOOR" {
			t.State, t.DoorWork = "DOCKED", "WAITING"
			// TrailerDocked reaches outbound-shipping.
			site, trailer := t.Site, t.ID
			y.w.out.deliver(y.w.out.lagDocked, func() { y.w.out.onTrailerDocked(site, dest.Code, trailer) })
		} else {
			t.State = "PARKED"
		}
		return ok(y.moveJSON(mv))
	})

	trailerRoute := func(method, tpl string, fn func(t *ytrailer, c *fctx) reply) {
		y.handle(method, tpl, func(c *fctx) reply {
			t, found := y.trailers[c.params["id"]]
			if !found {
				return refuse(404, "trailer-not-found")
			}
			return fn(t, c)
		})
	}
	trailerRoute("GET", "/trailers/{id}", func(t *ytrailer, c *fctx) reply { return ok(y.trailerJSON(t)) })
	trailerRoute("POST", "/trailers/{id}/door-work-starts", func(t *ytrailer, c *fctx) reply {
		if t.State != "DOCKED" || t.DoorWork != "WAITING" {
			return refuse(409, "illegal-transition")
		}
		t.DoorWork = "IN_PROGRESS"
		return ok(y.trailerJSON(t))
	})
	trailerRoute("POST", "/trailers/{id}/door-work-completions", func(t *ytrailer, c *fctx) reply {
		m, bad := c.obj()
		if bad != nil {
			return *bad
		}
		if t.DoorWork != "IN_PROGRESS" {
			return refuse(409, "illegal-transition")
		}
		t.DoorWork = "DONE"
		if ls, _ := m["loadState"].(string); ls != "" {
			t.LoadState = ls
		}
		return ok(y.trailerJSON(t))
	})
	trailerRoute("POST", "/trailers/{id}/seals", func(t *ytrailer, c *fctx) reply {
		m, bad := c.obj()
		if bad != nil {
			return *bad
		}
		st, _ := m["state"].(string)
		if st != "INTACT" && st != "APPLIED" && st != "BROKEN" {
			return refuse(400, "malformed-request-body")
		}
		t.SealState, t.SealNumber = st, fmt.Sprint(m["sealNumber"])
		return ok(y.trailerJSON(t))
	})

	y.handle("POST", "/visits/{id}/check-outs", func(c *fctx) reply {
		v, found := y.visits[c.params["id"]]
		if !found {
			return refuse(404, "visit-not-found")
		}
		m, bad := c.obj()
		if bad != nil {
			return *bad
		}
		if v.State != "CHECKED_IN" {
			return refuse(409, "illegal-transition")
		}
		outs, _ := m["trailersOut"].([]any)
		var ids []string
		for _, o := range outs {
			ids = append(ids, fmt.Sprint(o.(map[string]any)["trailerId"]))
		}
		// Every trailer taken out must be released BEFORE any departs (one transaction).
		for _, id := range ids {
			t, on := y.trailers[id]
			if !on || t.VisitID != v.ID {
				return refuse(404, "trailer-not-found")
			}
			if y.openMove(id) != nil || !(t.State == "PARKED" || (t.State == "DOCKED" && t.DoorWork == "DONE")) {
				return refuse(409, "trailer-not-released")
			}
			if t.LoadState == "LOADED" && v.Purpose == "LIVE_LOAD" && t.SealNumber == "" {
				return refuse(409, "seal-required")
			}
		}
		for _, id := range ids {
			t := y.trailers[id]
			site, seal, door := t.Site, t.SealNumber, ""
			if t.Loc != nil && t.Loc.Kind == "DOOR" {
				door = t.Loc.Code
			}
			t.State, t.Loc = "DEPARTED", nil
			v.TrailersOut = append(v.TrailersOut, id)
			_ = door
			y.w.out.deliver(y.w.out.lagDeparted, func() { y.w.out.onTrailerDeparted(site, id, seal) })
		}
		v.State = "CHECKED_OUT"
		if a := y.w.dock.appts[v.Appt]; a != nil {
			a.State = "COMPLETED"
		}
		return ok(y.visitJSON(v))
	})

	y.handle("GET", "/sites/{site}/yard-board", func(c *fctx) reply {
		site := c.params["site"]
		var codes []string
		for code, s := range y.w.facility.slots {
			if s.Site == site && (s.Role == "Dock" || s.Role == "Yard") {
				codes = append(codes, code)
			}
		}
		if len(codes) == 0 {
			return refuse(404, "site-not-found")
		}
		sort.Strings(codes)
		var spots []any
		for _, code := range codes {
			s := y.w.facility.slots[code]
			kind := "SPOT"
			it := map[string]any{"code": code}
			if s.Role == "Dock" {
				kind = "DOOR"
				it["dockFlow"] = s.DockFlow
			}
			it["kind"] = kind
			spots = append(spots, it)
		}
		return ok(map[string]any{"siteId": site, "asOf": "2026-11-14T00:00:00Z", "spots": spots})
	})
	return y
}

// freeSpot is the first free Yard slot of the site (the holding or parking spot a trailer goes to).
func (y *fakeYard) freeSpot(site string) yloc {
	var codes []string
	for code, s := range y.w.facility.slots {
		if s.Site == site && s.Role == "Yard" {
			codes = append(codes, code)
		}
	}
	sort.Strings(codes)
	for _, code := range codes {
		if y.occupant(site, yloc{"SPOT", code}) == nil {
			return yloc{"SPOT", code}
		}
	}
	return yloc{"SPOT", "NO-FREE-SPOT"}
}

// ---------------------------------------------------------------------
// outbound-shipping
// ---------------------------------------------------------------------

type oload struct {
	ID, Site, Cpt, Date, Method, State, Door, Trailer, Appt string
	Closed                                                  int
	Packages                                                []string
}

type opackage struct {
	ID, Site, SourceOrder, WorkUnit, Load, Staging, State string
	WeightG                                               int
}

type event struct {
	typ  string
	refs []string
}

type pending struct {
	left int
	fn   func()
}

type fakeOutbound struct {
	*fake
	schedules map[string]schedule
	loads     map[string]*oload
	loadOrder []string
	packages  map[string]*opackage
	pending   []*pending
	// Emitted integration events, as a Kafka tap would index them.
	emitted []event
	// lag in outbound-shipping requests before each consumed event takes effect.
	lagManifest, lagAppt, lagDocked, lagDeparted int
	// ignored counts events the consumer ignored (no schedule, cpt not a cutoff...).
	ignored []string
}

// deliver applies fn after `lag` further requests to this service (0 = now): the consumer lag.
func (o *fakeOutbound) deliver(lag int, fn func()) {
	if lag <= 0 {
		fn()
		return
	}
	o.pending = append(o.pending, &pending{left: lag, fn: fn})
}

func (o *fakeOutbound) tick() {
	rest := o.pending[:0]
	var due []*pending
	for _, p := range o.pending {
		if p.left == 0 {
			due = append(due, p)
		} else {
			p.left--
			rest = append(rest, p)
		}
	}
	o.pending = rest
	for _, p := range due {
		p.fn()
	}
}

// manifest is PackageManifested (ADR 0003): planned only for a site with a schedule whose cutoff
// matches the manifest's cpt on a listed day in the site's timezone.
func (o *fakeOutbound) manifest(p Package) {
	o.deliver(o.lagManifest, func() { o.onManifest(p) })
}

func (o *fakeOutbound) onManifest(p Package) {
	if p.SiteID == "" || p.CPT == "" {
		o.ignored = append(o.ignored, "no site_id or cpt: "+p.ID)
		return
	}
	s, have := o.schedules[p.SiteID]
	if !have {
		o.ignored = append(o.ignored, "no schedule: "+p.ID)
		return
	}
	loc, err := time.LoadLocation(s.Timezone)
	if err != nil {
		loc = time.UTC
	}
	now := time.Now().In(loc)
	var hit *Cutoff
	for i, c := range s.Cutoffs {
		if c.LocalTime != p.CPT {
			continue
		}
		for _, d := range c.DaysOfWeek {
			if d == now.Weekday().String()[:3] {
				hit = &s.Cutoffs[i]
			}
		}
	}
	if hit == nil {
		o.ignored = append(o.ignored, "cpt not a cutoff: "+p.ID)
		return
	}
	if _, known := o.packages[p.ID]; known {
		return
	}
	date := now.Format("2006-01-02")
	var ld *oload
	for _, id := range o.loadOrder {
		l := o.loads[id]
		if l.Site == p.SiteID && l.Cpt == hit.CptID && l.Date == date && (l.State == LoadPlanned || l.State == LoadLoading) {
			ld = l
		}
	}
	if ld == nil {
		ld = &oload{ID: "load-" + o.w.id("l"), Site: p.SiteID, Cpt: hit.CptID, Date: date, Method: hit.ShipMethod, State: LoadPlanned}
		o.loads[ld.ID] = ld
		o.loadOrder = append([]string{ld.ID}, o.loadOrder...) // newest first
	}
	ld.Packages = append(ld.Packages, p.ID)
	o.packages[p.ID] = &opackage{ID: p.ID, Site: p.SiteID, SourceOrder: p.SourceOrderID, WorkUnit: p.OrderRef, Load: ld.ID, State: PackageExpected, WeightG: p.WeightG}
}

// onAppointmentBooked: an OUTBOUND appointment naming a cpt gives the first open load of (site, cpt) its door;
// without a planned load the appointment is only copied (ignored for the door).
func (o *fakeOutbound) onAppointmentBooked(a *appt) {
	if a.Direction != "OUTBOUND" || a.CptID == "" {
		return
	}
	for i := len(o.loadOrder) - 1; i >= 0; i-- { // oldest first = "first open load"
		l := o.loads[o.loadOrder[i]]
		if l.Site == a.Site && l.Cpt == a.CptID && (l.State == LoadPlanned || l.State == LoadLoading) {
			l.Door, l.Appt = a.Door, a.ID
			return
		}
	}
	o.ignored = append(o.ignored, "appointment before a planned load: "+a.ID)
}

// onTrailerDocked is StartOnDock: the open load at (site, door), PLANNED first; no door = refused, no load = ignored.
func (o *fakeOutbound) onTrailerDocked(site, door, trailer string) {
	var hit *oload
	for _, st := range []string{LoadPlanned, LoadLoading} {
		for _, id := range o.loadOrder {
			l := o.loads[id]
			if hit == nil && l.Site == site && l.Door == door && l.State == st {
				hit = l
			}
		}
	}
	if hit == nil {
		o.ignored = append(o.ignored, "TrailerDocked at "+door+": no open load uses that door")
		return
	}
	hit.Trailer = trailer
	if hit.State == LoadPlanned {
		hit.State = LoadLoading
	}
}

// onTrailerDeparted is OnDeparture (ADR 0002).
func (o *fakeOutbound) onTrailerDeparted(site, trailer, seal string) {
	var l *oload
	for _, id := range o.loadOrder { // newest first
		if o.loads[id].Site == site && o.loads[id].Trailer == trailer {
			l = o.loads[id]
			break
		}
	}
	if l == nil {
		return
	}
	switch {
	case l.State == LoadClosed && seal != "":
		l.State = LoadShipConfirmed
		var orders []string
		for _, id := range l.Packages {
			if p := o.packages[id]; p.State == PackageLoaded {
				p.State = PackageShipped
				orders = append(orders, p.SourceOrder)
			}
		}
		o.emitted = append(o.emitted, event{"ShipmentConfirmed", orders})
	case l.State == LoadClosed:
		// no seal: ignored, the load stays CLOSED
	case l.State == LoadLoading || (l.State == LoadPlanned && l.Trailer != ""):
		for _, id := range l.Packages {
			if p := o.packages[id]; p.State != PackageLoaded && p.State != "SHORT_SHIPPED" {
				o.emitted = append(o.emitted, event{"PackageMissedLoad", []string{p.SourceOrder, p.WorkUnit, p.ID}})
			}
		}
	}
}

func (o *fakeOutbound) loadJSON(l *oload) map[string]any {
	m := map[string]any{
		"id": l.ID, "site_id": l.Site, "cpt_id": l.Cpt, "operating_date": l.Date, "ship_method": l.Method, "cutoff": "2026-11-14T23:00:00Z",
		"state": l.State, "version": 1, "package_count": l.Closed,
	}
	if l.Door != "" {
		m["door_code"] = l.Door
	}
	if l.Trailer != "" {
		m["trailer_id"] = l.Trailer
	}
	if l.Appt != "" {
		m["appointment_id"] = l.Appt
	}
	return m
}

func (o *fakeOutbound) packageJSON(p *opackage) map[string]any {
	m := map[string]any{"id": p.ID, "site_id": p.Site, "state": p.State, "version": 1, "load_id": p.Load}
	if p.SourceOrder != "" {
		m["source_order_id"] = p.SourceOrder
	}
	if p.WorkUnit != "" {
		m["work_unit_ref"] = p.WorkUnit
	}
	if p.Staging != "" {
		m["staging_location"] = p.Staging
	}
	return m
}

// Seen makes the fake an event tap: how many events of typ carried ref.
func (o *fakeOutbound) Seen(ref, typ string) int {
	o.w.mu.Lock()
	defer o.w.mu.Unlock()
	n := 0
	for _, e := range o.emitted {
		if e.typ != typ {
			continue
		}
		for _, r := range e.refs {
			if r == ref {
				n++
				break
			}
		}
	}
	return n
}

func newFakeOutbound(w *world) *fakeOutbound {
	o := &fakeOutbound{
		fake:      &fake{w: w, name: SvcOutbound, idem: map[string]stored{}, codeInProblem: true},
		schedules: map[string]schedule{}, loads: map[string]*oload{}, packages: map[string]*opackage{},
	}
	o.fake.before = o.tick

	o.handle("POST", "/packages/{id}/stagings", func(c *fctx) reply {
		m, bad := c.obj()
		if bad != nil {
			return *bad
		}
		p, known := o.packages[c.params["id"]]
		if !known {
			return refuse(404, "package-not-found")
		}
		loc, _ := m["staging_location"].(string)
		if loc == "" {
			return refuse(400, "bad-request")
		}
		sl, exists := o.w.facility.slots[loc]
		if !exists || sl.Role != "Staging" || sl.Site != p.Site {
			return refuse(422, "not-staging-location")
		}
		switch {
		case p.State == PackageExpected, p.State == PackageStaged && p.Staging == loc:
			p.State, p.Staging = PackageStaged, loc
			return ok(o.packageJSON(p))
		case p.State == PackageShipped:
			return refuse(409, "already-shipped")
		default:
			return refuse(409, "package-state")
		}
	})
	o.handle("POST", "/packages/{id}/loadings", func(c *fctx) reply {
		m, bad := c.obj()
		if bad != nil {
			return *bad
		}
		p, known := o.packages[c.params["id"]]
		if !known {
			return refuse(404, "package-not-found")
		}
		loadID, door := fmt.Sprint(m["load_id"]), fmt.Sprint(m["door_code"])
		l := o.loads[p.Load]
		if l == nil || l.ID != loadID || l.Door == "" || l.Door != door {
			return refuse(409, "wrong-load")
		}
		if p.State == PackageShipped {
			return refuse(409, "already-shipped")
		}
		if p.State != PackageExpected && p.State != PackageStaged {
			return refuse(409, "package-state")
		}
		p.State = PackageLoaded
		return ok(o.packageJSON(p))
	})
	o.handle("POST", "/loads/{id}/closures", func(c *fctx) reply {
		l, known := o.loads[c.params["id"]]
		if !known {
			return refuse(404, "load-not-found")
		}
		if l.State != LoadLoading {
			return refuse(409, "load-state")
		}
		n := 0
		for _, id := range l.Packages {
			switch o.packages[id].State {
			case PackageLoaded:
				n++
			case "SHORT_SHIPPED":
			default:
				return refuse(409, "packages-outstanding")
			}
		}
		l.State, l.Closed = LoadClosed, n
		return ok(o.loadJSON(l))
	})
	o.handle("GET", "/loads", func(c *fctx) reply {
		var items []any
		for _, id := range o.loadOrder {
			l := o.loads[id]
			if s := c.q("site_id"); s != "" && l.Site != s {
				continue
			}
			if s := c.q("state"); s != "" && l.State != s {
				continue
			}
			if s := c.q("operating_date"); s != "" && l.Date != s {
				continue
			}
			items = append(items, o.loadJSON(l))
		}
		return page(c, 200, items)
	}).allow("site_id", "state", "operating_date", "limit", "cursor")
	o.handle("GET", "/loads/{id}", func(c *fctx) reply {
		l, known := o.loads[c.params["id"]]
		if !known {
			return refuse(404, "load-not-found")
		}
		return ok(o.loadJSON(l))
	})
	o.handle("GET", "/loads/{id}/packages", func(c *fctx) reply {
		l, known := o.loads[c.params["id"]]
		if !known {
			return refuse(404, "load-not-found")
		}
		var items []any
		for _, id := range l.Packages {
			p := o.packages[id]
			if s := c.q("state"); s != "" && p.State != s {
				continue
			}
			items = append(items, o.packageJSON(p))
		}
		return page(c, 200, items)
	}).allow("state", "limit", "cursor")
	return o
}
