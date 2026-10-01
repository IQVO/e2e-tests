package main

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ---------------------------------------------------------------------
// associates
// ---------------------------------------------------------------------

type role string

const (
	rolePicker role = "picker"
	rolePacker role = "packer"
	roleSlam   role = "slam"
	roleRebin  role = "rebin"
	roleFlex   role = "flex"
)

type associate struct {
	id         string
	role       role
	station    string            // current station
	stations   map[string]string // path -> station (flex has two)
	path       string            // current workforce assignment
	certs      []string
	speed      float64 // 1.0 = at standard; >1 slower
	breakAt    [2]int  // sim hh, mm
	breakTaken bool
	location   string // last storage slot visited (pickers)
	tasks      int
	units      int
}

func (s *sim) staffTheFloor() {
	add := func(r role, n int, path string, certs ...string) {
		for i := 1; i <= n; i++ {
			a := &associate{
				id:       fmt.Sprintf("%s-%s-%02d", s.cfg.runID, strings.ToUpper(string(r)), i),
				role:     r,
				path:     path,
				certs:    certs,
				speed:    0.8 + s.rf()*0.5,
				stations: map[string]string{},
			}
			if i%2 == 0 {
				a.breakAt = [2]int{12, 30}
			} else {
				a.breakAt = [2]int{11, 30}
			}
			s.associates = append(s.associates, a)
		}
	}
	add(rolePicker, s.cfg.pickers, "PICK", "PICK")
	add(rolePacker, s.cfg.packers, "PACK", "PACK")
	add(roleSlam, s.cfg.slammers, "SLAM", "SLAM")
	add(roleRebin, s.cfg.rebinners, "REBIN", "REBIN")
	add(roleFlex, s.cfg.flex, "PICK", "PICK", "PACK")
}

func (s *sim) stationCount() int {
	n := 0
	for _, a := range s.associates {
		if a.role == roleFlex {
			n += 2
		} else {
			n++
		}
	}
	return n
}

// registerStations installs one physical station per associate (two for a
// flex associate) on the work-centre slots of the building map.
func (s *sim) registerStations(ctx context.Context) error {
	slot := 0
	reg := func(a *associate, path, capability string) error {
		slot++
		id := fmt.Sprintf("%s-ST-%s-%02d", s.cfg.runID, path, slot)
		r := s.api.call(ctx, svcFulfillment, http.MethodPost, "/stations", "/stations", map[string]any{
			"stationId": id, "capabilities": []string{capability}, "locationCode": s.workCentreCode(slot),
		})
		if !r.ok() {
			return fail("register station %s -> %d %s", id, r.status, truncate(string(r.body), 200))
		}
		a.stations[path] = id
		return nil
	}
	for _, a := range s.associates {
		switch a.role {
		case roleFlex:
			if err := reg(a, "PICK", "pick"); err != nil {
				return err
			}
			if err := reg(a, "PACK", "pack"); err != nil {
				return err
			}
		default:
			if err := reg(a, a.path, strings.ToLower(a.path)); err != nil {
				return err
			}
		}
		a.station = a.stations[a.path]
	}
	logf("facilities", "%d stations installed on work-centre slots", slot)
	return nil
}

func (s *sim) checkIn(ctx context.Context, a *associate) {
	r := s.api.call(ctx, svcFulfillment, http.MethodPost, "/stations/{id}/check-in", "/stations/"+a.station+"/check-in", map[string]any{"occupantId": a.id})
	if r.status == http.StatusConflict {
		s.api.call(ctx, svcFulfillment, http.MethodPost, "/stations/{id}/check-out", "/stations/"+a.station+"/check-out", nil)
		r = s.api.call(ctx, svcFulfillment, http.MethodPost, "/stations/{id}/check-in", "/stations/"+a.station+"/check-in", map[string]any{"occupantId": a.id})
	}
	if !r.ok() {
		s.finding("check-in %s at %s -> %d %s", a.id, a.station, r.status, truncate(string(r.body), 160))
	}
}

func (s *sim) checkOut(ctx context.Context, a *associate) {
	r := s.api.call(ctx, svcFulfillment, http.MethodPost, "/stations/{id}/check-out", "/stations/"+a.station+"/check-out", nil)
	if !r.ok() {
		s.finding("check-out %s at %s -> %d %s", a.id, a.station, r.status, truncate(string(r.body), 160))
	}
}

// startShift is the 06:00 clock-in: workforce shift + certifications,
// path assignment, and station check-in.
func (s *sim) startShift(ctx context.Context, a *associate) error {
	r := s.api.call(ctx, svcWorkforce, http.MethodPost, "/associates/{id}/start-shift", "/associates/"+a.id+"/start-shift", map[string]any{"certifications": a.certs})
	if !r.ok() {
		return fail("start-shift %s -> %d %s", a.id, r.status, truncate(string(r.body), 200))
	}
	r = s.api.call(ctx, svcWorkforce, http.MethodPost, "/associates/{id}/assignments", "/associates/"+a.id+"/assignments", map[string]any{"pathId": a.path})
	if !r.ok() {
		return fail("assign %s to %s -> %d %s", a.id, a.path, r.status, truncate(string(r.body), 200))
	}
	s.checkIn(ctx, a)
	return nil
}

func (s *sim) endShift(a *associate) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	s.checkOut(ctx, a)
	r := s.api.call(ctx, svcWorkforce, http.MethodPost, "/associates/{id}/end-shift", "/associates/"+a.id+"/end-shift", nil)
	if !r.ok() {
		s.finding("end-shift %s -> %d %s", a.id, r.status, truncate(string(r.body), 160))
	}
}

// maybeBreak sends the associate on their 30-minute lunch when due.
func (s *sim) maybeBreak(ctx context.Context, a *associate) {
	if a.breakTaken || !s.clock.after(a.breakAt[0], a.breakAt[1]) {
		return
	}
	a.breakTaken = true
	s.checkOut(ctx, a)
	r := s.api.call(ctx, svcWorkforce, http.MethodPost, "/associates/{id}/break/start", "/associates/"+a.id+"/break/start", nil)
	if !r.ok() {
		s.finding("break/start %s -> %d %s", a.id, r.status, truncate(string(r.body), 160))
	}
	s.m.breaks.Add(1)
	logf(a.id, "lunch break")
	sleepCtx(ctx, s.clock.simDur(30*time.Minute))
	ctx2, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r = s.api.call(ctx2, svcWorkforce, http.MethodPost, "/associates/{id}/break/end", "/associates/"+a.id+"/break/end", nil)
	if !r.ok() {
		s.finding("break/end %s -> %d %s", a.id, r.status, truncate(string(r.body), 160))
	}
	if ctx.Err() == nil {
		s.checkIn(ctx, a)
	}
}

// ---------------------------------------------------------------------
// shared look-ups a scanner would do
// ---------------------------------------------------------------------

type task struct {
	ID       string `json:"id"`
	Type     string `json:"type"`
	Status   string `json:"status"`
	CPT      string `json:"cpt"`
	OrderRef string `json:"orderRef"`
	Fragile  bool   `json:"fragile"`
	GiftWrap bool   `json:"giftWrap"`
}

// claimNext returns (task, true) on a claim, (zero, false) when nothing is
// claimable right now.
func (s *sim) claimNext(ctx context.Context, a *associate, taskType string) (task, bool) {
	var t task
	r := s.api.call(ctx, svcFulfillment, http.MethodPost, "/stations/{id}/claim-next", "/stations/"+a.station+"/claim-next", map[string]any{"taskType": taskType})
	switch {
	case r.status == http.StatusOK:
		if err := r.decode(&t); err != nil {
			s.finding("claim-next body: %v", err)
			return t, false
		}
		return t, true
	case r.status == http.StatusConflict, r.status == 0:
		return t, false
	default:
		s.finding("claim-next %s at %s -> %d %s", taskType, a.station, r.status, truncate(string(r.body), 160))
		return t, false
	}
}

func (s *sim) completeTask(ctx context.Context, a *associate, t task) bool {
	r := s.api.call(ctx, svcFulfillment, http.MethodPost, "/tasks/{id}/complete", "/tasks/"+url.PathEscape(t.ID)+"/complete", map[string]any{"stationId": a.station})
	if r.status == http.StatusConflict {
		s.m.completeConflicts.Add(1)
	}
	if !r.ok() {
		s.finding("complete %s task %s -> %d %s", t.Type, t.ID, r.status, truncate(string(r.body), 160))
		return false
	}
	a.tasks++
	return true
}

// splitWorkUnit parses wes-work-planning's deterministic work unit id
// "{orderId}-line-{n}".
func splitWorkUnit(ref string) (string, int, bool) {
	i := strings.LastIndex(ref, "-line-")
	if i <= 0 {
		return "", 0, false
	}
	n, err := strconv.Atoi(ref[i+len("-line-"):])
	if err != nil {
		return "", 0, false
	}
	return ref[:i], n, true
}

type allocation struct {
	StockUnitID string `json:"stockUnitId"`
	BinID       string `json:"binId"`
	Quantity    int    `json:"quantity"`
}

type reservation struct {
	ID          string       `json:"id"`
	SKU         string       `json:"sku"`
	Quantity    int          `json:"quantity"`
	DemandRef   string       `json:"demandRef"`
	Status      string       `json:"status"`
	Allocations []allocation `json:"allocations"`
}

func (s *sim) reservationsFor(ctx context.Context, orderID string) []reservation {
	var out []reservation
	r := s.api.call(ctx, svcInventory, http.MethodGet, "/reservations?demandRef", "/reservations?demandRef="+url.QueryEscape(orderID), nil)
	if r.ok() {
		_ = r.decode(&out)
	}
	return out
}

func (s *sim) travel(ctx context.Context, a *associate, to string) {
	if a.location == "" || a.location == to {
		a.location = to
		return
	}
	r := s.api.call(ctx, svcFacility, http.MethodGet, "/distance", "/distance?from="+url.QueryEscape(a.location)+"&to="+url.QueryEscape(to), nil)
	var d struct {
		MetresM float64 `json:"metresM"`
	}
	if r.ok() && r.decode(&d) == nil {
		s.m.travelMetres.Add(int64(d.MetresM))
	}
	a.location = to
}

// ---------------------------------------------------------------------
// pickers
// ---------------------------------------------------------------------

func (s *sim) pickOne(ctx context.Context, a *associate) bool {
	t, ok := s.claimNext(ctx, a, "PICK")
	if !ok {
		return false
	}
	orderID, lineNo, parsed := splitWorkUnit(t.OrderRef)
	o := s.order(orderID)
	if !parsed || o == nil {
		// Work the warehouse had before today (e.g. carried over from a
		// previous shift): a real picker still works it to clear the queue.
		s.m.foreignTasks.Add(1)
		sleepCtx(ctx, jitter(nil2(s), s.cfg.pickSeconds*a.speed, 0.2))
		s.completeTask(ctx, a, t)
		return true
	}

	// The RF gun shows the work unit, then the pick location(s) of the
	// line's reservation.
	wu := s.api.call(ctx, svcWES, http.MethodGet, "/work-units/{id}", "/work-units/"+url.PathEscape(t.OrderRef), nil)
	if wu.status == http.StatusNotFound || wu.status == http.StatusMethodNotAllowed {
		s.finding("wes-work-planning GET /work-units/{id} unavailable (%d) — deploy claudioed/wes-work-planning#118", wu.status)
	}
	om, _ := s.getOrder(ctx, orderID)
	var line omLine
	for _, l := range om.Lines {
		if l.LineNo == lineNo {
			line = l
		}
	}
	var res *reservation
	for _, rv := range s.reservationsFor(ctx, orderID) {
		if rv.ID == line.ReservationID || (line.ReservationID == "" && rv.SKU == line.SKU && rv.Status == "ACTIVE") {
			rv := rv
			res = &rv
			break
		}
	}
	if res == nil {
		s.finding("order %s line %d released to the floor without a findable reservation", orderID, lineNo)
		s.completeTask(ctx, a, t)
		return true
	}
	units := 0
	for i, al := range res.Allocations {
		if al.BinID == "" {
			s.m.picksNoLocation.Add(1)
			s.finding("reservation %s allocation has no binId — deploy inventory-storage#112 (pick location on allocations)", res.ID)
			continue
		}
		s.travel(ctx, a, al.BinID)
		sleepCtx(ctx, jitter(nil2(s), s.cfg.pickSeconds*a.speed*float64(max(1, al.Quantity))/1.5, 0.25))
		units += al.Quantity
		if i == 0 && len(res.Allocations) > 1 {
			if r := s.api.call(ctx, svcFulfillment, http.MethodPost, "/tasks/{id}/renew-lease", "/tasks/"+url.PathEscape(t.ID)+"/renew-lease", map[string]any{"stationId": a.station}); r.ok() {
				s.m.leaseRenewals.Add(1)
			}
		}
	}
	r := s.api.call(ctx, svcInventory, http.MethodPost, "/reservations/{id}/confirm-pick", "/reservations/"+url.PathEscape(res.ID)+"/confirm-pick", nil)
	if r.status == http.StatusConflict {
		s.m.confirmPickConflicts.Add(1)
		s.finding("confirm-pick %s -> 409 %s", res.ID, truncate(string(r.body), 160))
	} else if !r.ok() {
		s.finding("confirm-pick %s -> %d %s", res.ID, r.status, truncate(string(r.body), 160))
	}
	if !s.completeTask(ctx, a, t) {
		return true
	}
	s.m.picks.Add(1)
	a.units += units
	s.mu.Lock()
	o.pickedLines[lineNo] = true
	s.mu.Unlock()
	select {
	case s.totes <- tote{orderID: orderID, lineNo: lineNo, cpt: t.CPT}:
	case <-ctx.Done():
	}
	return true
}

// nil2 gives jitter a nil-safe random source (the sim's own, locked).
func nil2(s *sim) *randShim { return &randShim{s} }

type randShim struct{ s *sim }

func (r *randShim) Float64() float64 { return r.s.rf() }

// ---------------------------------------------------------------------
// rebin / put wall
// ---------------------------------------------------------------------

func (s *sim) rebinOne(ctx context.Context, a *associate, tt tote) {
	o := s.order(tt.orderID)
	if o == nil {
		return
	}
	var required []string
	for i := range o.lines {
		required = append(required, fmt.Sprintf("line-%d", i+1))
	}
	sleepCtx(ctx, jitter(nil2(s), 0.3*a.speed, 0.3))
	r := s.api.call(ctx, svcFulfillment, http.MethodPost, "/rebin/arrivals", "/rebin/arrivals", map[string]any{
		"orderRef": tt.orderID, "lineId": fmt.Sprintf("line-%d", tt.lineNo), "requiredLineIds": required,
		"packCpt": tt.cpt, "packRequiredCapabilities": []string{"pack"},
		"packFragile": o.fragile(), "packGiftWrap": o.giftWrap(),
	})
	if r.status != http.StatusNoContent {
		s.finding("rebin arrival %s line %d -> %d %s", tt.orderID, tt.lineNo, r.status, truncate(string(r.body), 160))
		return
	}
	s.m.rebinScans.Add(1)
	a.tasks++
}

// ---------------------------------------------------------------------
// packers
// ---------------------------------------------------------------------

type pkg struct {
	ID                string   `json:"id"`
	OrderRef          string   `json:"orderRef"`
	Status            string   `json:"status"`
	ScannedContents   []string `json:"scannedContents"`
	FragileHandling   bool     `json:"fragileHandling"`
	GiftWrapRequested bool     `json:"giftWrapRequested"`
	SortLane          string   `json:"sortLane"`
}

func (s *sim) packOne(ctx context.Context, a *associate) bool {
	t, ok := s.claimNext(ctx, a, "PACK")
	if !ok {
		return false
	}
	o := s.order(t.OrderRef)
	if o == nil {
		s.m.foreignTasks.Add(1)
		s.completeTask(ctx, a, t)
		return true
	}
	var contents []string
	units := 0
	for _, l := range o.lines {
		for i := 0; i < l.qty; i++ {
			contents = append(contents, s.sku(l.sku))
			units++
		}
	}
	if o.kind == kindMispickInjct {
		// the picker grabbed a wrong extra item; the packer scans what is
		// in the tote — SLAM's scale is what catches it.
		contents = append(contents, s.sku("LAMP"))
	}
	sleepCtx(ctx, jitter(nil2(s), s.cfg.packSeconds*a.speed*(1+0.2*float64(units-1)), 0.2))
	r := s.api.call(ctx, svcFulfillment, http.MethodPost, "/tasks/{id}/seal-package", "/tasks/"+url.PathEscape(t.ID)+"/seal-package", map[string]any{
		"stationId": a.station, "contents": contents,
	})
	if r.status != http.StatusCreated {
		s.finding("seal-package for order %s -> %d %s", o.id, r.status, truncate(string(r.body), 200))
		s.completeTask(ctx, a, t)
		return true
	}
	var p pkg
	_ = r.decode(&p)
	if p.FragileHandling != o.fragile() {
		s.finding("package %s fragileHandling=%v but order %s fragile=%v", p.ID, p.FragileHandling, o.id, o.fragile())
	}
	if p.GiftWrapRequested != o.giftWrap() {
		s.finding("package %s giftWrapRequested=%v but order %s giftWrap=%v", p.ID, p.GiftWrapRequested, o.id, o.giftWrap())
	}
	if !s.completeTask(ctx, a, t) {
		return true
	}
	s.m.packs.Add(1)
	a.units += units
	s.mu.Lock()
	o.packageID, o.sortLane = p.ID, p.SortLane
	s.mu.Unlock()
	select {
	case s.conveyor <- parcel{packageID: p.ID, orderID: o.id}:
	case <-ctx.Done():
	}
	return true
}

// ---------------------------------------------------------------------
// SLAM (scan, label, apply, manifest)
// ---------------------------------------------------------------------

func (s *sim) slamOne(ctx context.Context, a *associate, p parcel) {
	o := s.order(p.orderID)
	if o == nil {
		return
	}
	expected := cartonTareKg
	for _, l := range o.lines {
		expected += skuByKey(l.sku).weightKg * float64(l.qty)
	}
	actual := expected + (s.rf()*2-1)*0.01
	if o.kind == kindMispickInjct {
		actual += skuByKey("LAMP").weightKg
	}
	sleepCtx(ctx, jitter(nil2(s), 0.4*a.speed, 0.2))
	r := s.api.call(ctx, svcFulfillment, http.MethodPost, "/packages/{id}/slam", "/packages/"+url.PathEscape(p.packageID)+"/slam", map[string]any{
		"actualWeight": round3(actual), "expectedWeight": round3(expected),
	})
	if r.status != http.StatusNoContent {
		s.finding("SLAM package %s -> %d %s", p.packageID, r.status, truncate(string(r.body), 160))
		return
	}
	s.m.slams.Add(1)
	a.tasks++
	status := ""
	g := s.api.call(ctx, svcFulfillment, http.MethodGet, "/packages/{id}", "/packages/"+url.PathEscape(p.packageID), nil)
	if g.ok() {
		var got pkg
		_ = g.decode(&got)
		status = got.Status
	} else if g.status == http.StatusNotFound || g.status == http.StatusMethodNotAllowed {
		s.finding("fulfillment-execution GET /packages/{id} unavailable (%d) — deploy claudioed/fulfillment-execution#130", g.status)
		status = map[bool]string{true: "DIVERTED", false: "LABELED"}[o.kind == kindMispickInjct] + "?"
	}
	s.mu.Lock()
	o.packageStatus, o.labeledWall = status, time.Now()
	s.mu.Unlock()
	switch strings.TrimSuffix(status, "?") {
	case "LABELED":
		s.m.labeled.Add(1)
	case "DIVERTED":
		s.m.diverted.Add(1)
		logf(a.id, "package %s for order #%03d DIVERTED to problem-solve (weight %.3f vs %.3f)", short(p.packageID), o.seq, actual, expected)
	}
}

func round3(f float64) float64 { return float64(int(f*1000+0.5)) / 1000 }

// ---------------------------------------------------------------------
// the associate's working loop
// ---------------------------------------------------------------------

func (s *sim) work(ctx context.Context, a *associate, wg *sync.WaitGroup) {
	defer wg.Done()
	defer s.endShift(a)
	idle := 250 * time.Millisecond
	for ctx.Err() == nil {
		s.maybeBreak(ctx, a)
		switch a.role {
		case rolePicker:
			if !s.pickOne(ctx, a) {
				sleepCtx(ctx, idle)
			}
		case rolePacker:
			if !s.packOne(ctx, a) {
				sleepCtx(ctx, idle)
			}
		case roleRebin:
			select {
			case tt := <-s.totes:
				s.rebinOne(ctx, a, tt)
			case <-ctx.Done():
			case <-time.After(idle):
			}
		case roleSlam:
			select {
			case p := <-s.conveyor:
				s.slamOne(ctx, a, p)
			case <-ctx.Done():
			case <-time.After(idle):
			}
		case roleFlex:
			s.flexStep(ctx, a, idle)
		}
	}
}

func (s *sim) queueDepth(ctx context.Context, taskType string) int {
	var d struct {
		Depth int `json:"depth"`
	}
	r := s.api.call(ctx, svcFulfillment, http.MethodGet, "/queues/{taskType}/depth", "/queues/"+taskType+"/depth", nil)
	if r.ok() {
		_ = r.decode(&d)
	}
	return d.Depth
}

// flexStep: a cross-trained associate goes where the backlog is. Moving
// paths is a real workforce reassignment plus a station change.
func (s *sim) flexStep(ctx context.Context, a *associate, idle time.Duration) {
	pick, pack := s.queueDepth(ctx, "PICK"), s.queueDepth(ctx, "PACK")
	want := a.path
	switch {
	case a.path == "PICK" && pack > pick+2:
		want = "PACK"
	case a.path == "PACK" && pick > pack+2:
		want = "PICK"
	}
	if want != a.path {
		s.checkOut(ctx, a)
		r := s.api.call(ctx, svcWorkforce, http.MethodPost, "/associates/{id}/assignments", "/associates/"+a.id+"/assignments", map[string]any{"pathId": want})
		if !r.ok() {
			s.finding("reassign %s to %s -> %d %s", a.id, want, r.status, truncate(string(r.body), 160))
		} else {
			s.m.reassignments.Add(1)
			logf(a.id, "moved %s -> %s (PICK queue %d, PACK queue %d)", a.path, want, pick, pack)
		}
		a.path, a.station = want, a.stations[want]
		s.checkIn(ctx, a)
	}
	var worked bool
	if a.path == "PICK" {
		worked = s.pickOne(ctx, a)
	} else {
		worked = s.packOne(ctx, a)
	}
	if !worked {
		sleepCtx(ctx, idle)
	}
}

// ---------------------------------------------------------------------
// WES release scheduler (pull-based admission)
// ---------------------------------------------------------------------

func (s *sim) releaseScheduler(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	target := (s.cfg.pickers + s.cfg.flex) * 2
	for ctx.Err() == nil {
		depth := s.queueDepth(ctx, "PICK")
		for depth < target && ctx.Err() == nil {
			r := s.api.call(ctx, svcWES, http.MethodPost, "/paths/{pathId}/release", "/paths/"+s.cfg.wesPickPath+"/release", nil)
			if r.status == http.StatusOK {
				s.m.released.Add(1)
				depth++
				continue
			}
			if r.status == http.StatusConflict || r.status == http.StatusNotFound {
				s.m.releaseRejected.Add(1)
			} else {
				s.finding("WES release PICK -> %d %s", r.status, truncate(string(r.body), 160))
			}
			break
		}
		sleepCtx(ctx, 300*time.Millisecond)
	}
}

// ---------------------------------------------------------------------
// inventory control
// ---------------------------------------------------------------------

func (s *sim) inventoryControl(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	// 10:00 — cycle counts: three pick bins counted right, and the
	// overflow bin found 3 units short.
	if !sleepCtx(ctx, time.Until(s.clock.wallAt(10, 0))) {
		return
	}
	for i := 0; i < 3; i++ {
		bin := s.storage[s.ri(len(s.storage))]
		var b struct {
			Occupied int `json:"occupied"`
		}
		g := s.api.call(ctx, svcInventory, http.MethodGet, "/bins/{binId}", "/bins/"+url.PathEscape(bin), nil)
		if !g.ok() || g.decode(&b) != nil {
			continue
		}
		s.cycleCount(ctx, bin, b.Occupied, false)
	}
	s.cycleCount(ctx, s.overflow, skuByKey("SAMPLE").stock-3, true)

	// 13:00 — the CAMERA replenishment truck arrives; inventory control
	// retries every backordered order.
	if !sleepCtx(ctx, time.Until(s.clock.wallAt(13, 0))) {
		return
	}
	if err := s.receiveAndStow(ctx, "receiving", skuByKey("CAMERA"), 30, s.storage); err != nil {
		s.finding("CAMERA replenishment failed: %v", err)
		return
	}
	logf("receiving", "replenishment: 30 x CAMERA received and stowed")
	s.mu.Lock()
	var backordered []*orderRec
	for _, o := range s.orders {
		if o.kind == kindBackorder && o.id != "" {
			backordered = append(backordered, o)
		}
	}
	s.mu.Unlock()
	for _, o := range backordered {
		r := s.api.call(ctx, svcOrder, http.MethodPost, "/orders/{id}/retry-allocation", "/orders/"+o.id+"/retry-allocation", nil)
		var got omOrder
		_ = r.decode(&got)
		s.mu.Lock()
		o.retried = true
		s.mu.Unlock()
		logf("inventory-ctl", "retry-allocation order #%03d -> %d %s", o.seq, r.status, got.Status)
		if !r.ok() {
			s.finding("retry-allocation %s -> %d %s", o.id, r.status, truncate(string(r.body), 200))
		}
	}
}

func (s *sim) cycleCount(ctx context.Context, bin string, counted int, expectDiscrepancy bool) {
	r := s.api.call(ctx, svcInventory, http.MethodPost, "/bins/{binId}/cycle-count", "/bins/"+url.PathEscape(bin)+"/cycle-count", map[string]any{"countedQuantity": counted})
	var res struct {
		SystemQuantity int  `json:"systemQuantity"`
		Discrepancy    bool `json:"discrepancy"`
	}
	if !r.ok() || r.decode(&res) != nil {
		s.finding("cycle count %s -> %d %s", bin, r.status, truncate(string(r.body), 160))
		return
	}
	s.m.cycleCounts.Add(1)
	if res.Discrepancy {
		s.m.discrepancies.Add(1)
	}
	logf("inventory-ctl", "cycle count %s: counted %d, system %d, discrepancy=%v", bin, counted, res.SystemQuantity, res.Discrepancy)
	if res.Discrepancy != expectDiscrepancy {
		s.finding("cycle count %s: counted %d vs system %d, discrepancy=%v (expected %v) — bin occupancy and locatable stock disagree",
			bin, counted, res.SystemQuantity, res.Discrepancy, expectDiscrepancy)
	}
}

// ---------------------------------------------------------------------
// supervisor rounds and system sweeps
// ---------------------------------------------------------------------

func (s *sim) supervisor(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	for h := 7; h <= dayCloseHour; h++ {
		if !sleepCtx(ctx, time.Until(s.clock.wallAt(h, 0))) {
			return
		}
		var tel struct {
			BacklogDepth int    `json:"backlogDepth"`
			WIP          int    `json:"wip"`
			Mode         string `json:"mode"`
		}
		if r := s.api.call(ctx, svcWES, http.MethodGet, "/paths/{pathId}/telemetry", "/paths/"+s.cfg.wesPickPath+"/telemetry", nil); r.ok() {
			_ = r.decode(&tel)
		}
		var reb struct {
			Action string `json:"action"`
		}
		rb := s.api.call(ctx, svcWES, http.MethodGet, "/paths/{pathId}/rebalance", "/paths/"+s.cfg.wesPickPath+"/rebalance", nil)
		_ = rb.decode(&reb)
		gaps := []string{}
		for _, p := range []string{"PICK", "PACK"} {
			if !s.shiftPlanned {
				break
			}
			var g struct {
				PlannedHeads, ActiveHeads int
				Understaffed              bool
			}
			r := s.api.call(ctx, svcWorkforce, http.MethodGet, "/paths/{pathId}/staffing-gap",
				"/paths/"+p+"/staffing-gap?buildingId="+url.QueryEscape(s.cfg.buildingID)+"&shiftId="+url.QueryEscape(s.shiftID()), nil)
			if r.ok() && r.decode(&g) == nil {
				gaps = append(gaps, fmt.Sprintf("%s %d/%d%s", p, g.ActiveHeads, g.PlannedHeads, map[bool]string{true: " UNDERSTAFFED", false: ""}[g.Understaffed]))
			}
		}
		line := fmt.Sprintf("%02d:00 WES PICK backlog=%d wip=%d %s rebalance=%s | queues PICK=%d PACK=%d | staffing %s | picks=%d packs=%d shipped=%d diverted=%d",
			h, tel.BacklogDepth, tel.WIP, tel.Mode, reb.Action, s.queueDepth(ctx, "PICK"), s.queueDepth(ctx, "PACK"), strings.Join(gaps, ", "),
			s.m.picks.Load(), s.m.packs.Load(), s.m.labeled.Load(), s.m.diverted.Load())
		s.mu.Lock()
		s.snapshots = append(s.snapshots, line)
		s.mu.Unlock()
		logf("supervisor", "%s", line)
	}
}

func (s *sim) sweeps(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	for ctx.Err() == nil {
		s.api.call(ctx, svcFulfillment, http.MethodPost, "/tasks/expire-leases", "/tasks/expire-leases", nil)
		s.api.call(ctx, svcFulfillment, http.MethodPost, "/tasks/sweep-cpt-misses", "/tasks/sweep-cpt-misses", nil)
		sleepCtx(ctx, 3*time.Second)
	}
}
