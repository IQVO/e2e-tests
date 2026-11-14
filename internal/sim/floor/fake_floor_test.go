package floor

// Per-service contract fakes behind the world's one lock (see fake_test.go).

import (
	"fmt"
	"math"
	"net/http"
	"sort"
	"strings"
)

const (
	svcOM  = SvcOrders
	svcWES = SvcWES
	svcFE  = SvcExecution
	svcInv = SvcInventory
)

// ---------------------------------------------------------------------
// order-management
// ---------------------------------------------------------------------

type omLine struct {
	LineNo        int    `json:"lineNo"`
	SKU           string `json:"sku"`
	Quantity      int    `json:"quantity"`
	PathID        string `json:"pathId"`
	GiftWrap      bool   `json:"giftWrap"`
	Status        string `json:"status"`
	ReservationID string `json:"reservationId,omitempty"`
}

type omOrder struct {
	ID    string
	Site  string
	CPT   string
	lines []omLine
	// releasedAt: until this tick the order is HELD (its lines read Allocated): the network's
	// release (POST /orders/{id}/releases) has not been made or consumed yet.
	releasedAt int
	// final is the line status once released ("Released", or "Backordered" for a line without stock).
	final map[int]string
}

type omFake struct {
	w      *world
	orders map[string]*omOrder
}

func newOMFake(w *world) *omFake {
	f := &omFake{w: w, orders: map[string]*omOrder{}}
	w.route("GET", svcOM, "/orders/{id}", func(r *http.Request, _ []byte) reply {
		o, found := f.orders[r.PathValue("id")]
		if !found {
			return problem(404, svcOM, "order-not-found", "no order has this id")
		}
		lines := make([]omLine, len(o.lines))
		copy(lines, o.lines)
		status := "Released"
		for i := range lines {
			if w.tick < o.releasedAt {
				lines[i].Status = "Allocated"
			} else {
				lines[i].Status = o.final[lines[i].LineNo]
			}
			if lines[i].Status != "Released" {
				status = "PartiallyReleased"
			}
		}
		if w.tick < o.releasedAt {
			status = "Allocated"
		}
		out := map[string]any{"id": o.ID, "status": status, "allowPartialShipment": false, "releaseOnAllocation": false, "lines": lines}
		if o.Site != "" {
			out["fulfillmentSiteId"] = o.Site
		}
		if o.CPT != "" {
			out["promiseDate"] = o.CPT
		}
		return ok(out)
	})
	return f
}

// ---------------------------------------------------------------------
// wes-work-planning
// ---------------------------------------------------------------------

type wesUnit struct {
	ID         string `json:"id"`
	PathID     string `json:"pathId"`
	CPT        string `json:"cpt"`
	Reference  string `json:"reference"`
	State      string `json:"state"`
	GiftWrap   bool   `json:"giftWrap"`
	SKU        string `json:"sku,omitempty"`
	LineNo     int    `json:"lineNo,omitempty"`
	ReleasedAt string `json:"releasedAt,omitempty"`

	fragile   bool
	visibleAt int
}

type wesFake struct {
	w      *world
	units  []*wesUnit
	wipMax int
	// concurrentOnce makes the next N releases fail with 409 concurrent-modification.
	concurrentOnce int
	// released is the log of released unit ids, in order.
	released []string
}

func newWESFake(w *world) *wesFake {
	f := &wesFake{w: w, wipMax: 1000}
	w.route("GET", svcWES, "/work-units", func(r *http.Request, _ []byte) reply {
		ref := r.URL.Query().Get("reference")
		if ref == "" {
			return problem(400, svcWES, "invalid-request", "reference is required")
		}
		var items []*wesUnit
		for _, u := range f.units {
			if u.Reference == ref && w.tick >= u.visibleAt {
				items = append(items, u)
			}
		}
		sort.SliceStable(items, func(i, j int) bool {
			if items[i].CPT != items[j].CPT {
				return items[i].CPT < items[j].CPT
			}
			return items[i].ID < items[j].ID
		})
		anyItems := make([]any, len(items))
		for i, u := range items {
			anyItems[i] = u
		}
		return w.pageOf(svcWES, r, []string{"reference"}, anyItems)
	})
	w.route("POST", svcWES, "/paths/{pathId}/work-releases", func(r *http.Request, _ []byte) reply {
		path := r.PathValue("pathId")
		known := false
		var pending []*wesUnit
		wip := 0
		for _, u := range f.units {
			if u.PathID != path {
				continue
			}
			known = true
			if u.State == UnitReleased {
				wip++
			}
			if u.State == UnitPending && w.tick >= u.visibleAt {
				pending = append(pending, u)
			}
		}
		if !known {
			return problem(404, svcWES, "not-found", "no work pool has been provisioned for this path")
		}
		if f.concurrentOnce > 0 {
			f.concurrentOnce--
			return problem(409, svcWES, "concurrent-modification", "the work pool was modified concurrently")
		}
		if len(pending) == 0 {
			return problem(409, svcWES, "work-pool-empty", "the pool is empty")
		}
		if wip >= f.wipMax {
			return problem(409, svcWES, "wip-limit-reached", "release-fed pool WIP limit reached")
		}
		sort.SliceStable(pending, func(i, j int) bool {
			if pending[i].CPT != pending[j].CPT {
				return pending[i].CPT < pending[j].CPT
			}
			return pending[i].ID < pending[j].ID
		})
		u := pending[0]
		u.State = UnitReleased
		u.ReleasedAt = w.clock.Now().Format("2006-01-02T15:04:05Z")
		f.released = append(f.released, u.ID)
		w.later(w.fe.lag, func() { w.fe.createPick(u) })
		return ok(u)
	})
	return f
}

// onTaskCompleted: wes-work-planning consumes TaskCompleted and completes the unit.
func (f *wesFake) onTaskCompleted(unitID string) {
	for _, u := range f.units {
		if u.ID == unitID {
			u.State = UnitCompleted
		}
	}
}

// ---------------------------------------------------------------------
// inventory-storage
// ---------------------------------------------------------------------

type invAllocation struct {
	StockUnitID string `json:"stockUnitId"`
	BinID       string `json:"binId"`
	Quantity    int    `json:"quantity"`
}

type invRes struct {
	ID          string          `json:"id"`
	SKU         string          `json:"sku"`
	Quantity    int             `json:"quantity"`
	DemandRef   string          `json:"demandRef"`
	LineNo      int             `json:"lineNo,omitempty"`
	Status      string          `json:"status"`
	Allocations []invAllocation `json:"allocations"`
}

type invFake struct {
	w   *world
	res []*invRes
	// lag is the TaskCompleted consumer's lag in requests; consumerOff stops the consumer for good.
	lag         int
	consumerOff bool
	// concurrentOnce makes the next N confirmations of a reservation fail with 409 concurrent-modification.
	concurrentOnce map[string]int
	// confirmedBy records how each reservation was confirmed: "pick-confirmations" or "TaskCompleted".
	confirmedBy map[string]string
}

func newInvFake(w *world) *invFake {
	f := &invFake{w: w, lag: 2, concurrentOnce: map[string]int{}, confirmedBy: map[string]string{}}
	w.route("GET", svcInv, "/reservations", func(r *http.Request, _ []byte) reply {
		ref := r.URL.Query().Get("demandRef")
		if ref == "" {
			return problem(400, svcInv, "missing-demand-ref", "demandRef is required")
		}
		var items []any
		for _, x := range f.res {
			if x.DemandRef == ref {
				items = append(items, x)
			}
		}
		return w.pageOf(svcInv, r, []string{"demandRef"}, items)
	})
	w.route("POST", svcInv, "/reservations/{id}/pick-confirmations", func(r *http.Request, _ []byte) reply {
		x := f.find(r.PathValue("id"))
		if x == nil {
			return problem(404, svcInv, "reservation-not-found", "no reservation has this id")
		}
		if f.concurrentOnce[x.ID] > 0 {
			f.concurrentOnce[x.ID]--
			return problem(409, svcInv, "concurrent-modification", "a racing request modified the reservation")
		}
		if x.Status != "ACTIVE" {
			return problem(409, svcInv, "reservation-already-resolved", "the reservation is "+x.Status)
		}
		x.Status = "CONFIRMED"
		f.confirmedBy[x.ID] = "pick-confirmations"
		return noContent()
	})
	return f
}

func (f *invFake) find(id string) *invRes {
	for _, x := range f.res {
		if x.ID == id {
			return x
		}
	}
	return nil
}

// onPickCompleted: inventory-storage consumes TaskCompleted (ADR 0036) and confirms the line's reservation.
func (f *invFake) onPickCompleted(orderID string, lineNo int) {
	if f.consumerOff {
		return
	}
	f.w.later(f.lag, func() {
		for _, x := range f.res {
			if x.DemandRef == orderID && x.LineNo == lineNo && x.Status == "ACTIVE" {
				x.Status = "CONFIRMED"
				f.confirmedBy[x.ID] = "TaskCompleted"
			}
		}
	})
}

// ---------------------------------------------------------------------
// fulfillment-execution
// ---------------------------------------------------------------------

type feTask struct {
	ID                   string   `json:"id"`
	Type                 string   `json:"type"`
	Status               string   `json:"status"`
	CPT                  string   `json:"cpt"`
	OrderRef             string   `json:"orderRef"`
	RequiredCapabilities []string `json:"requiredCapabilities"`
	Fragile              bool     `json:"fragile"`
	GiftWrap             bool     `json:"giftWrap"`
	LeaseStationID       string   `json:"leaseStationId,omitempty"`

	unit *wesUnit
}

type feStation struct {
	ID       string
	Caps     []string
	Occupant string
	Loc      string
}

func (s *feStation) json() map[string]any {
	m := map[string]any{"id": s.ID, "capabilities": s.Caps, "occupied": s.Occupant != ""}
	if s.Loc != "" {
		m["locationCode"] = s.Loc
	}
	return m
}

type fePackage struct {
	ID              string   `json:"id"`
	OrderRef        string   `json:"orderRef"`
	Status          string   `json:"status"`
	ScannedContents []string `json:"scannedContents"`
	FragileHandling bool     `json:"fragileHandling"`
	GiftWrap        bool     `json:"giftWrapRequested"`
	SortLane        string   `json:"sortLane"`
}

type consolidation struct {
	required map[string]bool
	arrived  map[string]bool
	created  bool
}

type feFake struct {
	w *world
	// lag is the WorkReleased consumer's lag in requests.
	lag      int
	stations map[string]*feStation
	tasks    []*feTask
	packages []*fePackage
	consol   map[string]*consolidation
	// manifested counts PackageManifested events per order ref (visible after tapLag requests).
	manifested map[string]int
	tapLag     int
	// rebinPackCPT is the packCpt of the arrival that created each order's PACK task.
	rebinPackCPT map[string]string
	// completedBy lists the task ids each station completed.
	completedBy map[string][]string
	// sealContents is what each package was sealed with.
	sealContents map[string][]string
	// lastSlam is the weights of the last SLAM run per package id.
	lastSlam map[string][2]float64
	seq      int
	// autoComplete lists work unit ids whose PICK task is completed the moment it is created: an
	// associate who is not one of the floor's crew (another simulator's picker) got there first.
	autoComplete map[string]bool
	// rejectLocations makes POST /stations refuse a locationCode that is no WorkCenter (422).
	rejectLocations bool
	// sealRefusal, when set, makes the next seal answer that problem (status, slug).
	sealRefusal *[2]any
}

func newFEFake(w *world) *feFake {
	f := &feFake{w: w, lag: 2, tapLag: 2, stations: map[string]*feStation{}, consol: map[string]*consolidation{},
		manifested: map[string]int{}, rebinPackCPT: map[string]string{}, completedBy: map[string][]string{},
		sealContents: map[string][]string{}, lastSlam: map[string][2]float64{}}

	w.route("POST", svcFE, "/stations", func(_ *http.Request, body []byte) reply {
		var req struct {
			StationID    string   `json:"stationId"`
			Capabilities []string `json:"capabilities"`
			LocationCode *string  `json:"locationCode"`
		}
		if rp, good := decode(svcFE, body, &req); !good {
			return rp
		}
		if req.StationID == "" || len(req.Capabilities) == 0 {
			return problem(400, svcFE, "invalid-request", "stationId and capabilities are required")
		}
		if f.rejectLocations && req.LocationCode != nil {
			return problem(422, svcFE, "station-location-not-workcenter", "the location is not a WorkCenter")
		}
		s := f.stations[req.StationID]
		if s == nil {
			s = &feStation{ID: req.StationID}
			f.stations[req.StationID] = s
		}
		s.Caps = req.Capabilities
		if req.LocationCode != nil {
			s.Loc = *req.LocationCode
		}
		return created(s.json())
	})
	w.route("POST", svcFE, "/stations/{id}/check-ins", func(r *http.Request, body []byte) reply {
		s := f.stations[r.PathValue("id")]
		if s == nil {
			return problem(404, svcFE, "station-not-found", "no station is registered with this id")
		}
		var req struct {
			OccupantID string `json:"occupantId"`
		}
		if rp, good := decode(svcFE, body, &req); !good {
			return rp
		}
		if req.OccupantID == "" {
			return problem(400, svcFE, "invalid-request", "occupantId is required")
		}
		if s.Occupant != "" {
			return problem(409, svcFE, "station-occupied", "the station already has an occupant")
		}
		s.Occupant = req.OccupantID
		return ok(s.json())
	})
	w.route("POST", svcFE, "/stations/{id}/check-outs", func(r *http.Request, _ []byte) reply {
		s := f.stations[r.PathValue("id")]
		if s == nil {
			return problem(404, svcFE, "station-not-found", "no station is registered with this id")
		}
		if s.Occupant == "" {
			return problem(409, svcFE, "station-not-occupied", "the station has no occupant")
		}
		s.Occupant = ""
		return ok(s.json())
	})
	w.route("POST", svcFE, "/stations/{id}/task-claims", func(r *http.Request, body []byte) reply {
		s := f.stations[r.PathValue("id")]
		if s == nil {
			return problem(404, svcFE, "station-not-found", "no station is registered with this id")
		}
		var req struct {
			TaskType string `json:"taskType"`
		}
		if rp, good := decode(svcFE, body, &req); !good {
			return rp
		}
		var best *feTask
		for _, t := range f.tasks {
			if t.Type != req.TaskType || t.Status != TaskPending || !subset(t.RequiredCapabilities, s.Caps) {
				continue
			}
			if best == nil || t.CPT < best.CPT || (t.CPT == best.CPT && t.ID < best.ID) {
				best = t
			}
		}
		if best == nil {
			return problem(409, svcFE, "no-claimable-task", "no pending task of this type fits the station")
		}
		best.Status, best.LeaseStationID = TaskClaimed, s.ID
		return ok(best)
	})
	w.route("POST", svcFE, "/tasks/{id}/completions", func(r *http.Request, body []byte) reply {
		t := f.task(r.PathValue("id"))
		if t == nil {
			return problem(404, svcFE, "task-not-found", "no task exists with this id")
		}
		var req struct {
			StationID string `json:"stationId"`
		}
		if rp, good := decode(svcFE, body, &req); !good {
			return rp
		}
		if req.StationID == "" {
			return problem(400, svcFE, "invalid-request", "stationId is required")
		}
		switch {
		case t.Status == TaskCompleted:
			return problem(409, svcFE, "task-already-completed", "the task was already completed")
		case t.Status != TaskClaimed:
			return problem(409, svcFE, "task-not-claimed", "the task has no active claim")
		case t.LeaseStationID != req.StationID:
			return problem(409, svcFE, "task-not-owner", "the station does not own the active claim")
		}
		t.Status = TaskCompleted
		f.completedBy[req.StationID] = append(f.completedBy[req.StationID], t.ID)
		if t.Type == TypePick && t.unit != nil {
			unit := t.unit
			w.later(2, func() { w.wes.onTaskCompleted(unit.ID) })
			w.inv.onPickCompleted(unit.Reference, unit.LineNo)
		}
		return noContent()
	})
	w.route("POST", svcFE, "/rebin/arrivals", func(_ *http.Request, body []byte) reply {
		var req struct {
			OrderRef                 string   `json:"orderRef"`
			LineID                   string   `json:"lineId"`
			RequiredLineIDs          []string `json:"requiredLineIds"`
			PackCPT                  string   `json:"packCpt"`
			PackRequiredCapabilities []string `json:"packRequiredCapabilities"`
			PackFragile              *bool    `json:"packFragile"`
			PackGiftWrap             *bool    `json:"packGiftWrap"`
		}
		if rp, good := decode(svcFE, body, &req); !good {
			return rp
		}
		if req.OrderRef == "" || req.LineID == "" || len(req.RequiredLineIDs) == 0 || req.PackCPT == "" || len(req.PackRequiredCapabilities) == 0 {
			return problem(400, svcFE, "invalid-request", "orderRef, lineId, requiredLineIds, packCpt and packRequiredCapabilities are required")
		}
		c := f.consol[req.OrderRef]
		if c == nil {
			c = &consolidation{required: map[string]bool{}, arrived: map[string]bool{}}
			for _, l := range req.RequiredLineIDs {
				c.required[l] = true
			}
			f.consol[req.OrderRef] = c
		}
		if !c.required[req.LineID] {
			return problem(422, svcFE, "rebin-unknown-line", "the line is not part of the order's required set")
		}
		c.arrived[req.LineID] = true
		if !c.created && len(c.arrived) == len(c.required) {
			c.created = true
			f.seq++
			f.tasks = append(f.tasks, &feTask{ID: fmt.Sprintf("task-%d", f.seq), Type: TypePack, Status: TaskPending, CPT: req.PackCPT, OrderRef: req.OrderRef,
				RequiredCapabilities: req.PackRequiredCapabilities, Fragile: req.PackFragile != nil && *req.PackFragile, GiftWrap: req.PackGiftWrap != nil && *req.PackGiftWrap})
			f.rebinPackCPT[req.OrderRef] = req.PackCPT
		}
		return noContent()
	})
	w.route("POST", svcFE, "/tasks/{id}/package-seals", func(r *http.Request, body []byte) reply {
		t := f.task(r.PathValue("id"))
		if t == nil {
			return problem(404, svcFE, "task-not-found", "no task exists with this id")
		}
		var req struct {
			StationID string   `json:"stationId"`
			Contents  []string `json:"contents"`
		}
		if rp, good := decode(svcFE, body, &req); !good {
			return rp
		}
		if req.StationID == "" {
			return problem(400, svcFE, "invalid-request", "stationId is required")
		}
		if f.sealRefusal != nil {
			ref := *f.sealRefusal
			f.sealRefusal = nil
			return problem(ref[0].(int), svcFE, ref[1].(string), "scripted refusal")
		}
		switch {
		case t.Type != TypePack:
			return problem(422, svcFE, "wrong-task-type", "the task is not a Pack task")
		case t.Status != TaskClaimed:
			return problem(409, svcFE, "task-not-claimed", "the station has no active claim on this task")
		case t.LeaseStationID != req.StationID:
			return problem(409, svcFE, "task-not-owner", "another station holds the claim")
		case len(req.Contents) == 0:
			return problem(422, svcFE, "package-no-scanned-contents", "nothing was scanned")
		}
		f.seq++
		p := &fePackage{ID: fmt.Sprintf("pkg-%d", f.seq), OrderRef: t.OrderRef, Status: PackageSealed, ScannedContents: req.Contents,
			FragileHandling: t.Fragile, GiftWrap: t.GiftWrap, SortLane: "STANDARD"}
		f.packages = append(f.packages, p)
		f.sealContents[p.ID] = req.Contents
		return created(p)
	})
	w.route("POST", svcFE, "/packages/{id}/slam-runs", func(r *http.Request, body []byte) reply {
		p := f.pkg(r.PathValue("id"))
		if p == nil {
			return problem(404, svcFE, "package-not-found", "no package exists with this id")
		}
		var req struct {
			ActualWeight   float64 `json:"actualWeight"`
			ExpectedWeight float64 `json:"expectedWeight"`
		}
		if rp, good := decode(svcFE, body, &req); !good {
			return rp
		}
		switch p.Status {
		case PackageLabeled, PackageDiverted:
			return problem(409, svcFE, "package-already-processed", "the package was already labeled or diverted")
		case "OPEN":
			return problem(409, svcFE, "package-not-sealed", "the package is still open")
		}
		f.lastSlam[p.ID] = [2]float64{req.ActualWeight, req.ExpectedWeight}
		if math.Abs(req.ActualWeight-req.ExpectedWeight) <= 0.05 {
			p.Status = PackageLabeled
			ref := p.OrderRef
			w.later(f.tapLag, func() { f.manifested[ref]++ })
		} else {
			p.Status = PackageDiverted
		}
		return noContent()
	})
	w.route("GET", svcFE, "/packages/{id}", func(r *http.Request, _ []byte) reply {
		p := f.pkg(r.PathValue("id"))
		if p == nil {
			return problem(404, svcFE, "package-not-found", "no package exists with this id")
		}
		return ok(p)
	})
	w.route("GET", svcFE, "/packages", func(r *http.Request, _ []byte) reply {
		ref := r.URL.Query().Get("orderRef")
		if ref == "" {
			return problem(400, svcFE, "invalid-request", "orderRef is required")
		}
		var items []any
		for _, p := range f.packages {
			if p.OrderRef == ref {
				items = append(items, p)
			}
		}
		return w.pageOf(svcFE, r, []string{"orderRef"}, items)
	})
	w.route("GET", svcFE, "/tasks", func(r *http.Request, _ []byte) reply {
		ref := r.URL.Query().Get("orderRef")
		if ref == "" {
			return problem(400, svcFE, "invalid-request", "orderRef is required")
		}
		var items []any
		for _, t := range f.tasks {
			if t.OrderRef == ref {
				items = append(items, t)
			}
		}
		return w.pageOf(svcFE, r, []string{"orderRef"}, items)
	})
	return f
}

func subset(need, have []string) bool {
	for _, n := range need {
		found := false
		for _, h := range have {
			if h == n {
				found = true
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func (f *feFake) task(id string) *feTask {
	for _, t := range f.tasks {
		if t.ID == id {
			return t
		}
	}
	return nil
}

func (f *feFake) pkg(id string) *fePackage {
	for _, p := range f.packages {
		if p.ID == id {
			return p
		}
	}
	return nil
}

// createPick is the WorkReleased consumer: the released unit becomes a PENDING PICK task whose
// orderRef is the work unit's id (an order's PICK task is per line).
func (f *feFake) createPick(u *wesUnit) {
	f.seq++
	t := &feTask{ID: fmt.Sprintf("task-%d", f.seq), Type: TypePick, Status: TaskPending, CPT: u.CPT, OrderRef: u.ID,
		RequiredCapabilities: []string{"pick"}, Fragile: u.fragile, GiftWrap: u.GiftWrap, unit: u}
	f.tasks = append(f.tasks, t)
	if f.autoComplete[u.ID] {
		t.Status, t.LeaseStationID = TaskCompleted, "SOMEONE-ELSE"
		f.w.wes.onTaskCompleted(u.ID)
		f.w.inv.onPickCompleted(u.Reference, u.LineNo)
	}
}

// addForeignPick puts a PENDING PICK task of somebody else's order in the queue, visible at once.
func (f *feFake) addForeignPick(orderRef, cpt string) *feTask {
	f.seq++
	t := &feTask{ID: fmt.Sprintf("task-%d", f.seq), Type: TypePick, Status: TaskPending, CPT: cpt, OrderRef: orderRef, RequiredCapabilities: []string{"pick"}}
	f.tasks = append(f.tasks, t)
	return t
}

// addForeignPack puts a PENDING PACK task of somebody else's order in the queue.
func (f *feFake) addForeignPack(orderRef, cpt string) *feTask {
	f.seq++
	t := &feTask{ID: fmt.Sprintf("task-%d", f.seq), Type: TypePack, Status: TaskPending, CPT: cpt, OrderRef: orderRef, RequiredCapabilities: []string{"pack"}}
	f.tasks = append(f.tasks, t)
	return t
}

// ---------------------------------------------------------------------
// scenario builders
// ---------------------------------------------------------------------

const cptEvening = "2026-11-14T23:00:00Z" // 17:00 America/Chicago

type lineSpec struct {
	SKU     string
	Qty     int
	Path    string // WES path id; default "PICK"
	Gift    bool
	Fragile bool
	// Backordered: the line has no stock: order-management shows it Backordered, no work unit exists.
	Backordered bool
}

type orderSpec struct {
	ID    string
	Site  string
	CPT   string // default cptEvening
	Lines []lineSpec
	// HeldFor: the order is HELD for this many requests (its lines read Allocated).
	HeldFor int
	// UnitLag: the work units appear in wes-work-planning this many requests after now.
	UnitLag int
}

// addOrder creates the order in order-management, its reservations in inventory-storage and its
// work units in wes-work-planning, the way OrderAllocated leaves them.
func (w *world) addOrder(o orderSpec) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if o.CPT == "" {
		o.CPT = cptEvening
	}
	om := &omOrder{ID: o.ID, Site: o.Site, CPT: o.CPT, releasedAt: w.tick + o.HeldFor, final: map[int]string{}}
	for i, l := range o.Lines {
		no := i + 1
		path := l.Path
		if path == "" {
			path = "PICK"
		}
		line := omLine{LineNo: no, SKU: l.SKU, Quantity: l.Qty, PathID: path, GiftWrap: l.Gift}
		if l.Backordered {
			om.final[no] = "Backordered"
		} else {
			om.final[no] = "Released"
			line.ReservationID = fmt.Sprintf("res-%s-%d", o.ID, no)
			w.inv.res = append(w.inv.res, &invRes{ID: line.ReservationID, SKU: l.SKU, Quantity: l.Qty, DemandRef: o.ID, LineNo: no, Status: "ACTIVE",
				Allocations: []invAllocation{{StockUnitID: "su-" + l.SKU, BinID: "AUS3-PICK-" + l.SKU, Quantity: l.Qty}}})
			w.wes.units = append(w.wes.units, &wesUnit{ID: fmt.Sprintf("%s-line-%d", o.ID, no), PathID: path, CPT: o.CPT, Reference: o.ID, State: UnitPending,
				GiftWrap: l.Gift, SKU: l.SKU, LineNo: no, fragile: l.Fragile, visibleAt: w.tick + o.UnitLag})
		}
		om.lines = append(om.lines, line)
	}
	w.om.orders[o.ID] = om
}

// unitOf, taskOf and friends read the fakes' state for assertions.
func (w *world) unitState(id string) string {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, u := range w.wes.units {
		if u.ID == id {
			return u.State
		}
	}
	return ""
}

func (w *world) tasksOf(ref string) []feTask {
	w.mu.Lock()
	defer w.mu.Unlock()
	var out []feTask
	for _, t := range w.fe.tasks {
		if t.OrderRef == ref {
			out = append(out, *t)
		}
	}
	return out
}

func (w *world) packagesOf(ref string) []fePackage {
	w.mu.Lock()
	defer w.mu.Unlock()
	var out []fePackage
	for _, p := range w.fe.packages {
		if p.OrderRef == ref {
			out = append(out, *p)
		}
	}
	return out
}

func (w *world) reservationStatus(id string) string {
	w.mu.Lock()
	defer w.mu.Unlock()
	if x := w.inv.find(id); x != nil {
		return x.Status
	}
	return ""
}

func (w *world) pendingPicks() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	n := 0
	for _, t := range w.fe.tasks {
		if t.Type == TypePick && t.Status == TaskPending {
			n++
		}
	}
	return n
}

func joinSorted(in []string) string {
	c := append([]string(nil), in...)
	sort.Strings(c)
	return strings.Join(c, ",")
}

// addForeignUnit puts a Pending work unit of somebody else's order into a pool.
func (w *world) addForeignUnit(id, ref, path, cpt string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.wes.units = append(w.wes.units, &wesUnit{ID: id, PathID: path, CPT: cpt, Reference: ref, State: UnitPending, LineNo: 1})
}

func (w *world) taskState(id string) (string, string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if t := w.fe.task(id); t != nil {
		return t.Status, t.LeaseStationID
	}
	return "", ""
}
