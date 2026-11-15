package collect

import (
	"sort"
	"sync"

	"github.com/claudioed/warehouse-e2e/internal/retailday"
)

// The warehouse's side of the order: the order-management order(s) raised for the purchase
// order, the work units wes-work-planning planned for them, the PICK and PACK tasks and the
// packages fulfillment-execution holds, and the SHIP_CONFIRMED loads outbound-shipping closed.
//
// The id every one of them is keyed by is order-management's, which no published route links to
// the customer order or its purchase order (package comment): Settings.OrderIDs supplies it.

const warehouseFacts = "OMOrders, WorkUnits, PickTasksDone, PackTasks, Packages, ShipmentConfirmations"

// ---- the link ----

// orderIDs asks the runner's link for the order-management orders raised for the order's purchase
// orders. ok is false when there is no link or it failed: the whole warehouse side is then
// unknown, which is said, never read as "nothing exists".
func (r *run) orderIDs(id string, numbers []string) (ids []string, ok bool) {
	if r.c.set.OrderIDs == nil {
		r.note("unread %s: Settings.OrderIDs is not set, and no published route links order %s to its order-management order", warehouseFacts, id)
		return nil, false
	}
	p := r.p
	p.OrderID = id // the order the payment records named, when the shopper phase had none
	defer func() {
		if e := recover(); e != nil {
			r.note("unread %s: the link to order-management panicked: %v", warehouseFacts, e)
			ids, ok = nil, false
		}
	}()
	got, err := r.c.set.OrderIDs(r.ctx, p, numbers)
	if err != nil {
		r.note("unread %s: the link to order-management failed: %s", warehouseFacts, err)
		return nil, false
	}
	return unionOf(got), true // an id twice is one order; two ids are two orders and both are read
}

// ---- order-management, wes-work-planning, fulfillment-execution ----

type omOrderView struct {
	ID                string `json:"id"`
	Status            string `json:"status"`
	FulfillmentSiteID string `json:"fulfillmentSiteId"`
	Lines             []struct {
		SKU      string `json:"sku"`
		Quantity int    `json:"quantity"`
	} `json:"lines"`
}

type workUnitView struct {
	ID    string `json:"id"`
	State string `json:"state"`
}

type taskView struct {
	ID     string `json:"id"`
	Type   string `json:"type"`
	Status string `json:"status"`
}

type packageView struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

// count is a number that is only a fact when it could be read in full.
type count struct {
	n  int
	ok bool
}

// omResult is what one order-management order leads to.
type omResult struct {
	order   *retailday.OMOrder // nil: none exists, or it could not be read
	orderOK bool
	units   count
	picks   count
	packs   count
	pkgs    []retailday.Package
	pkgsOK  bool
}

// readOM reads everything keyed by one order-management order id, at the same time where the
// reads do not depend on each other.
func (r *run) readOM(omID string) omResult {
	var res omResult
	r.par(
		func() { res.order, res.orderOK = r.omOrder(omID) },
		func() { res.units, res.picks = r.workAndPicks(omID) },
		func() {
			tasks, oc := fetchList[taskView](r, "PackTasks", "fulfillment-execution", "/tasks?orderRef", "/tasks"+query("orderRef", omID))
			res.packs.ok = oc != failed
			for _, t := range tasks {
				if t.Type == "PACK" {
					res.packs.n++
				}
			}
		},
		func() {
			pkgs, oc := fetchList[packageView](r, "Packages", "fulfillment-execution", "/packages?orderRef", "/packages"+query("orderRef", omID))
			res.pkgsOK = oc != failed
			for _, p := range pkgs {
				res.pkgs = append(res.pkgs, retailday.Package{ID: p.ID, Outcome: p.Status})
			}
		},
	)
	return res
}

// omOrder reads one order-management order. The id came from the link, so a 404 contradicts it.
func (r *run) omOrder(omID string) (*retailday.OMOrder, bool) {
	v, oc := fetchOne[omOrderView](r, "OMOrders", "order-management", "/orders/{id}", "/orders/"+esc(omID))
	switch oc {
	case none:
		r.note("disagree OMOrders: the link names order-management order %s, which order-management does not have", omID)
		return nil, true
	case failed:
		return nil, false
	}
	// G2 (docs/retail-day-journey.md): a storefront order must be fulfilled at the building that
	// serves it, or the floor and outbound shipping never see it.
	if site := r.c.set.BuildingSite; site != "" && v.FulfillmentSiteID != site {
		if v.FulfillmentSiteID == "" {
			r.note("disagree OMOrders: order-management order %s has no fulfillmentSiteId, the storefront is served from %s", omID, site)
		} else {
			r.note("disagree OMOrders: order-management order %s is fulfilled at %s, the storefront is served from %s", omID, v.FulfillmentSiteID, site)
		}
	}
	if len(r.p.SKUs) > 0 {
		got := map[string]int{}
		for _, l := range v.Lines {
			got[l.SKU] += l.Quantity
		}
		if !sameQuantities(r.p.SKUs, got) {
			r.note("disagree Lines: the shopper bought %s, order-management order %s holds %s", showQuantities(r.p.SKUs), omID, showQuantities(got))
		}
	}
	id := v.ID
	if id == "" {
		id = omID
	}
	return &retailday.OMOrder{ID: id, Status: v.Status}, true
}

// workAndPicks reads the work units of the order and, for each, the PICK tasks (a PICK task's
// orderRef is its work unit's id: fulfillment-execution README, WorkReleased.work_unit_id).
// Units that were cancelled are not work and are not counted.
func (r *run) workAndPicks(omID string) (units, picks count) {
	us, oc := fetchList[workUnitView](r, "WorkUnits, PickTasksDone", "wes-work-planning", "/work-units?reference", "/work-units"+query("reference", omID))
	if oc == failed {
		return count{}, count{}
	}
	var live []workUnitView
	for _, u := range us {
		if u.State != "Cancelled" {
			live = append(live, u)
		}
	}
	units = count{n: len(live), ok: true}
	done := make([]int, len(live))
	okays := make([]bool, len(live))
	fns := make([]func(), len(live))
	for i, u := range live {
		fns[i] = func() {
			ts, oc := fetchList[taskView](r, "PickTasksDone", "fulfillment-execution", "/tasks?orderRef", "/tasks"+query("orderRef", u.ID))
			okays[i] = oc != failed
			for _, t := range ts {
				if t.Type == "PICK" && t.Status == "COMPLETED" {
					done[i]++
				}
			}
		}
	}
	r.par(fns...)
	picks.ok = true
	for i := range live {
		picks.n += done[i]
		picks.ok = picks.ok && okays[i]
	}
	if !picks.ok {
		picks.n = 0
	}
	return units, picks
}

// ---- outbound-shipping ----

type loadView struct {
	ID      string `json:"id"`
	Version int64  `json:"version"`
}

type loadPackageView struct {
	SourceOrderID string `json:"source_order_id"`
	State         string `json:"state"`
}

// loadMemo remembers the packages of a SHIP_CONFIRMED load. Such a load has left (ADR 0002: at most
// once), so its packages cannot change; the version it was read at is kept anyway, so a load that
// did change is read again.
type loadMemo struct {
	mu   sync.Mutex
	seen map[string]memoed
}

type memoed struct {
	version int64
	pkgs    []loadPackageView
}

func (m *loadMemo) get(id string, version int64) ([]loadPackageView, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.seen[id]
	if !ok || e.version != version {
		return nil, false
	}
	return e.pkgs, true
}

func (m *loadMemo) put(id string, version int64, pkgs []loadPackageView) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.seen == nil {
		m.seen = map[string]memoed{}
	}
	m.seen[id] = memoed{version, pkgs}
}

// shipments counts the SHIP_CONFIRMED loads, at the building's site, that carry a SHIPPED package of
// one of the order-management orders. ShipmentConfirmed itself cannot be counted over REST (package
// comment): this is the load-side proof that the order left and was confirmed once per load.
func (r *run) shipments(omIDs []string) count {
	if len(omIDs) == 0 {
		return count{ok: true}
	}
	kv := []string{"state", "SHIP_CONFIRMED"}
	if site := r.c.set.BuildingSite; site != "" {
		kv = append(kv, "site_id", site)
	}
	loads, more, resp := listUpTo[loadView](r.ctx, r.c, "outbound-shipping", "/loads", "/loads"+query(kv...), r.c.set.MaxLoads)
	switch classify(resp) {
	case failed:
		r.unread("ShipmentConfirmations", "outbound-shipping", "/loads", resp)
		return count{}
	case none:
		return count{ok: true}
	}
	if more {
		r.note("unread ShipmentConfirmations: outbound-shipping holds more than %d SHIP_CONFIRMED loads (Settings.MaxLoads); the newest %d were read and the order's may be older", r.c.set.MaxLoads, len(loads))
		return count{}
	}
	mine := map[string]bool{}
	for _, id := range omIDs {
		mine[id] = true
	}
	carries := make([]bool, len(loads))
	okays := make([]bool, len(loads))
	fns := make([]func(), len(loads))
	for i, l := range loads {
		fns[i] = func() {
			pkgs, ok := r.loadPackages(l)
			okays[i] = ok
			for _, p := range pkgs {
				if p.State == "SHIPPED" && mine[p.SourceOrderID] {
					carries[i] = true
				}
			}
		}
	}
	r.par(fns...)
	out := count{ok: true}
	for i := range loads {
		out.ok = out.ok && okays[i]
		if carries[i] {
			out.n++
		}
	}
	if !out.ok {
		return count{}
	}
	return out
}

// loadPackages is the packages of one load: from the memo, or read to the end of their pages.
func (r *run) loadPackages(l loadView) ([]loadPackageView, bool) {
	if pkgs, hit := r.c.memo.get(l.ID, l.Version); hit {
		return pkgs, true
	}
	pkgs, oc := fetchList[loadPackageView](r, "ShipmentConfirmations", "outbound-shipping", "/loads/{loadId}/packages", "/loads/"+esc(l.ID)+"/packages")
	switch oc {
	case failed:
		return nil, false
	case found:
		r.c.memo.put(l.ID, l.Version, pkgs)
	}
	return pkgs, true // a load that vanished between the two reads carries nothing
}

// warehouse reads the warehouse side for the order-management orders the link names.
func (r *run) warehouse(id string, poNumbers []string) (f warehouseResult) {
	ids, linked := r.orderIDs(id, poNumbers)
	if !linked {
		return f
	}
	res := make([]omResult, len(ids))
	var ship count
	fns := make([]func(), 0, len(ids)+1)
	for i, omID := range ids {
		fns = append(fns, func() { res[i] = r.readOM(omID) })
	}
	fns = append(fns, func() { ship = r.shipments(ids) })
	r.par(fns...)

	// A fact is its total over every order, or zero when any order's part could not be read.
	allOrders, allUnits, allPicks, allPacks, allPkgs := true, true, true, true, true
	for _, x := range res {
		allOrders, allUnits, allPicks = allOrders && x.orderOK, allUnits && x.units.ok, allPicks && x.picks.ok
		allPacks, allPkgs = allPacks && x.packs.ok, allPkgs && x.pkgsOK
	}
	for _, x := range res {
		if allOrders && x.order != nil {
			f.orders = append(f.orders, *x.order)
		}
		if allUnits {
			f.units += x.units.n
		}
		if allPicks {
			f.picks += x.picks.n
		}
		if allPacks {
			f.packs += x.packs.n
		}
		if allPkgs {
			f.pkgs = append(f.pkgs, x.pkgs...)
		}
	}
	sort.Slice(f.orders, func(i, j int) bool { return f.orders[i].ID < f.orders[j].ID })
	sort.Slice(f.pkgs, func(i, j int) bool { return f.pkgs[i].ID < f.pkgs[j].ID })
	if ship.ok {
		f.shipments = ship.n
	}
	return f
}

// warehouseResult is the warehouse side of OrderFacts.
type warehouseResult struct {
	orders    []retailday.OMOrder
	units     int
	picks     int
	packs     int
	pkgs      []retailday.Package
	shipments int
}
