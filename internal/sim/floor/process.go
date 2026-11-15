package floor

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Process takes one RELEASED order (order-management shows its lines Released, or it will within
// Settings.Lag) from wes-work-planning to a LABELED package:
//
//  1. wait for the order's lines to be Released and read them (order-management);
//  2. wait for wes-work-planning to hold one work unit per line (OrderAllocated is consumed there);
//  3. until every line is picked: the release operator admits the next unit of a path that still
//     holds a Pending unit of this order (pull release: it may admit other orders' units first),
//     and a picker claims the next PICK task, works it if it belongs to an order this floor knows
//     (completing it, optionally confirming the reservation first) and the rebin associate scans
//     the line into the order's slot; the arrival that completes the set creates the PACK task;
//  4. until the order's package is sealed: a packer claims the next PACK task, scans the order's
//     contents and seals them, and completes the task;
//  5. the SLAM operator weighs the package and reads LABELED or DIVERTED;
//  6. the reservations are read until inventory-storage shows them CONFIRMED (it consumes
//     TaskCompleted), and, with Settings.Events, PackageManifested of the order is awaited.
//
// A step a service refuses, or that does not happen within its bound, is a finding in
// Result.Problems; the run stops where it cannot go on and Result says how far it got. Nothing
// panics. Process is safe to call concurrently for different orders: they share the stations, and a
// station that is handed another order's task works it for that order.
func (f *Floor) Process(ctx context.Context, orderID string) (r Result) {
	r = Result{OrderID: orderID, OrderRef: orderID, SourceOrderID: orderID}
	add := func(format string, a ...any) { r.Problems = append(r.Problems, fmt.Sprintf(format, a...)) }
	if pk, pc := f.crewSize(); pk == 0 || pc == 0 {
		add("order %s: the floor has no crew (%d pickers, %d packers): call Provision first", orderID, pk, pc)
		return r
	}
	st := &orderState{id: orderID}
	if !f.register(st) {
		add("order %s is already being processed by this floor", orderID)
		return r
	}
	defer f.unregister(st)
	defer func() { f.fill(&r, st) }()

	if err := f.awaitOrder(ctx, st); err != nil {
		add("%v", err)
		return r
	}
	if f.set.Site != "" {
		switch site := st.siteID(); site {
		case f.set.Site:
		case "":
			add("order %s has no fulfillmentSiteId, the floor works for %s: its PackageManifested would carry no site_id and outbound-shipping ignores such a manifest (retail-day-journey G2)", orderID, f.set.Site)
		default:
			add("order %s is for fulfillmentSiteId %s, the floor works for %s", orderID, site, f.set.Site)
		}
	}
	if err := f.awaitUnits(ctx, st); err != nil {
		add("%v", err)
		return r
	}
	f.registerUnits(st)

	if err := f.pickLines(ctx, st); err != nil {
		add("%v", err)
		return r
	}
	if err := f.packOrder(ctx, st); err != nil {
		add("%v", err)
		return r
	}
	if err := f.slam(ctx, st, &r); err != nil {
		add("%v", err)
		return r
	}
	if r.Status != PackageLabeled {
		return r
	}
	if f.set.Events != nil {
		if err := f.awaitManifest(ctx, st); err != nil {
			add("%v", err)
		}
	}
	for _, p := range f.confirmStock(ctx, st) {
		add("%s", p)
	}
	if p := st.promiseFinding(); p != "" {
		add("%s", p)
	}
	return r
}

// ProcessAll runs Process for every order, at most parallel at a time (0 means all at once), and
// returns the results in the order of ids. An id given twice is processed once and reported on its
// second place.
func (f *Floor) ProcessAll(ctx context.Context, ids []string, parallel int) []Result {
	out := make([]Result, len(ids))
	if parallel <= 0 || parallel > len(ids) {
		parallel = len(ids)
	}
	sem := make(chan struct{}, max(parallel, 1))
	seen := map[string]bool{}
	var wg sync.WaitGroup
	for i, id := range ids {
		if seen[id] {
			out[i] = Result{OrderID: id, Problems: []string{fmt.Sprintf("order %s was given twice: processed once", id)}}
			continue
		}
		seen[id] = true
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			out[i] = f.Process(ctx, id)
		}()
	}
	wg.Wait()
	return out
}

// fill copies what the state knows into the result.
func (f *Floor) fill(r *Result, st *orderState) {
	st.mu.Lock()
	defer st.mu.Unlock()
	r.SiteID, r.CPT, r.PackTaskID = st.site, st.cpt, st.packTask
	for _, l := range st.lines {
		if l.Unit.ID != "" {
			r.Units = append(r.Units, l.Unit.ID)
		}
	}
	if st.pkg != nil {
		r.PackageID, r.SortLane, r.Contents = st.pkg.ID, st.pkg.SortLane, st.pkg.ScannedContents
		if r.Status == "" {
			r.Status = st.pkg.Status
		}
	}
}

func (f *Floor) register(st *orderState) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, busy := f.orders[st.id]; busy {
		return false
	}
	f.orders[st.id] = st
	return true
}

func (f *Floor) unregister(st *orderState) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.orders, st.id)
	for id, l := range f.units {
		if l.order == st {
			delete(f.units, id)
		}
	}
}

func (f *Floor) registerUnits(st *orderState) {
	f.mu.Lock()
	defer f.mu.Unlock()
	st.mu.Lock()
	defer st.mu.Unlock()
	for _, l := range st.lines {
		f.units[l.Unit.ID] = l
	}
}

// lookupUnit finds the line a PICK task (whose orderRef is the work unit id) belongs to.
func (f *Floor) lookupUnit(unitID string) (*orderState, *lineState) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if l, found := f.units[unitID]; found {
		return l.order, l
	}
	return nil, nil
}

// lookupOrder finds the order a PACK task (whose orderRef is the order id) belongs to.
func (f *Floor) lookupOrder(orderID string) *orderState {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.orders[orderID]
}

// awaitOrder waits until every line of the order that can be worked is Released and keeps them.
// A Backordered line is a finding at once: it has no stock, no work unit will ever exist for it, and
// releasing the rest would ship part of a ship-complete order.
func (f *Floor) awaitOrder(ctx context.Context, st *orderState) error {
	what := "order " + st.id
	return await(ctx, f.clock, f.set.Lag, what+": wait for its lines to be Released", func(ctx context.Context) (verdict, string, error) {
		r := f.call(ctx, SvcOrders, http.MethodGet, "/orders/{id}", "/orders/"+url.PathEscape(st.id), nil)
		if !r.OK() {
			return 0, "", refusal("read "+what, r)
		}
		var o orderDTO
		if err := r.Decode(&o); err != nil {
			return 0, "", fmt.Errorf("read %s: %w", what, err)
		}
		var lines []*lineState
		var held []int
		for _, l := range o.Lines {
			switch l.Status {
			case LineReleased:
				lines = append(lines, &lineState{order: st, No: l.LineNo, SKU: l.SKU, Qty: l.Quantity, Gift: l.GiftWrap, Reservation: l.ReservationID})
			case LineBackordered:
				return 0, "", fmt.Errorf("%s: line %d (%s) is Backordered: no stock was allocated, so no work unit will exist for it and the order cannot ship complete; the floor released nothing", what, l.LineNo, l.SKU)
			case LineCancelled:
			default:
				held = append(held, l.LineNo)
			}
		}
		if len(held) > 0 {
			return vWait, fmt.Sprintf("order is %s and lines %v are not Released yet (held: the release was not made, or not consumed)", o.Status, held), nil
		}
		if len(lines) == 0 {
			return 0, "", fmt.Errorf("%s: every line is cancelled, there is nothing to work", what)
		}
		sort.Slice(lines, func(i, j int) bool { return lines[i].No < lines[j].No })
		st.mu.Lock()
		st.lines, st.site, st.promise = lines, o.FulfillmentSiteID, o.PromiseDate
		st.mu.Unlock()
		return vDone, "", nil
	})
}

// unitLine is the order line a work unit is for: its lineNo, or, for a unit that predates it, the
// suffix of the deterministic id `<orderId>-line-<n>`.
func unitLine(u workUnit) int {
	if u.LineNo > 0 {
		return u.LineNo
	}
	if i := strings.LastIndex(u.ID, "-line-"); i > 0 {
		if n, err := strconv.Atoi(u.ID[i+len("-line-"):]); err == nil {
			return n
		}
	}
	return 0
}

// readUnits reads the work units of the order by their reference (the order id): the lookup that
// needs no knowledge of how ids are built.
func (f *Floor) readUnits(ctx context.Context, st *orderState) ([]workUnit, error) {
	units, r := listAll[workUnit](ctx, f.c, SvcWES, "/work-units?reference", "/work-units?reference="+url.QueryEscape(st.id))
	if !r.OK() {
		return nil, refusal("read the work units of order "+st.id, r)
	}
	return units, nil
}

// awaitUnits waits until wes-work-planning holds exactly one live unit for every line. A unit that
// was cancelled (an earlier attempt) is ignored; two live units for one line cannot be told apart
// and are a finding.
func (f *Floor) awaitUnits(ctx context.Context, st *orderState) error {
	what := "order " + st.id
	return await(ctx, f.clock, f.set.Lag, what+": wait for its work units", func(ctx context.Context) (verdict, string, error) {
		units, err := f.readUnits(ctx, st)
		if err != nil {
			return 0, "", err
		}
		byLine := map[int][]workUnit{}
		for _, u := range units {
			if u.State != UnitCancelled {
				byLine[unitLine(u)] = append(byLine[unitLine(u)], u)
			}
		}
		st.mu.Lock()
		defer st.mu.Unlock()
		var missing []int
		for _, l := range st.lines {
			switch us := byLine[l.No]; len(us) {
			case 0:
				missing = append(missing, l.No)
			case 1:
				l.Unit = us[0]
			default:
				return 0, "", fmt.Errorf("%s: line %d has %d live work units (%s, ...): which one the floor must release is ambiguous", what, l.No, len(us), us[0].ID)
			}
		}
		if len(missing) > 0 {
			return vWait, fmt.Sprintf("wes-work-planning holds no work unit with reference %s for lines %v (OrderAllocated not consumed yet); it holds %d for the order", st.id, missing, len(units)), nil
		}
		return vDone, "", nil
	})
}

func (o *orderState) siteID() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.site
}

// promiseFinding compares order-management's promise date with the cpt the PACK task carries: the
// manifest's cpt is the PACK task's, and outbound-shipping needs it to be a cutoff of the site's
// schedule, which order-management builds the promise from (retail-day ADR 0001 D2, open point).
func (o *orderState) promiseFinding() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.promise == "" || o.cpt == "" {
		return ""
	}
	p, err1 := time.Parse(time.RFC3339, o.promise)
	c, err2 := time.Parse(time.RFC3339, o.cpt)
	if err1 != nil || err2 != nil || p.Equal(c) {
		return ""
	}
	return fmt.Sprintf("order %s: order-management's promise date %s is not the PICK tasks' cpt %s that the PACK task carries and PackageManifested publishes", o.id, o.promise, o.cpt)
}
