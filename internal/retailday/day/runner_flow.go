package day

import (
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"

	"github.com/claudioed/warehouse-e2e/internal/retailday"
)

// ---- 4. the link ----

// link finds, for each order placed for delivery, the order-management order raised for its
// purchase order. Without it the floor would be handed the storefront's id, which order-management
// has never heard of.
func (s *run) link() (string, string) {
	var in []*journey
	for _, j := range s.js {
		if j.buyer.Expect == retailday.Delivered && !j.stopped && j.bought.Status == StatusPlaced && j.bought.OrderID != "" {
			in = append(in, j)
		}
	}
	if len(in) == 0 {
		return PhaseIdle, "no order was placed for delivery"
	}
	sem := make(chan struct{}, s.cfg.Parallel)
	var wg sync.WaitGroup
	for _, j := range in {
		select {
		case sem <- struct{}{}:
		case <-s.ctx.Done():
			j.stop()
			j.fail("phase/link", "not run: the context ended before this order was linked")
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			s.linkOne(j)
		}()
	}
	wg.Wait()
	if err := s.ctx.Err(); err != nil {
		return PhaseAborted, "interrupted: " + err.Error()
	}
	linked := 0
	for _, j := range in {
		if j.omID != "" {
			linked++
		}
	}
	if linked == 0 {
		return PhaseFailed, fmt.Sprintf("none of %d orders reached order-management", len(in))
	}
	return PhaseOK, fmt.Sprintf("%d of %d orders found in order-management", linked, len(in))
}

func (s *run) linkOne(j *journey) {
	var ids []string
	if err := guard("linker", func() (e error) { ids, e = s.r.Linker.OrderIDs(s.ctx, j.placed); return }); err != nil {
		j.stop()
		j.fail("link/order", "no order-management order was found for the order: %v", err)
		return
	}
	ids = slices.Compact(slices.Sorted(slices.Values(ids)))
	if len(ids) == 0 {
		j.stop()
		j.fail("link/order", "no order-management order was raised for the order")
		return
	}
	if len(ids) > 1 {
		// the floor works the first so the rest of the chain is still exercised; the evidence
		// reports the duplicate on its own
		j.fail("link/one-order", "%d order-management orders were raised for one order (%s), want 1; the floor works %s", len(ids), strings.Join(ids, ", "), ids[0])
	}
	j.omID = ids[0]
	j.note("order-management order %s", j.omID)
}

// ---- 5. the floor ----

func (s *run) floor() (string, string) {
	var in []*journey
	var ids []string
	for _, j := range s.js {
		if j.buyer.Expect == retailday.Delivered && !j.stopped && j.bought.Status == StatusPlaced && j.omID != "" {
			in, ids = append(in, j), append(ids, j.omID)
		}
	}
	if len(in) == 0 {
		return PhaseIdle, "no order reached the floor"
	}
	var res []FloorResult
	err := guard("floor", func() error { res = s.r.Floor.ProcessAll(s.ctx, slices.Clone(ids), s.cfg.Parallel); return nil })
	if err == nil && len(res) != len(ids) {
		err = fmt.Errorf("the floor answered %d results for %d orders", len(res), len(ids))
	}
	if err != nil {
		for _, j := range in {
			j.stop()
			j.fail("phase/floor", "the floor could not run: %v", err)
		}
		return PhaseFailed, err.Error()
	}
	labelled := 0
	for i, j := range in {
		r := res[i]
		if r.OrderID != "" && r.OrderID != ids[i] {
			j.stop()
			j.fail("phase/floor", "the floor answered order %s in the place of %s", r.OrderID, ids[i])
			continue
		}
		for _, p := range r.Problems {
			j.fail("phase/floor", "%s", p)
		}
		if !r.Labeled() {
			j.stop()
			rule := "floor/not-labeled"
			if r.Status == "DIVERTED" {
				rule = "floor/diverted"
			}
			j.fail(rule, "no labelled package came out of the floor: package %q is %q", r.PackageID, r.Status)
			continue
		}
		j.pkg = &Package{OrderID: j.bought.OrderID, PackageID: r.PackageID, OrderRef: orElse(r.OrderRef, j.omID), SourceOrderID: orElse(r.SourceOrderID, j.omID), SiteID: r.SiteID, CPT: r.CPT, WeightG: r.WeightG}
		j.note("package %s labelled (cpt %s)", r.PackageID, r.CPT)
		labelled++
	}
	if err := s.ctx.Err(); err != nil {
		return PhaseAborted, "interrupted: " + err.Error()
	}
	return PhaseOK, fmt.Sprintf("%d of %d orders labelled", labelled, len(in))
}

// copyMap hands a phase its own copy: no phase may change what the run later compares against.
func copyMap(in map[string]int) map[string]int {
	out := make(map[string]int, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func orElse(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

// ---- 5. the dock ----

func (s *run) ship() (string, string) {
	groups := map[string][]*journey{}
	for _, j := range s.js {
		if !j.stopped && j.pkg != nil {
			key := j.pkg.SiteID + "|" + j.pkg.CPT
			groups[key] = append(groups[key], j)
		}
	}
	if len(groups) == 0 {
		return PhaseIdle, "no labelled package to ship"
	}
	keys := make([]string, 0, len(groups))
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	sem := make(chan struct{}, s.cfg.Parallel)
	var wg sync.WaitGroup
	var mu sync.Mutex
	unusable := 0
	for _, k := range keys {
		select {
		case sem <- struct{}{}:
		case <-s.ctx.Done():
			for _, j := range groups[k] {
				j.stop()
				j.fail("phase/ship", "not run: the context ended before this load was handled")
			}
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			if !s.shipGroup(groups[k]) {
				mu.Lock()
				unusable++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if err := s.ctx.Err(); err != nil {
		return PhaseAborted, "interrupted: " + err.Error()
	}
	shipped := 0
	for _, j := range s.js {
		if j.pkg != nil && !j.stopped {
			shipped++
		}
	}
	if unusable > 0 {
		return PhaseFailed, fmt.Sprintf("%d of %d load(s) got no usable answer from the dock", unusable, len(keys))
	}
	return PhaseOK, fmt.Sprintf("%d package(s) in %d load(s) shipped", shipped, len(keys))
}

// shipGroup ships the packages that share a load in one call; it reports whether the dock gave a
// usable answer.
func (s *run) shipGroup(js []*journey) bool {
	pkgs := make([]Package, len(js))
	for i, j := range js {
		pkgs[i] = *j.pkg
	}
	var out []Shipment
	err := guard("shipper", func() error { out = s.r.Shipper.ShipAll(s.ctx, pkgs); return nil })
	if err == nil && len(out) != len(js) {
		err = fmt.Errorf("the dock answered %d shipments for %d packages", len(out), len(js))
	}
	if err != nil {
		for _, j := range js {
			j.stop()
			j.fail("phase/ship", "the dock could not run: %v", err)
		}
		return false
	}
	for i, j := range js {
		sh := out[i]
		for _, p := range sh.Problems {
			j.fail("phase/ship", "%s", p)
		}
		if !sh.Departed || !sh.Confirmed {
			j.stop()
			j.fail("ship/not-confirmed", "package %s did not leave the building confirmed (departed %v, confirmed %v)", j.pkg.PackageID, sh.Departed, sh.Confirmed)
			continue
		}
		j.note("shipped on load %s", sh.LoadID)
	}
	return true
}
