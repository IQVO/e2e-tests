package day

import (
	"fmt"
	"sync"
	"time"

	"github.com/claudioed/warehouse-e2e/internal/retailday"
)

// ---- 6. the doorstep ----

func (s *run) deliver() (string, string) {
	var in []*journey
	for _, j := range s.js {
		if !j.stopped && j.pkg != nil {
			in = append(in, j)
		}
	}
	if len(in) == 0 {
		return PhaseIdle, "no shipped order to deliver"
	}
	sem := make(chan struct{}, s.cfg.Parallel)
	var wg sync.WaitGroup
	for _, j := range in {
		select {
		case sem <- struct{}{}:
		case <-s.ctx.Done():
			j.stop()
			j.fail("phase/carrier", "not run: the context ended before this order was delivered")
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			s.deliverOne(j)
		}()
	}
	wg.Wait()
	if err := s.ctx.Err(); err != nil {
		return PhaseAborted, "interrupted: " + err.Error()
	}
	delivered := 0
	for _, j := range in {
		if !j.deliveredAt.IsZero() {
			delivered++
		}
	}
	return PhaseOK, fmt.Sprintf("%d of %d orders delivered", delivered, len(in))
}

func (s *run) deliverOne(j *journey) {
	var d Delivery
	// the floor makes one package per order (one pack task): that is the parcel's size
	if err := guard("carrier", func() error { d = s.r.Carrier.DeliverOrder(s.ctx, j.bought.OrderID, 1); return nil }); err != nil {
		j.stop()
		j.fail("phase/carrier", "the carrier could not run: %v", err)
		return
	}
	for _, p := range d.Problems {
		j.fail("phase/carrier", "%s", p)
	}
	if !d.Delivered() {
		j.stop()
		j.fail("carrier/not-delivered", "the carrier never delivered the parcel")
		return
	}
	j.deliveredAt = d.DeliveredAt
	j.note("delivered at %s", d.DeliveredAt.UTC().Format(time.RFC3339))
}

// ---- 7. the restock ----

func (s *run) restock() (string, string) {
	start := map[string]int{}
	for _, sku := range s.plan.ShelfSKUs() {
		start[sku] = s.before[sku].Usable
	}
	var res RestockResult
	if err := guard("restocker", func() (e error) { res, e = s.r.Restocker.Restock(s.ctx, copyMap(start)); return }); err != nil {
		s.estate = append(s.estate, retailday.Finding{Order: "-", Rule: "phase/restock", Detail: "the restock leg could not run: " + err.Error()})
		return PhaseFailed, err.Error()
	}
	s.restocked = res.Restocked
	for _, p := range res.Problems {
		s.estate = append(s.estate, retailday.Finding{Order: "-", Rule: "phase/restock", Detail: p})
	}
	units := 0
	for _, n := range res.Restocked {
		units += n
	}
	if err := s.ctx.Err(); err != nil {
		return PhaseAborted, "interrupted: " + err.Error()
	}
	return PhaseOK, fmt.Sprintf("%d unit(s) of %d SKU(s) received and stowed, %d SKU(s) still short", units, len(res.Restocked), len(res.Short))
}
