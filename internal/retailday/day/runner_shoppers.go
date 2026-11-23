package day

import (
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/claudioed/warehouse-e2e/internal/retailday"
)

// ---- 3. the shoppers ----

func (s *run) shoppers() (string, string) {
	sem := make(chan struct{}, s.cfg.Parallel)
	var wg sync.WaitGroup
	for _, j := range s.js {
		select {
		case sem <- struct{}{}:
		case <-s.ctx.Done():
			j.stop()
			j.fail("phase/shoppers", "not run: the context ended before this shopper started")
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			s.buy(j)
		}()
	}
	wg.Wait()
	if err := s.ctx.Err(); err != nil {
		return PhaseAborted, "interrupted: " + err.Error()
	}
	placed, refused := 0, 0
	for _, j := range s.js {
		if j.bought.OrderID != "" {
			placed++
		}
		if j.stopped {
			refused++
		}
	}
	return PhaseOK, fmt.Sprintf("%d buyers, %d orders placed, %d ended off plan", len(s.js), placed, refused)
}

// wantStatus is what a shopper must end with for each outcome the plan expects.
var wantStatus = map[retailday.Outcome]Status{
	retailday.Delivered: StatusPlaced,
	retailday.Cancelled: StatusCancelled,
	retailday.Declined:  StatusDeclined,
	retailday.NotServed: StatusNotServed,
	retailday.InDoubt:   StatusInDoubt,
}

func (s *run) buy(j *journey) {
	var b Bought
	if err := guard("shopper", func() (e error) { b, e = s.r.Shopper.Buy(s.ctx, j.buyer); return }); err != nil {
		j.stop()
		j.fail("phase/shopper", "the purchase did not run: %v", err)
		return
	}
	j.ran, j.bought = true, b
	j.placed.OrderID = b.OrderID
	j.placed.TotalMinor, j.placed.AuthorizationID, j.placed.PromiseEnd = b.TotalMinor, b.AuthorizationID, b.PromiseEnd
	j.placed.PlacedAt = s.cfg.Now()
	if b.OrderID != "" {
		j.label = b.OrderID
	}
	s.cfg.Log("%s: %s %s", j.label, j.buyer.Expect, b.Status)

	want := wantStatus[j.buyer.Expect]
	switch {
	case b.Status != want:
		j.stop()
		j.fail("shopper/outcome", "the shopper ended %q, the plan needs %q for an order expected %s%s", b.Status, want, j.buyer.Expect, problemTail(b.Problems))
	case j.buyer.Expect == retailday.Delivered || j.buyer.Expect == retailday.Cancelled:
		if b.OrderID == "" {
			j.stop()
			j.fail("shopper/order-id", "the shopper reports %q but no order id came back", b.Status)
		}
	case b.OrderID != "":
		j.stop()
		j.fail("shopper/outcome", "an order %s was placed for a buyer expected %s: nothing may exist for it", b.OrderID, j.buyer.Expect)
	}
	if b.CancelRefused != "" {
		j.stop()
		j.fail("shopper/cancel-refused", "the cancellation was refused (%s): the purchase order was already accepted, so the order goes on and the plan's cancellation cannot be judged", b.CancelRefused)
	}
	switch {
	case len(b.Problems) == 0:
	case b.Status != want:
		// the outcome finding above carries them
	case j.buyer.Expect == retailday.InDoubt:
		j.warn("shopper/in-doubt", "%s", strings.Join(b.Problems, "; "))
	default:
		j.fail("shopper/problems", "%d request(s) were refused or disagreed: %s", len(b.Problems), strings.Join(firstN(b.Problems, 3), "; "))
	}
	if j.buyer.Expect == retailday.Cancelled && b.Status == StatusCancelled {
		j.note("cancellation accepted; the order read %q (settled: %v)", b.OrderStatus, b.CancelSettled)
	}
	if !j.stopped && j.buyer.Expect == retailday.Delivered {
		j.note("placed order %s for %d minor units", b.OrderID, b.TotalMinor)
	}
}

func problemTail(ps []string) string {
	if len(ps) == 0 {
		return ""
	}
	return ": " + strings.Join(firstN(ps, 3), "; ")
}

func firstN(in []string, n int) []string {
	if len(in) <= n {
		return in
	}
	return append(slices.Clone(in[:n]), fmt.Sprintf("(+%d more)", len(in)-n))
}
