package day

import (
	"fmt"
	"strings"
	"sync"

	"github.com/claudioed/warehouse-e2e/internal/retailday"
)

// ---- 8. the evidence, order by order ----

func (s *run) evidence() (string, string) {
	sem := make(chan struct{}, s.cfg.Parallel)
	var wg sync.WaitGroup
	read := 0
	for _, j := range s.js {
		// A buyer that never got an order cannot have one to look up, unless the plan said
		// nothing may exist: then "nothing exists" is exactly what must be read.
		if !j.ran || ((j.buyer.Expect == retailday.Delivered || j.buyer.Expect == retailday.Cancelled) && j.bought.OrderID == "") {
			continue
		}
		select {
		case sem <- struct{}{}:
		case <-s.ctx.Done():
			continue
		}
		read++
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			s.judge(j)
		}()
	}
	wg.Wait()
	if err := s.ctx.Err(); err != nil {
		return PhaseAborted, "interrupted: " + err.Error()
	}
	unread := 0
	for _, j := range s.js {
		s.rep.NotVerified = append(s.rep.NotVerified, prefixed(j.label, j.evNotes)...)
		unread += len(j.evNotes)
	}
	return PhaseOK, fmt.Sprintf("%d order(s) read, %d note(s) of facts that could not be read", read, unread)
}

func prefixed(label string, notes []string) []string {
	out := make([]string, len(notes))
	for i, n := range notes {
		out[i] = label + ": " + n
	}
	return out
}

func (s *run) judge(j *journey) {
	var facts retailday.OrderFacts
	var notes []string
	if err := guard("evidence", func() error { facts, notes = s.r.Evidence.Order(s.ctx, j.placed); return nil }); err != nil {
		j.fail("phase/evidence", "the evidence could not be read: %v", err)
		return
	}
	// The plan is the authority on what the order must become and the shopper on its id.
	facts.Expect = j.placed.Expect
	facts.OrderID = j.label
	if facts.Lines == 0 {
		facts.Lines = j.placed.Lines()
	}
	j.facts, j.gotFacts, j.judged = facts, true, true
	j.findings = append(j.findings, retailday.AuditOrder(facts)...)
	if len(notes) > 0 {
		j.evNotes = notes
		detail := fmt.Sprintf("%d fact(s) could not be read: %s", len(notes), strings.Join(firstN(notes, 3), "; "))
		if s.cfg.AllowUnverified {
			j.warn("evidence/not-verified", "%s", detail)
		} else {
			j.fail("evidence/not-verified", "%s", detail)
		}
	}
}

// ---- 9. the estate ----

func (s *run) audit() (string, string) {
	skus := s.plan.ShelfSKUs()
	stock := s.auditShelf(skus)
	dead := s.auditDeadLetters()
	serverErrors := s.auditCallStats()
	s.estate = append(s.estate, retailday.AuditEstate(stock, dead, serverErrors, 0)...)
	return PhaseOK, fmt.Sprintf("%d SKU(s) accounted, %d dead letter(s), %d 5xx", len(stock), dead, serverErrors)
}

// auditShelf reads the shelf after the day and accounts every SKU: what the
// day shipped, what the restock brought back, and whether the shelf still
// holds what it started with. It returns the estate audit's stock facts.
func (s *run) auditShelf(skus []string) []retailday.StockFacts {
	var after map[string]retailday.Shelf
	var stock []retailday.StockFacts
	shelfErr := guard("evidence", func() (e error) { after, e = s.r.Evidence.Shelf(s.ctx, skus); return })
	if shelfErr != nil {
		s.estate = append(s.estate, retailday.Finding{Order: "-", Rule: "stock/unreadable", Detail: "the shelf could not be read after the day: " + shelfErr.Error()})
		s.rep.NotVerified = append(s.rep.NotVerified, "stock: the shelf could not be read after the day, so conservation of stock was not checked")
	}
	shipped := map[string]int{}
	for _, j := range s.js {
		// The units left the shelf when the floor labelled the package (the pick is confirmed by then),
		// whatever became of the order later: an order that failed at the dock or on the road fails on
		// its own, and the stock rule must not report its units as lost.
		if j.buyer.Expect == retailday.Delivered && j.pkg != nil {
			for sku, n := range j.placed.SKUs {
				shipped[sku] += n
			}
		}
	}
	for _, sku := range skus {
		sh, ok := after[sku]
		if shelfErr == nil && !ok {
			s.estate = append(s.estate, retailday.Finding{Order: "-", Rule: "stock/unreadable", Detail: sku + ": no reading after the day"})
			continue
		}
		if !ok {
			continue
		}
		b := s.before[sku]
		if sh.Usable < b.Usable {
			// Conservation (before - shipped + restocked = after) holds for a restock that did
			// nothing and said so; the day's own rule is that it ends at or above where it began.
			s.estate = append(s.estate, retailday.Finding{Order: "-", Rule: "stock/restocked", Detail: fmt.Sprintf("%s ends at %d usable units, below the %d it started with: the restock leg did not bring the shelf back (ADR 0001 D4)", sku, sh.Usable, b.Usable)})
		}
		stock = append(stock, retailday.StockFacts{SKU: sku, UsableBefore: b.Usable, UsableAfter: sh.Usable, Shipped: shipped[sku], Restocked: s.restocked[sku], AdvertisedNow: -1, Purchasable: sh.Purchasable})
		s.rep.Stock = append(s.rep.Stock, StockRow{SKU: sku, Before: b.Usable, After: sh.Usable, Shipped: shipped[sku], Restocked: s.restocked[sku], Purchasable: sh.Purchasable})
	}
	return stock
}

// auditDeadLetters counts the run's dead letters off the Kafka DLQ topics; a
// count that cannot be made is recorded as unverified, never as none.
func (s *run) auditDeadLetters() (dead int) {
	var n int
	var known bool
	dlqErr := guard("evidence", func() (e error) { n, known, e = s.r.Evidence.DeadLetters(s.ctx, s.cfg.Plan.RunID); return })
	switch {
	case dlqErr != nil:
		s.unverified("kafka/dead-letters-unverified", "dead letters: the count failed: "+dlqErr.Error())
	case !known:
		s.unverified("kafka/dead-letters-unverified", "dead letters: no published route can count them for run "+s.cfg.Plan.RunID+", so \"none\" is not established")
	default:
		dead = n
	}
	return dead
}

// auditCallStats folds the HTTP call statistics into the report and returns
// the day's 5xx and transport-error count.
func (s *run) auditCallStats() int {
	serverErrors := 0
	if s.r.Calls == nil {
		s.unverified("http/5xx-unverified", "call statistics are not wired: 5xx and transport errors were not counted")
	} else {
		serverErrors = s.r.Calls.Count5xx()
		s.rep.ServerErrors = serverErrors
		s.rep.Failures = s.r.Calls.Failures()
		s.rep.Calls = s.r.Calls.Lines()
	}
	if s.r.Suspensions != nil {
		if k := len(s.r.Suspensions.Snapshot()); k > 0 {
			s.rep.Suspensions = k
			s.rep.NotVerified = append(s.rep.NotVerified, fmt.Sprintf("5xx: the host was suspended %d time(s); the call statistics keep no timestamps, so no 5xx can be attributed to a suspension and every one counts", k))
		}
	}
	return serverErrors
}

// unverified records something the day could not establish: a NOT VERIFIED line and a finding
// that fails the day unless the run allows unverified evidence.
func (s *run) unverified(rule, detail string) {
	s.rep.NotVerified = append(s.rep.NotVerified, detail)
	s.estate = append(s.estate, retailday.Finding{Order: "-", Rule: rule, Warn: s.cfg.AllowUnverified, Detail: detail})
}
