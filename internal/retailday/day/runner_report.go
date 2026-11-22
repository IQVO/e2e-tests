package day

import (
	"slices"
	"sort"

	"github.com/claudioed/warehouse-e2e/internal/retailday"
)

// ---- the report ----

func (s *run) finish() Report {
	rep := s.rep
	rep.FinishedAt = s.cfg.Now()
	for _, j := range s.js {
		// An order nobody judged and that carries no failure would read as a pass: it never ran
		// the whole way, so it is a failure. (Every deliberate skip of the evidence comes with
		// its own failing finding.)
		if !j.judged && retailday.FailedFindings(j.findings) == 0 {
			j.fail("phase/evidence", "this order was never judged: the run did not reach the evidence for it")
		}
		rep.Orders = append(rep.Orders, row(j))
	}
	rep.Estate = s.estate
	sort.SliceStable(rep.Phases, func(a, b int) bool { return s.phaseOrder[rep.Phases[a].Name] < s.phaseOrder[rep.Phases[b].Name] })
	rep.Reasons = rep.failures()
	rep.Verdict = VerdictPassed
	if len(rep.Reasons) > 0 {
		rep.Verdict = VerdictFailed
	}
	return rep
}

func row(j *journey) OrderReport {
	o := OrderReport{Order: j.label, Buyer: j.buyer.Index, Expect: j.buyer.Expect, Outcome: OutcomePass, Findings: j.findings, Notes: slices.Clone(j.notes)}
	seen := map[string]bool{}
	for _, f := range j.findings {
		if f.Warn {
			if o.Outcome == OutcomePass {
				o.Outcome = OutcomeWarn
			}
			continue
		}
		o.Outcome = OutcomeFail
		if !seen[f.Rule] {
			seen[f.Rule] = true
			o.Rules = append(o.Rules, f.Rule)
		}
	}
	for _, n := range j.evNotes {
		o.Notes = append(o.Notes, "not verified: "+n)
	}
	return o
}
