package day

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/claudioed/warehouse-e2e/internal/retailday"
	"github.com/claudioed/warehouse-e2e/internal/sim"
)

// Row outcomes. A row is PASS only with no finding at all: an order that carries a warning
// (the in-doubt payment, a promise window that was not captured) is WARN, never green.
const (
	OutcomePass = "PASS"
	OutcomeWarn = "WARN"
	OutcomeFail = "FAIL"
)

// Verdicts of the day.
const (
	VerdictPassed = "PASSED"
	VerdictFailed = "FAILED"
)

// Phase states. A phase is Failed when it could not run at all, Aborted when something before it
// ended the day (or the context did), Idle when it had nothing to do.
const (
	PhaseOK      = "ok"
	PhaseIdle    = "idle"
	PhaseFailed  = "failed"
	PhaseAborted = "aborted"
)

// The phases of the day, in order. A report that lacks one of them did not run the day.
const (
	PhasePrepare  = "prepare"
	PhasePlan     = "plan"
	PhaseShelf    = "shelf"
	PhaseShoppers = "shoppers"
	PhaseFloor    = "floor"
	PhaseShip     = "ship"
	PhaseDeliver  = "deliver"
	PhaseRestock  = "restock"
	PhaseEvidence = "evidence"
	PhaseEstate   = "estate"
)

var requiredPhases = []string{PhasePrepare, PhasePlan, PhaseShelf, PhaseShoppers, PhaseFloor, PhaseShip, PhaseDeliver, PhaseRestock, PhaseEvidence, PhaseEstate}

// OrderReport is one buyer's row: the order, what the plan expected, how it was judged, the
// rules that failed and what the journey noted on the way.
type OrderReport struct {
	Order    string              `json:"order"` // customer-ordering's id, or buyer-NN when nothing was placed
	Buyer    int                 `json:"buyer"`
	Expect   retailday.Outcome   `json:"expect"`
	Outcome  string              `json:"outcome"`
	Rules    []string            `json:"rules,omitempty"` // the failed rules, in the order they were found
	Findings []retailday.Finding `json:"findings,omitempty"`
	Notes    []string            `json:"notes,omitempty"`
}

// PhaseStatus is how one phase went.
type PhaseStatus struct {
	Name   string `json:"name"`
	State  string `json:"state"`
	Detail string `json:"detail,omitempty"`
}

// StockRow is one shelf SKU's account.
type StockRow struct {
	SKU         string `json:"sku"`
	Before      int    `json:"before"`
	After       int    `json:"after"`
	Shipped     int    `json:"shipped"`
	Restocked   int    `json:"restocked"`
	Purchasable bool   `json:"purchasable"`
}

// Report is the day, judged.
type Report struct {
	RunID      string    `json:"runId"`
	Seed       int64     `json:"seed"`
	StartedAt  time.Time `json:"startedAt"`
	FinishedAt time.Time `json:"finishedAt"`
	Verdict    string    `json:"verdict"`
	// Reasons say, in a line each, why the verdict is FAILED.
	Reasons []string `json:"reasons,omitempty"`

	Notes   []string            `json:"notes,omitempty"` // facts about how the run was started, as given
	Planned int                 `json:"planned"`         // buyers the plan made; every one must have a row
	Shelf   []string            `json:"shelf,omitempty"`
	Orders  []OrderReport       `json:"orders"`
	Estate  []retailday.Finding `json:"estate,omitempty"`
	Stock   []StockRow          `json:"stock,omitempty"`
	Phases  []PhaseStatus       `json:"phases"`

	// NotVerified is everything the verdict could not establish: every note the evidence
	// returned, dead letters it could not count, anything a phase could not run. Unless
	// AllowUnverified is set, each of these also fails the day.
	NotVerified     []string `json:"notVerified,omitempty"`
	AllowUnverified bool     `json:"allowUnverified,omitempty"`

	ServerErrors int             `json:"serverErrors"`
	Suspensions  int             `json:"suspensions,omitempty"`
	Failures     []string        `json:"failures,omitempty"` // the first transport errors and 5xx
	Calls        []sim.RouteLine `json:"calls,omitempty"`
}

// Passed derives the verdict from the facts, not from the Verdict string: every planned order
// has a row and none failed, no estate rule failed, and every phase of the day ran.
func (r Report) Passed() bool { return len(r.failures()) == 0 }

// failures lists why the day did not pass, in a line each; empty means it passed.
func (r Report) failures() []string {
	var out []string
	if r.Planned == 0 {
		out = append(out, "no order was planned")
	}
	if len(r.Orders) != r.Planned {
		out = append(out, fmt.Sprintf("%d of %d planned orders are accounted for", len(r.Orders), r.Planned))
	}
	failedRows := 0
	for _, o := range r.Orders {
		if o.Outcome != OutcomePass && o.Outcome != OutcomeWarn || retailday.FailedFindings(o.Findings) > 0 {
			failedRows++
		}
	}
	if failedRows > 0 {
		out = append(out, fmt.Sprintf("%d of %d orders failed", failedRows, len(r.Orders)))
	}
	if n := retailday.FailedFindings(r.Estate); n > 0 {
		out = append(out, fmt.Sprintf("%d estate rule(s) failed", n))
	}
	have := map[string]PhaseStatus{}
	for _, p := range r.Phases {
		have[p.Name] = p
	}
	for _, name := range requiredPhases {
		p, ok := have[name]
		switch {
		case !ok:
			out = append(out, fmt.Sprintf("phase %s did not run", name))
		case p.State != PhaseOK && p.State != PhaseIdle:
			out = append(out, fmt.Sprintf("phase %s %s: %s", name, p.State, p.Detail))
		}
	}
	return out
}

// JSON writes the report as indented JSON.
func (r Report) JSON(w io.Writer) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(r)
}

// Text writes the report for a person: the per-order table, the estate verdict, the NOT VERIFIED
// section and the call statistics.
func (r Report) Text(w io.Writer) {
	fmt.Fprintf(w, "retail day %s, seed %d: %s\n", r.RunID, r.Seed, r.Verdict)
	if !r.StartedAt.IsZero() {
		fmt.Fprintf(w, "%s -> %s (%s)\n", r.StartedAt.Format(time.RFC3339), r.FinishedAt.Format(time.RFC3339), r.FinishedAt.Sub(r.StartedAt).Round(time.Second))
	}
	for _, why := range r.Reasons {
		fmt.Fprintf(w, "  - %s\n", why)
	}
	for _, n := range r.Notes {
		fmt.Fprintf(w, "  note: %s\n", n)
	}

	fmt.Fprintf(w, "\nPHASES\n")
	tw := tabwriter.NewWriter(w, 2, 4, 2, ' ', 0)
	for _, p := range r.Phases {
		fmt.Fprintf(tw, "  %s\t%s\t%s\n", p.Name, p.State, p.Detail)
	}
	tw.Flush()

	fmt.Fprintf(w, "\nORDERS (%d planned, %d accounted for)\n", r.Planned, len(r.Orders))
	tw = tabwriter.NewWriter(w, 2, 4, 2, ' ', 0)
	fmt.Fprintf(tw, "  ORDER\tEXPECTED\tOUTCOME\tFAILED RULES\n")
	for _, o := range r.Orders {
		fmt.Fprintf(tw, "  %s\t%s\t%s\t%s\n", o.Order, o.Expect, o.Outcome, strings.Join(o.Rules, ", "))
	}
	tw.Flush()
	for _, o := range r.Orders {
		if len(o.Findings) == 0 && len(o.Notes) == 0 {
			continue
		}
		fmt.Fprintf(w, "  %s (%s)\n", o.Order, o.Expect)
		for _, f := range o.Findings {
			fmt.Fprintf(w, "    %s %s: %s\n", mark(f), f.Rule, f.Detail)
		}
		for _, n := range o.Notes {
			fmt.Fprintf(w, "    note: %s\n", n)
		}
	}

	fmt.Fprintf(w, "\nESTATE\n")
	if len(r.Estate) == 0 {
		fmt.Fprintf(w, "  no finding\n")
	}
	for _, f := range r.Estate {
		fmt.Fprintf(w, "  %s %s: %s\n", mark(f), f.Rule, f.Detail)
	}
	if len(r.Stock) > 0 {
		tw = tabwriter.NewWriter(w, 2, 4, 2, ' ', 0)
		fmt.Fprintf(tw, "  SKU\tBEFORE\tSHIPPED\tRESTOCKED\tAFTER\tSOLD\n")
		for _, s := range r.Stock {
			fmt.Fprintf(tw, "  %s\t%d\t%d\t%d\t%d\t%v\n", s.SKU, s.Before, s.Shipped, s.Restocked, s.After, s.Purchasable)
		}
		tw.Flush()
	}

	fmt.Fprintf(w, "\nNOT VERIFIED")
	if r.AllowUnverified {
		fmt.Fprintf(w, " (-allow-unverified: listed, not failed)")
	} else {
		fmt.Fprintf(w, " (each of these fails the day unless -allow-unverified)")
	}
	fmt.Fprintln(w)
	if len(r.NotVerified) == 0 {
		fmt.Fprintf(w, "  nothing\n")
	}
	for _, n := range r.NotVerified {
		fmt.Fprintf(w, "  - %s\n", n)
	}

	fmt.Fprintf(w, "\nCALLS: %d route(s), %d transport error(s) or 5xx", len(r.Calls), r.ServerErrors)
	if r.Suspensions > 0 {
		fmt.Fprintf(w, ", host suspended %d time(s)", r.Suspensions)
	}
	fmt.Fprintln(w)
	for _, f := range r.Failures {
		fmt.Fprintf(w, "  ! %s\n", f)
	}
	tw = tabwriter.NewWriter(w, 2, 4, 2, ' ', 0)
	for _, c := range r.Calls {
		fmt.Fprintf(tw, "  %s\t%d\t%s\t%.0f ms\n", c.Route, c.Calls, statuses(c.Statuses), c.AvgMs)
	}
	tw.Flush()
	fmt.Fprintf(w, "\nVERDICT: %s\n", r.Verdict)
}

func mark(f retailday.Finding) string {
	if f.Warn {
		return "warn"
	}
	return "FAIL"
}

func statuses(m map[int]int) string {
	codes := make([]int, 0, len(m))
	for c := range m {
		codes = append(codes, c)
	}
	sort.Ints(codes)
	parts := make([]string, len(codes))
	for i, c := range codes {
		parts[i] = fmt.Sprintf("%d:%d", c, m[c])
	}
	return strings.Join(parts, " ")
}
