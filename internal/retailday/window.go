// Package retailday holds what the retail-day run decides before it touches a
// shopper: when the network will release a purchase order (the release window),
// which contexts it needs, and the read-only preflight that refuses to start on
// a cluster that cannot pass.
//
// The rules come from docs/adr/0001-retail-day.md; every number here is
// overridable because the code, not the live cluster, was what established it.
package retailday

import (
	"fmt"
	"strings"
	"time"
)

// Window is the interval in which an order placed on the storefront is released
// to the node at once, and the pickup cutoff it belongs to.
//
// order-sourcing releases a group at max(now, cutoff - handling - budget) and
// prefers the latest pickup that reaches the same delivery wave, so for a node
// with cutoffs 11:00 and 17:00 an order placed any time before 16:00 waits for
// 15:30 and one placed between 15:30 and 16:00 is released immediately.
type Window struct {
	Start, End time.Time // [Start, End): placing orders now releases them at once
	Cutoff     time.Time // the pickup cutoff this window serves
	InWindow   bool
	Wait       time.Duration // until Start (0 when InWindow)
	Remaining  time.Duration // until End (0 when not InWindow)
}

// Weekdays is the closed set process-path-management uses for daysOfWeek.
var Weekdays = map[string]time.Weekday{
	"Sun": time.Sunday, "Mon": time.Monday, "Tue": time.Tuesday, "Wed": time.Wednesday,
	"Thu": time.Thursday, "Fri": time.Friday, "Sat": time.Saturday,
}

// ReleaseWindow returns the window at or after now for the pickup that cuts off
// at cutoffLocal ("HH:MM", wall clock in loc) on the given days (empty = every
// day). handling is the node's handling time, budget the 30 minute ack budget
// order-sourcing subtracts on top of it.
func ReleaseWindow(now time.Time, loc *time.Location, cutoffLocal string, days []string, handling, budget time.Duration) (Window, error) {
	var hh, mm int
	if _, err := fmt.Sscanf(cutoffLocal, "%d:%d", &hh, &mm); err != nil || hh < 0 || hh > 23 || mm < 0 || mm > 59 || len(cutoffLocal) != 5 {
		return Window{}, fmt.Errorf("cutoff %q is not HH:MM", cutoffLocal)
	}
	if handling < 0 || budget < 0 {
		return Window{}, fmt.Errorf("handling %s and budget %s must not be negative", handling, budget)
	}
	allowed := map[time.Weekday]bool{}
	for _, d := range days {
		wd, ok := Weekdays[d]
		if !ok {
			return Window{}, fmt.Errorf("day %q is not one of Sun..Sat", d)
		}
		allowed[wd] = true
	}
	local := now.In(loc)
	// Eight days cover a one-day schedule, a missed window today and a DST change.
	for i := 0; i <= 8; i++ {
		day := local.AddDate(0, 0, i)
		if len(allowed) > 0 && !allowed[day.Weekday()] {
			continue
		}
		cutoff := time.Date(day.Year(), day.Month(), day.Day(), hh, mm, 0, 0, loc)
		start, end := cutoff.Add(-handling-budget), cutoff.Add(-handling)
		if !now.Before(end) {
			continue
		}
		w := Window{Start: start, End: end, Cutoff: cutoff}
		if now.Before(start) {
			w.Wait = start.Sub(now)
		} else {
			w.InWindow = true
			w.Remaining = end.Sub(now)
		}
		return w, nil
	}
	return Window{}, fmt.Errorf("no cutoff %s on days %v within the next 8 days", cutoffLocal, days)
}

// Cutoff is the slice of a CPT cutoff the window needs.
type Cutoff struct {
	LocalTime  string
	DaysOfWeek []string
}

// LatestCutoff returns the latest local time of the day among cutoffs and the
// days it runs on: order-sourcing prefers the latest pickup that feeds the same
// wave, so that is the one whose release time governs the run.
func LatestCutoff(cutoffs []Cutoff) (Cutoff, bool) {
	var best Cutoff
	found := false
	for _, c := range cutoffs {
		if !found || strings.Compare(c.LocalTime, best.LocalTime) > 0 {
			best, found = c, true
		}
	}
	return best, found
}

// String renders the window for a report line.
func (w Window) String() string {
	zone, _ := w.Start.Zone()
	span := fmt.Sprintf("%s-%s %s", w.Start.Format("15:04"), w.End.Format("15:04"), zone)
	if w.InWindow {
		return fmt.Sprintf("in the release window %s, %s left", span, w.Remaining.Round(time.Second))
	}
	return fmt.Sprintf("next release window %s on %s, in %s", span, w.Start.Format("Mon 2 Jan"), w.Wait.Round(time.Minute))
}
