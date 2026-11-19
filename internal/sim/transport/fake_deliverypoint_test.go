package transport

// The delivery-point chain of the contract fakes (roadmap C5), the three hops the actor observes:
//
//   - last-mile-delivery (its ADR 0014-delivery-point-profile): RecordAttempt folds every attempt into the
//     DeliveryPointProfile of the package's stop key and raises DeliveryPointFlagged when two counted failed
//     VISITS (routes) are open. Rules enforced, from internal/domain/deliverypoint.Observe:
//     only FAILED + NOT_HOME or ACCESS_PROBLEM count (OTHER and "not reported" never do); one strike per route
//     (two parcels failing at one stop on one route are ONE strike) and the strike names the FIRST package of
//     the visit; a DELIVERED attempt ends the episode unless a strike of the same route exists; the event names
//     the tracking numbers of the strikes not yet notified (distinct, not already told); reason is ACCESS_PROBLEM
//     when any open strike has it, else NOT_HOME; strikes older than 60 days drop and close the flag; at most 5
//     strikes; a stop key starting with `pkg:` (no label data) has no profile and raises nothing.
//   - shipping-and-tracking (its ADR 0014-relay-the-delivery-point-flag-by-order-id) relays the event per package;
//     a package of a terminal journey is refused, an older flag is stale.
//   - customer-ordering (its ADR 0015) stores it, newest wins, and shows `deliveryPointAdvisory {reason,
//     flaggedAt}` on GET /orders/{id}/tracking while the package has not ended (not DELIVERED, RETURNED, LOST,
//     VOIDED). There is no "cleared" event: the advisory ends with the package.
//
// Both consumer hops are event-fed: the advisory appears after `advisoryLag` reads of the tracking view and
// the tap sees the event after `tapLag` reads of Seen: a per-event request countdown, never a sleep.

import (
	"slices"
	"sync"
	"time"
)

const (
	fStrikeWindow = 60 * 24 * time.Hour
	fMaxStrikes   = 5
)

type fstrike struct {
	route, tn, category string
	at                  time.Time
	notified            bool
}

type fprofile struct {
	strikes   []fstrike
	flaggedAt time.Time
	last      time.Time
}

func (p *fprofile) hasStrike(route string) bool {
	return slices.ContainsFunc(p.strikes, func(s fstrike) bool { return s.route == route })
}

// fflagged is the DeliveryPointFlagged event.
type fflagged struct {
	StopKey  string
	Reason   string
	Failures int
	TNs      []string
	At       time.Time
}

// observe is deliverypoint.Profile.Observe for one attempt (steps 1 to 6 of last-mile-delivery's ADR 0014).
func (p *fprofile) observe(stopKey, route, tn, outcome, detail string, at time.Time) *fflagged {
	ref := at
	if p.last.After(ref) {
		ref = p.last
	}
	cut := ref.Add(-fStrikeWindow)
	p.strikes = slices.DeleteFunc(p.strikes, func(s fstrike) bool { return s.at.Before(cut) })
	if len(p.strikes) < FlagAfterStrikes {
		p.flaggedAt = time.Time{}
	}
	if outcome == OutcomeFailed && Counted(detail) && !p.hasStrike(route) {
		p.strikes = append(p.strikes, fstrike{route: route, tn: tn, category: detail, at: at})
		if over := len(p.strikes) - fMaxStrikes; over > 0 {
			p.strikes = slices.Clone(p.strikes[over:])
		}
	}
	if outcome == OutcomeDelivered && !p.hasStrike(route) {
		p.strikes, p.flaggedAt = nil, time.Time{}
	}
	p.last = ref
	if len(p.strikes) < FlagAfterStrikes {
		return nil
	}
	if p.flaggedAt.IsZero() {
		p.flaggedAt = at
	}
	told := map[string]bool{}
	for _, s := range p.strikes {
		if s.notified {
			told[s.tn] = true
		}
	}
	var fresh []string
	for _, s := range p.strikes {
		if !s.notified && !told[s.tn] && !slices.Contains(fresh, s.tn) {
			fresh = append(fresh, s.tn)
		}
	}
	if len(fresh) == 0 {
		return nil
	}
	reason := DetailNotHome
	for i := range p.strikes {
		if slices.Contains(fresh, p.strikes[i].tn) {
			p.strikes[i].notified = true
		}
		if p.strikes[i].category == DetailAccessProblem {
			reason = DetailAccessProblem
		}
	}
	return &fflagged{StopKey: stopKey, Reason: reason, Failures: len(p.strikes), TNs: fresh, At: at}
}

// pendingAdvisory is a relayed flag the customer-ordering consumer has not applied yet.
type pendingAdvisory struct {
	tn, reason string
	at         time.Time
	left       int
}

// flagPipeline is the world's delivery-point chain: the profiles, the relay and the tap.
type flagPipeline struct {
	w        *world
	profiles map[string]*fprofile
	// advisoryLag: tracking-view reads before a relayed flag shows; tapLag: Seen reads before the tap sees it.
	advisoryLag, tapLag int
	// relayReason, when set, is the reason the relay hands customer-ordering instead of the event's (a relay defect).
	relayReason string

	published []fflagged // every DeliveryPointFlagged, in order
	refused   []string   // flags the relay refused: closed journey, stale

	pending []pendingAdvisory
	flags   map[string]Advisory // stored by customer-ordering, by tracking number

	tap *fakeTap
}

func newFlagPipeline(w *world) *flagPipeline {
	f := &flagPipeline{w: w, profiles: map[string]*fprofile{}, flags: map[string]Advisory{}}
	f.tap = &fakeTap{tracked: map[string]bool{}, seen: map[string]int{}}
	return f
}

// attempt is last-mile-delivery's updateProfile: a package without an address key has no profile.
func (f *flagPipeline) attempt(stopKey, route, tn, outcome, detail string, at time.Time) {
	if len(stopKey) >= 4 && stopKey[:4] == "pkg:" {
		return
	}
	p := f.profiles[stopKey]
	if p == nil {
		p = &fprofile{}
		f.profiles[stopKey] = p
	}
	if ev := p.observe(stopKey, route, tn, outcome, detail, at); ev != nil {
		f.publish(*ev)
	}
}

func (f *flagPipeline) publish(ev fflagged) {
	f.published = append(f.published, ev)
	f.tap.publish(ev, f.tapLag)
	for _, tn := range ev.TNs {
		j := f.w.tracking.journeys[tn]
		switch {
		case j == nil:
			f.refused = append(f.refused, tn+": unknown journey")
		case terminal(j.state):
			f.refused = append(f.refused, tn+": journey "+j.state)
		default:
			reason := ev.Reason
			if f.relayReason != "" {
				reason = f.relayReason
			}
			f.pending = append(f.pending, pendingAdvisory{tn: tn, reason: reason, at: ev.At, left: f.advisoryLag})
		}
	}
}

func terminal(state string) bool {
	return state == "DELIVERED" || state == "RETURNED" || state == "LOST" || state == "VOIDED"
}

// tick is one read of the tracking view: the consumer catches up by one request.
func (f *flagPipeline) tick() {
	var rest []pendingAdvisory
	for _, p := range f.pending {
		if p.left > 0 {
			p.left--
			rest = append(rest, p)
			continue
		}
		if old, ok := f.flags[p.tn]; ok && p.at.Before(old.FlaggedAt) {
			f.refused = append(f.refused, p.tn+": stale flag") // newest wins
			continue
		}
		f.flags[p.tn] = Advisory{Reason: p.reason, FlaggedAt: p.at.UTC()}
	}
	f.pending = rest
}

// advisory is what the tracking view shows for a package in the given state.
func (f *flagPipeline) advisory(tn, state string) map[string]any {
	a, ok := f.flags[tn]
	if !ok || terminal(state) {
		return nil
	}
	return map[string]any{"reason": a.Reason, "flaggedAt": rfc(a.FlaggedAt)}
}

// fakeTap is a sim.EventTap on the last-mile topic: it indexes only the messages that arrive AFTER Track
// (like the real one, whose ByRef is filled at consume time) and counts them per tracked reference.
type fakeTap struct {
	mu      sync.Mutex
	tracked map[string]bool
	seen    map[string]int
	delayed []delayedEvent
	err     error
	tracks  int // Track calls: a second Track of one ref would count every event twice
}

type delayedEvent struct {
	tns  []string
	left int
}

func (t *fakeTap) Track(ref string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.tracks++
	t.tracked[ref] = true
}

func (t *fakeTap) publish(ev fflagged, lag int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	// The payload contains every tracking number of the event; the tap matches a tracked ref at consume time.
	var tns []string
	for _, tn := range ev.TNs {
		if t.tracked[tn] {
			tns = append(tns, tn)
		}
	}
	t.delayed = append(t.delayed, delayedEvent{tns: tns, left: lag})
}

func (t *fakeTap) Seen(ref, typ string) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	var rest []delayedEvent
	for _, e := range t.delayed {
		if e.left > 0 {
			e.left--
			rest = append(rest, e)
			continue
		}
		for _, tn := range e.tns {
			t.seen[tn]++
		}
	}
	t.delayed = rest
	if typ != EventDeliveryPointFlagged {
		return 0
	}
	return t.seen[ref]
}

// Err is what a tap that cannot read its topic reports (the retail-day tap over a port-forwarded broker does).
func (t *fakeTap) Err() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.err
}
