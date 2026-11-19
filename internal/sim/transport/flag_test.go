package transport

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

// The delivery-point chain end to end over the contract fakes (roadmap C5, gap G67): the driver reports
// outcomeDetail on its failed attempts, last-mile-delivery flags the stop after two counted failed visits,
// shipping-and-tracking relays the flag and customer-ordering shows the advisory on the order's tracking view.

const flagOrder = "ord-flag-1"

// flagWorld is deliverWorld plus an order of the same packages whose tracking view follows the journeys,
// one shared stop (the packages go to one address) and a tap on last-mile-delivery's topic.
func flagWorld(t *testing.T) (*world, *Transport, Parcel) {
	t.Helper()
	w, tp, p, _ := deliverWorld(t)
	o := w.addOrder(flagOrder, 0, tnA, tnB)
	o.followJourney = true
	w.shareStop("stop-aaa", tnA, tnB) // a stop key that is not `pkg:<tn>`: the package has label data
	tp.Tap = w.flags.tap
	p.OrderID = flagOrder
	return w, tp, p
}

func attemptsOf(rt *froute) []string {
	var out []string
	for _, a := range rt.Attempts {
		out = append(out, a.TN+" "+a.Outcome+" "+a.Detail)
	}
	return out
}

// advisoryOf reads the advisory the fake's tracking view shows for a package right now.
func advisoryOf(w *world, tn string) map[string]any {
	j := w.tracking.journeys[tn]
	return w.flags.advisory(tn, j.state)
}

func TestTwoNotHomeFailuresFlagTheDeliveryPointAndTheAdvisoryShows(t *testing.T) {
	w, tp, p := flagWorld(t)
	w.flags.tapLag, w.flags.advisoryLag = 2, 3 // both consumer hops are behind

	d := tp.Deliver(context.Background(), p, Options{FailedAttempts: 2})
	if !d.OK() {
		t.Fatalf("delivery = %+v", d)
	}

	// the driver: two failed routes then the delivering one, every failed attempt reporting NOT_HOME
	var got []string
	for _, a := range d.Attempts {
		got = append(got, a.RouteID+" "+a.Outcome+" "+a.OutcomeDetail)
	}
	want := []string{
		"route-0001 FAILED NOT_HOME", "route-0001 FAILED NOT_HOME",
		"route-0002 FAILED NOT_HOME", "route-0002 FAILED NOT_HOME",
		"route-0003 DELIVERED ", "route-0003 DELIVERED ",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("attempts = %v, want %v", got, want)
	}
	if len(d.Waves) != 3 || len(d.Routes) != 3 {
		t.Fatalf("waves %v routes %v: each failed round needs its own wave and route", d.Waves, d.Routes)
	}
	// what the service stored is what the driver said
	if g := attemptsOf(w.lastmile.routes["route-0001"]); !reflect.DeepEqual(g, []string{tnA + " FAILED NOT_HOME", tnB + " FAILED NOT_HOME"}) {
		t.Fatalf("route-0001 attempts stored as %v", g)
	}

	// last-mile-delivery flagged the stop ONCE, after the second visit, with reason NOT_HOME
	if n := len(w.flags.published); n != 1 {
		t.Fatalf("%d DeliveryPointFlagged events, want 1: %+v", n, w.flags.published)
	}
	ev := w.flags.published[0]
	if ev.Reason != DetailNotHome || ev.Failures != FlagAfterStrikes || ev.StopKey != "stop-aaa" {
		t.Fatalf("event = %+v", ev)
	}
	// and the run OBSERVED it: on the tap, and as the advisory on the tracking view
	if len(d.Flagged) == 0 || d.FlagReason != DetailNotHome {
		t.Fatalf("flagged %v reason %q", d.Flagged, d.FlagReason)
	}
	for _, tn := range d.Flagged {
		if !strings.HasPrefix(tn, "RN") || w.flags.tap.seen[tn] == 0 {
			t.Fatalf("package %s reported flagged but the tap never saw it", tn)
		}
		if _, ok := w.flags.flags[tn]; !ok {
			t.Fatalf("package %s reported flagged but customer-ordering stored no advisory", tn)
		}
	}
	if w.flags.tap.tracks != 2 {
		t.Fatalf("%d Track calls for 2 packages: EventTap.Track appends, a second Track of a ref counts every event twice", w.flags.tap.tracks)
	}
	// the advisory ends with the package: delivered packages show none
	for _, tn := range []string{tnA, tnB} {
		if a := advisoryOf(w, tn); a != nil {
			t.Fatalf("package %s delivered but the view still shows %v", tn, a)
		}
	}
	w.assertEveryPostKeyed()
}

// A strike holds ONE tracking number per visit (the first package of the route's stop), so the event names the
// first package only: the second package of the same order never gets the advisory. This pins what
// last-mile-delivery does at origin/develop (internal/domain/deliverypoint/observe.go: addStrike keeps one
// strike per route, untold() reads the strikes' tracking numbers); the actor needs at least one package.
// When the producer names every package of the stop, update this test and nothing else.
func TestTheEventNamesTheFirstPackageOfAVisitOnly(t *testing.T) {
	w, tp, p := flagWorld(t)
	d := tp.Deliver(context.Background(), p, Options{FailedAttempts: 2})
	if !d.OK() {
		t.Fatalf("delivery = %+v", d)
	}
	if got := w.flags.published[0].TNs; !reflect.DeepEqual(got, []string{tnA}) {
		t.Fatalf("event names %v, want only the first package %s", got, tnA)
	}
	if !reflect.DeepEqual(d.Flagged, []string{tnA}) {
		t.Fatalf("flagged %v, want [%s]", d.Flagged, tnA)
	}
}

func TestAccessProblemFlagsWithThatReason(t *testing.T) {
	w, tp, p := flagWorld(t)
	d := tp.Deliver(context.Background(), p, Options{FailedAttempts: 2, FailureDetail: DetailAccessProblem})
	if !d.OK() || d.FlagReason != DetailAccessProblem {
		t.Fatalf("delivery = %+v", d)
	}
	if ev := w.flags.published[0]; ev.Reason != DetailAccessProblem {
		t.Fatalf("event = %+v", ev)
	}
	if a := w.flags.flags[tnA]; a.Reason != DetailAccessProblem {
		t.Fatalf("advisory = %+v", a)
	}
}

func TestOneCountedFailureRaisesNoFlag(t *testing.T) {
	w, tp, p := flagWorld(t)
	d := tp.Deliver(context.Background(), p, Options{FailedAttempts: 1})
	if !d.OK() || len(d.Flagged) != 0 || d.FlagReason != "" {
		t.Fatalf("delivery = %+v", d)
	}
	if len(w.flags.published) != 0 || len(w.flags.flags) != 0 {
		t.Fatalf("published %+v advisories %v after ONE failed visit", w.flags.published, w.flags.flags)
	}
	if w.flags.tap.tracks != 0 {
		t.Fatalf("%d packages tracked on the tap though no flag was to be expected", w.flags.tap.tracks)
	}
}

// A relay that hands customer-ordering another reason than the event carried, or a code outside the contract's
// two, is a finding at once: waiting cannot change what the view says.
func TestAnAdvisoryWithTheWrongOrAnUnknownReasonEndsTheWaitAtOnce(t *testing.T) {
	for name, c := range map[string]struct{ relay, want string }{
		"another reason":          {DetailAccessProblem, "want NOT_HOME"},
		"a code outside the list": {DetailOther, "is not NOT_HOME or ACCESS_PROBLEM"},
	} {
		t.Run(name, func(t *testing.T) {
			w, tp, p := flagWorld(t)
			w.flags.relayReason = c.relay
			before := w.clock.Now()
			d := tp.Deliver(context.Background(), p, Options{FailedAttempts: 2})
			if len(d.Problems) == 0 || !strings.Contains(d.Problems[0], c.want) {
				t.Fatalf("problems = %q, want one containing %q", d.Problems, c.want)
			}
			if spent := w.clock.Now().Sub(before); spent > 10*time.Minute {
				t.Fatalf("%s of fake time: a wrong reason must not be waited out", spent)
			}
		})
	}
}

// OTHER is tallied, never counted: two failed visits reporting it are no strikes, so the run does not wait
// for a flag and none is raised.
func TestOtherIsNeverCountedSoNoFlagIsExpectedOrRaised(t *testing.T) {
	w, tp, p := flagWorld(t)
	d := tp.Deliver(context.Background(), p, Options{FailedAttempts: 2, FailureDetail: DetailOther})
	if !d.OK() || len(d.Flagged) != 0 {
		t.Fatalf("delivery = %+v", d)
	}
	if len(w.flags.published) != 0 {
		t.Fatalf("events %+v for OTHER failures", w.flags.published)
	}
	for _, a := range d.Attempts {
		if a.Outcome == OutcomeFailed && a.OutcomeDetail != DetailOther {
			t.Fatalf("attempt %+v", a)
		}
	}
	if w.flags.tap.tracks != 0 {
		t.Fatalf("%d packages tracked on the tap though OTHER raises no flag", w.flags.tap.tracks)
	}
}

// Two failures that never reach the profile (a stop key `pkg:<tn>`: the label data had not arrived when the
// package was inducted) raise nothing: both legs are findings that say what is missing, and the run still
// delivers the parcel.
func TestAFlagThatNeverComesIsAFindingPerMissingLeg(t *testing.T) {
	w, tp, p := flagWorld(t)
	delete(w.lastmile.stopKeys, tnA)
	delete(w.lastmile.stopKeys, tnB)

	d := tp.Deliver(context.Background(), p, Options{FailedAttempts: 2})
	if !d.Delivered() || d.OK() {
		t.Fatalf("delivery = %+v: the parcel is delivered, the run is not clean", d)
	}
	if len(d.Flagged) != 0 || d.FlagReason != "" {
		t.Fatalf("flagged %v reason %q with no flag", d.Flagged, d.FlagReason)
	}
	var tapLeg, viewLeg bool
	for _, pr := range d.Problems {
		tapLeg = tapLeg || (strings.Contains(pr, "published no DeliveryPointFlagged") && strings.Contains(pr, TopicLastMile))
		viewLeg = viewLeg || (strings.Contains(pr, "deliveryPointAdvisory") && strings.Contains(pr, flagOrder))
	}
	if !tapLeg || !viewLeg || len(d.Problems) != 2 {
		t.Fatalf("problems = %q", d.Problems)
	}
}

// The event is published but the relay never reaches the tracking view: the tap leg is clean, the view leg is
// the finding, and what was seen on the tap is still reported.
func TestARelayThatNeverReachesTheTrackingViewIsOneFinding(t *testing.T) {
	w, tp, p := flagWorld(t)
	w.flags.advisoryLag = 1 << 30

	d := tp.Deliver(context.Background(), p, Options{FailedAttempts: 2})
	if !d.Delivered() || len(d.Problems) != 1 || !strings.Contains(d.Problems[0], "customer-ordering GET /orders/{id}/tracking") ||
		!strings.Contains(d.Problems[0], "no deliveryPointAdvisory") {
		t.Fatalf("delivery = %+v", d)
	}
	if !reflect.DeepEqual(d.Flagged, []string{tnA}) || d.FlagReason != DetailNotHome {
		t.Fatalf("the tap saw the event: flagged %v reason %q", d.Flagged, d.FlagReason)
	}
}

// Without the order id only the tap can observe the flag; without both the run says it cannot.
func TestTheFlagIsObservedOnTheTapAloneAndNotAtAllWithoutAWitness(t *testing.T) {
	w, tp, p := flagWorld(t)
	p.OrderID = ""
	d := tp.Deliver(context.Background(), p, Options{FailedAttempts: 2})
	if !d.OK() || !reflect.DeepEqual(d.Flagged, []string{tnA}) {
		t.Fatalf("tap only: delivery = %+v", d)
	}
	if w.countCalls(SvcOrdering, "GET /orders/"+flagOrder+"/tracking") != 0 {
		t.Fatal("the tracking view was read without an order id")
	}

	w2, tp2, p2 := flagWorld(t)
	tp2.Tap, p2.OrderID = nil, ""
	d2 := tp2.Deliver(context.Background(), p2, Options{FailedAttempts: 2})
	if !d2.Delivered() || len(d2.Problems) != 1 || !strings.Contains(d2.Problems[0], "cannot be observed") {
		t.Fatalf("no witness: delivery = %+v", d2)
	}
	if len(w2.flags.published) != 1 {
		t.Fatal("the producer fake must still have flagged")
	}
}

// SkipVerify judges the delivery by last-mile-delivery's answers only: no flag is waited for, no read of the tracking view.
func TestSkipVerifyDoesNotWaitForTheFlag(t *testing.T) {
	w, tp, p := flagWorld(t)
	d := tp.Deliver(context.Background(), p, Options{FailedAttempts: 2, SkipVerify: true})
	if !d.OK() || len(d.Flagged) != 0 {
		t.Fatalf("delivery = %+v", d)
	}
	if w.countCalls(SvcOrdering, "GET /orders/"+flagOrder+"/tracking") != 0 || w.flags.tap.tracks != 0 {
		t.Fatal("SkipVerify read the tracking view or tracked the tap")
	}
}

// An advisory that does not end with the delivery is a finding (customer-ordering ADR 0015, rule 4).
func TestAnAdvisoryThatOutlivesTheDeliveryIsAFinding(t *testing.T) {
	w, tp, p := flagWorld(t)
	w.ordering.orders[flagOrder].followJourney = false // the view keeps showing the package as LABELLED
	d := tp.Deliver(context.Background(), p, Options{FailedAttempts: 2})
	if !d.Delivered() || len(d.Problems) != 1 || !strings.Contains(d.Problems[0], "still carry the delivery-point advisory") {
		t.Fatalf("delivery = %+v", d)
	}
}

// A flag raised for a package that is already delivered (or any terminal journey) is refused by the relay.
func TestARelayedFlagIsRefusedForAClosedJourneyAndStaleOnesLose(t *testing.T) {
	w, _, _ := flagWorld(t)
	w.tracking.journeys[tnA].state = "DELIVERED"
	w.flags.publish(fflagged{StopKey: "s", Reason: DetailNotHome, Failures: 2, TNs: []string{tnA}, At: t0})
	if len(w.flags.pending) != 0 || len(w.flags.refused) != 1 {
		t.Fatalf("pending %v refused %v", w.flags.pending, w.flags.refused)
	}
	w.flags.publish(fflagged{StopKey: "s", Reason: DetailAccessProblem, Failures: 2, TNs: []string{tnB}, At: t0.Add(time.Minute)})
	w.flags.publish(fflagged{StopKey: "s", Reason: DetailNotHome, Failures: 2, TNs: []string{tnB}, At: t0})
	w.flags.tick()
	if a := w.flags.flags[tnB]; a.Reason != DetailAccessProblem {
		t.Fatalf("stored %+v: the newest flag wins", a)
	}
}

func TestOptionsOutsideTheContractAreRefusedBeforeAnyCall(t *testing.T) {
	cases := map[string]Options{
		"a third failure returns the package":                 {FailedAttempts: 3},
		"a negative count":                                    {FailedAttempts: -1},
		"SAFE_PLACE_USED is for DELIVERED":                    {FailedAttempts: 1, FailureDetail: DetailSafePlaceUsed},
		"a vocabulary the contract does not have":             {FailedAttempts: 1, FailureDetail: "BAD_DOG"},
		"NOT_HOME is for FAILED, not for a delivered attempt": {DeliveredDetail: DetailNotHome},
	}
	for name, o := range cases {
		t.Run(name, func(t *testing.T) {
			w, tp, p := flagWorld(t)
			d := tp.Deliver(context.Background(), p, o)
			if len(d.Problems) == 0 || d.Delivered() {
				t.Fatalf("delivery = %+v", d)
			}
			if n := len(w.sequence(SvcLastMile)); n != 0 {
				t.Fatalf("%d last-mile writes before the options were refused", n)
			}
			if n := len(w.sequence(SvcLinehaul)); n != 0 {
				t.Fatalf("%d linehaul writes before the options were refused", n)
			}
		})
	}
}

func TestSafePlaceUsedIsReportedOnTheDeliveredAttempt(t *testing.T) {
	w, tp, p := flagWorld(t)
	d := tp.Deliver(context.Background(), p, Options{DeliveredDetail: DetailSafePlaceUsed})
	if !d.OK() || len(d.Flagged) != 0 {
		t.Fatalf("delivery = %+v", d)
	}
	for _, a := range d.Attempts {
		if a.OutcomeDetail != DetailSafePlaceUsed {
			t.Fatalf("attempt %+v", a)
		}
	}
	if g := attemptsOf(w.lastmile.routes["route-0001"]); !reflect.DeepEqual(g, []string{tnA + " DELIVERED SAFE_PLACE_USED", tnB + " DELIVERED SAFE_PLACE_USED"}) {
		t.Fatalf("stored %v", g)
	}
}

// A delivered visit at the stop ends the episode: strikes of an earlier visit no longer count, so the next
// failures start again (last-mile-delivery ADR 0014, step 4).
func TestADeliveryAtTheStopEndsTheEpisode(t *testing.T) {
	p := &fprofile{}
	at := t0
	step := func(route, tn, outcome, detail string) *fflagged {
		at = at.Add(time.Minute)
		return p.observe("s", route, tn, outcome, detail, at)
	}
	if step("r1", "T1", OutcomeFailed, DetailNotHome) != nil {
		t.Fatal("one strike flagged")
	}
	if step("r2", "T2", OutcomeDelivered, "") != nil || len(p.strikes) != 0 {
		t.Fatalf("a delivery must clear the strikes: %+v", p.strikes)
	}
	if step("r3", "T3", OutcomeFailed, DetailNotHome) != nil {
		t.Fatal("the first strike after the episode ended flagged")
	}
	if ev := step("r4", "T3", OutcomeFailed, DetailAccessProblem); ev == nil || ev.Reason != DetailAccessProblem || !reflect.DeepEqual(ev.TNs, []string{"T3"}) {
		t.Fatalf("second strike = %+v", ev)
	}
	// a later failure of the same package at the flagged point tells nobody again
	if ev := step("r5", "T3", OutcomeFailed, DetailNotHome); ev != nil {
		t.Fatalf("package already told, event %+v", ev)
	}
	// a parcel of another order failing at the flagged point is told too, once
	if ev := step("r6", "T9", OutcomeFailed, DetailNotHome); ev == nil || !reflect.DeepEqual(ev.TNs, []string{"T9"}) {
		t.Fatalf("another order's parcel = %+v", ev)
	}
}

func TestAPackageIsTrackedOnTheTapOnceHoweverOftenItIsRegistered(t *testing.T) {
	w, tp, _ := flagWorld(t)
	tp.trackFlags([]string{tnA, tnB})
	tp.trackFlags([]string{tnB, tnA})
	if w.flags.tap.tracks != 2 {
		t.Fatalf("%d Track calls for 2 packages", w.flags.tap.tracks)
	}
}

// The advisory is waited out only for packages that were DELIVERED: a flagged package that was not delivered
// legitimately keeps it, and with nothing delivered there is nothing to read.
func TestOnlyDeliveredPackagesAreWaitedOnToLoseTheAdvisory(t *testing.T) {
	w, tp, _ := flagWorld(t)
	w.flags.flags[tnA] = Advisory{Reason: DetailNotHome, FlaggedAt: t0}
	w.tracking.journeys[tnA].state = "AT_STATION"
	ctx := context.Background()

	var d Delivery
	tp.awaitAdvisoryEnded(ctx, &d, flagOrder, []string{tnA}, []string{tnB}) // tnA was not delivered
	if len(d.Problems) != 0 {
		t.Fatalf("problems for a package that was not delivered: %v", d.Problems)
	}
	if n := w.countCalls(SvcOrdering, "GET /orders/"+flagOrder+"/tracking"); n != 0 {
		t.Fatalf("%d reads of the tracking view with no delivered flagged package", n)
	}
	tp.awaitAdvisoryEnded(ctx, &d, flagOrder, []string{tnA}, []string{tnA}) // delivered, yet the view still shows it
	if len(d.Problems) != 1 || !strings.Contains(d.Problems[0], tnA) {
		t.Fatalf("problems = %v", d.Problems)
	}
}

func TestTheFlagReasonIsAccessProblemWhenAnyCountedFailureSaidSo(t *testing.T) {
	for _, c := range []struct {
		details []string
		want    string
	}{
		{[]string{DetailNotHome, DetailNotHome}, DetailNotHome},
		{[]string{DetailAccessProblem, DetailNotHome}, DetailAccessProblem},
		{[]string{DetailNotHome, DetailAccessProblem}, DetailAccessProblem},
		{[]string{DetailOther, DetailNotHome}, DetailNotHome},
		{[]string{DetailOther, ""}, ""},
		{nil, ""},
	} {
		if got := FlagReason(c.details...); got != c.want {
			t.Errorf("FlagReason(%v) = %q, want %q", c.details, got, c.want)
		}
	}
}

// A tap that could not read the topic says so in the finding: "published nothing" and "could not look" differ.
func TestATapThatCannotReadItsTopicSaysWhyInTheFinding(t *testing.T) {
	w, tp, p := flagWorld(t)
	delete(w.lastmile.stopKeys, tnA)
	delete(w.lastmile.stopKeys, tnB)
	w.flags.tap.err = errors.New("dial tcp 127.0.0.1:9092: connect: connection refused")
	d := tp.Deliver(context.Background(), p, Options{FailedAttempts: 2})
	found := false
	for _, pr := range d.Problems {
		found = found || (strings.Contains(pr, "published no DeliveryPointFlagged") && strings.Contains(pr, "the tap could not read the topic: dial tcp 127.0.0.1:9092"))
	}
	if !found {
		t.Fatalf("problems = %q", d.Problems)
	}
}
