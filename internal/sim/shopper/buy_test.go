package shopper

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/warehouse-e2e/internal/sim"
)

// two lines on two fulfilment nodes: two shipment groups
var basket = []Line{{"sku-001", 2}, {"sku-005", 1}}

const (
	basketItems = 2*1100 + 1500 // minor units
)

func plan(token string) Plan { return Plan{Lines: basket, Prefix: "787", Token: token} }

func mustBuy(t *testing.T, s *Shopper, p Plan) Outcome {
	t.Helper()
	o, err := s.Buy(ctx(t), p)
	if err != nil {
		t.Fatalf("Buy returned an error (only a setup bug may): %v", err)
	}
	return o
}

func stepNames(o Outcome) []string {
	var out []string
	for _, st := range o.Timeline {
		if st.Attempt == 0 {
			out = append(out, st.Name)
		}
	}
	return out
}

func TestBuyHappyPath(t *testing.T) {
	f := newFakeBFF()
	defer f.close()
	s, stats := newShopper(t, f, Options{Email: "happy@example.com"})
	p := plan(TokenApprove)
	p.Tier = TierTwoDay

	o := mustBuy(t, s, p)
	if o.Status != StatusPlaced {
		t.Fatalf("status %s, problems %v", o.Status, o.Problems)
	}
	if len(o.Problems) != 0 {
		t.Errorf("a clean run has no problems: %v", o.Problems)
	}
	for name, v := range map[string]string{
		"ShopperID": o.ShopperID, "CartID": o.CartID, "SessionID": o.SessionID, "OrderID": o.OrderID,
		"AuthorizationID": o.AuthorizationID, "AddressID": o.AddressID, "Currency": o.Currency,
	} {
		if v == "" {
			t.Errorf("Outcome.%s is empty: %+v", name, o)
		}
	}
	if o.Email != "happy@example.com" {
		t.Errorf("email %q", o.Email)
	}
	if want := int64(basketItems + 2*899); o.TotalMinor != want || o.Currency != "USD" {
		t.Errorf("total %d %s, want %d USD (items + two TWO_DAY groups)", o.TotalMinor, o.Currency, want)
	}
	if len(o.PromiseWindows) != 2 {
		t.Fatalf("promise windows %+v, want one per group", o.PromiseWindows)
	}
	for _, w := range o.PromiseWindows {
		if w.Tier != TierTwoDay || w.TimeZone != "America/Chicago" || w.DeliverByEarliest == "" || w.DeliverByLatest == "" || w.ShipBy == "" {
			t.Errorf("a window is not as the order returned it: %+v", w)
		}
		if _, err := time.Parse(time.RFC3339, w.DeliverByEarliest); err != nil {
			t.Errorf("window instants are returned as RFC 3339: %v", err)
		}
	}
	if o.Tiers[1] != TierTwoDay || o.Tiers[2] != TierTwoDay {
		t.Errorf("tiers %v", o.Tiers)
	}
	if !o.Listed || o.OrderStatus != "PLACED" {
		t.Errorf("the order must be in the history and PLACED: listed %v status %s", o.Listed, o.OrderStatus)
	}
	want := []string{"register", "add-address", "product", "product", "page-quote", "new-cart", "add-line", "add-line",
		"set-destination", "quote-cart", "select-delivery", "select-delivery", "checkout", "confirm", "order", "orders"}
	if got := stepNames(o); !slices.Equal(got, want) {
		t.Errorf("timeline\n got %v\nwant %v", got, want)
	}
	for i := 1; i < len(o.Timeline); i++ {
		if o.Timeline[i].At.Before(o.Timeline[i-1].At) {
			t.Errorf("timeline step %d starts before step %d", i, i-1)
		}
	}
	if f.placements != 1 || f.replays != 0 || o.Replays != 0 {
		t.Errorf("placements %d replays %d/%d", f.placements, f.replays, o.Replays)
	}
	if n := stats.Count5xx(); n != 0 {
		t.Errorf("%d 5xx: %v", n, stats.Failures())
	}
	var confirm bool
	for _, l := range stats.Lines() {
		confirm = confirm || l.Route == "storefront POST /checkout/{id}/confirmations" && l.Statuses[200] == 1
	}
	if !confirm {
		t.Errorf("the per-route statistics must see the confirm: %+v", stats.Lines())
	}
}

func TestBuyTierPreference(t *testing.T) {
	cases := []struct {
		name     string
		nextDay  bool
		prefer   Tier
		wantTier Tier
		delivery int64
	}{
		{"cheapest when no preference", false, "", TierStandard, 499},
		{"STANDARD", false, TierStandard, TierStandard, 499},
		{"TWO_DAY", false, TierTwoDay, TierTwoDay, 899},
		{"NEXT_DAY when it is quoted", true, TierNextDay, TierNextDay, 1499},
		{"NEXT_DAY is not quoted after the morning cutoff: the cheapest", false, TierNextDay, TierStandard, 499},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeBFF()
			defer f.close()
			f.nextDay = tc.nextDay
			s, _ := newShopper(t, f, Options{})
			p := plan(TokenApprove)
			p.Tier = tc.prefer
			o := mustBuy(t, s, p)
			if o.Status != StatusPlaced || o.Tiers[1] != tc.wantTier || o.Tiers[2] != tc.wantTier {
				t.Fatalf("status %s tiers %v problems %v, want tier %s", o.Status, o.Tiers, o.Problems, tc.wantTier)
			}
			if want := basketItems + 2*tc.delivery; o.TotalMinor != want {
				t.Errorf("total %d, want %d", o.TotalMinor, want)
			}
		})
	}
}

func TestBuyDeclinedThenSecondTokenSucceeds(t *testing.T) {
	f := newFakeBFF()
	defer f.close()
	s, _ := newShopper(t, f, Options{})
	p := plan(TokenDecline)
	p.FallbackToken = TokenApprove

	o := mustBuy(t, s, p)
	if o.Status != StatusPlaced || o.Declines != 1 {
		t.Fatalf("status %s declines %d problems %v", o.Status, o.Declines, o.Problems)
	}
	if f.placements != 1 || f.count("POST /checkout") != 1 {
		t.Errorf("placements %d, sessions opened %d: the second token pays the SAME session", f.placements, f.count("POST /checkout"))
	}
	confirms := 0
	for _, st := range o.Timeline {
		if st.Name == "confirm" {
			confirms++
		}
	}
	if confirms != 2 {
		t.Errorf("%d confirms, want 2", confirms)
	}
	// every confirm is its own logical action: 2 keys for the 2 confirms (and no replay)
	if o.Replays != 0 {
		t.Errorf("a second token is a new request, not a replay: %d", o.Replays)
	}
}

func TestBuyDeclinedWithNoSecondToken(t *testing.T) {
	f := newFakeBFF()
	defer f.close()
	s, _ := newShopper(t, f, Options{})
	o := mustBuy(t, s, plan(TokenDecline))
	if o.Status != StatusDeclined || o.OrderID != "" || o.Declines != 1 {
		t.Fatalf("status %s order %q declines %d", o.Status, o.OrderID, o.Declines)
	}
	if f.placements != 0 || len(o.Problems) != 0 {
		t.Errorf("a decline is an outcome, not a problem: placements %d problems %v", f.placements, o.Problems)
	}
	if o.SessionID == "" || o.TotalMinor == 0 {
		t.Errorf("the declined session is still reported: %+v", o)
	}
}

func TestBuyTimeoutEndsInDoubtAndNeverHangs(t *testing.T) {
	f := newFakeBFF()
	defer f.close()
	s, _ := newShopper(t, f, Options{PollEvery: 2 * time.Millisecond, PollTimeout: 80 * time.Millisecond})

	start := time.Now()
	o := mustBuy(t, s, plan(TokenTimeout))
	took := time.Since(start)
	if o.Status != StatusInDoubt || o.InDoubtState != StateAuthorizing {
		t.Fatalf("status %s state %q, want in-doubt AUTHORIZING (journey gap G8)", o.Status, o.InDoubtState)
	}
	if took > 5*time.Second {
		t.Errorf("took %v: the wait must be bounded by PollTimeout", took)
	}
	if o.OrderID != "" || f.placements != 0 {
		t.Errorf("nothing was placed: %+v", o)
	}
	if !strings.Contains(strings.Join(o.Problems, "\n"), "in doubt") {
		t.Errorf("problems %v", o.Problems)
	}
	// the actor does not retry the confirm: one POST, then polls
	if n := f.count("POST /checkout/{id}/confirmations"); n != 1 {
		t.Errorf("%d confirms, want exactly 1: a stuck session is not paid again", n)
	}
}

func TestBuyAcceptedThenPolledToCompletion(t *testing.T) {
	f := newFakeBFF()
	defer f.close()
	f.slowPolls = 4
	s, _ := newShopper(t, f, Options{})
	o := mustBuy(t, s, plan("tok_slow"))
	if o.Status != StatusPlaced || o.OrderID == "" || f.placements != 1 {
		t.Fatalf("status %s order %q problems %v", o.Status, o.OrderID, o.Problems)
	}
	if n := f.count("GET /checkout/{id}"); n != 4 {
		t.Errorf("%d polls, want 4", n)
	}
	var saw202 bool
	for _, st := range o.Timeline {
		saw202 = saw202 || st.Name == "confirm" && st.Status == http.StatusAccepted
	}
	if !saw202 {
		t.Error("the timeline must show the 202")
	}
}

func TestBuyRequotesOnceThenPlaces(t *testing.T) {
	f := newFakeBFF()
	defer f.close()
	f.requotes = 1
	s, _ := newShopper(t, f, Options{})
	p := plan("tok_requote")
	p.Tier = TierStandard

	o := mustBuy(t, s, p)
	if o.Status != StatusPlaced || o.Requotes != 1 {
		t.Fatalf("status %s requotes %d problems %v", o.Status, o.Requotes, o.Problems)
	}
	// REQUOTE_REQUIRED closes the session and there is no accept route: a re-quote, a re-selection
	// and a NEW session, not a second confirm on the old one
	if f.count("POST /carts/{id}/delivery-quotes") != 2 || f.count("POST /checkout") != 2 || f.count("POST /checkout/{id}/confirmations") != 2 {
		t.Errorf("seen %v", f.seen)
	}
	if f.placements != 1 {
		t.Errorf("placements %d", f.placements)
	}
	// the outcome carries the FINAL session: delivery charges moved by 50 per group with the requote
	if want := int64(basketItems + 2*(499+50)); o.TotalMinor != want {
		t.Errorf("total %d, want %d (the requoted price)", o.TotalMinor, want)
	}
	if len(o.Problems) != 0 {
		t.Errorf("a requote is an outcome, not a problem: %v", o.Problems)
	}
}

func TestBuyGivesUpAfterTheRequoteBudget(t *testing.T) {
	f := newFakeBFF()
	defer f.close()
	f.requotes = 99
	s, _ := newShopper(t, f, Options{})
	o := mustBuy(t, s, plan("tok_requote"))
	if o.Status != StatusRequoteExhausted || o.Requotes != 1 || o.OrderID != "" {
		t.Fatalf("status %s requotes %d order %q", o.Status, o.Requotes, o.OrderID)
	}
	if f.placements != 0 || f.count("POST /checkout/{id}/confirmations") != 2 {
		t.Errorf("placements %d, confirms %d: exactly one re-confirm", f.placements, f.count("POST /checkout/{id}/confirmations"))
	}

	// and MaxRequotes is the budget
	f2 := newFakeBFF()
	defer f2.close()
	f2.requotes = 99
	s2, _ := newShopper(t, f2, Options{MaxRequotes: 3})
	if o := mustBuy(t, s2, plan("tok_requote")); o.Status != StatusRequoteExhausted || o.Requotes != 3 {
		t.Errorf("MaxRequotes 3: status %s requotes %d", o.Status, o.Requotes)
	}
	f3 := newFakeBFF()
	defer f3.close()
	f3.requotes = 1
	s3, _ := newShopper(t, f3, Options{MaxRequotes: -1})
	if o := mustBuy(t, s3, plan("tok_requote")); o.Status != StatusRequoteExhausted || o.Requotes != 0 {
		t.Errorf("MaxRequotes -1: status %s requotes %d", o.Status, o.Requotes)
	}
}

func TestBuyRequotesWhenTheOfferIsAboutToExpire(t *testing.T) {
	f := newFakeBFF()
	defer f.close()
	f.firstQuoteTTL = time.Minute // checkout wants 2 more minutes: 422 offer-not-quotable
	s, _ := newShopper(t, f, Options{})
	o := mustBuy(t, s, plan(TokenApprove))
	if o.Status != StatusPlaced || o.Requotes != 1 {
		t.Fatalf("status %s requotes %d problems %v", o.Status, o.Requotes, o.Problems)
	}
	if !o.HasProblem("offer-not-quotable") {
		t.Errorf("the 422 is a finding even though the re-quote recovered: %v", o.Problems)
	}
}

func TestBuyRequotesWhenThePriceQuoteExpired(t *testing.T) {
	f := newFakeBFF()
	defer f.close()
	f.expireFirstSession = true
	s, _ := newShopper(t, f, Options{})
	o := mustBuy(t, s, plan(TokenApprove))
	if o.Status != StatusPlaced || o.Requotes != 1 || f.placements != 1 {
		t.Fatalf("status %s requotes %d placements %d problems %v", o.Status, o.Requotes, f.placements, o.Problems)
	}
	if !o.HasProblem("price-quote-expired") {
		t.Errorf("problems %v", o.Problems)
	}
}

func TestBuyRequotesOnAStaleSelection(t *testing.T) {
	f := newFakeBFF()
	defer f.close()
	f.addFaultSlug("PUT /carts/{id}/delivery-selection", faultBefore, 409, "stale-delivery-selection", 1)
	s, _ := newShopper(t, f, Options{})
	o := mustBuy(t, s, plan(TokenApprove))
	if o.Status != StatusPlaced || o.Requotes != 1 || !o.HasProblem("stale-delivery-selection") {
		t.Fatalf("status %s requotes %d problems %v", o.Status, o.Requotes, o.Problems)
	}
}

func TestBuyOutOfAreaQuotesNotServedAndNeverPlaces(t *testing.T) {
	for _, emptyGroups := range []bool{false, true} {
		f := newFakeBFF()
		f.emptyGroups = emptyGroups
		s, _ := newShopper(t, f, Options{})
		p := Plan{Lines: basket, Prefix: "606", ExpectOutOfArea: true}

		o := mustBuy(t, s, p)
		if o.Status != StatusNotServed || !o.NotServed {
			t.Fatalf("emptyGroups=%v: status %s notServed %v problems %v", emptyGroups, o.Status, o.NotServed, o.Problems)
		}
		if len(o.Problems) != 0 {
			t.Errorf("an expected out-of-area is not a problem: %v", o.Problems)
		}
		if f.count("POST /checkout") != 0 || f.placements != 0 || o.OrderID != "" || o.SessionID != "" {
			t.Errorf("an out-of-area shopper must never check out: %v", f.seen)
		}
		if o.CartID == "" || o.ShopperID == "" {
			t.Errorf("it did get as far as a cart: %+v", o)
		}
		f.close()
	}
}

func TestBuyUnexpectedOutOfAreaIsAFindingAndStillNeverPlaces(t *testing.T) {
	f := newFakeBFF()
	defer f.close()
	s, _ := newShopper(t, f, Options{})
	p := plan(TokenApprove)
	p.Prefix = "606"
	o := mustBuy(t, s, p)
	if o.Status != StatusNotServed || len(o.Problems) == 0 || !strings.Contains(strings.Join(o.Problems, "\n"), "not served") {
		t.Fatalf("status %s problems %v", o.Status, o.Problems)
	}
	if f.placements != 0 {
		t.Error("placed in an area that is not served")
	}
}

func TestBuyExpectedOutOfAreaButServedRefusesToPlace(t *testing.T) {
	f := newFakeBFF()
	defer f.close()
	s, _ := newShopper(t, f, Options{})
	p := plan(TokenApprove) // 787 is served
	p.ExpectOutOfArea = true
	o := mustBuy(t, s, p)
	if o.Status != StatusRefused || len(o.Problems) == 0 {
		t.Fatalf("status %s problems %v", o.Status, o.Problems)
	}
	if f.count("POST /checkout") != 0 || f.placements != 0 {
		t.Errorf("the plan said out of area: never place. seen %v", f.seen)
	}
}

func TestBuyCancelBeforeTheLock(t *testing.T) {
	f := newFakeBFF()
	defer f.close()
	s, _ := newShopper(t, f, Options{})
	p := plan(TokenApprove)
	p.CancelAfterPlacement = true
	p.CancelReason = "wrong size"

	o := mustBuy(t, s, p)
	if o.Status != StatusCancelled || !o.CancelSettled || o.OrderStatus != OrderCancelled || o.CancelRefused != "" {
		t.Fatalf("status %s settled %v order %s refused %q problems %v", o.Status, o.CancelSettled, o.OrderStatus, o.CancelRefused, o.Problems)
	}
	if o.OrderID == "" || o.AuthorizationID == "" {
		t.Errorf("a cancelled order was still placed and paid: %+v", o)
	}
	for _, w := range o.PromiseWindows {
		if w.Status != "CANCELLED" {
			t.Errorf("the final windows must be as last returned: %+v", w)
		}
	}
	if len(o.Problems) != 0 {
		t.Errorf("problems %v", o.Problems)
	}
}

func TestBuyCancelRequestThatNeverSettlesIsBounded(t *testing.T) {
	f := newFakeBFF()
	defer f.close()
	f.settleReads = -1 // order-sourcing never answers
	s, _ := newShopper(t, f, Options{PollTimeout: 40 * time.Millisecond})
	p := plan(TokenApprove)
	p.CancelAfterPlacement = true
	start := time.Now()
	o := mustBuy(t, s, p)
	if o.Status != StatusCancelled || o.CancelSettled || o.OrderStatus == OrderCancelled {
		t.Fatalf("status %s settled %v order %s: accepted but not settled", o.Status, o.CancelSettled, o.OrderStatus)
	}
	if time.Since(start) > 5*time.Second {
		t.Error("the settle wait is not bounded")
	}
}

func TestBuyCancelAfterTheLockIsRefusedAndTheOrderStands(t *testing.T) {
	f := newFakeBFF()
	defer f.close()
	f.lockOnPlace = true
	s, _ := newShopper(t, f, Options{})
	p := plan(TokenApprove)
	p.CancelAfterPlacement = true

	o := mustBuy(t, s, p)
	if o.Status != StatusPlaced || o.CancelRefused != "cancel-after-lock" || o.CancelSettled {
		t.Fatalf("status %s refused %q settled %v", o.Status, o.CancelRefused, o.CancelSettled)
	}
	if !o.HasProblem("cancel-after-lock") {
		t.Errorf("the 409 must be in Problems with its slug: %v", o.Problems)
	}
	if ord, _ := s.Order(ctx(t), o.OrderID); ord.Status != "IN_FULFILLMENT" {
		t.Errorf("the refused cancellation must leave the order alone: %s", ord.Status)
	}
}

// ---- idempotency in a whole purchase ----

func TestBuyReplaysALostConfirmAnswerInsteadOfPlacingTwice(t *testing.T) {
	f := newFakeBFF()
	defer f.close()
	f.addFault("POST /checkout/{id}/confirmations", faultDrop, 0, 1)
	parent := NewClient(f.base(), nil)
	parent.HTTP.Transport = &http.Transport{DisableKeepAlives: true} // no hidden transport retry
	s, err := New(parent, Options{PollEvery: 2 * time.Millisecond, PollTimeout: 5 * time.Second, RetryDelay: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	o := mustBuy(t, s, plan(TokenApprove))
	if o.Status != StatusPlaced {
		t.Fatalf("status %s problems %v", o.Status, o.Problems)
	}
	if f.placements != 1 || f.replays != 1 || o.Replays != 1 {
		t.Errorf("placements %d, server replays %d, outcome replays %d: want 1, 1, 1", f.placements, f.replays, o.Replays)
	}
	if !strings.Contains(strings.Join(o.Problems, "\n"), "transport error") {
		t.Errorf("the lost answer is a finding: %v", o.Problems)
	}
	var keys int
	for _, n := range f.keysSeen {
		if n == 2 {
			keys++
		}
	}
	if keys != 1 {
		t.Errorf("exactly one key must have been sent twice (the confirm's): %v", f.keysSeen)
	}
}

func TestEveryCreatingPostGetsItsOwnKey(t *testing.T) {
	f := newFakeBFF()
	defer f.close()
	s, _ := newShopper(t, f, Options{})
	mustBuy(t, s, plan(TokenApprove))
	// POSTs of a happy path: shoppers, me/addresses, delivery-date-quotes, carts, 2 lines,
	// delivery-quotes, checkout, confirmations = 9 logical actions, 9 distinct keys, 0 replays
	if f.posts != 9 || len(f.keysSeen) != 9 || f.replays != 0 {
		t.Errorf("posts %d, distinct keys %d, replays %d", f.posts, len(f.keysSeen), f.replays)
	}
}

// ---- findings, not panics ----

func TestBuyRecordsA5xxAndStopsWhenTheStepIsRequired(t *testing.T) {
	f := newFakeBFF()
	defer f.close()
	f.addFault("POST /carts/{id}/lines", faultBefore, 500, -1)
	s, stats := newShopper(t, f, Options{})
	o := mustBuy(t, s, plan(TokenApprove))
	if o.Status != StatusRefused || o.CartID == "" {
		t.Fatalf("status %s", o.Status)
	}
	if !strings.Contains(strings.Join(o.Problems, "\n"), "500 upstream-unavailable") {
		t.Errorf("the 5xx must be recorded with its problem slug: %v", o.Problems)
	}
	if f.count("POST /checkout") != 0 || f.placements != 0 {
		t.Error("an empty cart must not be checked out")
	}
	if stats.Count5xx() == 0 {
		t.Error("the 5xx must reach the shared statistics")
	}
}

func TestBuyRecordsA5xxOnAReadAndGoesOn(t *testing.T) {
	f := newFakeBFF()
	defer f.close()
	f.addFault("GET /products/{id}", faultBefore, 500, 1)
	s, _ := newShopper(t, f, Options{})
	o := mustBuy(t, s, plan(TokenApprove))
	if o.Status != StatusPlaced {
		t.Fatalf("a failed product page does not stop a purchase: %s %v", o.Status, o.Problems)
	}
	if len(o.Problems) != 1 || !strings.Contains(o.Problems[0], "GET /products/{id} -> 500") {
		t.Errorf("problems %v", o.Problems)
	}
}

func TestBuyConfirmServerErrorLeavesTheSessionOpenAndRefused(t *testing.T) {
	f := newFakeBFF()
	defer f.close()
	f.addFault("POST /checkout/{id}/confirmations", faultBefore, 500, -1)
	s, stats := newShopper(t, f, Options{})
	o := mustBuy(t, s, plan(TokenApprove))
	if o.Status != StatusRefused || f.placements != 0 || o.OrderID != "" {
		t.Fatalf("status %s placements %d", o.Status, f.placements)
	}
	if !o.HasProblem("upstream-unavailable") || stats.Count5xx() == 0 {
		t.Errorf("problems %v", o.Problems)
	}
}

func TestBuyRefusesAnUnpurchasableItem(t *testing.T) {
	f := newFakeBFF()
	defer f.close()
	s, _ := newShopper(t, f, Options{})
	p := plan(TokenApprove)
	p.Lines = []Line{{"sku-001", 1}, {"sku-004", 1}} // sku-004 is not purchasable
	o := mustBuy(t, s, p)
	if o.Status != StatusRefused || !o.HasProblem("item-not-purchasable") {
		t.Fatalf("status %s problems %v", o.Status, o.Problems)
	}
	if !strings.Contains(strings.Join(o.Problems, "\n"), "sku-004 is not purchasable") {
		t.Errorf("the product page already said so: %v", o.Problems)
	}
	if f.count("POST /checkout") != 0 {
		t.Error("checked out a cart that was refused a line")
	}
}

func TestBuyFailedSessionIsRefusedWithItsFailureCode(t *testing.T) {
	f := newFakeBFF()
	defer f.close()
	s, _ := newShopper(t, f, Options{})
	o := mustBuy(t, s, plan("tok_fail"))
	if o.Status != StatusRefused || o.FailureCode != "contract-violation" || f.placements != 0 {
		t.Fatalf("status %s failureCode %q placements %d", o.Status, o.FailureCode, f.placements)
	}
}

func TestBuyAPlanThatIsASetupBugReturnsAnErrorAndSendsNothing(t *testing.T) {
	f := newFakeBFF()
	defer f.close()
	s, _ := newShopper(t, f, Options{})
	bad := map[string]Plan{
		"no lines":         {Prefix: "787", Token: TokenApprove},
		"zero quantity":    {Lines: []Line{{"sku-001", 0}}, Prefix: "787", Token: TokenApprove},
		"no item":          {Lines: []Line{{"", 1}}, Prefix: "787", Token: TokenApprove},
		"2-char prefix":    {Lines: basket, Prefix: "78", Token: TokenApprove},
		"5-char prefix":    {Lines: basket, Prefix: "78701", Token: TokenApprove},
		"punctuation":      {Lines: basket, Prefix: "7 7", Token: TokenApprove},
		"no token":         {Lines: basket, Prefix: "787"},
		"unknown tier":     {Lines: basket, Prefix: "787", Token: TokenApprove, Tier: "SOMETIME"},
		"quantity over 99": {Lines: []Line{{"sku-001", 100}}, Prefix: "787", Token: TokenApprove},
	}
	for name, p := range bad {
		if o, err := s.Buy(ctx(t), p); err == nil || o.Status != "" {
			t.Errorf("%s: got %v %+v, want a setup error and an empty outcome", name, err, o)
		}
	}
	if len(f.seen) != 0 {
		t.Errorf("a setup bug must not reach the BFF: %v", f.seen)
	}
}

func TestBuyEndsWithTheContext(t *testing.T) {
	f := newFakeBFF()
	defer f.close()
	s, _ := newShopper(t, f, Options{})
	c, cancel := context.WithCancel(context.Background())
	cancel()
	o, err := s.Buy(c, plan(TokenApprove))
	if !errors.Is(err, context.Canceled) || o.Status != StatusRefused {
		t.Errorf("got %v status %s", err, o.Status)
	}
	if f.placements != 0 {
		t.Error("placed after cancel")
	}
}

// ---- one shopper, several purchases; several shoppers, one report ----

func TestTheSameShopperBuysAgainWithoutRegisteringAgain(t *testing.T) {
	f := newFakeBFF()
	defer f.close()
	s, _ := newShopper(t, f, Options{})
	first := mustBuy(t, s, plan(TokenApprove))
	second := mustBuy(t, s, plan(TokenApprove))
	if first.Status != StatusPlaced || second.Status != StatusPlaced || first.OrderID == second.OrderID {
		t.Fatalf("%s %s %q %q", first.Status, second.Status, first.OrderID, second.OrderID)
	}
	if n := f.count("POST /shoppers"); n != 1 {
		t.Errorf("registered %d times", n)
	}
	if n := f.count("POST /me/addresses"); n != 1 {
		t.Errorf("saved the address %d times", n)
	}
	if second.ShopperID != first.ShopperID || second.AddressID != first.AddressID || second.CartID == first.CartID {
		t.Errorf("a new cart for the same shopper and address: %+v / %+v", first, second)
	}
	if len(second.Timeline) >= len(first.Timeline) {
		t.Error("the second timeline must not include the first purchase")
	}
	// a new destination needs a new address
	other := plan(TokenApprove)
	other.Prefix = "786"
	third := mustBuy(t, s, other)
	if third.Status != StatusPlaced || third.AddressID == first.AddressID || f.count("POST /me/addresses") != 2 {
		t.Errorf("third %+v", third)
	}
}

func TestAShopperWhoseEmailIsTakenSignsInAndRecordsTheCollision(t *testing.T) {
	f := newFakeBFF()
	defer f.close()
	owner, _ := newShopper(t, f, Options{Email: "taken@example.com"})
	if _, err := owner.Register(ctx(t)); err != nil {
		t.Fatal(err)
	}
	s, _ := newShopper(t, f, Options{Email: "taken@example.com"})
	o := mustBuy(t, s, plan(TokenApprove))
	if o.Status != StatusPlaced || o.ShopperID != owner.ShopperID {
		t.Fatalf("status %s shopper %s, want it signed in as %s: %v", o.Status, o.ShopperID, owner.ShopperID, o.Problems)
	}
	if !o.HasProblem("shopper-exists") {
		t.Errorf("the collision is a finding: %v", o.Problems)
	}
}

func TestShoppersShareOneReportAndNoCart(t *testing.T) {
	f := newFakeBFF()
	defer f.close()
	stats := sim.NewCallStats()
	parent := NewClient(f.base(), stats)
	var carts []string
	for i := 0; i < 3; i++ {
		s, err := New(parent, Options{PollEvery: time.Millisecond, PollTimeout: 5 * time.Second, RetryDelay: time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		o := mustBuy(t, s, plan(TokenApprove))
		if o.Status != StatusPlaced {
			t.Fatalf("shopper %d: %s %v", i, o.Status, o.Problems)
		}
		carts = append(carts, o.CartID)
	}
	slices.Sort(carts)
	if len(slices.Compact(carts)) != 3 {
		t.Errorf("carts %v: shoppers share a cart", carts)
	}
	for _, l := range stats.Lines() {
		if l.Route == "storefront POST /carts" && l.Calls != 3 {
			t.Errorf("%+v: three shoppers, three carts, one shared statistic", l)
		}
	}
	if f.placements != 3 {
		t.Errorf("placements %d", f.placements)
	}
}

func TestEmailsAreUniquePerShopper(t *testing.T) {
	f := newFakeBFF()
	defer f.close()
	seen := map[string]bool{}
	for i := 0; i < 50; i++ {
		s, _ := newShopper(t, f, Options{})
		if seen[s.Email] || !strings.HasSuffix(s.Email, "@example.com") {
			t.Fatalf("email %q repeats or is not on the reserved example domain", s.Email)
		}
		seen[s.Email] = true
	}
}

func TestFindingsAreBounded(t *testing.T) {
	f := newFakeBFF()
	defer f.close()
	s, _ := newShopper(t, f, Options{Retries: -1})
	for i := 0; i < maxFindings+25; i++ {
		if _, err := s.Product(ctx(t), "sku-999"); !Is(err, "not-found") {
			t.Fatalf("got %v", err)
		}
	}
	got := s.Findings()
	if len(got) != maxFindings+1 || !strings.Contains(got[len(got)-1], "+25 more") {
		t.Errorf("%d findings, last %q", len(got), got[len(got)-1])
	}
}
