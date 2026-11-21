package shopper

import (
	"errors"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"
)

// atCheckout walks a fresh shopper to an OPEN checkout session of two lines on two nodes
// (so two shipment groups) shipping to prefix.
func atCheckout(t *testing.T, f *fakeBFF, s *Shopper, prefix string, prefer Tier) Session {
	t.Helper()
	c := ctx(t)
	if _, err := s.Register(c); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if _, err := s.AddAddress(c, AddressFor(prefix, "")); err != nil {
		t.Fatalf("AddAddress: %v", err)
	}
	if _, err := s.NewCart(c); err != nil {
		t.Fatalf("NewCart: %v", err)
	}
	for _, l := range []Line{{"sku-001", 2}, {"sku-005", 1}} {
		if _, err := s.AddLine(c, l); err != nil {
			t.Fatalf("AddLine: %v", err)
		}
	}
	if _, err := s.SetDestination(c, prefix); err != nil {
		t.Fatalf("SetDestination: %v", err)
	}
	cart, err := s.QuoteCart(c)
	if err != nil || cart.DeliveryQuote == nil {
		t.Fatalf("QuoteCart: %v %+v", err, cart)
	}
	if _, _, err := s.SelectOffers(c, *cart.DeliveryQuote, prefer); err != nil {
		t.Fatalf("SelectOffers: %v", err)
	}
	sess, err := s.Checkout(c)
	if err != nil {
		t.Fatalf("Checkout: %v", err)
	}
	return sess
}

func TestCartStepsBuildAQuotedCartAndSelectEveryGroup(t *testing.T) {
	f := newFakeBFF()
	defer f.close()
	s, _ := newShopper(t, f, Options{})
	c := ctx(t)
	if _, err := s.Register(c); err != nil {
		t.Fatal(err)
	}
	cart, err := s.NewCart(c)
	if err != nil || s.CartID == "" || s.State().CartID != s.CartID || cart.ShopperID != s.ShopperID {
		t.Fatalf("NewCart: %v %+v state %+v", err, cart, s.State())
	}
	for _, l := range []Line{{"sku-001", 1}, {"sku-005", 1}} {
		if _, err := s.AddLine(c, l); err != nil {
			t.Fatal(err)
		}
	}
	if cart, err = s.SetQuantity(c, "sku-001", 3); err != nil || cart.Lines[0].Qty != 3 {
		t.Fatalf("SetQuantity: %v %+v", err, cart.Lines)
	}
	if _, err := s.SetQuantity(c, "sku-002", 3); !Is(err, "not-found") {
		t.Errorf("a line that is not in the cart: got %v, want not-found", err)
	}
	if _, err := s.QuoteCart(c); !Is(err, "no-destination") {
		t.Fatalf("quote without destination: got %v, want no-destination", err)
	}
	if _, err := s.SetDestination(c, "78701"); !Is(err, "validation-failed") {
		t.Errorf("a 5-character destination: got %v, want validation-failed", err)
	}
	if cart, err = s.SetDestination(c, "787"); err != nil || cart.DestinationPostalPrefix != "787" {
		t.Fatalf("SetDestination: %v %+v", err, cart)
	}
	if cart, err = s.QuoteCart(c); err != nil {
		t.Fatal(err)
	}
	q := cart.DeliveryQuote
	if q == nil || len(q.Groups) != 2 || q.Offers() != 4 || q.NotServed() {
		t.Fatalf("two nodes must quote two groups of two tiers: %+v", q)
	}
	if cart.CheckoutReadiness.Ready {
		t.Error("a cart whose groups are not selected is not ready")
	}

	cart, chosen, err := s.SelectOffers(c, *q, TierTwoDay)
	if err != nil {
		t.Fatal(err)
	}
	if len(chosen) != 2 || len(cart.Selections) != 2 || !cart.CheckoutReadiness.Ready {
		t.Fatalf("every group must be selected and the cart ready: %+v %+v", chosen, cart)
	}
	for _, o := range chosen {
		if o.Tier != TierTwoDay {
			t.Errorf("group %d chose %s, want the preferred TWO_DAY", o.GroupNo, o.Tier)
		}
	}
	// 3 x 1100 + 1 x 1500 of items, two TWO_DAY groups at 899
	if want := int64(3*1100 + 1500 + 2*899); cart.Estimate.TotalMinor != want {
		t.Errorf("estimate %d, want %d", cart.Estimate.TotalMinor, want)
	}
}

func TestChooseOffers(t *testing.T) {
	offer := func(g int, tier Tier) Offer { return Offer{OfferID: string(tier), GroupNo: g, Tier: tier} }
	q := Quote{Groups: []QuoteGroup{
		{GroupNo: 1, Offers: []Offer{offer(1, TierNextDay), offer(1, TierStandard), offer(1, TierTwoDay)}},
		{GroupNo: 2, Offers: []Offer{offer(2, TierTwoDay), offer(2, TierStandard)}},
		{GroupNo: 3},
	}}
	tiers := func(o []Offer) []Tier {
		var out []Tier
		for _, x := range o {
			out = append(out, x.Tier)
		}
		return out
	}
	cases := []struct {
		prefer      Tier
		want        []Tier
		wantMissing []int
	}{
		{"", []Tier{TierStandard, TierStandard}, []int{3}},
		{TierStandard, []Tier{TierStandard, TierStandard}, []int{3}},
		{TierTwoDay, []Tier{TierTwoDay, TierTwoDay}, []int{3}},
		// NEXT_DAY exists in group 1 only: group 2 falls back to the cheapest
		{TierNextDay, []Tier{TierNextDay, TierStandard}, []int{3}},
		{TierSameDay, []Tier{TierStandard, TierStandard}, []int{3}},
	}
	for _, tc := range cases {
		got, missing := ChooseOffers(q, tc.prefer)
		if !slices.Equal(tiers(got), tc.want) || !slices.Equal(missing, tc.wantMissing) {
			t.Errorf("prefer %q: got %v missing %v, want %v missing %v", tc.prefer, tiers(got), missing, tc.want, tc.wantMissing)
		}
	}
}

func TestSelectingAStaleOfferIsRejected(t *testing.T) {
	f := newFakeBFF()
	defer f.close()
	s, _ := newShopper(t, f, Options{})
	c := ctx(t)
	_ = atCheckout(t, f, s, "787", "")
	first, _ := s.Cart(c)
	old := first.DeliveryQuote.Groups[0].Offers[0]
	fresh, err := s.QuoteCart(c) // a re-quote replaces the quote: the old offer ids are gone
	if err != nil {
		t.Fatal(err)
	}
	if fresh.DeliveryQuote.Groups[0].Offers[0].OfferID == old.OfferID {
		t.Fatal("a re-quote must mint new offers")
	}
	if _, err := s.SelectDelivery(c, 1, old.OfferID); !Is(err, "stale-delivery-selection") {
		t.Errorf("got %v, want stale-delivery-selection", err)
	}
}

func TestCheckoutNeedsReadyCartAndValidOffers(t *testing.T) {
	f := newFakeBFF()
	defer f.close()
	s, _ := newShopper(t, f, Options{})
	c := ctx(t)
	if _, err := s.Register(c); err != nil {
		t.Fatal(err)
	}
	for _, step := range []func() error{
		func() error { _, err := s.AddAddress(c, AddressFor("787", "")); return err },
		func() error { _, err := s.NewCart(c); return err },
		func() error { _, err := s.AddLine(c, Line{"sku-001", 1}); return err },
		func() error { _, err := s.SetDestination(c, "787"); return err },
	} {
		if err := step(); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Checkout(c); !Is(err, "cart-not-checkout-ready") {
		t.Fatalf("no quote: got %v, want cart-not-checkout-ready", err)
	}
	cart, err := s.QuoteCart(c)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.SelectOffers(c, *cart.DeliveryQuote, ""); err != nil {
		t.Fatal(err)
	}
	f.advance(29 * time.Minute) // the offer lives 30 minutes: 1 minute left, checkout wants 2
	if _, err := s.Checkout(c); !Is(err, "offer-not-quotable") {
		t.Fatalf("an offer about to expire: got %v, want offer-not-quotable", err)
	}
	f.advance(time.Hour)
	if c2, _ := s.Cart(c); c2.CheckoutReadiness.NoOfferExpired {
		t.Error("the cart must say an offer expired")
	}
}

func TestConfirmApproveDeclineAndTryAnotherToken(t *testing.T) {
	f := newFakeBFF()
	defer f.close()
	s, _ := newShopper(t, f, Options{})
	sess := atCheckout(t, f, s, "787", TierStandard)
	if sess.State != StateOpen || sess.TotalMinor != 2*1100+1500+2*499 {
		t.Fatalf("opened session: %+v", sess)
	}

	declined, err := s.Confirm(ctx(t), TokenDecline)
	if err != nil || declined.State != StateDeclined || declined.OrderID != "" {
		t.Fatalf("decline: %v %+v", err, declined)
	}
	done, err := s.Confirm(ctx(t), TokenApprove)
	if err != nil || done.State != StateCompleted || done.OrderID == "" || done.AuthorizationID == "" {
		t.Fatalf("approve after a decline: %v %+v", err, done)
	}
	if f.placements != 1 {
		t.Errorf("%d orders placed, want 1", f.placements)
	}
	if _, err := s.Confirm(ctx(t), TokenApprove); !Is(err, "session-closed") {
		t.Errorf("a confirm on a COMPLETED session: got %v, want session-closed", err)
	}
	if f.placements != 1 {
		t.Errorf("a second confirm placed another order: %d", f.placements)
	}
}

func TestConfirmRefusesACardNumber(t *testing.T) {
	f := newFakeBFF()
	defer f.close()
	s, _ := newShopper(t, f, Options{})
	atCheckout(t, f, s, "787", "")
	if _, err := s.Confirm(ctx(t), "4242424242424242"); !Is(err, "validation-failed") {
		t.Errorf("a card number as a token: got %v, want validation-failed", err)
	}
}

func TestConfirmTimeoutIsBoundedAndReportsInDoubt(t *testing.T) {
	f := newFakeBFF()
	defer f.close()
	s, _ := newShopper(t, f, Options{PollEvery: 2 * time.Millisecond, PollTimeout: 60 * time.Millisecond})
	atCheckout(t, f, s, "787", "")

	start := time.Now()
	sess, err := s.Confirm(ctx(t), TokenTimeout)
	if !errors.Is(err, ErrInDoubt) || sess.State != StateAuthorizing {
		t.Fatalf("got %v %+v, want ErrInDoubt with the session AUTHORIZING", err, sess)
	}
	if took := time.Since(start); took < 50*time.Millisecond || took > 3*time.Second {
		t.Errorf("waited %v: the wait must last PollTimeout (60ms) and no longer", took)
	}
	took := time.Since(start)
	if polls := f.count("GET /checkout/{id}"); polls < 1 || polls > int(took/(2*time.Millisecond))+1 {
		t.Errorf("%d polls in %v at one per 2ms: not polling, or not pausing between polls", polls, took)
	}
	// the session has no way out (G8): the BFF refuses a second confirm (409 illegal-transition),
	// and the shopper reads that as "still in doubt" instead of paying again
	sess, err = s.Confirm(ctx(t), TokenApprove)
	if !errors.Is(err, ErrInDoubt) || sess.State != StateAuthorizing {
		t.Errorf("confirm on a stuck session: got %v %+v, want ErrInDoubt", err, sess)
	}
	if f.placements != 0 {
		t.Error("a stuck session must never place an order")
	}
}

func TestConfirmAcceptedThenPollsToCompletion(t *testing.T) {
	f := newFakeBFF()
	defer f.close()
	f.slowPolls = 3
	s, _ := newShopper(t, f, Options{})
	atCheckout(t, f, s, "787", "")

	sess, err := s.Confirm(ctx(t), "tok_slow")
	if err != nil || sess.State != StateCompleted || sess.OrderID == "" {
		t.Fatalf("202 PLACING then polls: %v %+v", err, sess)
	}
	if polls := f.count("GET /checkout/{id}"); polls != 3 {
		t.Errorf("%d polls, want exactly 3 (the third completes it)", polls)
	}
	var seen202 bool
	for _, st := range s.Timeline() {
		seen202 = seen202 || st.Name == "confirm" && st.Status == 202
	}
	if !seen202 {
		t.Error("the timeline must show the 202")
	}
}

func TestConfirmRequoteClosesTheSession(t *testing.T) {
	f := newFakeBFF()
	defer f.close()
	f.requotes = 1
	s, _ := newShopper(t, f, Options{})
	first := atCheckout(t, f, s, "787", TierStandard)
	sess, err := s.Confirm(ctx(t), "tok_requote")
	if err != nil || sess.State != StateRequoteRequired || len(sess.RequotedOffers) != len(first.Groups) {
		t.Fatalf("got %v %+v, want REQUOTE_REQUIRED carrying the requoted offers", err, sess)
	}
	if _, err := s.Confirm(ctx(t), "tok_requote"); !Is(err, "session-closed") {
		t.Errorf("re-confirming a REQUOTE_REQUIRED session: got %v, want session-closed (there is no accept route)", err)
	}
	if f.placements != 0 {
		t.Errorf("a requote placed %d orders", f.placements)
	}
}

func TestConfirmAfterTheQuoteExpiredNeedsANewSession(t *testing.T) {
	f := newFakeBFF()
	defer f.close()
	s, _ := newShopper(t, f, Options{})
	atCheckout(t, f, s, "787", "")
	f.advance(time.Hour)
	if _, err := s.Confirm(ctx(t), TokenApprove); !Is(err, "price-quote-expired") {
		t.Fatalf("got %v, want price-quote-expired", err)
	}
	if f.placements != 0 {
		t.Error("an expired quote must not place an order")
	}
}

// A lost answer: the command ran and was stored, the connection was cut before the reply. The
// Go transport may retry such a request by itself when it carries an Idempotency-Key and rode a
// reused connection; either way the same key reaches the server twice and nothing is placed twice.
func TestAConfirmWhoseAnswerWasLostReplaysInsteadOfPlacingTwice(t *testing.T) {
	f := newFakeBFF()
	defer f.close()
	s, _ := newShopper(t, f, Options{})
	atCheckout(t, f, s, "787", "")
	f.addFault("POST /checkout/{id}/confirmations", faultDrop, 0, 1)

	sess, err := s.Confirm(ctx(t), TokenApprove)
	if err != nil || sess.State != StateCompleted {
		t.Fatalf("got %v %+v", err, sess)
	}
	if f.placements != 1 || f.replays != 1 {
		t.Errorf("placements %d replays %d, want 1 and 1: the retry must carry the SAME Idempotency-Key", f.placements, f.replays)
	}
	var replayed bool
	for _, st := range s.Timeline() {
		replayed = replayed || st.Name == "confirm" && st.Replayed
	}
	if !replayed {
		t.Errorf("the timeline must show the replayed answer: %+v", s.Timeline())
	}
}

// The same on fresh connections (keep-alives off), where Go never retries by itself: the shopper's
// own retry reuses the key.
func TestTheShoppersOwnRetryReusesTheKeyAndReplays(t *testing.T) {
	f := newFakeBFF()
	defer f.close()
	parent := NewClient(f.base(), nil)
	parent.HTTP.Transport = &http.Transport{DisableKeepAlives: true}
	s, err := New(parent, Options{PollEvery: 2 * time.Millisecond, PollTimeout: 5 * time.Second, RetryDelay: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	atCheckout(t, f, s, "787", "")
	f.addFault("POST /checkout/{id}/confirmations", faultDrop, 0, 1)

	sess, err := s.Confirm(ctx(t), TokenApprove)
	if err != nil || sess.State != StateCompleted {
		t.Fatalf("got %v %+v", err, sess)
	}
	if f.placements != 1 || f.replays != 1 {
		t.Errorf("placements %d replays %d, want 1 and 1", f.placements, f.replays)
	}
	var attempts []Step
	for _, st := range s.Timeline() {
		if st.Name == "confirm" {
			attempts = append(attempts, st)
		}
	}
	if len(attempts) != 2 || attempts[0].Status != 0 || attempts[1].Attempt != 1 || !attempts[1].Replayed {
		t.Errorf("want a transport error then a replayed retry: %+v", attempts)
	}
	if got := s.Findings(); len(got) != 1 || !strings.Contains(got[0], "transport error") {
		t.Errorf("the lost answer is a finding even though the retry recovered: %v", got)
	}
}

func TestAnAnswerLostBeforeTheCommandRanIsRetriedAndRunsOnce(t *testing.T) {
	f := newFakeBFF()
	defer f.close()
	s, _ := newShopper(t, f, Options{})
	atCheckout(t, f, s, "787", "")
	f.addFault("POST /checkout/{id}/confirmations", faultBefore, 503, 2)
	sess, err := s.Confirm(ctx(t), TokenApprove)
	if err != nil || sess.State != StateCompleted || f.placements != 1 || f.replays != 0 {
		t.Fatalf("two 503s then success: %v %+v placements %d replays %d", err, sess, f.placements, f.replays)
	}
	if len(s.Findings()) != 2 {
		t.Errorf("both 503s are findings: %v", s.Findings())
	}
}

func TestPersistentServerErrorOnConfirmIsRejectedAfterPolling(t *testing.T) {
	f := newFakeBFF()
	defer f.close()
	s, _ := newShopper(t, f, Options{})
	atCheckout(t, f, s, "787", "")
	f.addFault("POST /checkout/{id}/confirmations", faultBefore, 500, -1)
	sess, err := s.Confirm(ctx(t), TokenApprove)
	var rej *Rejection
	if !errors.As(err, &rej) || rej.Status != 500 || sess.State != StateOpen {
		t.Fatalf("got %v %+v, want the 500 with the session still OPEN (the confirm never took effect)", err, sess)
	}
	if f.placements != 0 {
		t.Error("nothing may be placed")
	}
}

func TestOrderOrdersAndCancelBeforeAndAfterTheLock(t *testing.T) {
	f := newFakeBFF()
	defer f.close()
	s, _ := newShopper(t, f, Options{})
	atCheckout(t, f, s, "787", TierTwoDay)
	sess, err := s.Confirm(ctx(t), TokenApprove)
	if err != nil {
		t.Fatal(err)
	}
	o, err := s.Order(ctx(t), sess.OrderID)
	if err != nil || o.Status != "PLACED" || len(o.Groups) != 2 || o.DestinationPostalPrefix != "787" {
		t.Fatalf("Order: %v %+v", err, o)
	}
	for _, g := range o.Groups {
		if g.Tier != TierTwoDay || g.TimeZone != "America/Chicago" || g.DeliverByEarliest == "" || g.Status != GroupPromised {
			t.Errorf("the promise window must come back as returned: %+v", g)
		}
	}
	if o.Totals == nil || o.Totals.TotalMinor == nil || *o.Totals.TotalMinor != sess.TotalMinor {
		t.Errorf("the cookie's checkout placed the order, so its totals are joined: %+v", o.Totals)
	}
	// a second browser never saw that checkout: no totals (journey gap G11)
	other, _ := newShopper(t, f, Options{Email: o.ShopperID + "@example.com"})
	if _, err := other.Register(ctx(t)); err != nil {
		t.Fatal(err)
	}
	if o2, err := other.Order(ctx(t), sess.OrderID); err != nil || o2.Totals != nil {
		t.Errorf("another cookie must not get the totals: %v %+v", err, o2.Totals)
	}

	list, err := s.Orders(ctx(t))
	if err != nil || len(list) != 1 || list[0].OrderID != sess.OrderID {
		t.Fatalf("Orders: %v %+v", err, list)
	}

	// before the lock the cancellation is a request, settled by the next read
	if _, err := s.Cancel(ctx(t), sess.OrderID, ""); !Is(err, "validation-failed") {
		t.Errorf("an empty reason: got %v, want validation-failed", err)
	}
	c1, err := s.Cancel(ctx(t), sess.OrderID, "changed my mind")
	if err != nil || c1.Groups[0].Status != GroupCancelRequest {
		t.Fatalf("cancel before the lock: %v %+v", err, c1)
	}
	if c2, err := s.Order(ctx(t), sess.OrderID); err != nil || c2.Status != OrderCancelled {
		t.Fatalf("the order must settle to CANCELLED: %v %+v", err, c2)
	}

	// after the lock it is refused
	s2, _ := newShopper(t, f, Options{})
	atCheckout(t, f, s2, "786", "")
	sess2, _ := s2.Confirm(ctx(t), TokenApprove)
	f.lock(sess2.OrderID)
	if _, err := s2.Cancel(ctx(t), sess2.OrderID, "too late"); !Is(err, "cancel-after-lock") {
		t.Errorf("cancel after the lock: got %v, want cancel-after-lock", err)
	}
	if o3, _ := s2.Order(ctx(t), sess2.OrderID); o3.Status != "IN_FULFILLMENT" {
		t.Errorf("a refused cancellation must leave the order alone: %+v", o3)
	}
}

func TestOrdersWalkEveryPage(t *testing.T) {
	f := newFakeBFF()
	defer f.close()
	f.pageCap = 2
	s, _ := newShopper(t, f, Options{})
	c := ctx(t)
	if _, err := s.Orders(c); !Is(err, "session-required") {
		t.Errorf("before Register: got %v, want session-required", err)
	}
	atCheckout(t, f, s, "787", "")
	if _, err := s.Confirm(c, TokenApprove); err != nil {
		t.Fatal(err)
	}
	before := f.count("GET /orders")
	for i := 0; i < 4; i++ {
		if _, err := s.NewCart(c); err != nil {
			t.Fatal(err)
		}
		if _, err := s.AddLine(c, Line{"sku-002", 1}); err != nil {
			t.Fatal(err)
		}
		if _, err := s.SetDestination(c, "787"); err != nil {
			t.Fatal(err)
		}
		cart, err := s.QuoteCart(c)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := s.SelectOffers(c, *cart.DeliveryQuote, ""); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Checkout(c); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Confirm(c, TokenApprove); err != nil {
			t.Fatal(err)
		}
	}
	all, err := s.Orders(c)
	if err != nil || len(all) != 5 {
		t.Fatalf("%v: %d orders, want 5 across 3 pages", err, len(all))
	}
	if got := f.count("GET /orders") - before; got != 3 {
		t.Errorf("%d list requests, want 3 pages", got)
	}
}
