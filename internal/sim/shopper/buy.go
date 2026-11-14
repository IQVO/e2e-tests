package shopper

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

// Status is how one purchase ended.
type Status string

// The ways a Buy ends. Everything the BFF refused is in Outcome.Problems, whatever the status.
const (
	// StatusPlaced: the session is COMPLETED and an order exists.
	StatusPlaced Status = "placed"
	// StatusDeclined: the payment was declined and no other token was left to try.
	StatusDeclined Status = "declined"
	// StatusInDoubt: the bounded wait ended with the session still AUTHORIZING or PLACING; the
	// shopper does not know whether it paid (tok_timeout today: journey gap G8).
	StatusInDoubt Status = "in-doubt"
	// StatusRequoteExhausted: the stack asked for a new quote more often than the shopper re-quotes.
	StatusRequoteExhausted Status = "requote-exhausted"
	// StatusRefused: a step was rejected (4xx/5xx), the session FAILED or EXPIRED, or the quote had
	// nothing the shopper could buy. The reason is in Problems.
	StatusRefused Status = "refused"
	// StatusCancelled: the order was placed and its cancellation accepted (Outcome.CancelSettled
	// says whether the order already reads CANCELLED).
	StatusCancelled Status = "cancelled"
	// StatusNotServed: the delivery quote said the destination is outside every service area; the
	// shopper did not check out.
	StatusNotServed Status = "not-served"
)

// Plan is one purchase.
type Plan struct {
	// Lines is what goes in the cart.
	Lines []Line
	// Prefix is the 3-character postal prefix of the destination (786 and 787 are served).
	Prefix string
	// Tier is the preferred delivery tier: the matching offer is selected for every group that has
	// one, the cheapest otherwise. Empty means the cheapest.
	Tier Tier
	// Token is the payment-method token the shopper pays with (tok_approve, tok_decline,
	// tok_timeout, ...). It may be empty only with ExpectOutOfArea.
	Token string
	// FallbackToken, when set, is tried once after Token was DECLINED (the session stays
	// payable while its price quote is valid).
	FallbackToken string
	// CancelAfterPlacement cancels the order right after it was placed. The cancellation is a
	// request that only succeeds until the purchase order is accepted; later it is refused with
	// cancel-after-lock, which is recorded in Outcome.CancelRefused and the order stays placed.
	CancelAfterPlacement bool
	// CancelReason is the free text of the cancellation (default "changed my mind").
	CancelReason string
	// ExpectOutOfArea says the prefix is not served: the shopper quotes, records that the quote
	// says so, and never checks out. If the quote does offer delivery that is a finding, and the
	// shopper still does not place.
	ExpectOutOfArea bool
}

func (p Plan) validate() error {
	if len(p.Lines) == 0 {
		return errors.New("shopper: the plan has no lines")
	}
	for i, l := range p.Lines {
		if l.CatalogItemID == "" || l.Qty < 1 || l.Qty > 99 {
			return fmt.Errorf("shopper: plan line %d: need a catalogItemId and a quantity of 1 to 99, got %+v", i, l)
		}
	}
	if !validPrefix(p.Prefix) {
		return fmt.Errorf("shopper: the plan's postal prefix %q is not %d letters or digits", p.Prefix, prefixLen)
	}
	switch p.Tier {
	case "", TierStandard, TierTwoDay, TierNextDay, TierSameDay:
	default:
		return fmt.Errorf("shopper: unknown tier %q", p.Tier)
	}
	if p.Token == "" && !p.ExpectOutOfArea {
		return errors.New("shopper: the plan has no payment token")
	}
	return nil
}

// PromiseWindow is one shipment group's promise as the order returned it: instants in RFC 3339,
// to be read in TimeZone when it is set (UTC otherwise).
type PromiseWindow struct {
	GroupNo           int    `json:"groupNo"`
	Tier              Tier   `json:"tier"`
	ShipBy            string `json:"shipBy"`
	DeliverByEarliest string `json:"deliverByEarliest"`
	DeliverByLatest   string `json:"deliverByLatest"`
	TimeZone          string `json:"timeZone"`
	Status            string `json:"status"`
}

// Outcome is everything one Buy learned. A rejected step is a FINDING recorded here, never a panic.
type Outcome struct {
	ShopperID       string `json:"shopperId"`
	Email           string `json:"email"`
	AddressID       string `json:"addressId,omitempty"`
	CartID          string `json:"cartId,omitempty"`
	SessionID       string `json:"sessionId,omitempty"`
	OrderID         string `json:"orderId,omitempty"`
	AuthorizationID string `json:"authorizationId,omitempty"`
	// TotalMinor and Currency are the binding total of the LAST checkout session.
	TotalMinor int64  `json:"totalMinor,omitempty"`
	Currency   string `json:"currency,omitempty"`
	// PromiseWindows has one entry per shipment group, as the order returned them.
	PromiseWindows []PromiseWindow `json:"promiseWindows,omitempty"`
	// Tiers is the tier selected for each group of the last quote.
	Tiers  map[int]Tier `json:"tiers,omitempty"`
	Status Status       `json:"status"`
	// Problems are the 4xx/5xx answers (with the problem slug and detail), transport errors and
	// contract disagreements met on the way, retried ones included. Empty means a clean run.
	Problems []string `json:"problems,omitempty"`
	// Timeline is every request of the Buy in order.
	Timeline []Step `json:"timeline,omitempty"`

	Requotes int `json:"requotes,omitempty"` // times the shopper re-quoted
	Declines int `json:"declines,omitempty"` // DECLINED answers
	Replays  int `json:"replays,omitempty"`  // answers replayed from the idempotency store
	// FailureCode is the failureCode of a FAILED session.
	FailureCode string `json:"failureCode,omitempty"`
	// InDoubtState is the session state when the wait ended (AUTHORIZING, PLACING, or "" when the
	// session could not be read).
	InDoubtState string `json:"inDoubtState,omitempty"`
	// OrderStatus is the last order status read (PLACED, CANCELLED, ...); Listed says the order
	// was in GET /orders.
	OrderStatus string `json:"orderStatus,omitempty"`
	Listed      bool   `json:"listed,omitempty"`
	// CancelRefused is the problem slug of a refused cancellation (cancel-after-lock);
	// CancelSettled says the order read CANCELLED within the bounded wait.
	CancelRefused string `json:"cancelRefused,omitempty"`
	CancelSettled bool   `json:"cancelSettled,omitempty"`
	// NotServed says the delivery quote reported DESTINATION_NOT_SERVED.
	NotServed bool `json:"notServed,omitempty"`
}

// HasProblem reports whether a problem slug (for example "cancel-after-lock") is among Problems.
func (o Outcome) HasProblem(slug string) bool {
	for _, p := range o.Problems {
		if strings.Contains(p, " "+slug) {
			return true
		}
	}
	return false
}

// maxFindings bounds the findings of one shopper: a persistent 5xx behind a poll loop must not
// grow the report without limit.
const maxFindings = 200

// problem records a finding that is not an HTTP rejection (a contract disagreement, a plan
// expectation that did not hold).
func (s *Shopper) problem(format string, args ...any) {
	s.addFinding(fmt.Sprintf(format, args...))
}

// attempt is how one pass of quote, select, check out and pay ended.
type attempt int

const (
	attemptDone attempt = iota
	attemptRequote
)

// payState survives across the passes of one Buy: the token in use, and whether the fallback
// token was already spent.
type payState struct {
	token        string
	fallbackUsed bool
}

// Buy plays the whole purchase of p: register (or sign in) and save an address when the shopper
// does not have them yet, read the product pages and the delivery dates, fill a cart, set the
// destination, quote delivery, select an offer for every group, open a checkout, pay with the
// token, wait (bounded) for a definitive outcome, read the order, check it is in the history and
// optionally cancel it.
//
// The only errors are setup bugs (an invalid plan) and the end of ctx. Every other failure is a
// finding in Outcome.Problems with the Status that says how far the shopper got. The same Shopper
// may Buy again: it keeps its registration, cookie and saved addresses.
func (s *Shopper) Buy(ctx context.Context, p Plan) (Outcome, error) {
	if err := p.validate(); err != nil {
		return Outcome{}, err
	}
	s.findings, s.suppressed = nil, 0
	first := len(s.timeline)
	o := Outcome{Email: s.Email}
	err := s.buy(ctx, p, &o)
	if err == nil && ctx.Err() != nil {
		err = ctx.Err()
	}
	if o.Status == "" {
		o.Status = StatusRefused
	}
	o.ShopperID, o.CartID, o.SessionID, o.AddressID = s.ShopperID, s.CartID, s.SessionID, s.AddressID
	o.Problems = s.Findings()
	o.Timeline = append([]Step(nil), s.timeline[first:]...)
	for _, st := range o.Timeline {
		if st.Replayed {
			o.Replays++
		}
	}
	return o, err
}

func (s *Shopper) buy(ctx context.Context, p Plan, o *Outcome) error {
	refuse := func() error {
		o.Status = StatusRefused
		return ctx.Err()
	}

	// who: register, or sign in when the e-mail is taken; then an address for the destination
	if s.ShopperID == "" {
		if _, err := s.Register(ctx); err != nil {
			if ctx.Err() != nil || !Is(err, "shopper-exists") {
				return refuse()
			}
			if _, err := s.SignIn(ctx); err != nil {
				return refuse()
			}
		}
		if got := s.State().ShopperID; got != s.ShopperID {
			s.problem("the sf_state cookie carries shopper %q after registering %q", got, s.ShopperID)
		}
	}
	if sa, have := s.addressFor(p.Prefix); have {
		s.AddressID = sa.AddressID
	} else if _, err := s.AddAddress(ctx, AddressFor(p.Prefix, "")); err != nil {
		return refuse()
	}

	// browse: the product pages, and the delivery dates the first one promises for the destination
	for _, l := range p.Lines {
		pr, err := s.Product(ctx, l.CatalogItemID)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err == nil && !pr.Purchasable {
			s.problem("product %s is not purchasable", l.CatalogItemID)
		}
	}
	if dd, err := s.QuoteDelivery(ctx, p.Lines[0].CatalogItemID, p.Prefix); err == nil {
		switch {
		case dd.NotServed() && !p.ExpectOutOfArea:
			s.problem("the product page says %s is not served", p.Prefix)
		case !dd.NotServed() && p.ExpectOutOfArea:
			s.problem("the product page quotes %d tier(s) for %s, which the plan expects to be out of area", len(dd.Tiers), p.Prefix)
		}
	} else if ctx.Err() != nil {
		return ctx.Err()
	}

	// the cart
	if _, err := s.NewCart(ctx); err != nil {
		return refuse()
	}
	for _, l := range p.Lines {
		if _, err := s.AddLine(ctx, l); err != nil {
			return refuse()
		}
	}
	if _, err := s.SetDestination(ctx, p.Prefix); err != nil {
		return refuse()
	}

	// quote, select, check out, pay: again when the stack asks for a new quote
	pay := &payState{token: p.Token}
	maxRequotes := max(s.opt.MaxRequotes, 0)
	for {
		res, err := s.pass(ctx, p, o, pay)
		if err != nil {
			return err
		}
		if res == attemptDone {
			return nil
		}
		if o.Requotes >= maxRequotes {
			o.Status = StatusRequoteExhausted
			return nil
		}
		o.Requotes++
		s.logf("requote %d of %d", o.Requotes, maxRequotes)
	}
}

// pass is one run of quote, select, checkout and pay. Because a REQUOTE_REQUIRED session is closed
// and the BFF has no route to accept the new offers (journey gap G9), the way forward is a new
// pass: re-quote the cart, re-select every group, open a NEW checkout session, pay again.
func (s *Shopper) pass(ctx context.Context, p Plan, o *Outcome, pay *payState) (attempt, error) {
	done := func(st Status) (attempt, error) {
		o.Status = st
		return attemptDone, ctx.Err()
	}

	cart, err := s.QuoteCart(ctx)
	if err != nil {
		return done(StatusRefused)
	}
	q := cart.DeliveryQuote
	if q == nil {
		s.problem("the cart has no delivery quote after POST delivery-quotes")
		return done(StatusRefused)
	}
	if q.Offers() == 0 {
		if q.NotServed() {
			o.NotServed = true
			if !p.ExpectOutOfArea {
				s.problem("the delivery quote says %s is not served (%s)", p.Prefix, ConstraintNotServed)
			}
			return done(StatusNotServed)
		}
		s.problem("the delivery quote has no offer: %s", describeUnavailable(q.Unavailable))
		return done(StatusRefused)
	}
	if p.ExpectOutOfArea {
		s.problem("the delivery quote offers delivery to %s, which the plan expects to be out of area: not placing", p.Prefix)
		return done(StatusRefused)
	}

	cart, chosen, err := s.SelectOffers(ctx, *q, p.Tier)
	if err != nil {
		if Is(err, "stale-delivery-selection") {
			return attemptRequote, nil
		}
		if _, missing := ChooseOffers(*q, p.Tier); len(missing) > 0 {
			s.problem("no delivery offer for group(s) %v: the cart cannot be checked out", missing)
		}
		return done(StatusRefused)
	}
	o.Tiers = map[int]Tier{}
	for _, c := range chosen {
		o.Tiers[c.GroupNo] = c.Tier
	}
	if r := cart.CheckoutReadiness; r != nil && !r.Ready {
		s.problem("the cart is not checkout-ready after selecting every group: %+v", *r)
		return done(StatusRefused)
	}

	sess, err := s.Checkout(ctx)
	if err != nil {
		if Is(err, "offer-not-quotable") {
			return attemptRequote, nil // the BFF's contract: refresh the cart's delivery quote
		}
		return done(StatusRefused)
	}
	o.Currency, o.TotalMinor = sess.Currency, sess.TotalMinor

	for {
		sess, err = s.Confirm(ctx, pay.token)
		if sess.SessionID != "" {
			o.Currency, o.TotalMinor = sess.Currency, sess.TotalMinor
			o.AuthorizationID = sess.AuthorizationID
		}
		switch {
		case ctx.Err() != nil:
			return attemptDone, ctx.Err()
		case errors.Is(err, ErrInDoubt):
			o.InDoubtState = sess.State
			s.problem("outcome in doubt: session %s still %s after %s", s.SessionID, orUnknown(sess.State), s.opt.PollTimeout)
			return done(StatusInDoubt)
		case Is(err, "price-quote-expired"):
			return attemptRequote, nil
		case err != nil:
			return done(StatusRefused)
		}
		switch sess.State {
		case StateCompleted:
			return s.placed(ctx, p, o, sess)
		case StateDeclined:
			o.Declines++
			if p.FallbackToken != "" && !pay.fallbackUsed {
				pay.token, pay.fallbackUsed = p.FallbackToken, true
				continue
			}
			return done(StatusDeclined)
		case StateRequoteRequired:
			return attemptRequote, nil
		default:
			o.FailureCode = sess.FailureCode
			s.problem("checkout session %s ended %s (failureCode %q)", sess.SessionID, sess.State, sess.FailureCode)
			return done(StatusRefused)
		}
	}
}

func orUnknown(s string) string {
	if s == "" {
		return "in an unknown state"
	}
	return s
}

func describeUnavailable(us []Unavailable) string {
	if len(us) == 0 {
		return "no reason given"
	}
	parts := make([]string, 0, len(us))
	for _, u := range us {
		parts = append(parts, strings.TrimSpace(string(u.Tier)+" "+u.Reason()))
	}
	return strings.Join(parts, ", ")
}

// placed reads the order a COMPLETED session made, checks it against the plan, looks it up in the
// history and cancels it when the plan says so.
func (s *Shopper) placed(ctx context.Context, p Plan, o *Outcome, sess Session) (attempt, error) {
	o.OrderID = sess.OrderID
	if sess.OrderID == "" {
		s.problem("checkout session %s is COMPLETED but carries no orderId", sess.SessionID)
		o.Status = StatusRefused
		return attemptDone, ctx.Err()
	}
	o.Status = StatusPlaced

	ord, err := s.Order(ctx, sess.OrderID)
	if ctx.Err() != nil {
		return attemptDone, ctx.Err()
	}
	if err == nil {
		o.OrderStatus = ord.Status
		o.PromiseWindows = windows(ord)
		s.checkOrder(p, sess, ord)
	}
	if list, err := s.Orders(ctx); err == nil {
		for _, x := range list {
			o.Listed = o.Listed || x.OrderID == sess.OrderID
		}
		if !o.Listed {
			s.problem("order %s is not in the shopper's order history (%d orders)", sess.OrderID, len(list))
		}
	} else if ctx.Err() != nil {
		return attemptDone, ctx.Err()
	}

	if p.CancelAfterPlacement {
		s.cancelFlow(ctx, p, o)
	}
	return attemptDone, ctx.Err()
}

func windows(ord Order) []PromiseWindow {
	out := make([]PromiseWindow, 0, len(ord.Groups))
	for _, g := range ord.Groups {
		out = append(out, PromiseWindow{
			GroupNo: g.GroupNo, Tier: g.Tier, ShipBy: g.ShipBy, DeliverByEarliest: g.DeliverByEarliest,
			DeliverByLatest: g.DeliverByLatest, TimeZone: g.TimeZone, Status: g.Status,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].GroupNo < out[j].GroupNo })
	return out
}

// checkOrder compares the order with what the shopper asked for and paid.
func (s *Shopper) checkOrder(p Plan, sess Session, ord Order) {
	if ord.OrderID != sess.OrderID {
		s.problem("GET /orders/%s answered order %s", sess.OrderID, ord.OrderID)
	}
	if ord.ShopperID != s.ShopperID {
		s.problem("order %s belongs to shopper %q, not %q", ord.OrderID, ord.ShopperID, s.ShopperID)
	}
	if ord.DestinationPostalPrefix != p.Prefix {
		s.problem("order %s ships to prefix %q, the plan said %q", ord.OrderID, ord.DestinationPostalPrefix, p.Prefix)
	}
	want, got := map[string]int{}, map[string]int{}
	for _, l := range p.Lines {
		want[l.CatalogItemID] += l.Qty
	}
	for _, l := range ord.Lines {
		got[l.CatalogItemID] += l.Quantity
	}
	if fmt.Sprint(want) != fmt.Sprint(got) {
		s.problem("order %s has lines %v, the plan asked for %v", ord.OrderID, got, want)
	}
	if len(ord.Groups) != len(sess.Groups) {
		s.problem("order %s has %d shipment group(s), the checkout session had %d", ord.OrderID, len(ord.Groups), len(sess.Groups))
	}
	if ord.Totals != nil && ord.Totals.TotalMinor != nil && *ord.Totals.TotalMinor != sess.TotalMinor {
		s.problem("order %s totals %d, the checkout session said %d", ord.OrderID, *ord.Totals.TotalMinor, sess.TotalMinor)
	}
}

// cancelFlow asks for the cancellation and, when it was accepted, waits (bounded) for the order to
// read CANCELLED. A refusal leaves the order placed.
func (s *Shopper) cancelFlow(ctx context.Context, p Plan, o *Outcome) {
	reason := p.CancelReason
	if reason == "" {
		reason = "changed my mind"
	}
	ord, err := s.Cancel(ctx, o.OrderID, reason)
	if err != nil {
		var rej *Rejection
		if errors.As(err, &rej) && rej.Slug() != "" {
			o.CancelRefused = rej.Slug()
		} else {
			o.CancelRefused = "failed"
		}
		return
	}
	o.Status = StatusCancelled
	deadline := time.Now().Add(s.opt.PollTimeout)
	for ord.Status != OrderCancelled && time.Now().Before(deadline) {
		if !sleep(ctx, s.opt.PollEvery) {
			break
		}
		next, err := s.Order(ctx, o.OrderID)
		if err != nil {
			if Is(err, "not-found") {
				break
			}
			continue
		}
		ord = next
	}
	o.OrderStatus = ord.Status
	o.CancelSettled = ord.Status == OrderCancelled
	o.PromiseWindows = windows(ord)
}
