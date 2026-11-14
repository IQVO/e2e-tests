package shopper

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/claudioed/warehouse-e2e/internal/paging"
)

// NewCart opens an empty cart (POST /carts). The BFF stores its id in the sf_state cookie and adopts
// the cookie's shopper.
func (s *Shopper) NewCart(ctx context.Context) (Cart, error) {
	var c Cart
	rp := s.call(ctx, "new-cart", http.MethodPost, "POST /carts", "/carts", nil)
	if err := s.decode("POST /carts", rp, &c); err != nil {
		return Cart{}, err
	}
	s.CartID = c.CartID
	return c, nil
}

func (s *Shopper) cartPath(suffix string) string {
	return "/carts/" + url.PathEscape(s.CartID) + suffix
}

// Cart reads the shopper's cart (GET /carts/{id}).
func (s *Shopper) Cart(ctx context.Context) (Cart, error) {
	var c Cart
	rp := s.call(ctx, "cart", http.MethodGet, "GET /carts/{id}", s.cartPath(""), nil)
	if err := s.decode("GET /carts/{id}", rp, &c); err != nil {
		return Cart{}, err
	}
	return c, nil
}

// AddLine adds a line (POST /carts/{id}/lines {catalogItemId, quantity}); an item already in the
// cart merges its quantity. It clears the cart's delivery quote.
func (s *Shopper) AddLine(ctx context.Context, l Line) (Cart, error) {
	var c Cart
	rp := s.call(ctx, "add-line", http.MethodPost, "POST /carts/{id}/lines", s.cartPath("/lines"), l)
	if err := s.decode("POST /carts/{id}/lines", rp, &c); err != nil {
		return Cart{}, err
	}
	return c, nil
}

// SetQuantity replaces the quantity of a line (PATCH /carts/{id}/lines/{item}).
func (s *Shopper) SetQuantity(ctx context.Context, item string, qty int) (Cart, error) {
	var c Cart
	rp := s.call(ctx, "set-quantity", http.MethodPatch, "PATCH /carts/{id}/lines/{item}",
		s.cartPath("/lines/"+url.PathEscape(item)), map[string]int{"quantity": qty})
	if err := s.decode("PATCH /carts/{id}/lines/{item}", rp, &c); err != nil {
		return Cart{}, err
	}
	return c, nil
}

// SetDestination sets the 3-character postal prefix the cart ships to
// (PUT /carts/{id}/destination). It clears the cart's delivery quote.
func (s *Shopper) SetDestination(ctx context.Context, prefix string) (Cart, error) {
	var c Cart
	rp := s.call(ctx, "set-destination", http.MethodPut, "PUT /carts/{id}/destination", s.cartPath("/destination"),
		map[string]string{"destinationPostalPrefix": prefix})
	if err := s.decode("PUT /carts/{id}/destination", rp, &c); err != nil {
		return Cart{}, err
	}
	return c, nil
}

// QuoteCart asks for the cart's delivery offers (POST /carts/{id}/delivery-quotes). Each offer
// expires: a quote older than its expiresAt cannot be checked out.
func (s *Shopper) QuoteCart(ctx context.Context) (Cart, error) {
	var c Cart
	rp := s.call(ctx, "quote-cart", http.MethodPost, "POST /carts/{id}/delivery-quotes", s.cartPath("/delivery-quotes"), nil)
	if err := s.decode("POST /carts/{id}/delivery-quotes", rp, &c); err != nil {
		return Cart{}, err
	}
	return c, nil
}

// SelectDelivery picks the offer of one shipment group (PUT /carts/{id}/delivery-selection
// {groupNo, offerId}). An offer that is not in the cart's current quote is a stale-delivery-selection
// rejection.
func (s *Shopper) SelectDelivery(ctx context.Context, groupNo int, offerID string) (Cart, error) {
	var c Cart
	rp := s.call(ctx, "select-delivery", http.MethodPut, "PUT /carts/{id}/delivery-selection", s.cartPath("/delivery-selection"),
		Selection{GroupNo: groupNo, OfferID: offerID})
	if err := s.decode("PUT /carts/{id}/delivery-selection", rp, &c); err != nil {
		return Cart{}, err
	}
	return c, nil
}

// ChooseOffers picks one offer per shipment group: the one of the preferred tier when the group
// has it, otherwise the cheapest. The quote carries no price, so "cheapest" is the slowest tier
// (STANDARD < TWO_DAY < NEXT_DAY < SAME_DAY), the order of every charge schedule the storefront
// provisions. missing lists the groups that have no offer at all.
func ChooseOffers(q Quote, prefer Tier) (chosen []Offer, missing []int) {
	for _, g := range q.Groups {
		var best, exact *Offer
		for i := range g.Offers {
			o := &g.Offers[i]
			if prefer != "" && o.Tier == prefer && exact == nil {
				exact = o
			}
			if best == nil || tierRank(o.Tier) < tierRank(best.Tier) {
				best = o
			}
		}
		if exact != nil {
			best = exact
		}
		if best == nil {
			missing = append(missing, g.GroupNo)
			continue
		}
		o := *best
		o.GroupNo = g.GroupNo
		chosen = append(chosen, o)
	}
	return chosen, missing
}

// SelectOffers selects the offer ChooseOffers picks for every group of the cart's current quote and
// returns the cart after the last selection together with the choices. A group without any offer
// is an error: the cart could not be checked out.
func (s *Shopper) SelectOffers(ctx context.Context, q Quote, prefer Tier) (Cart, []Offer, error) {
	chosen, missing := ChooseOffers(q, prefer)
	if len(missing) > 0 {
		return Cart{}, nil, fmt.Errorf("shopper: no delivery offer for group(s) %v", missing)
	}
	var cart Cart
	for _, o := range chosen {
		var err error
		if cart, err = s.SelectDelivery(ctx, o.GroupNo, o.OfferID); err != nil {
			return Cart{}, chosen, err
		}
	}
	return cart, chosen, nil
}

// Checkout opens a checkout session for the cart, shipping to the saved address (POST /checkout
// {cartId, shipToAddressId}). The session id lands in the sf_state cookie.
func (s *Shopper) Checkout(ctx context.Context) (Session, error) {
	var sess Session
	rp := s.call(ctx, "checkout", http.MethodPost, "POST /checkout", "/checkout",
		map[string]string{"cartId": s.CartID, "shipToAddressId": s.AddressID})
	if err := s.decode("POST /checkout", rp, &sess); err != nil {
		return Session{}, err
	}
	s.SessionID = sess.SessionID
	return sess, nil
}

// CheckoutSession reads a checkout session (GET /checkout/{id}); the page polls it while a
// placement is in doubt.
func (s *Shopper) CheckoutSession(ctx context.Context, id string) (Session, error) {
	var sess Session
	rp := s.call(ctx, "poll-checkout", http.MethodGet, "GET /checkout/{id}", "/checkout/"+url.PathEscape(id), nil)
	if err := s.decode("GET /checkout/{id}", rp, &sess); err != nil {
		return Session{}, err
	}
	return sess, nil
}

func inDoubt(state string) bool { return state == StateAuthorizing || state == StatePlacing }

// Confirm pays the session with a payment-method token
// (POST /checkout/{id}/confirmations {paymentMethodToken}) and returns the session once the
// outcome is definitive: 200 answers COMPLETED, DECLINED, REQUOTE_REQUIRED or FAILED; a 202
// (AUTHORIZING or PLACING) is polled with GET /checkout/{id} every PollEvery for at most
// PollTimeout, after which Confirm returns the last session it saw and ErrInDoubt. It never waits
// longer: tok_timeout leaves the session AUTHORIZING with no way out (journey gap G8).
//
// An answer that leaves the outcome unknown (a 504, a 5xx after the retries, a transport error)
// is polled the same way: the confirm may have taken effect. Only when the session is still OPEN
// afterwards is the original error returned. A 409 placement-in-doubt or illegal-transition (the
// session is already PLACING or AUTHORIZING) is polled too: it is never paid a second time.
func (s *Shopper) Confirm(ctx context.Context, token string) (Session, error) {
	id := s.SessionID
	if id == "" {
		return Session{}, errors.New("shopper: Confirm before Checkout: no session")
	}
	rp := s.call(ctx, "confirm", http.MethodPost, "POST /checkout/{id}/confirmations",
		"/checkout/"+url.PathEscape(id)+"/confirmations", map[string]string{"paymentMethodToken": token})
	const route = "POST /checkout/{id}/confirmations"
	if rp.Status == http.StatusOK || rp.Status == http.StatusAccepted {
		var sess Session
		if err := s.decode(route, rp, &sess); err != nil {
			return Session{}, err
		}
		return s.await(ctx, sess)
	}
	if rp.Status == 0 || rp.Status >= 500 {
		orig := rejection(route, rp.Resp)
		if ctx.Err() != nil {
			return Session{}, ctx.Err()
		}
		sess, err := s.await(ctx, Session{SessionID: id})
		if sess.State == StateOpen {
			return sess, orig
		}
		return sess, err
	}
	rej := rejection(route, rp.Resp)
	if rej.Slug() == "placement-in-doubt" || rej.Slug() == "illegal-transition" {
		// the session is PLACING or AUTHORIZING already (an earlier confirm took effect): the outcome
		// is still unknown, so wait for it instead of paying twice
		return s.await(ctx, Session{SessionID: id})
	}
	return Session{}, rej
}

// await polls the session while its outcome is in doubt, bounded by PollTimeout. A session whose
// state is not known yet (state "") is read first.
func (s *Shopper) await(ctx context.Context, sess Session) (Session, error) {
	id := sess.SessionID
	if id == "" {
		id = s.SessionID
	}
	deadline := time.Now().Add(s.opt.PollTimeout)
	known := sess.State != ""
	for {
		if known && !inDoubt(sess.State) {
			return sess, nil
		}
		if !time.Now().Before(deadline) {
			return sess, ErrInDoubt
		}
		if known && !sleep(ctx, s.opt.PollEvery) {
			return sess, ctx.Err()
		}
		next, err := s.CheckoutSession(ctx, id)
		switch {
		case err == nil:
			sess, known = next, true
		case ctx.Err() != nil:
			return sess, ctx.Err()
		case Is(err, "not-found"):
			return sess, err
		default:
			// a transient failure: the finding is recorded; wait and read again
			if !known && !sleep(ctx, s.opt.PollEvery) {
				return sess, ctx.Err()
			}
		}
	}
}

// Order reads an order with its tracking and, when the cookie's last checkout placed it, the
// checkout totals (GET /orders/{id}).
func (s *Shopper) Order(ctx context.Context, id string) (Order, error) {
	var o Order
	rp := s.call(ctx, "order", http.MethodGet, "GET /orders/{id}", "/orders/"+url.PathEscape(id), nil)
	if err := s.decode("GET /orders/{id}", rp, &o); err != nil {
		return Order{}, err
	}
	return o, nil
}

// Orders reads every page of the shopper's order history (GET /orders, {items, nextCursor}).
func (s *Shopper) Orders(ctx context.Context) ([]Order, error) {
	var all []Order
	cursor := ""
	for pages := 0; pages < 10000; pages++ {
		rp := s.call(ctx, "orders", http.MethodGet, "GET /orders", paging.With("/orders", cursor), nil)
		if !rp.OK() {
			return all, rejection("GET /orders", rp.Resp)
		}
		items, next, err := paging.Decode[Order](rp.Body)
		if err != nil {
			rej := &Rejection{Route: "GET /orders", Status: rp.Status, Detail: "contract violation: " + err.Error()}
			s.addFinding(rej.Error())
			return all, rej
		}
		all = append(all, items...)
		if next == "" || next == cursor {
			return all, nil
		}
		cursor = next
	}
	return all, errors.New("shopper: the order list did not end")
}

// Cancel asks for the order's cancellation (POST /orders/{id}/cancellations {reason}). It is a
// request: every group still PROMISED becomes CANCELLATION_REQUESTED and the order CANCELLED once
// order-sourcing agrees. Once every group is locked to a node it is a cancel-after-lock rejection.
func (s *Shopper) Cancel(ctx context.Context, orderID, reason string) (Order, error) {
	var o Order
	rp := s.call(ctx, "cancel", http.MethodPost, "POST /orders/{id}/cancellations",
		"/orders/"+url.PathEscape(orderID)+"/cancellations", map[string]string{"reason": reason})
	if err := s.decode("POST /orders/{id}/cancellations", rp, &o); err != nil {
		return Order{}, err
	}
	return o, nil
}
