package shopper

import (
	"fmt"
	"net/http"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"
)

// The commerce half of the fake: carts and their delivery quotes (shopping-cart, delivery-promise),
// checkout sessions and their payment tokens (checkout, customer-payments), orders and
// cancellations (customer-ordering). See fakeBFF for the invariants it enforces.

type fakeLine struct {
	Item string
	Qty  int
}

type fakeOffer struct {
	ID                       string
	GroupNo                  int
	Tier                     string
	Earliest, Latest, Expiry time.Time
	Charge                   int64
}

type fakeGroup struct {
	No      int
	LineNos []int
	Offers  []fakeOffer
}

type fakeQuote struct {
	No        int
	Groups    []fakeGroup
	QuotedAt  time.Time
	NotServed bool
}

type fakeCart struct {
	ID, ShopperID, State string
	Lines                []fakeLine
	Dest                 string
	Quote                *fakeQuote
	Selections           map[int]string // group -> offer id
	Version              int
	Quotes               int
}

type fakeSessGroup struct {
	No      int
	Offer   fakeOffer
	LineNos []int
	Amount  int64
}

type fakeSession struct {
	ID, State, CartID, ShopperID, AddressID string
	Lines                                   []fakeLine
	Groups                                  []fakeSessGroup
	Total                                   int64
	QuoteExpires                            time.Time
	AuthID, OrderID, FailureCode            string
	Requoted                                []fakeOffer
	PollsLeft                               int
	Version                                 int
}

type fakeOrderGroup struct {
	No                 int
	PromiseID, Tier    string
	ShipBy, Earliest   time.Time
	Latest             time.Time
	LineNos            []int
	Status, CancelNote string
}

type fakeOrder struct {
	ID, ShopperID, SessionID, Dest string
	PlacedAt                       time.Time
	CancelledAt                    time.Time
	Lines                          []fakeLine
	Groups                         []*fakeOrderGroup
	Status                         string
	CancelReadsLeft                int
	Total                          int64
}

func registerCommerceRoutes() {
	addRoute(http.MethodPost, "/carts", true, (*fakeBFF).hNewCart)
	addRoute(http.MethodGet, "/carts/{id}", false, (*fakeBFF).hGetCart)
	addRoute(http.MethodPost, "/carts/{id}/lines", true, (*fakeBFF).hAddLine)
	addRoute(http.MethodPatch, "/carts/{id}/lines/{item}", true, (*fakeBFF).hSetQty)
	addRoute(http.MethodPut, "/carts/{id}/destination", false, (*fakeBFF).hDestination)
	addRoute(http.MethodPost, "/carts/{id}/delivery-quotes", true, (*fakeBFF).hCartQuote)
	addRoute(http.MethodPut, "/carts/{id}/delivery-selection", false, (*fakeBFF).hSelect)
	addRoute(http.MethodPost, "/checkout", true, (*fakeBFF).hOpenCheckout)
	addRoute(http.MethodGet, "/checkout/{id}", false, (*fakeBFF).hGetCheckout)
	addRoute(http.MethodPost, "/checkout/{id}/confirmations", true, (*fakeBFF).hConfirm)
	addRoute(http.MethodGet, "/orders", false, (*fakeBFF).hListOrders)
	addRoute(http.MethodGet, "/orders/{id}", false, (*fakeBFF).hGetOrder)
	addRoute(http.MethodPost, "/orders/{id}/cancellations", true, (*fakeBFF).hCancel)
}

// ---- carts ----

func rfc(t time.Time) string { return t.UTC().Format(time.RFC3339) }
func day(t time.Time) string { return t.UTC().Format("2006-01-02") }

func (f *fakeBFF) cart(id string) (*fakeCart, *fakeReply) {
	c := f.carts[id]
	if c == nil {
		r := fakeProblem(404, "not-found", "unknown cart")
		return nil, &r
	}
	return c, nil
}

func (f *fakeBFF) open(c *fakeCart) (*fakeCart, *fakeReply) {
	if c.State != "OPEN" {
		r := fakeProblem(409, "cart-closed", "the cart is "+c.State)
		return nil, &r
	}
	return c, nil
}

func offerJSON(o fakeOffer) map[string]any {
	return map[string]any{
		"offerId": o.ID, "groupNo": o.GroupNo, "tier": o.Tier,
		"deliverByEarliest": day(o.Earliest), "deliverByLatest": day(o.Latest), "expiresAt": rfc(o.Expiry),
	}
}

func (f *fakeBFF) selectedOffer(c *fakeCart, group int) (fakeOffer, bool) {
	id, found := c.Selections[group]
	if !found || c.Quote == nil {
		return fakeOffer{}, false
	}
	for _, g := range c.Quote.Groups {
		if g.No != group {
			continue
		}
		for _, o := range g.Offers {
			if o.ID == id {
				return o, true
			}
		}
	}
	return fakeOffer{}, false
}

func (f *fakeBFF) itemsTotal(lines []fakeLine) int64 {
	var sum int64
	for _, l := range lines {
		p, _ := f.product(l.Item)
		sum += p.PriceMinor * int64(l.Qty)
	}
	return sum
}

func (f *fakeBFF) cartJSON(c *fakeCart) map[string]any {
	lines := make([]any, 0, len(c.Lines))
	for _, l := range c.Lines {
		lines = append(lines, map[string]any{"catalogItemId": l.Item, "quantity": l.Qty})
	}
	var dest any
	if c.Dest != "" {
		dest = c.Dest
	}
	var shopper any
	if c.ShopperID != "" {
		shopper = c.ShopperID
	}
	sels := make([]any, 0)
	groupNos := make([]int, 0, len(c.Selections))
	for g := range c.Selections {
		groupNos = append(groupNos, g)
	}
	sort.Ints(groupNos)
	for _, g := range groupNos {
		sels = append(sels, map[string]any{"groupNo": g, "offerId": c.Selections[g]})
	}
	var delivery int64
	allSelected, noExpired, quotedGroups := true, true, 0
	if c.Quote != nil {
		for _, g := range c.Quote.Groups {
			if len(g.Offers) == 0 {
				continue
			}
			quotedGroups++
			o, picked := f.selectedOffer(c, g.No)
			if !picked {
				allSelected = false
				continue
			}
			delivery += o.Charge
			if !o.Expiry.After(f.clock) {
				noExpired = false
			}
		}
	}
	allSelected = allSelected && quotedGroups > 0
	items := f.itemsTotal(c.Lines)
	out := map[string]any{
		"cartId": c.ID, "shopperId": shopper, "state": c.State, "lines": lines, "destinationPostalPrefix": dest,
		"selections": sels,
		"estimate": map[string]any{
			"currency": "USD", "itemsMinor": items, "deliveryMinor": delivery, "taxMinor": 0,
			"totalMinor": items + delivery, "estimateComplete": c.Dest != "" && allSelected,
		},
		"checkoutReadiness": map[string]any{
			"ready": allSelected && noExpired && len(c.Lines) > 0, "allGroupsSelected": allSelected, "noOfferExpired": noExpired,
		},
		"version": c.Version,
	}
	if c.Quote != nil {
		groups := make([]any, 0, len(c.Quote.Groups))
		for _, g := range c.Quote.Groups {
			offers := make([]any, 0, len(g.Offers))
			for _, o := range g.Offers {
				offers = append(offers, offerJSON(o))
			}
			groups = append(groups, map[string]any{"groupNo": g.No, "lineNos": g.LineNos, "offers": offers})
		}
		unavailable, unavailableTiers := []any{}, []any{}
		if c.Quote.NotServed {
			for _, t := range f.tiers() {
				unavailable = append(unavailable, map[string]any{"tier": t, "constraint": "DESTINATION_NOT_SERVED"})
				unavailableTiers = append(unavailableTiers, t)
			}
		}
		out["deliveryQuote"] = map[string]any{
			"groups": groups, "unavailableTiers": unavailableTiers, "unavailable": unavailable, "quotedAt": rfc(c.Quote.QuotedAt),
		}
	}
	return out
}

func (f *fakeBFF) hNewCart(c *fakeCall) fakeReply {
	shopper := c.st.s
	if len(c.body) > 0 {
		var in struct {
			ShopperID string `json:"shopperId"`
		}
		if !decodeBody(c, &in) {
			return fakeProblem(400, "malformed-request", "body")
		}
		if in.ShopperID != "" {
			shopper = in.ShopperID
		}
	}
	cart := &fakeCart{ID: f.next("cart"), ShopperID: shopper, State: "OPEN", Selections: map[int]string{}, Version: 1}
	f.carts[cart.ID] = cart
	return fakeReply{status: 201, body: f.cartJSON(cart), set: &statePatch{c: &cart.ID}}
}

func (f *fakeBFF) hGetCart(c *fakeCall) fakeReply {
	cart, bad := f.cart(c.params[0])
	if bad != nil {
		return *bad
	}
	return okJSON(f.cartJSON(cart))
}

func (f *fakeBFF) changed(c *fakeCart) fakeReply {
	c.Version++
	return okJSON(f.cartJSON(c))
}

func (f *fakeBFF) resetQuote(c *fakeCart) {
	c.Quote = nil
	c.Selections = map[int]string{}
}

var itemPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,63}$`)

func (f *fakeBFF) hAddLine(c *fakeCall) fakeReply {
	cart, bad := f.cart(c.params[0])
	if bad != nil {
		return *bad
	}
	if _, bad := f.open(cart); bad != nil {
		return *bad
	}
	var in fakeLineIn
	if !decodeBody(c, &in) || !itemPattern.MatchString(in.CatalogItemID) {
		return fakeProblem(422, "validation-failed", "catalogItemId")
	}
	if in.Quantity < 1 || in.Quantity > 99 {
		return fakeProblem(422, "quantity-out-of-range", "1 to 99")
	}
	p, found := f.product(in.CatalogItemID)
	if !found || !p.Purchasable {
		return fakeProblem(422, "item-not-purchasable", in.CatalogItemID)
	}
	merged := false
	for i := range cart.Lines {
		if cart.Lines[i].Item == in.CatalogItemID {
			cart.Lines[i].Qty += in.Quantity
			merged = true
		}
	}
	if !merged {
		cart.Lines = append(cart.Lines, fakeLine{Item: in.CatalogItemID, Qty: in.Quantity})
	}
	f.resetQuote(cart)
	return f.changed(cart)
}

type fakeLineIn struct {
	CatalogItemID string `json:"catalogItemId"`
	Quantity      int    `json:"quantity"`
}

func (f *fakeBFF) hSetQty(c *fakeCall) fakeReply {
	cart, bad := f.cart(c.params[0])
	if bad != nil {
		return *bad
	}
	if _, bad := f.open(cart); bad != nil {
		return *bad
	}
	var in struct {
		Quantity int `json:"quantity"`
	}
	if !decodeBody(c, &in) {
		return fakeProblem(400, "malformed-request", "body")
	}
	if in.Quantity < 1 || in.Quantity > 99 {
		return fakeProblem(422, "quantity-out-of-range", "1 to 99")
	}
	for i := range cart.Lines {
		if cart.Lines[i].Item == c.params[1] {
			cart.Lines[i].Qty = in.Quantity
			f.resetQuote(cart)
			return f.changed(cart)
		}
	}
	return fakeProblem(404, "not-found", "no such line")
}

func (f *fakeBFF) hDestination(c *fakeCall) fakeReply {
	cart, bad := f.cart(c.params[0])
	if bad != nil {
		return *bad
	}
	if _, bad := f.open(cart); bad != nil {
		return *bad
	}
	var in struct {
		Prefix string `json:"destinationPostalPrefix"`
	}
	if !decodeBody(c, &in) || !prefixPattern.MatchString(in.Prefix) {
		return fakeProblem(422, "validation-failed", "destinationPostalPrefix is a 3-character prefix")
	}
	cart.Dest = in.Prefix
	f.resetQuote(cart)
	return f.changed(cart)
}

func (f *fakeBFF) hCartQuote(c *fakeCall) fakeReply {
	cart, bad := f.cart(c.params[0])
	if bad != nil {
		return *bad
	}
	if _, bad := f.open(cart); bad != nil {
		return *bad
	}
	if cart.Dest == "" {
		return fakeProblem(422, "no-destination", "set the destination first")
	}
	if len(cart.Lines) == 0 {
		return fakeProblem(422, "validation-failed", "the cart is empty")
	}
	cart.Quotes++
	q := &fakeQuote{No: cart.Quotes, QuotedAt: f.clock, NotServed: !f.isServed(cart.Dest)}
	ttl := f.quoteTTL
	if f.firstQuoteTTL > 0 && cart.Quotes == 1 {
		ttl = f.firstQuoteTTL
	}
	byNode := map[string][]int{}
	for i, l := range cart.Lines {
		p, _ := f.product(l.Item)
		byNode[p.Node] = append(byNode[p.Node], i+1)
	}
	nodes := make([]string, 0, len(byNode))
	for n := range byNode {
		nodes = append(nodes, n)
	}
	sort.Strings(nodes)
	switch {
	case q.NotServed && f.emptyGroups:
	default:
		for gi, n := range nodes {
			g := fakeGroup{No: gi + 1, LineNos: byNode[n]}
			if !q.NotServed {
				for _, t := range f.tiers() {
					g.Offers = append(g.Offers, fakeOffer{
						ID:      fmt.Sprintf("of-%s-q%d-g%d-%s", cart.ID, q.No, g.No, strings.ToLower(t)),
						GroupNo: g.No, Tier: t, Charge: f.charge(t),
						Earliest: f.clock.Add(tierDays(t) * 24 * time.Hour), Latest: f.clock.Add((tierDays(t) + 1) * 24 * time.Hour),
						Expiry: f.clock.Add(ttl),
					})
				}
			}
			q.Groups = append(q.Groups, g)
		}
	}
	cart.Quote, cart.Selections = q, map[int]string{}
	return f.changed(cart)
}

func (f *fakeBFF) hSelect(c *fakeCall) fakeReply {
	cart, bad := f.cart(c.params[0])
	if bad != nil {
		return *bad
	}
	if _, bad := f.open(cart); bad != nil {
		return *bad
	}
	var in struct {
		GroupNo int    `json:"groupNo"`
		OfferID string `json:"offerId"`
	}
	if !decodeBody(c, &in) {
		return fakeProblem(400, "malformed-request", "body")
	}
	if cart.Quote != nil {
		for _, g := range cart.Quote.Groups {
			if g.No != in.GroupNo {
				continue
			}
			for _, o := range g.Offers {
				if o.ID == in.OfferID {
					cart.Selections[g.No] = o.ID
					return f.changed(cart)
				}
			}
		}
	}
	return fakeProblem(409, "stale-delivery-selection", "the offer is not in the cart's current quote")
}

// ---- checkout ----

func sessionJSON(s *fakeSession) map[string]any {
	lines := make([]any, 0, len(s.Lines))
	for i, l := range s.Lines {
		lines = append(lines, map[string]any{"lineNo": i + 1, "catalogItemId": l.Item, "quantity": l.Qty})
	}
	groups := make([]any, 0, len(s.Groups))
	for _, g := range s.Groups {
		groups = append(groups, map[string]any{
			"groupNo": g.No, "offerId": g.Offer.ID, "tier": g.Offer.Tier, "lineNos": g.LineNos, "amountMinor": g.Amount,
		})
	}
	var auth, order any
	if s.AuthID != "" {
		auth = s.AuthID
	}
	if s.OrderID != "" {
		order = s.OrderID
	}
	var failure any
	if s.FailureCode != "" {
		failure = s.FailureCode
	}
	out := map[string]any{
		"sessionId": s.ID, "state": s.State, "cartId": s.CartID, "cartVersion": 1, "shopperId": s.ShopperID,
		"shipToAddressId": s.AddressID, "lines": lines, "groups": groups, "priceQuoteId": "pq-" + s.ID,
		"currency": "USD", "totalMinor": s.Total, "quoteExpiresAt": rfc(s.QuoteExpires),
		"authorizationId": auth, "orderId": order, "failureCode": failure, "version": s.Version,
	}
	if s.State == "REQUOTE_REQUIRED" {
		offers := make([]any, 0, len(s.Requoted))
		for _, o := range s.Requoted {
			offers = append(offers, offerJSON(o))
		}
		out["requotedOffers"] = offers
	}
	return out
}

func (f *fakeBFF) hOpenCheckout(c *fakeCall) fakeReply {
	shopper, bad := f.cookieShopper(c)
	if bad != nil {
		return *bad
	}
	var in struct {
		CartID          string `json:"cartId"`
		ShipToAddressID string `json:"shipToAddressId"`
	}
	if !decodeBody(c, &in) || in.ShipToAddressID == "" {
		return fakeProblem(422, "validation-failed", "shipToAddressId")
	}
	if in.CartID == "" {
		in.CartID = c.st.c
	}
	cart := f.carts[in.CartID]
	if cart == nil {
		return fakeProblem(422, "validation-failed", "unknown cart")
	}
	owned := false
	for _, a := range shopper.Addresses {
		owned = owned || a["addressId"] == in.ShipToAddressID
	}
	if !owned {
		return fakeProblem(422, "validation-failed", "shipToAddressId is not a saved address of the shopper")
	}
	if cart.State != "OPEN" {
		return fakeProblem(409, "cart-not-checkout-ready", "the cart is "+cart.State)
	}
	groups := 0
	if cart.Quote != nil {
		for _, g := range cart.Quote.Groups {
			if len(g.Offers) > 0 {
				groups++
			}
		}
	}
	if groups == 0 || len(cart.Selections) != groups {
		return fakeProblem(409, "cart-not-checkout-ready", "select a delivery offer for every group")
	}
	s := &fakeSession{
		ID: f.next("cs"), State: "OPEN", CartID: cart.ID, ShopperID: shopper.ID, AddressID: in.ShipToAddressID,
		Lines: slices.Clone(cart.Lines), QuoteExpires: f.clock.Add(f.quoteTTL), Version: 2,
	}
	if f.expireFirstSession && f.seq["cs"] == 1 {
		s.QuoteExpires = f.clock
	}
	s.Total = f.itemsTotal(cart.Lines)
	for _, g := range cart.Quote.Groups {
		o, picked := f.selectedOffer(cart, g.No)
		if !picked {
			return fakeProblem(409, "cart-not-checkout-ready", "select a delivery offer for every group")
		}
		if o.Expiry.Before(f.clock.Add(2 * time.Minute)) {
			return fakeProblem(422, "offer-not-quotable", o.ID+" expires within 2 minutes")
		}
		var items int64
		for _, ln := range g.LineNos {
			l := cart.Lines[ln-1]
			p, _ := f.product(l.Item)
			items += p.PriceMinor * int64(l.Qty)
		}
		s.Groups = append(s.Groups, fakeSessGroup{No: g.No, Offer: o, LineNos: g.LineNos, Amount: items + o.Charge})
		s.Total += o.Charge
	}
	f.sessions[s.ID] = s
	return fakeReply{status: 201, body: sessionJSON(s), set: &statePatch{c: &cart.ID, k: &s.ID}}
}

func (f *fakeBFF) hGetCheckout(c *fakeCall) fakeReply {
	s := f.sessions[c.params[0]]
	if s == nil {
		return fakeProblem(404, "not-found", "unknown session")
	}
	if s.State == "PLACING" && s.PollsLeft > 0 {
		s.PollsLeft--
		if s.PollsLeft == 0 {
			f.place(s)
		}
	}
	return okJSON(sessionJSON(s))
}

var cardShaped = regexp.MustCompile(`^[0-9 -]{12,}$`)

func (f *fakeBFF) hConfirm(c *fakeCall) fakeReply {
	s := f.sessions[c.params[0]]
	if s == nil {
		return fakeProblem(404, "not-found", "unknown session")
	}
	var in map[string]any
	if !decodeBody(c, &in) || len(in) != 1 {
		return fakeProblem(422, "validation-failed", "body is exactly {paymentMethodToken}")
	}
	token, _ := in["paymentMethodToken"].(string)
	if token == "" || len(token) > 256 || cardShaped.MatchString(token) {
		return fakeProblem(422, "validation-failed", "paymentMethodToken must be a PSP token, never a card number")
	}
	switch s.State {
	case "COMPLETED", "EXPIRED", "FAILED", "REQUOTE_REQUIRED":
		return fakeProblem(409, "session-closed", "the session is "+s.State)
	case "AUTHORIZING":
		return fakeProblem(409, "illegal-transition", "a confirm while AUTHORIZING")
	case "PLACING":
		return fakeProblem(409, "placement-in-doubt", "the placement outcome is unknown")
	}
	if !f.clock.Before(s.QuoteExpires) {
		s.State = "EXPIRED"
		return fakeProblem(422, "price-quote-expired", "open a new session")
	}
	s.Version++
	switch token {
	case "tok_decline":
		s.State = "DECLINED"
		return okJSON(sessionJSON(s))
	case "tok_timeout":
		s.State = "AUTHORIZING"
		return fakeReply{status: 202, body: sessionJSON(s)}
	case "tok_slow":
		s.State, s.AuthID, s.PollsLeft = "PLACING", f.next("pay"), max(f.slowPolls, 1)
		return fakeReply{status: 202, body: sessionJSON(s)}
	case "tok_requote":
		if f.requotes > 0 {
			f.requotes--
			f.priceEpoch++
			s.State = "REQUOTE_REQUIRED"
			s.Requoted = nil
			for _, g := range s.Groups {
				o := g.Offer
				o.ID, o.Charge = o.ID+"-rq", f.charge(o.Tier)
				s.Requoted = append(s.Requoted, o)
			}
			return okJSON(sessionJSON(s))
		}
	case "tok_fail":
		s.State, s.FailureCode = "FAILED", "contract-violation"
		return okJSON(sessionJSON(s))
	case "tok_approve":
	default:
		return fakeProblem(422, "validation-failed", "unknown token "+token)
	}
	s.AuthID = f.next("pay")
	f.place(s)
	return okJSON(sessionJSON(s))
}

// place creates the order of a session, once: the placement key is the session id.
func (f *fakeBFF) place(s *fakeSession) {
	if s.OrderID == "" {
		o := &fakeOrder{
			ID: f.next("ord"), ShopperID: s.ShopperID, SessionID: s.ID, PlacedAt: f.clock, Status: "PLACED",
			Lines: slices.Clone(s.Lines), Total: s.Total,
		}
		if cart := f.carts[s.CartID]; cart != nil {
			o.Dest = cart.Dest
			cart.State = "CHECKED_OUT"
		}
		for _, g := range s.Groups {
			o.Groups = append(o.Groups, &fakeOrderGroup{
				No: g.No, PromiseID: f.next("pr"), Tier: g.Offer.Tier, ShipBy: g.Offer.Earliest.Add(-24 * time.Hour),
				Earliest: g.Offer.Earliest, Latest: g.Offer.Latest, LineNos: g.LineNos, Status: "PROMISED",
			})
		}
		if f.lockOnPlace {
			for _, g := range o.Groups {
				g.Status = "LOCKED"
			}
			f.refreshStatus(o)
		}
		f.orders[o.ID] = o
		s.OrderID = o.ID
		f.placements++
	}
	s.State = "COMPLETED"
	s.Version++
}

// lock moves every PROMISED group of the order to LOCKED: the vendor accepted the purchase order.
func (f *fakeBFF) lock(orderID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	o := f.orders[orderID]
	for _, g := range o.Groups {
		if g.Status == "PROMISED" {
			g.Status = "LOCKED"
		}
	}
	f.refreshStatus(o)
}

func (f *fakeBFF) refreshStatus(o *fakeOrder) {
	allCancelled, locked := true, false
	for _, g := range o.Groups {
		allCancelled = allCancelled && g.Status == "CANCELLED"
		locked = locked || g.Status == "LOCKED"
	}
	switch {
	case allCancelled:
		o.Status = "CANCELLED"
	case locked:
		o.Status = "IN_FULFILLMENT"
	default:
		o.Status = "PLACED"
	}
}

// ---- orders ----

func orderJSON(o *fakeOrder) map[string]any {
	lines := make([]any, 0, len(o.Lines))
	for i, l := range o.Lines {
		lines = append(lines, map[string]any{"lineNo": i + 1, "catalogItemId": l.Item, "quantity": l.Qty})
	}
	groups := make([]any, 0, len(o.Groups))
	for _, g := range o.Groups {
		gj := map[string]any{
			"groupNo": g.No, "promiseId": g.PromiseID, "tier": g.Tier, "shipBy": rfc(g.ShipBy),
			"deliverByEarliest": rfc(g.Earliest), "deliverByLatest": rfc(g.Latest), "timeZone": "America/Chicago",
			"lineNos": g.LineNos, "status": g.Status,
		}
		if g.CancelNote != "" {
			gj["cancelReason"] = g.CancelNote
		}
		groups = append(groups, gj)
	}
	out := map[string]any{
		"orderId": o.ID, "shopperId": o.ShopperID, "status": o.Status, "destinationPostalPrefix": o.Dest,
		"placedAt": rfc(o.PlacedAt), "lines": lines, "groups": groups,
	}
	if !o.CancelledAt.IsZero() {
		out["cancelledAt"] = rfc(o.CancelledAt)
	}
	return out
}

func (f *fakeBFF) hGetOrder(c *fakeCall) fakeReply {
	o := f.orders[c.params[0]]
	if o == nil {
		return fakeProblem(404, "not-found", "unknown order")
	}
	if o.CancelReadsLeft > 0 {
		o.CancelReadsLeft--
		if o.CancelReadsLeft == 0 {
			for _, g := range o.Groups {
				if g.Status == "CANCELLATION_REQUESTED" {
					g.Status = "CANCELLED"
				}
			}
			f.refreshStatus(o)
			if o.Status == "CANCELLED" {
				o.CancelledAt = f.clock
			}
		}
	}
	out := orderJSON(o)
	out["tracking"] = map[string]any{"orderId": o.ID, "status": o.Status, "groups": []any{}}
	// journey gap G11: the totals are joined only when the cookie's checkout placed this order
	if c.st.k == o.SessionID {
		out["totals"] = map[string]any{"currency": "USD", "totalMinor": o.Total}
	}
	return okJSON(out)
}

func (f *fakeBFF) hListOrders(c *fakeCall) fakeReply {
	offset, size, bad := f.pageParams(c, "shopperId")
	if bad != nil {
		return *bad
	}
	shopper := c.q.Get("shopperId")
	if shopper == "" {
		shopper = c.st.s
	}
	if shopper == "" {
		return fakeProblem(422, "session-required", "no shopper")
	}
	var mine []*fakeOrder
	for _, o := range f.orders {
		if o.ShopperID == shopper {
			mine = append(mine, o)
		}
	}
	sort.Slice(mine, func(i, j int) bool { return mine[i].ID > mine[j].ID }) // newest first
	items := make([]any, 0, len(mine))
	for _, o := range mine {
		items = append(items, orderJSON(o))
	}
	return okJSON(page(items, offset, size))
}

func (f *fakeBFF) hCancel(c *fakeCall) fakeReply {
	o := f.orders[c.params[0]]
	if o == nil {
		return fakeProblem(404, "not-found", "unknown order")
	}
	var in map[string]any
	if !decodeBody(c, &in) || len(in) != 1 {
		return fakeProblem(422, "validation-failed", "body is exactly {reason}")
	}
	reason, _ := in["reason"].(string)
	if strings.TrimSpace(reason) == "" || len(reason) > 200 {
		return fakeProblem(422, "validation-failed", "reason is 1 to 200 characters")
	}
	if o.Status == "CANCELLED" {
		return okJSON(orderJSON(o))
	}
	unlocked, pending := 0, 0
	for _, g := range o.Groups {
		switch g.Status {
		case "PROMISED":
			unlocked++
		case "CANCELLATION_REQUESTED":
			pending++
		}
	}
	if unlocked == 0 && pending == 0 {
		return fakeProblem(409, "cancel-after-lock", "every shipment group is locked to a node")
	}
	for _, g := range o.Groups {
		if g.Status == "PROMISED" {
			g.Status, g.CancelNote = "CANCELLATION_REQUESTED", reason
		}
	}
	if o.CancelReadsLeft == 0 && f.settleReads >= 0 {
		o.CancelReadsLeft = max(f.settleReads, 1)
	}
	return okJSON(orderJSON(o))
}
