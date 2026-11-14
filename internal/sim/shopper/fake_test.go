package shopper

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// fakeBFF is a contract fake of the storefront BFF and the five contexts behind it, served as one
// handler under /api/storefront and, like the real BFF behind a path-stripping proxy, without the
// prefix. It ENFORCES the published invariants a shopper must respect, independently of the
// shopper's own checks (the pattern of cmd/seed-storefront's fake):
//
//   - every POST and PATCH needs an Idempotency-Key of 1..128 visible ASCII characters (400
//     idempotency-key-required / idempotency-key-invalid); the same key with the same request
//     replays the stored answer with Idempotency-Replayed: true; the same key with a different
//     request is 409 idempotency-key-reused; a 5xx is never stored; PUT and GET carry no key;
//   - the sf_state cookie (Path=/api/storefront, value = url-escaped c=&s=&k=) is state: the
//     shopper routes (/me/addresses, POST /checkout, GET /orders) answer 422 session-required
//     without a shopper in it, and the BFF rewrites it on /shoppers, /sessions, /carts, /checkout;
//   - a destination is a 3-character alphanumeric prefix (422 validation-failed), 786 and 787 are
//     served (anything else quotes DESTINATION_NOT_SERVED and no offer);
//   - lists answer {items, nextCursor} and take limit (1..500, else 400 invalid-limit) and cursor;
//     an undeclared query parameter is 400 unknown-query-parameter;
//   - delivery offers expire (the fake clock moves with advance), a selection must name an offer of
//     the cart's CURRENT quote (409 stale-delivery-selection), a checkout opens only on a ready
//     cart whose offers are valid for 2 more minutes (409 cart-not-checkout-ready, 422
//     offer-not-quotable), a confirm needs a valid price quote (422 price-quote-expired);
//   - tokens: tok_approve completes, tok_decline DECLINES (and may be re-confirmed while the quote is
//     valid), tok_timeout answers 202 and the session stays AUTHORIZING for ever (journey gap G8),
//     tok_slow answers 202 PLACING and completes after `slowPolls` reads, tok_requote answers
//     REQUOTE_REQUIRED (a CLOSED session) while `requotes` lasts and approves afterwards; a card
//     number as a token is 422;
//   - an order is placed once per checkout session (the placement key is the session id);
//   - a cancellation is a request: groups still PROMISED become CANCELLATION_REQUESTED (and the
//     order CANCELLED after `settleReads` reads); once every group is locked it is 409
//     cancel-after-lock.
type fakeBFF struct {
	mu  sync.Mutex
	srv *httptest.Server

	clock time.Time

	// knobs
	pageCap     int             // the most items a list returns, whatever the limit
	served      map[string]bool // postal prefixes the network serves
	nextDay     bool            // quote NEXT_DAY too (the real tier exists only before 11:00 CT)
	quoteTTL    time.Duration   // how long an offer and a price quote live
	requotes    int             // tok_requote answers REQUOTE_REQUIRED this many times
	slowPolls   int             // GET /checkout/{id} reads before a tok_slow session completes
	settleReads int             // GET /orders/{id} reads before a cancellation request settles
	reverseList bool            // list products in reverse id order (the order is not part of the contract)
	emptyGroups bool            // a not-served quote has no group at all (otherwise one group with no offer)
	faults      []*fault
	priceEpoch  int64 // grows on every requote: delivery charges change with it

	// state
	seq        map[string]int
	products   []fakeProduct
	shoppers   map[string]*fakeShopper
	byEmail    map[string]string
	carts      map[string]*fakeCart
	sessions   map[string]*fakeSession
	orders     map[string]*fakeOrder
	byKey      map[string]fakeStored
	seen       []string // "METHOD /templated/route" of every request, replays included
	placements int      // orders created
	replays    int      // answers replayed from the idempotency store
	posts      int      // POST and PATCH received
	keysSeen   map[string]int
}

type fakeProduct struct {
	ID, Title   string
	PriceMinor  int64
	Node        string // products of one node ship together; two nodes make two groups
	Purchasable bool
}

type fakeShopper struct {
	ID, Email string
	Addresses []map[string]any
}

type fakeStored struct {
	fp    string
	reply fakeReply
}

type fakeReply struct {
	status int
	body   any
	set    *statePatch
}

type statePatch struct{ c, s, k *string }

type faultKind int

const (
	faultBefore faultKind = iota // answer status before the command runs (nothing stored)
	faultDrop                    // run and store the command, then cut the connection: a lost answer
)

type fault struct {
	route  string
	kind   faultKind
	status int
	n      int // times left; -1 is for ever
}

type fakeCall struct {
	w      http.ResponseWriter
	r      *http.Request
	body   []byte
	params []string
	st     fakeState // the cookie as sent
	q      url.Values
}

type fakeState struct{ c, s, k string }

type fakeRoute struct {
	method, tmpl string
	re           *regexp.Regexp
	keyed        bool
	h            func(f *fakeBFF, c *fakeCall) fakeReply
}

const fakeBase = "/api/storefront"

func newFakeBFF() *fakeBFF {
	f := &fakeBFF{
		clock:    time.Date(2026, 11, 14, 15, 0, 0, 0, time.UTC),
		pageCap:  500,
		served:   map[string]bool{"786": true, "787": true},
		quoteTTL: 30 * time.Minute,
		seq:      map[string]int{},
		shoppers: map[string]*fakeShopper{}, byEmail: map[string]string{},
		carts: map[string]*fakeCart{}, sessions: map[string]*fakeSession{}, orders: map[string]*fakeOrder{},
		byKey: map[string]fakeStored{}, keysSeen: map[string]int{},
	}
	for i := 1; i <= 7; i++ {
		node := "n1"
		if i > 4 {
			node = "n2"
		}
		f.products = append(f.products, fakeProduct{
			ID: fmt.Sprintf("sku-%03d", i), Title: fmt.Sprintf("Product %d", i),
			PriceMinor: int64(1000 + 100*i), Node: node, Purchasable: i != 4,
		})
	}
	f.srv = httptest.NewServer(f)
	return f
}

func (f *fakeBFF) close() { f.srv.Close() }

// base is the URL a shopper is configured with (STOREFRONT_BASE_URL).
func (f *fakeBFF) base() string { return f.srv.URL + fakeBase }

func (f *fakeBFF) advance(d time.Duration) {
	f.mu.Lock()
	f.clock = f.clock.Add(d)
	f.mu.Unlock()
}

func (f *fakeBFF) addFault(route string, kind faultKind, status, n int) {
	f.mu.Lock()
	f.faults = append(f.faults, &fault{route: route, kind: kind, status: status, n: n})
	f.mu.Unlock()
}

func (f *fakeBFF) takeFault(route string, kind faultKind) *fault {
	for _, ft := range f.faults {
		if ft.route == route && ft.kind == kind && ft.n != 0 {
			if ft.n > 0 {
				ft.n--
			}
			return ft
		}
	}
	return nil
}

// counts returns how many times each route was called.
func (f *fakeBFF) count(route string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, s := range f.seen {
		if s == route {
			n++
		}
	}
	return n
}

func (f *fakeBFF) next(prefix string) string {
	f.seq[prefix]++
	return fmt.Sprintf("%s-%d", prefix, f.seq[prefix])
}

// ---- wire helpers ----

func fakeProblem(status int, slug, detail string) fakeReply {
	return fakeReply{status: status, body: map[string]any{
		"type": "/problems/" + slug, "title": slug, "status": status, "detail": detail,
	}}
}

func okJSON(body any) fakeReply { return fakeReply{status: http.StatusOK, body: body} }

func (f *fakeBFF) write(w http.ResponseWriter, rep fakeReply, st fakeState, replayed bool) {
	if rep.set != nil {
		if rep.set.c != nil {
			st.c = *rep.set.c
		}
		if rep.set.s != nil {
			st.s = *rep.set.s
		}
		if rep.set.k != nil {
			st.k = *rep.set.k
		}
		v := url.Values{}
		for k, id := range map[string]string{"c": st.c, "s": st.s, "k": st.k} {
			if id != "" {
				v.Set(k, id)
			}
		}
		http.SetCookie(w, &http.Cookie{
			Name: "sf_state", Value: url.QueryEscape(v.Encode()), Path: fakeBase,
			HttpOnly: true, SameSite: http.SameSiteLaxMode, MaxAge: 30 * 24 * 3600,
		})
	}
	if replayed {
		w.Header().Set("Idempotency-Replayed", "true")
	}
	if rep.status >= 400 {
		w.Header().Set("Content-Type", "application/problem+json")
	} else {
		w.Header().Set("Content-Type", "application/json")
	}
	w.WriteHeader(rep.status)
	_ = json.NewEncoder(w).Encode(rep.body)
}

func cookieState(r *http.Request) fakeState {
	ck, err := r.Cookie("sf_state")
	if err != nil {
		return fakeState{}
	}
	raw, err := url.QueryUnescape(ck.Value)
	if err != nil {
		return fakeState{}
	}
	v, err := url.ParseQuery(raw)
	if err != nil {
		return fakeState{}
	}
	return fakeState{c: v.Get("c"), s: v.Get("s"), k: v.Get("k")}
}

var validKey = regexp.MustCompile(`^[\x21-\x7e]{1,128}$`)

// ---- routing ----

var fakeRoutes []fakeRoute

func addRoute(method, tmpl string, keyed bool, h func(f *fakeBFF, c *fakeCall) fakeReply) {
	re := regexp.MustCompile("^" + regexp.MustCompile(`\{[a-z]+\}`).ReplaceAllString(tmpl, `([A-Za-z0-9._-]+)`) + "$")
	fakeRoutes = append(fakeRoutes, fakeRoute{method: method, tmpl: tmpl, re: re, keyed: keyed, h: h})
}

func init() {
	addRoute(http.MethodPost, "/shoppers", true, (*fakeBFF).hRegister)
	addRoute(http.MethodPost, "/sessions", true, (*fakeBFF).hSignIn)
	addRoute(http.MethodPost, "/me/addresses", true, (*fakeBFF).hAddAddress)
	addRoute(http.MethodGet, "/me", false, (*fakeBFF).hMe)
	addRoute(http.MethodGet, "/products", false, (*fakeBFF).hProducts)
	addRoute(http.MethodGet, "/products/{id}", false, (*fakeBFF).hProduct)
	addRoute(http.MethodPost, "/products/{id}/delivery-date-quotes", true, (*fakeBFF).hDateQuote)
	registerCommerceRoutes()
}

func (f *fakeBFF) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	p := strings.TrimPrefix(r.URL.Path, fakeBase)
	if p == "" {
		p = "/"
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	var rt *fakeRoute
	var params []string
	pathKnown := false
	for i := range fakeRoutes {
		m := fakeRoutes[i].re.FindStringSubmatch(p)
		if m == nil {
			continue
		}
		pathKnown = true
		if fakeRoutes[i].method == r.Method {
			rt, params = &fakeRoutes[i], m[1:]
			break
		}
	}
	st := cookieState(r)
	if rt == nil {
		f.seen = append(f.seen, r.Method+" "+p)
		if pathKnown {
			f.write(w, fakeProblem(http.StatusMethodNotAllowed, "method-not-allowed", r.Method), st, false)
			return
		}
		f.write(w, fakeProblem(http.StatusNotFound, "resource-not-found", p), st, false)
		return
	}
	f.seen = append(f.seen, r.Method+" "+rt.tmpl)
	c := &fakeCall{w: w, r: r, body: body, params: params, st: st, q: r.URL.Query()}

	key := ""
	if rt.keyed {
		f.posts++
		vals := r.Header.Values("Idempotency-Key")
		switch {
		case len(vals) == 0 || vals[0] == "":
			f.write(w, fakeProblem(400, "idempotency-key-required", "send an Idempotency-Key header"), st, false)
			return
		case len(vals) > 1 || !validKey.MatchString(vals[0]):
			f.write(w, fakeProblem(400, "idempotency-key-invalid", "1 to 128 visible ASCII characters"), st, false)
			return
		}
		key = vals[0]
		f.keysSeen[key]++
		fp := r.Method + " " + p + " " + string(body)
		if e, found := f.byKey[key]; found {
			if e.fp != fp {
				f.write(w, fakeProblem(409, "idempotency-key-reused", "the Idempotency-Key was already used with a different request"), st, false)
				return
			}
			f.replays++
			f.write(w, e.reply, st, true)
			return
		}
	}
	routeName := rt.method + " " + rt.tmpl
	if ft := f.takeFault(routeName, faultBefore); ft != nil {
		f.write(w, fakeProblem(ft.status, "upstream-unavailable", "injected"), st, false)
		return
	}
	rep := rt.h(f, c)
	if rt.keyed && rep.status < 500 {
		f.byKey[key] = fakeStored{fp: r.Method + " " + p + " " + string(body), reply: rep}
	}
	if ft := f.takeFault(routeName, faultDrop); ft != nil {
		if hj, okHJ := w.(http.Hijacker); okHJ {
			if conn, _, err := hj.Hijack(); err == nil {
				_ = conn.Close()
				return
			}
		}
	}
	f.write(w, rep, st, false)
}

// ---- accounts ----

func decodeBody(c *fakeCall, into any) bool {
	dec := json.NewDecoder(strings.NewReader(string(c.body)))
	return dec.Decode(into) == nil
}

func (f *fakeBFF) shopperJSON(s *fakeShopper) map[string]any {
	addrs := make([]map[string]any, len(s.Addresses))
	copy(addrs, s.Addresses)
	return map[string]any{
		"shopperId": s.ID, "email": s.Email, "status": "ACTIVE",
		"registeredAt": f.clock.Format(time.RFC3339), "savedAddresses": addrs,
	}
}

func validShipTo(a map[string]any) bool {
	for _, k := range []string{"name", "line1", "city", "region", "postalCode", "countryCode"} {
		if v, _ := a[k].(string); v == "" {
			return false
		}
	}
	cc, _ := a["countryCode"].(string)
	return len(cc) == 2
}

func (f *fakeBFF) mintAddress(a map[string]any, key string) map[string]any {
	sum := sha256.Sum256([]byte(key + fmt.Sprint(a["line1"])))
	out := map[string]any{"addressId": "addr-" + hex.EncodeToString(sum[:])[:8]}
	for k, v := range a {
		out[k] = v
	}
	return out
}

func (f *fakeBFF) hRegister(c *fakeCall) fakeReply {
	var in struct {
		Email          string           `json:"email"`
		SavedAddresses []map[string]any `json:"savedAddresses"`
	}
	if !decodeBody(c, &in) || !strings.Contains(in.Email, "@") || len(in.Email) > 254 {
		return fakeProblem(422, "validation-failed", "email")
	}
	email := strings.ToLower(strings.TrimSpace(in.Email))
	if _, taken := f.byEmail[email]; taken {
		return fakeProblem(409, "shopper-exists", "the e-mail is registered")
	}
	s := &fakeShopper{ID: f.next("shopper"), Email: in.Email}
	for _, a := range in.SavedAddresses {
		if !validShipTo(a) {
			return fakeProblem(422, "validation-failed", "savedAddresses")
		}
		s.Addresses = append(s.Addresses, f.mintAddress(a, c.r.Header.Get("Idempotency-Key")+s.ID))
	}
	f.shoppers[s.ID], f.byEmail[email] = s, s.ID
	return fakeReply{status: 201, body: f.shopperJSON(s), set: &statePatch{s: &s.ID}}
}

func (f *fakeBFF) hSignIn(c *fakeCall) fakeReply {
	var in map[string]any
	if !decodeBody(c, &in) || len(in) != 1 {
		return fakeProblem(422, "validation-failed", "body is exactly {email}")
	}
	email, _ := in["email"].(string)
	id, found := f.byEmail[strings.ToLower(strings.TrimSpace(email))]
	if !found {
		return fakeProblem(404, "not-found", "no shopper with that e-mail")
	}
	return fakeReply{status: 200, body: f.shopperJSON(f.shoppers[id]), set: &statePatch{s: &id}}
}

func (f *fakeBFF) cookieShopper(c *fakeCall) (*fakeShopper, *fakeReply) {
	if c.st.s == "" {
		r := fakeProblem(422, "session-required", "no shopper in the sf_state cookie")
		return nil, &r
	}
	s := f.shoppers[c.st.s]
	if s == nil {
		r := fakeProblem(404, "not-found", "unknown shopper")
		return nil, &r
	}
	return s, nil
}

func (f *fakeBFF) hMe(c *fakeCall) fakeReply {
	s, bad := f.cookieShopper(c)
	if bad != nil {
		return *bad
	}
	return okJSON(f.shopperJSON(s))
}

func (f *fakeBFF) hAddAddress(c *fakeCall) fakeReply {
	s, bad := f.cookieShopper(c)
	if bad != nil {
		return *bad
	}
	var a map[string]any
	if !decodeBody(c, &a) || !validShipTo(a) {
		return fakeProblem(422, "validation-failed", "address")
	}
	if len(s.Addresses) >= 20 {
		return fakeProblem(422, "too-many-addresses", "20 saved addresses")
	}
	sa := f.mintAddress(a, c.r.Header.Get("Idempotency-Key"))
	s.Addresses = append(s.Addresses, sa)
	return fakeReply{status: 201, body: sa}
}

// ---- catalogue ----

func (f *fakeBFF) productHit(p fakeProduct) map[string]any {
	h := map[string]any{
		"catalogItemId": p.ID, "title": p.Title, "inStock": p.Purchasable, "purchasable": p.Purchasable,
		"categoryId": "cat-" + p.Node,
	}
	if p.Purchasable {
		h["effectivePrice"] = map[string]any{"amountMinor": p.PriceMinor, "currency": "USD"}
	}
	return h
}

func (f *fakeBFF) product(id string) (fakeProduct, bool) {
	for _, p := range f.products {
		if p.ID == id {
			return p, true
		}
	}
	return fakeProduct{}, false
}

// pageParams validates the list parameters like the BFF and returns the offset and page size.
func (f *fakeBFF) pageParams(c *fakeCall, allowed ...string) (offset, size int, bad *fakeReply) {
	known := map[string]bool{"limit": true, "cursor": true}
	for _, a := range allowed {
		known[a] = true
	}
	for k := range c.q {
		if !known[k] {
			r := fakeProblem(400, "unknown-query-parameter", "unknown query parameter "+k)
			return 0, 0, &r
		}
	}
	size = 100
	if l := c.q.Get("limit"); l != "" || c.q.Has("limit") {
		n, err := strconv.Atoi(l)
		if err != nil || n < 1 || n > 500 {
			r := fakeProblem(400, "invalid-limit", "limit is an integer 1-500")
			return 0, 0, &r
		}
		size = n
	}
	if cur := c.q.Get("cursor"); cur != "" {
		n, err := strconv.Atoi(strings.TrimPrefix(cur, "c-"))
		if err != nil || !strings.HasPrefix(cur, "c-") || n < 0 {
			r := fakeProblem(400, "invalid-cursor", "not a cursor of this provider")
			return 0, 0, &r
		}
		offset = n
	}
	return offset, min(size, f.pageCap), nil
}

func page(items []any, offset, size int) map[string]any {
	if offset > len(items) {
		offset = len(items)
	}
	end := min(offset+size, len(items))
	out := map[string]any{"items": append([]any{}, items[offset:end]...)}
	if end < len(items) {
		out["nextCursor"] = fmt.Sprintf("c-%d", end)
	}
	return out
}

func (f *fakeBFF) hProducts(c *fakeCall) fakeReply {
	offset, size, bad := f.pageParams(c, "q", "categoryId", "inStock", "priceMin", "priceMax", "sort")
	if bad != nil {
		return *bad
	}
	q := strings.ToLower(c.q.Get("q"))
	ps := append([]fakeProduct(nil), f.products...)
	sort.Slice(ps, func(i, j int) bool { return (ps[i].ID < ps[j].ID) != f.reverseList })
	var items []any
	for _, p := range ps {
		if q == "" || strings.Contains(strings.ToLower(p.Title), q) {
			items = append(items, f.productHit(p))
		}
	}
	out := page(items, offset, size)
	out["facets"] = []any{}
	return okJSON(out)
}

func (f *fakeBFF) hProduct(c *fakeCall) fakeReply {
	p, found := f.product(c.params[0])
	if !found {
		return fakeProblem(404, "not-found", "unknown product")
	}
	return okJSON(f.productHit(p))
}

func (f *fakeBFF) isServed(prefix string) bool { return f.served[prefix] }

var prefixPattern = regexp.MustCompile(`^[A-Za-z0-9]{3}$`)

func (f *fakeBFF) hDateQuote(c *fakeCall) fakeReply {
	if _, found := f.product(c.params[0]); !found {
		return fakeProblem(404, "not-found", "unknown product")
	}
	var in struct {
		PostalPrefix string `json:"postalPrefix"`
	}
	if !decodeBody(c, &in) || !prefixPattern.MatchString(in.PostalPrefix) {
		return fakeProblem(422, "validation-failed", "postalPrefix is a 3-character prefix")
	}
	out := map[string]any{"postalPrefix": in.PostalPrefix, "tiers": []any{}, "unavailable": []any{}}
	if !f.isServed(in.PostalPrefix) {
		var un []any
		for _, t := range f.tiers() {
			un = append(un, map[string]any{"tier": t, "constraint": "DESTINATION_NOT_SERVED"})
		}
		out["unavailable"] = un
		return okJSON(out)
	}
	var tiers []any
	for _, t := range f.tiers() {
		tiers = append(tiers, map[string]any{
			"tier": t, "deliverByEarliest": f.clock.Add(tierDays(t) * 24 * time.Hour).Format(time.RFC3339),
			"deliverByLatest": f.clock.Add((tierDays(t) + 1) * 24 * time.Hour).Format(time.RFC3339),
			"timeZone":        "America/Chicago", "offerExpiresAt": f.clock.Add(f.quoteTTL).Format(time.RFC3339),
			"charge": map[string]any{"amountMinor": f.charge(t), "currency": "USD"},
		})
	}
	out["tiers"] = tiers
	return okJSON(out)
}

// tiers are the tiers this network quotes, cheapest first.
func (f *fakeBFF) tiers() []string {
	t := []string{"STANDARD", "TWO_DAY"}
	if f.nextDay {
		t = append(t, "NEXT_DAY")
	}
	return t
}

func tierDays(t string) time.Duration {
	switch t {
	case "STANDARD":
		return 4
	case "TWO_DAY":
		return 2
	}
	return 1
}

// charge is the delivery charge of one group in the tier; it moves with every requote.
func (f *fakeBFF) charge(t string) int64 {
	base := map[string]int64{"STANDARD": 499, "TWO_DAY": 899, "NEXT_DAY": 1499}[t]
	return base + 50*f.priceEpoch
}
