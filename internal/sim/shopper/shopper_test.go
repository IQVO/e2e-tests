package shopper

import (
	"context"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/warehouse-e2e/internal/sim"
)

// newShopper builds a shopper against the fake with timings short enough for a test.
func newShopper(t *testing.T, f *fakeBFF, opt Options) (*Shopper, *sim.CallStats) {
	t.Helper()
	return newShopperAt(t, f.base(), opt)
}

func newShopperAt(t *testing.T, base string, opt Options) (*Shopper, *sim.CallStats) {
	t.Helper()
	stats := sim.NewCallStats()
	if opt.PollEvery == 0 {
		opt.PollEvery = 2 * time.Millisecond
	}
	if opt.PollTimeout == 0 {
		opt.PollTimeout = 150 * time.Millisecond
	}
	if opt.RetryDelay == 0 {
		opt.RetryDelay = time.Millisecond
	}
	s, err := New(NewClient(base, stats), opt)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s, stats
}

func ctx(t *testing.T) context.Context {
	t.Helper()
	c, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	return c
}

func TestNewRefusesASetupBug(t *testing.T) {
	if _, err := New(nil, Options{}); err == nil {
		t.Error("a nil client must be refused")
	}
	if _, err := New(sim.NewClient(map[string]string{}, nil), Options{}); err == nil || !strings.Contains(err.Error(), EnvBaseURL) {
		t.Errorf("a client without the storefront must name %s, got %v", EnvBaseURL, err)
	}
	if _, err := New(NewClient("not a url", nil), Options{}); err == nil {
		t.Error("a base URL without scheme and host must be refused")
	}
}

func TestBaseURLFromEnv(t *testing.T) {
	t.Setenv(EnvBaseURL, "")
	if got := BaseURLFromEnv(); got != DefaultBaseURL {
		t.Errorf("unset: got %q, want the default %q", got, DefaultBaseURL)
	}
	t.Setenv(EnvBaseURL, " http://localhost:8000/api/storefront/ ")
	if got := BaseURLFromEnv(); got != "http://localhost:8000/api/storefront" {
		t.Errorf("set: got %q", got)
	}
}

func TestRegisterSetsTheCookieAndTheIDs(t *testing.T) {
	f := newFakeBFF()
	defer f.close()
	s, stats := newShopper(t, f, Options{Email: "ada@example.com"})

	a, err := s.Register(ctx(t))
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if a.ShopperID == "" || s.ShopperID != a.ShopperID {
		t.Fatalf("shopper id not adopted: account %+v, shopper %q", a, s.ShopperID)
	}
	if st := s.State(); st.ShopperID != a.ShopperID {
		t.Errorf("the sf_state cookie carries shopper %q, want %q", st.ShopperID, a.ShopperID)
	}
	found := false
	for _, l := range stats.Lines() {
		found = found || l.Route == "storefront POST /shoppers" && l.Calls == 1
	}
	if !found {
		t.Errorf("the call did not reach the shared statistics: %+v", stats.Lines())
	}
}

func TestEveryShopperHasItsOwnJar(t *testing.T) {
	f := newFakeBFF()
	defer f.close()
	parent := NewClient(f.base(), nil)
	a, _ := New(parent, Options{Email: "a@example.com", RetryDelay: time.Millisecond})
	b, _ := New(parent, Options{Email: "b@example.com", RetryDelay: time.Millisecond})
	if _, err := a.Register(ctx(t)); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Register(ctx(t)); err != nil {
		t.Fatal(err)
	}
	if a.State().ShopperID == b.State().ShopperID || a.State().ShopperID == "" {
		t.Errorf("two shoppers share a cookie: %+v vs %+v", a.State(), b.State())
	}
	if a.ShopperID == b.ShopperID {
		t.Errorf("two shoppers got one id %q", a.ShopperID)
	}
}

func TestShopperRoutesNeedTheCookie(t *testing.T) {
	f := newFakeBFF()
	defer f.close()
	// the shared client has no jar: it is the bare browser that never got a cookie
	bare := NewClient(f.base(), nil)
	r := bare.Call(ctx(t), Service, http.MethodPost, "POST /me/addresses", "/me/addresses", AddressFor("787", ""))
	if r.Status != 422 || !strings.Contains(string(r.Body), "session-required") {
		t.Fatalf("without the cookie: got %d %s, want 422 session-required", r.Status, r.Body)
	}

	s, _ := newShopper(t, f, Options{})
	if _, err := s.AddAddress(ctx(t), AddressFor("787", "")); !Is(err, "session-required") {
		t.Fatalf("before Register: got %v, want a session-required rejection", err)
	}
	if _, err := s.Register(ctx(t)); err != nil {
		t.Fatal(err)
	}
	sa, err := s.AddAddress(ctx(t), AddressFor("787", ""))
	if err != nil {
		t.Fatalf("after Register: %v", err)
	}
	if sa.AddressID == "" || s.AddressID != sa.AddressID || sa.Prefix() != "787" {
		t.Errorf("address not adopted: %+v / %q", sa, s.AddressID)
	}
	if len(s.Findings()) != 1 {
		t.Errorf("the rejected step must be exactly one finding, got %v", s.Findings())
	}
}

func TestCookieSurvivesAnEdgeWithoutThePathPrefix(t *testing.T) {
	f := newFakeBFF()
	defer f.close()
	// the BFF also answers without /api/storefront (behind a path-stripping proxy); it still scopes
	// the cookie to Path=/api/storefront, which a plain jar would never send back to /me/addresses
	s, _ := newShopperAt(t, f.srv.URL, Options{})
	if _, err := s.Register(ctx(t)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AddAddress(ctx(t), AddressFor("786", "")); err != nil {
		t.Fatalf("the cookie was not sent back: %v", err)
	}
}

func TestRegisteringTwiceIsAFindingAndSignInRecovers(t *testing.T) {
	f := newFakeBFF()
	defer f.close()
	first, _ := newShopper(t, f, Options{Email: "dup@example.com"})
	if _, err := first.Register(ctx(t)); err != nil {
		t.Fatal(err)
	}
	if _, err := first.AddAddress(ctx(t), AddressFor("787", "")); err != nil {
		t.Fatal(err)
	}

	second, _ := newShopper(t, f, Options{Email: "DUP@example.com"})
	_, err := second.Register(ctx(t))
	if !Is(err, "shopper-exists") {
		t.Fatalf("got %v, want shopper-exists", err)
	}
	a, err := second.SignIn(ctx(t))
	if err != nil {
		t.Fatalf("SignIn: %v", err)
	}
	if a.ShopperID != first.ShopperID || second.ShopperID != first.ShopperID {
		t.Errorf("sign-in found shopper %q, want %q", a.ShopperID, first.ShopperID)
	}
	if second.AddressID != first.AddressID || second.State().ShopperID != first.ShopperID {
		t.Errorf("sign-in did not bring the saved address and the cookie: %q %+v", second.AddressID, second.State())
	}
	if _, err := newUnknown(t, f).SignIn(ctx(t)); !Is(err, "not-found") {
		t.Errorf("an unknown e-mail must be a not-found rejection, got %v", err)
	}
}

func newUnknown(t *testing.T, f *fakeBFF) *Shopper {
	s, _ := newShopper(t, f, Options{Email: "nobody@example.com"})
	return s
}

func TestRegisterWithSavedAddresses(t *testing.T) {
	f := newFakeBFF()
	defer f.close()
	s, _ := newShopper(t, f, Options{})
	a, err := s.Register(ctx(t), AddressFor("786", "Home"), AddressFor("787", "Work"))
	if err != nil {
		t.Fatal(err)
	}
	if len(a.SavedAddresses) != 2 {
		t.Fatalf("saved %d addresses, want 2", len(a.SavedAddresses))
	}
	home, found := s.addressFor("786")
	if !found || home.Name != "Home" {
		t.Errorf("address for 786: %+v %v", home, found)
	}
	if _, found := s.addressFor("606"); found {
		t.Error("no address for 606 was saved")
	}
}

// ---- the contract the fake enforces (so the shopper's tests mean something) ----

func TestFakeEnforcesTheIdempotencyContract(t *testing.T) {
	f := newFakeBFF()
	defer f.close()
	c := NewClient(f.base(), nil)
	post := func(key, email string) sim.Resp {
		return c.CallWith(ctx(t), Service, http.MethodPost, "POST /shoppers", "/shoppers",
			map[string]string{"email": email}, map[string]string{"Idempotency-Key": key})
	}

	if r := post("", "k0@example.com"); r.Status != 400 || !strings.Contains(string(r.Body), "idempotency-key-required") {
		t.Errorf("no key: got %d %s", r.Status, r.Body)
	}
	if r := post(strings.Repeat("k", 129), "k0@example.com"); r.Status != 400 || !strings.Contains(string(r.Body), "idempotency-key-invalid") {
		t.Errorf("129-character key: got %d %s", r.Status, r.Body)
	}
	first := post("key-1", "k1@example.com")
	if first.Status != 201 {
		t.Fatalf("first: %d %s", first.Status, first.Body)
	}
	replay := post("key-1", "k1@example.com")
	if replay.Status != 201 || string(replay.Body) != string(first.Body) {
		t.Errorf("a repeat must replay the stored answer: got %d %s", replay.Status, replay.Body)
	}
	if f.replays != 1 || len(f.shoppers) != 1 {
		t.Errorf("replays %d, shoppers %d: want 1 replay and still one shopper", f.replays, len(f.shoppers))
	}
	if r := post("key-1", "other@example.com"); r.Status != 409 || !strings.Contains(string(r.Body), "idempotency-key-reused") {
		t.Errorf("same key, different body: got %d %s, want 409 idempotency-key-reused", r.Status, r.Body)
	}
	if len(f.shoppers) != 1 {
		t.Errorf("the refused request must not register anyone, have %d shoppers", len(f.shoppers))
	}
	// a 5xx is never stored: the same key retries the real work
	f.addFault("POST /shoppers", faultBefore, 503, 1)
	if r := post("key-2", "k2@example.com"); r.Status != 503 {
		t.Fatalf("injected fault: %d", r.Status)
	}
	if r := post("key-2", "k2@example.com"); r.Status != 201 || f.replays != 1 {
		t.Errorf("retry after a 5xx: got %d (replays %d), want a fresh 201", r.Status, f.replays)
	}
}

func TestEveryPostOfTheShopperCarriesAFreshKey(t *testing.T) {
	f := newFakeBFF()
	defer f.close()
	s, _ := newShopper(t, f, Options{})
	if _, err := s.Register(ctx(t)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := s.AddAddress(ctx(t), AddressFor("787", "")); err != nil {
			t.Fatal(err)
		}
	}
	// register + three addresses = four logical POSTs, four distinct keys, no replay
	if len(f.keysSeen) != 4 || f.replays != 0 {
		t.Errorf("keys seen %d, replays %d: want 4 fresh keys and no replay", len(f.keysSeen), f.replays)
	}
}

// ---- browsing ----

func TestBrowseSearchAndProductPage(t *testing.T) {
	f := newFakeBFF()
	defer f.close()
	f.pageCap = 3
	s, _ := newShopper(t, f, Options{})

	page1, err := s.Browse(ctx(t), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(page1) != 3 {
		t.Errorf("a page is capped by the provider: got %d items, want 3", len(page1))
	}
	hits, err := s.Search(ctx(t), "product 6", 10)
	if err != nil || len(hits) != 1 || hits[0].CatalogItemID != "sku-006" {
		t.Fatalf("search: %v %+v", err, hits)
	}
	p, err := s.Product(ctx(t), "sku-002")
	if err != nil || p.Price == nil || p.Price.AmountMinor != 1200 || !p.Purchasable {
		t.Fatalf("product page: %v %+v", err, p)
	}
	if _, err := s.Product(ctx(t), "sku-999"); !Is(err, "not-found") {
		t.Errorf("unknown product: got %v, want not-found", err)
	}
	if _, err := s.Search(ctx(t), "", 501); !Is(err, "invalid-limit") {
		t.Errorf("limit 501: got %v, want invalid-limit", err)
	}
}

func TestQuoteDeliveryOnTheProductPage(t *testing.T) {
	f := newFakeBFF()
	defer f.close()
	s, _ := newShopper(t, f, Options{})

	d, err := s.QuoteDelivery(ctx(t), "sku-001", "787")
	if err != nil || d.NotServed() || len(d.Tiers) != 2 || d.Tiers[0].TimeZone != "America/Chicago" {
		t.Fatalf("served prefix: %v %+v", err, d)
	}
	out, err := s.QuoteDelivery(ctx(t), "sku-001", "606")
	if err != nil || !out.NotServed() || len(out.Tiers) != 0 {
		t.Fatalf("a prefix outside 786/787 must quote DESTINATION_NOT_SERVED and no tier: %v %+v", err, out)
	}
	if _, err := s.QuoteDelivery(ctx(t), "sku-001", "78701"); !Is(err, "validation-failed") {
		t.Errorf("a 5-character prefix: got %v, want validation-failed (the destination is a 3-character prefix)", err)
	}
	if _, err := s.QuoteDelivery(ctx(t), "sku-404", "787"); !Is(err, "not-found") {
		t.Errorf("unknown product: got %v", err)
	}
}

// ---- the catalogue ----

func TestCatalogueFollowsEveryPageAndKeepsThePurchasable(t *testing.T) {
	f := newFakeBFF()
	defer f.close()
	f.pageCap = 2 // 7 products in 4 pages
	s, _ := newShopper(t, f, Options{})

	cat, err := s.Catalogue(ctx(t))
	if err != nil {
		t.Fatal(err)
	}
	if got := f.count("GET /products"); got != 4 {
		t.Errorf("%d list requests, want 4 pages", got)
	}
	if cat.Len() != 6 {
		t.Fatalf("%d purchasable products, want 6 (sku-004 is not purchasable)", cat.Len())
	}
	if _, found := cat.Get("sku-004"); found {
		t.Error("an unpurchasable product is on the shelf")
	}
	if p, found := cat.Get("sku-005"); !found || p.Price == nil || p.Price.AmountMinor != 1500 {
		t.Errorf("sku-005: %+v %v", p, found)
	}
}

func TestCatalogueFailureIsAnErrorNotAnEmptyShelf(t *testing.T) {
	f := newFakeBFF()
	defer f.close()
	f.addFault("GET /products", faultBefore, 500, -1)
	s, _ := newShopper(t, f, Options{})
	if _, err := s.Catalogue(ctx(t)); err == nil || !strings.Contains(err.Error(), "500") {
		t.Errorf("got %v, want the 500 named", err)
	}
}

func ids(ps []Product) []string {
	out := make([]string, len(ps))
	for i, p := range ps {
		out[i] = p.CatalogItemID
	}
	return out
}

func TestPickIsDeterministicAndIndependentOfListingOrder(t *testing.T) {
	f := newFakeBFF()
	defer f.close()
	s, _ := newShopper(t, f, Options{})
	forward, err := s.Catalogue(ctx(t))
	if err != nil {
		t.Fatal(err)
	}
	f.reverseList = true
	backward, err := s.Catalogue(ctx(t))
	if err != nil {
		t.Fatal(err)
	}

	a, b := forward.Pick(3, 42), backward.Pick(3, 42)
	if !slices.Equal(ids(a), ids(b)) {
		t.Errorf("the shelf depends on the listing order: %v vs %v", ids(a), ids(b))
	}
	if !slices.Equal(ids(a), ids(forward.Pick(3, 42))) {
		t.Error("the same seed must give the same shelf")
	}
	if len(a) != 3 || !slices.IsSorted(ids(a)) {
		t.Errorf("shelf %v: want 3 distinct ids in order", ids(a))
	}
	seen := map[string]bool{}
	for _, id := range ids(a) {
		if seen[id] || id == "sku-004" {
			t.Errorf("shelf %v repeats a product or holds the unpurchasable one", ids(a))
		}
		seen[id] = true
	}
	differs := false
	for seed := int64(1); seed <= 20 && !differs; seed++ {
		differs = !slices.Equal(ids(forward.Pick(3, seed)), ids(a))
	}
	if !differs {
		t.Error("twenty seeds all gave the same shelf: the seed is ignored")
	}
	if all := forward.Pick(99, 1); len(all) != forward.Len() {
		t.Errorf("n above the set returns the whole set, got %d", len(all))
	}
	if forward.Pick(0, 1) != nil || NewCatalogue(nil).Pick(3, 1) != nil {
		t.Error("an empty pick must be nil")
	}
}
