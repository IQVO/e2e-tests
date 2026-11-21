package shopper

import (
	"context"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// Register creates the shopper (POST /shoppers {email, savedAddresses?}). The answer sets the
// sf_state cookie, which is what every later step (a saved address, a checkout, the order list)
// relies on. A taken e-mail is a *Rejection with slug shopper-exists: SignIn instead.
func (s *Shopper) Register(ctx context.Context, saved ...Address) (Account, error) {
	body := map[string]any{"email": s.Email}
	if len(saved) > 0 {
		body["savedAddresses"] = saved
	}
	var a Account
	rp := s.call(ctx, "register", http.MethodPost, "POST /shoppers", "/shoppers", body)
	if err := s.decode("POST /shoppers", rp, &a); err != nil {
		return Account{}, err
	}
	s.adopt(a)
	return a, nil
}

// SignIn looks the shopper up by e-mail (POST /sessions {email}). There is no password: the
// cookie only remembers which shopper the browser acts for.
func (s *Shopper) SignIn(ctx context.Context) (Account, error) {
	var a Account
	rp := s.call(ctx, "sign-in", http.MethodPost, "POST /sessions", "/sessions", map[string]string{"email": s.Email})
	if err := s.decode("POST /sessions", rp, &a); err != nil {
		return Account{}, err
	}
	s.adopt(a)
	return a, nil
}

func (s *Shopper) adopt(a Account) {
	s.ShopperID = a.ShopperID
	s.addrs = append([]SavedAddress(nil), a.SavedAddresses...)
	if n := len(s.addrs); n > 0 {
		s.AddressID = s.addrs[n-1].AddressID
	}
}

// AddAddress saves an address for the cookie's shopper (POST /me/addresses) and makes it the
// address a checkout ships to.
func (s *Shopper) AddAddress(ctx context.Context, a Address) (SavedAddress, error) {
	var sa SavedAddress
	rp := s.call(ctx, "add-address", http.MethodPost, "POST /me/addresses", "/me/addresses", a)
	if err := s.decode("POST /me/addresses", rp, &sa); err != nil {
		return SavedAddress{}, err
	}
	s.addrs = append(s.addrs, sa)
	s.AddressID = sa.AddressID
	return sa, nil
}

// addressFor returns the saved address whose postal code starts with prefix, newest first.
func (s *Shopper) addressFor(prefix string) (SavedAddress, bool) {
	for i := len(s.addrs) - 1; i >= 0; i-- {
		if s.addrs[i].Prefix() == prefix {
			return s.addrs[i], true
		}
	}
	return SavedAddress{}, false
}

// AddressFor is a synthetic ship-to address whose postal code starts with prefix.
func AddressFor(prefix, name string) Address {
	if name == "" {
		name = "Sim Shopper"
	}
	return Address{
		Name: name, Line1: "1 Simulation Way", City: "Austin", Region: "TX",
		PostalCode: prefix + "01", CountryCode: "US",
	}
}

// Browse reads one page of the product list (GET /products?limit=n).
func (s *Shopper) Browse(ctx context.Context, limit int) ([]Product, error) {
	return s.Search(ctx, "", limit)
}

// Search reads one page of products matching q (GET /products?q=..&limit=n). An empty q is a plain
// listing; limit <= 0 takes the BFF default.
func (s *Shopper) Search(ctx context.Context, q string, limit int) ([]Product, error) {
	v := url.Values{}
	if q != "" {
		v.Set("q", q)
	}
	if limit > 0 {
		v.Set("limit", strconv.Itoa(limit))
	}
	path := "/products"
	if enc := v.Encode(); enc != "" {
		path += "?" + enc
	}
	var page struct {
		Items []Product `json:"items"`
	}
	rp := s.call(ctx, "search", http.MethodGet, "GET /products", path, nil)
	if err := s.decode("GET /products", rp, &page); err != nil {
		return nil, err
	}
	return page.Items, nil
}

// Product reads one product page (GET /products/{id}).
func (s *Shopper) Product(ctx context.Context, id string) (Product, error) {
	var p Product
	rp := s.call(ctx, "product", http.MethodGet, "GET /products/{id}", "/products/"+url.PathEscape(id), nil)
	if err := s.decode("GET /products/{id}", rp, &p); err != nil {
		return Product{}, err
	}
	return p, nil
}

// QuoteDelivery is the product page's delivery-date quote for one unit
// (POST /products/{id}/delivery-date-quotes {postalPrefix}). A prefix that is not served answers
// 200 with no tier and DESTINATION_NOT_SERVED among the unavailable reasons: see
// DeliveryDates.NotServed.
func (s *Shopper) QuoteDelivery(ctx context.Context, productID, prefix string) (DeliveryDates, error) {
	var d DeliveryDates
	rp := s.call(ctx, "page-quote", http.MethodPost, "POST /products/{id}/delivery-date-quotes",
		"/products/"+url.PathEscape(productID)+"/delivery-date-quotes", map[string]string{"postalPrefix": prefix})
	if err := s.decode("POST /products/{id}/delivery-date-quotes", rp, &d); err != nil {
		return DeliveryDates{}, err
	}
	return d, nil
}

// validPrefix reports whether p is a 3-character alphanumeric postal prefix.
func validPrefix(p string) bool {
	if len(p) != prefixLen {
		return false
	}
	return strings.IndexFunc(p, func(r rune) bool {
		alnum := r >= '0' && r <= '9' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z'
		return !alnum
	}) < 0
}
