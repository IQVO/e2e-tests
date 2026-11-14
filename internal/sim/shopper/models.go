package shopper

import (
	"encoding/json"
	"strings"
)

// Tier is a delivery service tier as the storefront quotes it.
type Tier string

// The tiers, cheapest (slowest) first. The cart quote carries no price, only a tier, so
// "cheapest" is read from this order (see tierRank).
const (
	TierStandard Tier = "STANDARD"
	TierTwoDay   Tier = "TWO_DAY"
	TierNextDay  Tier = "NEXT_DAY"
	TierSameDay  Tier = "SAME_DAY"
)

// tierRank orders tiers by assumed price, cheapest first. An unknown tier sorts last.
func tierRank(t Tier) int {
	switch t {
	case TierStandard:
		return 0
	case TierTwoDay:
		return 1
	case TierNextDay:
		return 2
	case TierSameDay:
		return 3
	}
	return 4
}

// Checkout session states (the checkout context's SessionState).
const (
	StateOpen            = "OPEN"
	StateAuthorizing     = "AUTHORIZING"
	StateDeclined        = "DECLINED"
	StatePlacing         = "PLACING"
	StateRequoteRequired = "REQUOTE_REQUIRED"
	StateCompleted       = "COMPLETED"
	StateExpired         = "EXPIRED"
	StateFailed          = "FAILED"
)

// Order and group states the shopper reads (customer-ordering's OrderStatus and GroupStatus).
const (
	OrderCancelled     = "CANCELLED"
	GroupPromised      = "PROMISED"
	GroupCancelRequest = "CANCELLATION_REQUESTED"
)

// Payment tokens the payment provider simulator understands.
const (
	TokenApprove = "tok_approve" // authorised, a 7-day hold
	TokenDecline = "tok_decline" // DECLINED; another token may be tried while the quote is valid
	TokenTimeout = "tok_timeout" // provider timeout: today the session stays AUTHORIZING (journey gap G8)
)

// Money is an amount in minor units.
type Money struct {
	AmountMinor int64  `json:"amountMinor"`
	Currency    string `json:"currency"`
}

// Address is a synthetic ship-to address (customer-ordering's ShipTo).
type Address struct {
	Name        string `json:"name"`
	Line1       string `json:"line1"`
	Line2       string `json:"line2,omitempty"`
	City        string `json:"city"`
	Region      string `json:"region"`
	PostalCode  string `json:"postalCode"`
	CountryCode string `json:"countryCode"`
	Phone       string `json:"phone,omitempty"`
}

// SavedAddress is an Address the shopper saved; AddressID is what a checkout names.
type SavedAddress struct {
	AddressID string `json:"addressId"`
	Address
}

// Prefix is the 3-character postal prefix of the address.
func (a Address) Prefix() string {
	if len(a.PostalCode) < prefixLen {
		return a.PostalCode
	}
	return a.PostalCode[:prefixLen]
}

// Account is a registered shopper (customer-ordering's Shopper).
type Account struct {
	ShopperID      string         `json:"shopperId"`
	Email          string         `json:"email"`
	Status         string         `json:"status"`
	RegisteredAt   string         `json:"registeredAt"`
	SavedAddresses []SavedAddress `json:"savedAddresses"`
}

// Product is a product hit or view (product-discovery). Price is the effective price.
type Product struct {
	CatalogItemID string `json:"catalogItemId"`
	Title         string `json:"title"`
	Price         *Money `json:"effectivePrice,omitempty"`
	CategoryID    string `json:"categoryId,omitempty"`
	InStock       bool   `json:"inStock"`
	Purchasable   bool   `json:"purchasable"`
}

// Unavailable is a tier (or the whole quote) that has no offer, and why. delivery-promise names
// the reason "constraint" on a quote and "code" on an estimate; both are read.
type Unavailable struct {
	Tier       Tier   `json:"tier,omitempty"`
	Constraint string `json:"constraint,omitempty"`
	Code       string `json:"code,omitempty"`
	GroupNo    int    `json:"groupNo,omitempty"`
}

// Reason is the constraint code, whichever field carried it.
func (u Unavailable) Reason() string {
	if u.Constraint != "" {
		return u.Constraint
	}
	return u.Code
}

// ConstraintNotServed is delivery-promise's code for a destination no service area covers.
const ConstraintNotServed = "DESTINATION_NOT_SERVED"

// DateTier is one tier of a product page's delivery-date quote.
type DateTier struct {
	Tier              Tier            `json:"tier"`
	DeliverByEarliest string          `json:"deliverByEarliest"`
	DeliverByLatest   string          `json:"deliverByLatest"`
	TimeZone          string          `json:"timeZone,omitempty"`
	OfferExpiresAt    string          `json:"offerExpiresAt"`
	Charge            json.RawMessage `json:"charge,omitempty"`
}

// DeliveryDates is the answer of POST /products/{id}/delivery-date-quotes.
type DeliveryDates struct {
	PostalPrefix string        `json:"postalPrefix"`
	Tiers        []DateTier    `json:"tiers"`
	Unavailable  []Unavailable `json:"unavailable"`
}

// NotServed reports whether the quote says the destination is outside every service area: no tier
// is offered and DESTINATION_NOT_SERVED is among the reasons.
func (d DeliveryDates) NotServed() bool {
	if len(d.Tiers) > 0 {
		return false
	}
	for _, u := range d.Unavailable {
		if u.Reason() == ConstraintNotServed {
			return true
		}
	}
	return false
}

// Line is one cart line, and one line of a Plan.
type Line struct {
	CatalogItemID string `json:"catalogItemId"`
	Qty           int    `json:"quantity"`
}

// Offer is one quoted delivery offer of a shipment group.
type Offer struct {
	OfferID           string `json:"offerId"`
	GroupNo           int    `json:"groupNo"`
	Tier              Tier   `json:"tier"`
	DeliverByEarliest string `json:"deliverByEarliest"`
	DeliverByLatest   string `json:"deliverByLatest"`
	ExpiresAt         string `json:"expiresAt"`
}

// QuoteGroup is a shipment group of a quote and the offers for it.
type QuoteGroup struct {
	GroupNo int     `json:"groupNo"`
	LineNos []int   `json:"lineNos"`
	Offers  []Offer `json:"offers"`
}

// Quote is the cart's delivery quote.
type Quote struct {
	Groups           []QuoteGroup  `json:"groups"`
	UnavailableTiers []Tier        `json:"unavailableTiers"`
	Unavailable      []Unavailable `json:"unavailable"`
	QuotedAt         string        `json:"quotedAt"`
}

// Offers counts the offers across every group.
func (q Quote) Offers() int {
	n := 0
	for _, g := range q.Groups {
		n += len(g.Offers)
	}
	return n
}

// NotServed reports whether the quote has no offer at all because the destination is not served.
func (q Quote) NotServed() bool {
	if q.Offers() > 0 {
		return false
	}
	for _, u := range q.Unavailable {
		if u.Reason() == ConstraintNotServed {
			return true
		}
	}
	return false
}

// Selection is the offer chosen for a group.
type Selection struct {
	GroupNo int    `json:"groupNo"`
	OfferID string `json:"offerId"`
}

// Estimate is the cart's non-binding total.
type Estimate struct {
	Currency         string `json:"currency"`
	ItemsMinor       int64  `json:"itemsMinor"`
	DeliveryMinor    int64  `json:"deliveryMinor"`
	TaxMinor         int64  `json:"taxMinor"`
	TotalMinor       int64  `json:"totalMinor"`
	EstimateComplete bool   `json:"estimateComplete"`
}

// Readiness says whether the cart can be checked out.
type Readiness struct {
	Ready             bool `json:"ready"`
	AllGroupsSelected bool `json:"allGroupsSelected"`
	NoOfferExpired    bool `json:"noOfferExpired"`
}

// Cart is shopping-cart's cart as the BFF relays it.
type Cart struct {
	CartID                  string      `json:"cartId"`
	ShopperID               string      `json:"shopperId"`
	State                   string      `json:"state"`
	Lines                   []Line      `json:"lines"`
	DestinationPostalPrefix string      `json:"destinationPostalPrefix"`
	DeliveryQuote           *Quote      `json:"deliveryQuote,omitempty"`
	Selections              []Selection `json:"selections"`
	Estimate                *Estimate   `json:"estimate,omitempty"`
	CheckoutReadiness       *Readiness  `json:"checkoutReadiness,omitempty"`
	Version                 int64       `json:"version"`
}

// SessionGroup is a shipment group of a checkout session.
type SessionGroup struct {
	GroupNo     int    `json:"groupNo"`
	OfferID     string `json:"offerId"`
	Tier        Tier   `json:"tier"`
	LineNos     []int  `json:"lineNos"`
	AmountMinor int64  `json:"amountMinor"`
}

// Session is a checkout session (the checkout context's CheckoutSession).
type Session struct {
	SessionID       string         `json:"sessionId"`
	State           string         `json:"state"`
	CartID          string         `json:"cartId"`
	ShopperID       string         `json:"shopperId"`
	ShipToAddressID string         `json:"shipToAddressId"`
	Groups          []SessionGroup `json:"groups"`
	PriceQuoteID    string         `json:"priceQuoteId"`
	Currency        string         `json:"currency"`
	TotalMinor      int64          `json:"totalMinor"`
	QuoteExpiresAt  string         `json:"quoteExpiresAt"`
	AuthorizationID string         `json:"authorizationId"`
	OrderID         string         `json:"orderId"`
	RequotedOffers  []Offer        `json:"requotedOffers,omitempty"`
	FailureCode     string         `json:"failureCode"`
	Version         int64          `json:"version"`
}

// OrderGroup is a shipment group of an order with its promise window as returned: instants in
// RFC 3339, read in TimeZone when it is set (UTC otherwise).
type OrderGroup struct {
	GroupNo           int    `json:"groupNo"`
	PromiseID         string `json:"promiseId"`
	Tier              Tier   `json:"tier"`
	ShipBy            string `json:"shipBy"`
	DeliverByEarliest string `json:"deliverByEarliest"`
	DeliverByLatest   string `json:"deliverByLatest"`
	TimeZone          string `json:"timeZone"`
	LineNos           []int  `json:"lineNos"`
	Status            string `json:"status"`
	CancelReason      string `json:"cancelReason,omitempty"`
}

// Order is customer-ordering's order as the BFF relays it. Totals and Tracking are best effort and
// absent when their upstream failed or the cookie's checkout did not place this order.
type Order struct {
	OrderID                 string          `json:"orderId"`
	ShopperID               string          `json:"shopperId"`
	Status                  string          `json:"status"`
	DestinationPostalPrefix string          `json:"destinationPostalPrefix"`
	PlacedAt                string          `json:"placedAt"`
	CancelledAt             string          `json:"cancelledAt,omitempty"`
	Lines                   []orderLine     `json:"lines"`
	Groups                  []OrderGroup    `json:"groups"`
	Tracking                json.RawMessage `json:"tracking,omitempty"`
	Totals                  *orderTotals    `json:"totals,omitempty"`
}

type orderLine struct {
	LineNo        int    `json:"lineNo"`
	CatalogItemID string `json:"catalogItemId"`
	Quantity      int    `json:"quantity"`
}

type orderTotals struct {
	Currency   string `json:"currency"`
	TotalMinor *int64 `json:"totalMinor"`
}

// problemBody is the RFC 9457 body of a 4xx/5xx.
type problemBody struct {
	Type   string `json:"type"`
	Title  string `json:"title"`
	Status int    `json:"status"`
	Detail string `json:"detail"`
}

// slug is the last element of a problem type ("/problems/cancel-after-lock" -> "cancel-after-lock").
func (p problemBody) slug() string {
	t := strings.TrimRight(p.Type, "/")
	if i := strings.LastIndex(t, "/"); i >= 0 {
		return t[i+1:]
	}
	return t
}
