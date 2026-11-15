package day

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"sort"
	"strings"

	"github.com/claudioed/warehouse-e2e/internal/retailday"
)

// Item is one purchasable product of the storefront and the warehouse SKU behind it. The
// storefront sells by catalogItemId; inventory-storage and order-management know the SKU, and
// the dictionary between them is the day's catalogue (ADR 0001 D4).
type Item struct {
	CatalogItemID string `json:"catalogItemId"`
	SKU           string `json:"sku"`
}

// Line is one cart line of a buyer.
type Line struct {
	Item Item `json:"item"`
	Qty  int  `json:"qty"`
}

// Tiers the day's delivered orders use (ADR 0001 D3: NEXT_DAY exists only before 11:00).
const (
	TierStandard = "STANDARD"
	TierTwoDay   = "TWO_DAY"
)

// Payment tokens of the payment provider simulator.
const (
	TokenApprove = "tok_approve"
	TokenDecline = "tok_decline"
	TokenTimeout = "tok_timeout"
)

// servedPrefixes are the postal prefixes the network delivers to; unservedPrefixes are three
// that it does not (ADR 0001, journey page section 2).
var (
	servedPrefixes   = []string{"786", "787"}
	unservedPrefixes = []string{"100", "331", "606", "900"}
)

// minShoppers is the smallest day: one delivered order plus one each of the four exceptions.
const minShoppers = 5

// Buyer is one planned shopper: what they buy, how they pay and what must become of the order.
// Placed carries what the plan already knows of it (run id, expectation, SKUs); the runner fills
// the rest from the shopper's answers and hands it to the evidence.
type Buyer struct {
	Index  int               `json:"index"` // 1-based, in plan order
	Expect retailday.Outcome `json:"expect"`
	Lines  []Line            `json:"lines"`
	Prefix string            `json:"prefix"`
	Tier   string            `json:"tier,omitempty"`
	Token  string            `json:"token"`
	// FallbackToken is tried once after Token was declined; the day's declined buyer has none.
	FallbackToken        string `json:"fallbackToken,omitempty"`
	CancelAfterPlacement bool   `json:"cancelAfterPlacement,omitempty"`
	ExpectOutOfArea      bool   `json:"expectOutOfArea,omitempty"`

	Placed retailday.Placed `json:"placed"`
}

// Label names the buyer in a report while it has no order id.
func (b Buyer) Label() string { return fmt.Sprintf("buyer-%02d", b.Index) }

// PlanConfig is everything a plan is a pure function of.
type PlanConfig struct {
	RunID     string
	Seed      int64
	Shoppers  int // all buyers, the four exceptions included
	ShelfSize int // SKUs on the day's shelf
	// MaxLines (default 3) and MaxQty (default 2) bound a delivered order.
	MaxLines, MaxQty int
}

// Plan is the day, decided before anything is bought: the shelf and the buyers.
type Plan struct {
	Seed   int64   `json:"seed"`
	Shelf  []Item  `json:"shelf"`
	Buyers []Buyer `json:"buyers"`
}

// BuildPlan decides the day deterministically from the seed: the same candidates (in any order)
// and the same config always give the same plan. It refuses what cannot be planned rather than
// shrinking it quietly.
func BuildPlan(cfg PlanConfig, candidates []Item) (Plan, error) {
	if strings.TrimSpace(cfg.RunID) == "" {
		return Plan{}, errors.New("plan: a run id is required: it is stamped on every order the day places")
	}
	if cfg.Shoppers < minShoppers {
		return Plan{}, fmt.Errorf("plan: %d shoppers cannot make the day's mix: need at least %d (one delivered order plus one each of declined, cancelled, not-served and in-doubt)", cfg.Shoppers, minShoppers)
	}
	if cfg.ShelfSize < 1 {
		return Plan{}, fmt.Errorf("plan: a shelf of %d SKUs: need at least 1", cfg.ShelfSize)
	}
	maxLines, maxQty := cfg.MaxLines, cfg.MaxQty
	if maxLines <= 0 {
		maxLines = 3
	}
	if maxQty <= 0 {
		maxQty = 2
	}
	items, err := cleanCandidates(candidates)
	if err != nil {
		return Plan{}, err
	}
	if cfg.ShelfSize > len(items) {
		return Plan{}, fmt.Errorf("plan: a shelf of %d SKUs needs %d purchasable products with a SKU, the catalogue has %d", cfg.ShelfSize, cfg.ShelfSize, len(items))
	}

	r := newRand(cfg.Seed)
	shelf := pickN(r, items, cfg.ShelfSize)

	expects := make([]retailday.Outcome, 0, cfg.Shoppers)
	for range cfg.Shoppers - (minShoppers - 1) {
		expects = append(expects, retailday.Delivered)
	}
	expects = append(expects, retailday.Declined, retailday.Cancelled, retailday.NotServed, retailday.InDoubt)
	shuffle(r, expects)

	p := Plan{Seed: cfg.Seed, Shelf: shelf}
	for i, e := range expects {
		b := Buyer{Index: i + 1, Expect: e, Prefix: servedPrefixes[below(r, len(servedPrefixes))], Token: TokenApprove}
		nLines := 1 + below(r, min(maxLines, len(shelf)))
		for _, it := range pickN(r, shelf, nLines) {
			b.Lines = append(b.Lines, Line{Item: it, Qty: 1 + below(r, maxQty)})
		}
		switch e {
		case retailday.Delivered:
			b.Tier = []string{TierStandard, TierTwoDay}[below(r, 2)]
		case retailday.Declined:
			b.Token = TokenDecline
		case retailday.Cancelled:
			b.CancelAfterPlacement = true
		case retailday.NotServed:
			b.Prefix = unservedPrefixes[below(r, len(unservedPrefixes))]
			b.ExpectOutOfArea = true
		case retailday.InDoubt:
			b.Token = TokenTimeout
		}
		skus := map[string]int{}
		for _, l := range b.Lines {
			skus[l.Item.SKU] += l.Qty
		}
		b.Placed = retailday.Placed{RunID: cfg.RunID, Expect: e, SKUs: skus}
		p.Buyers = append(p.Buyers, b)
	}
	return p, nil
}

// cleanCandidates validates the catalogue and sorts it by catalogue id, so the plan does not
// depend on the order the storefront listed its products in.
func cleanCandidates(in []Item) ([]Item, error) {
	items := slices.Clone(in)
	sort.Slice(items, func(i, j int) bool { return items[i].CatalogItemID < items[j].CatalogItemID })
	cats, skus := map[string]bool{}, map[string]bool{}
	for _, it := range items {
		switch {
		case it.CatalogItemID == "" || it.SKU == "":
			return nil, fmt.Errorf("plan: the product %+v has no catalogue id or no SKU: the day cannot tell which stock it consumes", it)
		case cats[it.CatalogItemID]:
			return nil, fmt.Errorf("plan: catalogue id %s is listed twice", it.CatalogItemID)
		case skus[it.SKU]:
			return nil, fmt.Errorf("plan: SKU %s is behind two products: an order for one of them could not be told from the other", it.SKU)
		}
		cats[it.CatalogItemID], skus[it.SKU] = true, true
	}
	return items, nil
}

// ShelfSKUs returns the shelf's SKUs, sorted.
func (p Plan) ShelfSKUs() []string {
	out := make([]string, len(p.Shelf))
	for i, it := range p.Shelf {
		out[i] = it.SKU
	}
	sort.Strings(out)
	return out
}

// Demand is the units of each SKU the day could take off the shelf: every buyer but the declined
// and the not-served, which provably never place an order. A cancelled order counts, because a
// cancellation after the purchase order was accepted is refused and the order ships.
func (p Plan) Demand() map[string]int {
	out := map[string]int{}
	for _, b := range p.Buyers {
		if b.Expect == retailday.Declined || b.Expect == retailday.NotServed {
			continue
		}
		for _, l := range b.Lines {
			out[l.Item.SKU] += l.Qty
		}
	}
	return out
}

// CheckDemand refuses a day whose demand exceeds the usable stock the shelf starts with, and a
// shelf SKU whose stock was not read: the run would otherwise fail on a sold-out line it could
// have known about.
func (p Plan) CheckDemand(usable map[string]int) error {
	demand := p.Demand()
	var bad []string
	for _, sku := range p.ShelfSKUs() {
		have, ok := usable[sku]
		switch {
		case !ok:
			bad = append(bad, fmt.Sprintf("%s: no stock reading", sku))
		case demand[sku] > have:
			bad = append(bad, fmt.Sprintf("%s: the day buys up to %d, usable stock is %d", sku, demand[sku], have))
		}
	}
	if len(bad) > 0 {
		return fmt.Errorf("the day's demand exceeds the shelf's start stock (ADR 0001 D4): %s", strings.Join(bad, "; "))
	}
	return nil
}

// newRand is PCG, whose stream is specified, and every draw below uses only Uint64: the plan does
// not move when the standard library reorganises Perm, Shuffle or IntN.
func newRand(seed int64) *rand.Rand {
	return rand.New(rand.NewPCG(uint64(seed), 0x9e3779b97f4a7c15))
}

func below(r *rand.Rand, n int) int { return int(r.Uint64() % uint64(n)) }

func shuffle[T any](r *rand.Rand, s []T) {
	for i := len(s) - 1; i > 0; i-- {
		j := below(r, i+1)
		s[i], s[j] = s[j], s[i]
	}
}

// pickN draws n distinct elements and returns them in their original order.
func pickN(r *rand.Rand, from []Item, n int) []Item {
	idx := make([]int, len(from))
	for i := range idx {
		idx[i] = i
	}
	shuffle(r, idx)
	idx = idx[:n]
	slices.Sort(idx)
	out := make([]Item, n)
	for i, j := range idx {
		out[i] = from[j]
	}
	return out
}
