package shopper

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"

	"github.com/claudioed/warehouse-e2e/internal/sim"
)

// Catalogue is the purchasable products of the storefront, the day's candidate shelf (ADR 0001 D4).
type Catalogue struct {
	products []Product // purchasable only, sorted by CatalogItemID
}

// NewCatalogue keeps the purchasable products of ps, sorted by id (duplicates collapse).
func NewCatalogue(ps []Product) *Catalogue {
	seen := map[string]bool{}
	var keep []Product
	for _, p := range ps {
		if !p.Purchasable || p.CatalogItemID == "" || seen[p.CatalogItemID] {
			continue
		}
		seen[p.CatalogItemID] = true
		keep = append(keep, p)
	}
	slices.SortFunc(keep, func(a, b Product) int { return strings.Compare(a.CatalogItemID, b.CatalogItemID) })
	return &Catalogue{products: keep}
}

// LoadCatalogue reads every page of GET /products (the BFF answers {items, nextCursor}, followed
// with sim.ListAll) and keeps the purchasable products. c needs the storefront in its Base; no
// cookie is involved, so the shared client will do.
func LoadCatalogue(ctx context.Context, c *sim.Client) (*Catalogue, error) {
	all, last := sim.ListAll[Product](ctx, c, Service, "GET /products", "/products")
	if !last.OK() {
		return nil, fmt.Errorf("shopper: list products stopped after %d products: status %d %s", len(all), last.Status, sim.Truncate(string(last.Body), 200))
	}
	return NewCatalogue(all), nil
}

// Catalogue is LoadCatalogue through this shopper's client.
func (s *Shopper) Catalogue(ctx context.Context) (*Catalogue, error) { return LoadCatalogue(ctx, s.c) }

// Len is the number of purchasable products.
func (c *Catalogue) Len() int { return len(c.products) }

// Products returns the purchasable products, sorted by id.
func (c *Catalogue) Products() []Product { return slices.Clone(c.products) }

// Get returns one product by id.
func (c *Catalogue) Get(id string) (Product, bool) {
	i, ok := slices.BinarySearchFunc(c.products, id, func(p Product, id string) int { return strings.Compare(p.CatalogItemID, id) })
	if !ok {
		return Product{}, false
	}
	return c.products[i], true
}

// Pick chooses n distinct products for the day's shelf. It is a pure function of the purchasable set
// and the seed: the same set and seed give the same shelf (sorted by id) whatever order the BFF
// listed them in, and a different seed gives a different one. n above the set's size returns it all.
func (c *Catalogue) Pick(n int, seed int64) []Product {
	if n <= 0 || len(c.products) == 0 {
		return nil
	}
	n = min(n, len(c.products))
	r := rand.New(rand.NewPCG(uint64(seed), 0x9e3779b97f4a7c15))
	idx := r.Perm(len(c.products))[:n]
	slices.Sort(idx)
	out := make([]Product, n)
	for i, j := range idx {
		out[i] = c.products[j]
	}
	return out
}
