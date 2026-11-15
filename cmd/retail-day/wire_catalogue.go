package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/claudioed/warehouse-e2e/internal/retailday/day"
	"github.com/claudioed/warehouse-e2e/internal/sim"
	"github.com/claudioed/warehouse-e2e/internal/sim/outbound"
	"github.com/claudioed/warehouse-e2e/internal/sim/shopper"
)

// The Catalogue phase: the storefront's purchasable products (shopper.LoadCatalogue) with the SKU
// behind each. The SKU <-> catalogItemId dictionary is not in fixtures/storefront/catalogue.json
// (that file carries SKUs, departments and prices only: the network generates the item ids). It is
// derived exactly as cmd/seed-storefront does (seed.go buildDesired):
//
//	fixtures/storefront/catalogue.json   the SKUs of the storefront
//	warehouse-infra retail-network-seed  SKU -> the catalogue item's title (its natural key)
//	offer-availability GET /catalog      title -> catalogItemId (exactly one ACTIVE item per title)

// dictionary is the SKU <-> catalogItemId pairing the day works with.
type dictionary struct {
	SKUByItem  map[string]string // catalogItemId -> SKU
	ItemBySKU  map[string]string // SKU -> catalogItemId
	Unresolved []string          // one line per fixture SKU that has no ACTIVE catalogue item
}

type catalogItem struct {
	CatalogItemID string `json:"catalogItemId"`
	Title         string `json:"title"`
	State         string `json:"state"`
}

// resolveDictionary pairs every fixture SKU with its one ACTIVE catalogue item. A SKU that cannot be
// paired is listed in Unresolved (and is not a candidate for the shelf); two SKUs behind one
// catalogue item are an error, because an order for it could not be told apart.
func resolveDictionary(skus []string, titles map[string]string, catalog []catalogItem) (dictionary, error) {
	byTitle := map[string][]catalogItem{}
	for _, it := range catalog {
		byTitle[it.Title] = append(byTitle[it.Title], it)
	}
	d := dictionary{SKUByItem: map[string]string{}, ItemBySKU: map[string]string{}}
	for _, sku := range skus {
		title := strings.TrimSpace(titles[sku])
		if title == "" {
			d.Unresolved = append(d.Unresolved, sku+": no title in the retail-network data file")
			continue
		}
		var active []catalogItem
		for _, it := range byTitle[title] {
			if it.State == "ACTIVE" {
				active = append(active, it)
			}
		}
		switch {
		case len(byTitle[title]) == 0:
			d.Unresolved = append(d.Unresolved, fmt.Sprintf("%s: no catalogue item titled %q", sku, title))
			continue
		case len(active) != 1:
			d.Unresolved = append(d.Unresolved, fmt.Sprintf("%s: %d ACTIVE catalogue item(s) titled %q, want exactly 1", sku, len(active), title))
			continue
		case active[0].CatalogItemID == "":
			d.Unresolved = append(d.Unresolved, fmt.Sprintf("%s: the catalogue item titled %q has no id", sku, title))
			continue
		}
		id := active[0].CatalogItemID
		if other, dup := d.SKUByItem[id]; dup {
			return dictionary{}, fmt.Errorf("catalogue item %s is behind two SKUs (%s and %s): an order for it could not be told from the other", id, other, sku)
		}
		d.SKUByItem[id] = sku
		d.ItemBySKU[sku] = id
	}
	return d, nil
}

// fixtureSKUs reads the SKUs of fixtures/storefront/catalogue.json.
func fixtureSKUs(path string) ([]string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("storefront fixture: %w", err)
	}
	var doc struct {
		Products []struct {
			SKU string `json:"sku"`
		} `json:"products"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("storefront fixture %s: %w", path, err)
	}
	var skus []string
	for _, p := range doc.Products {
		if p.SKU != "" {
			skus = append(skus, p.SKU)
		}
	}
	if len(skus) == 0 {
		return nil, fmt.Errorf("storefront fixture %s lists no product", path)
	}
	return skus, nil
}

// networkTitles reads warehouse-infra's scripts/data/retail-network-seed.json: SKU -> catalogue title.
func networkTitles(path string) (map[string]string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("retail-network data file (warehouse-infra scripts/data/retail-network-seed.json, flag -network-data): %w", err)
	}
	var doc struct {
		Catalog []struct {
			SKU   string `json:"sku"`
			Title string `json:"title"`
		} `json:"catalog"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("retail-network data file %s: %w", path, err)
	}
	out := make(map[string]string, len(doc.Catalog))
	for _, c := range doc.Catalog {
		out[c.SKU] = c.Title
	}
	return out, nil
}

// readCatalog pages through offer-availability GET /catalog (a read).
func readCatalog(ctx context.Context, c *sim.Client) ([]catalogItem, error) {
	items, last := sim.ListAll[catalogItem](ctx, c, "offer-availability", "GET /catalog", "/catalog")
	if !last.OK() {
		return nil, fmt.Errorf("offer-availability GET /catalog stopped after %d item(s): %d %s", len(items), last.Status, sim.Truncate(string(last.Body), 160))
	}
	return items, nil
}

// loadDictionary derives the dictionary from the two files and the cluster. It fails when no SKU can
// be paired: the day would have nothing to sell.
func loadDictionary(ctx context.Context, c *sim.Client, fixturePath, networkDataPath string) (dictionary, error) {
	skus, err := fixtureSKUs(fixturePath)
	if err != nil {
		return dictionary{}, err
	}
	titles, err := networkTitles(networkDataPath)
	if err != nil {
		return dictionary{}, err
	}
	catalog, err := readCatalog(ctx, c)
	if err != nil {
		return dictionary{}, err
	}
	d, err := resolveDictionary(skus, titles, catalog)
	if err != nil {
		return dictionary{}, err
	}
	if len(d.ItemBySKU) == 0 {
		return dictionary{}, fmt.Errorf("none of the %d fixture SKUs has an ACTIVE catalogue item; first: %s", len(skus), firstOf(d.Unresolved))
	}
	return d, nil
}

func firstOf(s []string) string {
	if len(s) == 0 {
		return "(none)"
	}
	return s[0]
}

// catalogueAdapter satisfies day.Catalogue: the purchasable products that have a SKU. A product the
// dictionary does not know (another team's item) is not a candidate: the day could not tell which
// stock it consumes.
type catalogueAdapter struct {
	list func(ctx context.Context) ([]shopper.Product, error)
	dict map[string]string // catalogItemId -> SKU
}

var _ day.Catalogue = catalogueAdapter{}

// newCatalogueLister is shopper.LoadCatalogue over the shared client.
func newCatalogueLister(c *sim.Client) func(ctx context.Context) ([]shopper.Product, error) {
	return func(ctx context.Context) ([]shopper.Product, error) {
		cat, err := shopper.LoadCatalogue(ctx, c)
		if err != nil {
			return nil, err
		}
		return cat.Products(), nil
	}
}

func (a catalogueAdapter) Items(ctx context.Context) ([]day.Item, error) {
	ps, err := a.list(ctx)
	if err != nil {
		return nil, err
	}
	var out []day.Item
	for _, p := range ps {
		if !p.Purchasable {
			continue
		}
		if sku, ok := a.dict[p.CatalogItemID]; ok {
			out = append(out, day.Item{CatalogItemID: p.CatalogItemID, SKU: sku})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CatalogItemID < out[j].CatalogItemID })
	return out, nil
}

// siteFixture is fixtures/storefront/fulfilment-site.json: the storefront's building and the CPT
// schedule written under it. The cutoff fields are the wire names outbound.Cutoff declares.
type siteFixture struct {
	SiteID          string            `json:"siteId"`
	WarehouseSiteID string            `json:"warehouseSiteId"`
	Timezone        string            `json:"timezone"`
	Cutoffs         []outbound.Cutoff `json:"cutoffs"`
}

func loadSiteFixture(path string) (siteFixture, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return siteFixture{}, fmt.Errorf("fulfilment site fixture: %w", err)
	}
	var f siteFixture
	if err := json.Unmarshal(raw, &f); err != nil {
		return siteFixture{}, fmt.Errorf("fulfilment site fixture %s: %w", path, err)
	}
	if f.WarehouseSiteID == "" || len(f.Cutoffs) == 0 {
		return siteFixture{}, fmt.Errorf("fulfilment site fixture %s names no warehouseSiteId or no cutoff", path)
	}
	return f, nil
}
