package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"
)

// Config is one seeder run.
type Config struct {
	BaseURL         string // Kong, each context at <base>/<context>
	StorefrontURL   string // the storefront BFF (/api/storefront)
	FixturePath     string
	ProductsPath    string // fixtures/products/fulfilment-products.json
	NetworkDataPath string // warehouse-infra scripts/data/retail-network-seed.json
	MediaBase       string
	DryRun          bool
	VerifyOnly      bool
	VerifyTimeout   time.Duration
	VerifyInterval  time.Duration
	QuotePrefix     string
	HTTP            *http.Client
	Out             io.Writer
}

// Result is what a run did; Run's error says whether it succeeded.
type Result struct {
	Plan        *Plan
	Writes      int
	WritesByKey map[string]int
	Verify      *VerifyReport
}

// errNetworkFirst prefixes every CHECK failure that the network seed fixes.
const errNetworkFirst = "run the retail-network seed first (warehouse-infra: python3 scripts/seed-retail-network.py, or make provision-storefront which runs it)"

// Run executes CHECK -> PLAN -> APPLY -> VERIFY. Nothing is written before CHECK and PLAN passed.
func Run(ctx context.Context, cfg Config) (*Result, error) {
	out := cfg.Out
	if out == nil {
		out = os.Stdout
	}
	logf := func(format string, args ...any) { fmt.Fprintf(out, format+"\n", args...) }
	readOnly := cfg.DryRun || cfg.VerifyOnly
	c := newClient(cfg.BaseURL, cfg.StorefrontURL, cfg.HTTP, readOnly)
	res := &Result{}

	// ---- CHECK: inputs ----
	fx, err := LoadFixture(cfg.FixturePath)
	if err != nil {
		return res, err
	}
	phys, err := LoadPhysical(cfg.ProductsPath)
	if err != nil {
		return res, err
	}
	titles, err := LoadNetworkTitles(cfg.NetworkDataPath)
	if err != nil {
		return res, fmt.Errorf("check: %w", err)
	}
	if problems := fx.Validate(phys, titles); len(problems) > 0 {
		return res, fmt.Errorf("check: the fixture is inconsistent with the real data (%d problems):\n  %s", len(problems), strings.Join(problems, "\n  "))
	}
	logf("check: fixture %d products in %d departments, currency %s; network data %s", len(fx.Products), len(fx.Departments), fx.Currency, cfg.NetworkDataPath)

	// ---- CHECK: catalogue items by exact title ----
	catalog, err := readCatalog(ctx, c)
	if err != nil {
		return res, fmt.Errorf("check: offer-availability catalogue: %w", err)
	}
	byTitle := map[string][]CatalogItem{}
	for _, it := range catalog {
		byTitle[it.Title] = append(byTitle[it.Title], it)
	}
	desired, missing := buildDesired(fx, phys, titles, byTitle, cfg.MediaBase)
	if len(missing) > 0 {
		return res, fmt.Errorf("check: %d of %d storefront products have no usable catalogue item, nothing written; %s:\n  %s",
			len(missing), len(fx.Products), errNetworkFirst, strings.Join(missing, "\n  "))
	}
	logf("check: %d products resolved to ACTIVE catalogue items by title (%d catalogue items read)", len(desired.Products), len(catalog))

	// ---- CHECK: observed storefront state ----
	obs, err := observe(ctx, c, desired)
	if err != nil {
		return res, fmt.Errorf("check: %w", err)
	}

	// ---- PLAN ----
	plan := BuildPlan(*desired, *obs)
	res.Plan = plan
	logf("plan:")
	for _, l := range plan.Summary() {
		logf("  %s", l)
	}
	for _, b := range plan.Blocked {
		logf("  BLOCKED %s", b)
	}
	if cfg.DryRun {
		logf("dry run: nothing written")
		if len(plan.Blocked) > 0 {
			return res, fmt.Errorf("plan: %d products are blocked (see BLOCKED above)", len(plan.Blocked))
		}
		return res, nil
	}

	// ---- APPLY ----
	if !cfg.VerifyOnly {
		start := time.Now()
		err := apply(ctx, c, plan, obs.Tree, fx.Currency)
		res.Writes, res.WritesByKey = c.totalWrites(), c.writes
		logf("apply: %d writes in %s (%s)", res.Writes, time.Since(start).Round(time.Millisecond), c.writeSummary())
		if err != nil {
			return res, fmt.Errorf("apply: %w (re-run: every write is keyed by its natural key and converges)", err)
		}
	}

	// ---- VERIFY ----
	tree := CategoryTree{}
	if err := c.get(ctx, c.base+"/product-content/categories", &tree); err != nil {
		return res, fmt.Errorf("verify: %w", err)
	}
	rep := verify(ctx, c, desired, fx, tree, cfg)
	res.Verify = rep
	for _, l := range rep.Lines() {
		logf("%s", l)
	}
	if !rep.OK() {
		return res, fmt.Errorf("verify failed: %s", strings.Join(rep.Failures, "; "))
	}
	if len(plan.Blocked) > 0 {
		return res, fmt.Errorf("%d products are blocked (see BLOCKED above)", len(plan.Blocked))
	}
	return res, nil
}

// readCatalog pages through offer-availability GET /catalog with the nextCursor.
func readCatalog(ctx context.Context, c *client) ([]CatalogItem, error) {
	var all []CatalogItem
	after := ""
	for pages := 0; pages < 10000; pages++ {
		u := c.base + "/offer-availability/catalog?limit=100"
		if after != "" {
			u += "&after=" + url.QueryEscape(after)
		}
		var page struct {
			Items      []CatalogItem `json:"items"`
			NextCursor *string       `json:"nextCursor"`
		}
		if err := c.get(ctx, u, &page); err != nil {
			return nil, err
		}
		all = append(all, page.Items...)
		if page.NextCursor == nil || *page.NextCursor == "" || len(page.Items) == 0 || *page.NextCursor == after {
			return all, nil
		}
		after = *page.NextCursor
	}
	return nil, fmt.Errorf("catalogue paging did not end")
}

// buildDesired resolves every fixture product (sku -> network title -> catalogue item) and derives
// its content. It returns one line per product that cannot be resolved.
func buildDesired(fx *Fixture, phys map[string]PhysicalProduct, titles map[string]string, byTitle map[string][]CatalogItem, mediaBase string) (*Desired, []string) {
	depts := fx.departmentBySlug()
	d := &Desired{Currency: fx.Currency, TaxBands: fx.TaxBands, Charges: fx.DeliveryCharges}
	var missing []string
	seenPath := map[string]bool{}
	var deptSpecs, subSpecs []CategorySpec
	for _, p := range fx.Products {
		dept := depts[p.Department]
		title := titles[p.SKU]
		items := byTitle[title]
		var active []CatalogItem
		for _, it := range items {
			if it.State == "ACTIVE" {
				active = append(active, it)
			}
		}
		switch {
		case len(items) == 0:
			missing = append(missing, fmt.Sprintf("%s: no catalogue item titled %q", p.SKU, title))
			continue
		case len(active) == 0:
			missing = append(missing, fmt.Sprintf("%s: catalogue item %q is %s", p.SKU, title, items[0].State))
			continue
		case len(active) > 1:
			missing = append(missing, fmt.Sprintf("%s: %d ACTIVE catalogue items share the title %q (the title is the natural key)", p.SKU, len(active), title))
			continue
		}
		pp := phys[p.SKU]
		facts := ProductFacts{
			SKU: p.SKU, Title: title, DepartmentSlug: dept.Slug, DepartmentName: dept.Name, Subcategory: p.Subcategory,
			Dims: *pp.Dimensions, Fragile: hasTag(pp, "Fragile"), HighValue: hasTag(pp, "HighValue"),
		}
		path := dept.Name + "/" + p.Subcategory
		d.Products = append(d.Products, DesiredProduct{
			SKU: p.SKU, Title: title, CatalogItemID: active[0].CatalogItemID, DepartmentName: dept.Name,
			CategoryPath: path, ListPriceMinor: p.ListPriceMinor, Body: DeriveContent(facts, mediaBase),
		})
		if !seenPath[dept.Name] {
			seenPath[dept.Name] = true
			deptSpecs = append(deptSpecs, CategorySpec{Path: dept.Name, Name: dept.Name})
		}
		if !seenPath[path] {
			seenPath[path] = true
			subSpecs = append(subSpecs, CategorySpec{Path: path, Name: p.Subcategory, ParentPath: dept.Name})
		}
	}
	sort.Slice(deptSpecs, func(i, j int) bool { return deptSpecs[i].Path < deptSpecs[j].Path })
	sort.Slice(subSpecs, func(i, j int) bool { return subSpecs[i].Path < subSpecs[j].Path })
	d.Categories = append(deptSpecs, subSpecs...)
	return d, missing
}

// observe reads the current storefront state of everything the desired state names.
func observe(ctx context.Context, c *client, d *Desired) (*Observed, error) {
	o := &Observed{Prices: map[string]*ItemPrice{}, Contents: map[string]*ProductContent{}}
	var tax TaxTable
	switch err := c.get(ctx, c.base+"/pricing/tax-tables/current", &tax); {
	case err == nil:
		o.Tax = &tax
	case !isStatus(err, http.StatusNotFound):
		return nil, fmt.Errorf("pricing tax table: %w", err)
	}
	var sched ChargeSchedule
	switch err := c.get(ctx, c.base+"/pricing/delivery-charge-schedules/current", &sched); {
	case err == nil:
		o.Charges = &sched
	case !isStatus(err, http.StatusNotFound):
		return nil, fmt.Errorf("pricing charge schedule: %w", err)
	}
	if err := c.get(ctx, c.base+"/product-content/categories", &o.Tree); err != nil {
		return nil, fmt.Errorf("product-content categories: %w", err)
	}
	for _, p := range d.Products {
		id := url.PathEscape(p.CatalogItemID)
		var ip ItemPrice
		switch err := c.get(ctx, c.base+"/pricing/item-prices/"+id, &ip); {
		case err == nil:
			o.Prices[p.CatalogItemID] = &ip
		case !isStatus(err, http.StatusNotFound):
			return nil, fmt.Errorf("pricing item price %s: %w", p.SKU, err)
		}
		var pc ProductContent
		switch err := c.get(ctx, c.base+"/product-content/product-contents/"+id, &pc); {
		case err == nil:
			o.Contents[p.CatalogItemID] = &pc
		case !isStatus(err, http.StatusNotFound):
			return nil, fmt.Errorf("product content %s: %w", p.SKU, err)
		}
	}
	return o, nil
}

// apply sends the plan's writes in dependency order: tax table, charge schedule, categories
// (parents first), prices, contents (create or revise, then publish). It stops at the first
// failure: a re-run reads the new state and converges.
func apply(ctx context.Context, c *client, p *Plan, tree CategoryTree, currency string) error {
	if p.TaxTable != nil {
		body := struct {
			Version int       `json:"version"`
			Bands   []TaxBand `json:"bands"`
		}{p.TaxTable.Version, p.TaxTable.Bands}
		if err := c.write(ctx, "tax-table", "/pricing/tax-tables", body, nil, nil, http.StatusCreated); err != nil {
			return fmt.Errorf("publish tax table v%d: %w", p.TaxTable.Version, err)
		}
	}
	if p.ChargeSchedule != nil {
		body := struct {
			Version int          `json:"version"`
			Tiers   []TierCharge `json:"tiers"`
		}{p.ChargeSchedule.Version, p.ChargeSchedule.Tiers}
		if err := c.write(ctx, "charge-schedule", "/pricing/delivery-charge-schedules", body, nil, nil, http.StatusCreated); err != nil {
			return fmt.Errorf("publish delivery charge schedule v%d: %w", p.ChargeSchedule.Version, err)
		}
	}

	idx := treeIndex(tree)
	for _, cs := range p.Categories {
		body := struct {
			ParentID *string `json:"parentId,omitempty"`
			Name     string  `json:"name"`
		}{Name: cs.Name}
		if cs.ParentPath != "" {
			parent, ok := idx[strings.ToLower(cs.ParentPath)]
			if !ok {
				return fmt.Errorf("category %q: parent %q was not created", cs.Path, cs.ParentPath)
			}
			body.ParentID = &parent.CategoryID
		}
		var created Category
		if err := c.write(ctx, "category", "/product-content/categories", body, nil, &created, http.StatusCreated); err != nil {
			return fmt.Errorf("create category %q: %w", cs.Path, err)
		}
		if created.Path == "" {
			created.Path = cs.Path
		}
		idx[strings.ToLower(cs.Path)] = created
	}

	for _, pa := range p.Prices {
		price := Money{AmountMinor: pa.AmountMinor, Currency: currency}
		if pa.Create {
			body := struct {
				CatalogItemID string `json:"catalogItemId"`
				ListPrice     Money  `json:"listPrice"`
			}{pa.CatalogItemID, price}
			if err := c.write(ctx, "item-price", "/pricing/item-prices", body, nil, nil, http.StatusCreated); err != nil {
				return fmt.Errorf("create price of %s: %w", pa.SKU, err)
			}
			continue
		}
		body := struct {
			ListPrice Money `json:"listPrice"`
			Version   int   `json:"version"`
		}{price, pa.Version}
		path := "/pricing/item-prices/" + url.PathEscape(pa.CatalogItemID) + ":set-list-price"
		if err := c.write(ctx, "item-price", path, body, nil, nil, http.StatusOK); err != nil {
			return fmt.Errorf("set list price of %s: %w", pa.SKU, err)
		}
	}

	for _, ca := range p.Contents {
		cat, ok := idx[strings.ToLower(ca.CategoryPath)]
		if !ok {
			return fmt.Errorf("content of %s: category %q does not exist", ca.SKU, ca.CategoryPath)
		}
		body := ca.Body
		body.PrimaryCategoryID = cat.CategoryID
		base := "/product-content/product-contents/" + url.PathEscape(ca.CatalogItemID)
		revision := ca.FromRevision
		var got ProductContent
		switch {
		case ca.Create:
			create := struct {
				CatalogItemID string `json:"catalogItemId"`
				ContentBody
			}{ca.CatalogItemID, body}
			if err := c.write(ctx, "content-create", "/product-content/product-contents", create, nil, &got, http.StatusCreated); err != nil {
				return fmt.Errorf("create content of %s: %w", ca.SKU, err)
			}
			revision = got.Revision
		case ca.Revise:
			material := struct {
				FromRevision int         `json:"fromRevision"`
				Body         ContentBody `json:"body"`
			}{ca.FromRevision, body}
			if err := c.write(ctx, "content-revise", base+":revise", body, material, &got, http.StatusOK); err != nil {
				return fmt.Errorf("revise content of %s: %w", ca.SKU, err)
			}
			revision = got.Revision
		}
		if ca.Publish {
			material := struct {
				Revision int `json:"revision"`
			}{revision}
			if err := c.write(ctx, "content-publish", base+":publish", nil, material, nil, http.StatusOK); err != nil {
				return fmt.Errorf("publish content of %s (revision %d): %w", ca.SKU, revision, err)
			}
		}
	}
	return nil
}
