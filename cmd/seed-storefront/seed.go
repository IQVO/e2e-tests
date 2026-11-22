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

	"github.com/claudioed/warehouse-e2e/internal/paging"
)

// Config is one seeder run.
type Config struct {
	BaseURL         string // Kong, each context at <base>/<context>
	StorefrontURL   string // the storefront BFF (/api/storefront)
	FixturePath     string
	SitePath        string // fixtures/storefront/fulfilment-site.json (the WMS CPT schedule)
	ProductsPath    string // fixtures/products/fulfilment-products.json
	NetworkDataPath string // warehouse-infra scripts/data/retail-network-seed.json
	MediaBase       string
	DryRun          bool
	VerifyOnly      bool
	WMSOnly         bool // the WMS step alone (the CPT schedule), before the network tier
	VerifyTimeout   time.Duration
	VerifyInterval  time.Duration
	// OfferRecomputeInterval is network-fulfillment's capability-offer recompute period (0: one
	// minute): VERIFY stops waiting once every offer was recomputed this long after the CPT write.
	OfferRecomputeInterval time.Duration
	QuotePrefix            string
	HTTP                   *http.Client
	Out                    io.Writer
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
	fx, phys, titles, err := checkInputs(cfg)
	if err != nil {
		return res, err
	}
	logf("check: fixture %d products in %d departments, currency %s; network data %s", len(fx.Products), len(fx.Departments), fx.Currency, cfg.NetworkDataPath)

	// ---- CHECK: the WMS side of the promise (the fulfilment site's CPT schedule) ----
	site, cpt, building, err := checkWMSSide(ctx, c, cfg, logf)
	if err != nil {
		return res, err
	}
	if cfg.WMSOnly {
		return runWMSOnly(ctx, c, cfg, fixtureSKUs(fx), site, cpt, building, res, logf)
	}

	// ---- CHECK: catalogue items by exact title ----
	desired, err := checkCatalogue(ctx, c, cfg, fx, phys, titles, logf)
	if err != nil {
		return res, err
	}

	// ---- CHECK: observed storefront state ----
	obs, err := observe(ctx, c, desired)
	if err != nil {
		return res, fmt.Errorf("check: %w", err)
	}
	desired.Site, obs.CPT, obs.Building = site, cpt, building

	// ---- PLAN ----
	plan := BuildPlan(*desired, *obs)
	res.Plan = plan
	logPlan(plan, logf)
	if cfg.DryRun {
		logf("dry run: nothing written")
		if len(plan.Blocked) > 0 {
			return res, fmt.Errorf("plan: %d products are blocked (see BLOCKED above)", len(plan.Blocked))
		}
		return res, nil
	}

	// ---- APPLY ----
	var cptWritten time.Time
	if !cfg.VerifyOnly {
		cptWritten, err = writeStage(ctx, c, plan, site, obs, fx, res, logf)
		if err != nil {
			return res, err
		}
	}

	// ---- VERIFY ----
	if err := verifyStage(ctx, c, cfg, site, desired, fx, plan, cptWritten, res, logf); err != nil {
		return res, err
	}
	return res, nil
}

// checkInputs loads the three data inputs and cross-validates the fixture
// against the real product and network data.
func checkInputs(cfg Config) (fx *Fixture, phys map[string]PhysicalProduct, titles map[string]string, err error) {
	fx, err = LoadFixture(cfg.FixturePath)
	if err != nil {
		return nil, nil, nil, err
	}
	phys, err = LoadPhysical(cfg.ProductsPath)
	if err != nil {
		return nil, nil, nil, err
	}
	titles, err = LoadNetworkTitles(cfg.NetworkDataPath)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("check: %w", err)
	}
	if problems := fx.Validate(phys, titles); len(problems) > 0 {
		return nil, nil, nil, fmt.Errorf("check: the fixture is inconsistent with the real data (%d problems):\n  %s", len(problems), strings.Join(problems, "\n  "))
	}
	return fx, phys, titles, nil
}

// checkWMSSide validates the WMS half of the promise: the fulfilment site's
// fixture, its agreement with the network's node profile, the Active process
// paths its cutoffs name, and the CPT schedules already in force for the site
// and the warehouse building that serves it.
func checkWMSSide(ctx context.Context, c *client, cfg Config, logf func(string, ...any)) (site *SiteFixture, cpt, building *CPTSchedule, err error) {
	site, err = LoadSiteFixture(cfg.SitePath)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("check: %w", err)
	}
	if problems := site.Validate(); len(problems) > 0 {
		return nil, nil, nil, fmt.Errorf("check: the fulfilment-site fixture %s is invalid (%d problems):\n  %s", cfg.SitePath, len(problems), strings.Join(problems, "\n  "))
	}
	node, err := LoadNodeProfile(cfg.NetworkDataPath, site.SiteID)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("check: %w", err)
	}
	if problems := site.AgreeWith(node); len(problems) > 0 {
		return nil, nil, nil, fmt.Errorf("check: the CPT schedule of %s disagrees with the network's node profile (%s); both declare the same node, fix the one that is wrong:\n  %s", cfg.SitePath, cfg.NetworkDataPath, strings.Join(problems, "\n  "))
	}
	active, err := readActivePaths(ctx, c)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("check: process-path-management process paths: %w", err)
	}
	for _, ct := range site.Cutoffs {
		for _, p := range ct.EligiblePathIDs {
			if !active[p] {
				return nil, nil, nil, fmt.Errorf("check: cutoff %s names process path %q, which is not an Active path in process-path-management (seed the paths: scripts/seed-process-paths.sh); nothing written", ct.CptID, p)
			}
		}
	}
	cpt, err = observeCPT(ctx, c, site.SiteID)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("check: process-path-management CPT schedule of %s: %w", site.SiteID, err)
	}
	logf("check: fulfilment site %s: CPT schedule %s agrees with the network node profile; eligible paths Active", site.SiteID, describeSchedule(site.Timezone, site.Cutoffs))
	// the warehouse building serving the node (ADR 0001 D1): the same schedule under its own id,
	// and only for a site facility-layout knows (never created here)
	fsite, err := checkWarehouseSite(ctx, c, site, cfg.SitePath)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("check: %w", err)
	}
	building, err = observeCPT(ctx, c, site.WarehouseSiteID)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("check: process-path-management CPT schedule of %s: %w", site.WarehouseSiteID, err)
	}
	logf("check: warehouse building %s (serves node %s): facility-layout knows the site (%s); it carries the same CPT schedule as the node, one list of cutoffs for both ids", site.WarehouseSiteID, site.SiteID, fsite.Status)
	return site, cpt, building, nil
}

// checkCatalogue resolves the fixture products to ACTIVE offer-availability
// catalogue items by exact title and derives their desired content.
func checkCatalogue(ctx context.Context, c *client, cfg Config, fx *Fixture, phys map[string]PhysicalProduct, titles map[string]string, logf func(string, ...any)) (*Desired, error) {
	catalog, err := readCatalog(ctx, c)
	if err != nil {
		return nil, fmt.Errorf("check: offer-availability catalogue: %w", err)
	}
	byTitle := map[string][]CatalogItem{}
	for _, it := range catalog {
		byTitle[it.Title] = append(byTitle[it.Title], it)
	}
	desired, missing := buildDesired(fx, phys, titles, byTitle, cfg.MediaBase)
	if len(missing) > 0 {
		return nil, fmt.Errorf("check: %d of %d storefront products have no usable catalogue item, nothing written; %s:\n  %s",
			len(missing), len(fx.Products), errNetworkFirst, strings.Join(missing, "\n  "))
	}
	logf("check: %d products resolved to ACTIVE catalogue items by title (%d catalogue items read)", len(desired.Products), len(catalog))
	return desired, nil
}

// fixtureSKUs lists the fixture's SKUs in fixture order.
func fixtureSKUs(fx *Fixture) []string {
	skus := make([]string, 0, len(fx.Products))
	for _, p := range fx.Products {
		skus = append(skus, p.SKU)
	}
	return skus
}

// logPlan prints the plan summary (and its blocked products, if any).
func logPlan(plan *Plan, logf func(string, ...any)) {
	logf("plan:")
	for _, l := range plan.Summary() {
		logf("  %s", l)
	}
	for _, b := range plan.Blocked {
		logf("  BLOCKED %s", b)
	}
}

// writeStage applies the plan — the WMS CPT schedules first, the storefront
// tier second — and returns the moment the CPT write landed.
func writeStage(ctx context.Context, c *client, plan *Plan, site *SiteFixture, obs *Observed, fx *Fixture, res *Result, logf func(string, ...any)) (time.Time, error) {
	var cptWritten time.Time
	start := time.Now()
	var err error
	// the WMS step first: the promise of every storefront product depends on it
	cptWritten, err = applyCPTSchedules(ctx, c, plan, site, logf)
	if err == nil {
		err = apply(ctx, c, plan, obs.Tree, fx.Currency)
	}
	res.Writes, res.WritesByKey = c.totalWrites(), c.writes
	logf("apply: %d writes in %s (%s)", res.Writes, time.Since(start).Round(time.Millisecond), c.writeSummary())
	if err != nil {
		return cptWritten, fmt.Errorf("apply: %w (re-run: every write is keyed by its natural key and converges)", err)
	}
	return cptWritten, nil
}

// verifyStage reads the storefront back and judges it; the report lands in res.
func verifyStage(ctx context.Context, c *client, cfg Config, site *SiteFixture, desired *Desired, fx *Fixture, plan *Plan, cptWritten time.Time, res *Result, logf func(string, ...any)) error {
	skus := make([]string, 0, len(desired.Products))
	for _, p := range desired.Products {
		skus = append(skus, p.SKU)
	}
	offers := verifyOffers(ctx, c, site.SiteID, skus, cptWritten, cfg)
	tree := CategoryTree{}
	if err := c.get(ctx, c.base+"/product-content/categories", &tree); err != nil {
		return fmt.Errorf("verify: %w", err)
	}
	rep := verify(ctx, c, desired, fx, tree, cfg)
	rep.Offers = offers
	if line, problem := verifyBuildingCPT(ctx, c, site); problem != "" {
		rep.Failures = append(rep.Failures, problem)
	} else {
		rep.Building = line
	}
	rep.Warnings = append(offers.Warnings(), rep.Warnings...)
	res.Verify = rep
	for _, l := range rep.Lines() {
		logf("%s", l)
	}
	if !rep.OK() {
		return fmt.Errorf("verify failed: %s", strings.Join(rep.Failures, "; "))
	}
	if len(plan.Blocked) > 0 {
		return fmt.Errorf("%d products are blocked (see BLOCKED above)", len(plan.Blocked))
	}
	return nil
}

// readCatalog pages through offer-availability GET /catalog, following nextCursor until it is absent.
func readCatalog(ctx context.Context, c *client) ([]CatalogItem, error) {
	return paging.All[CatalogItem](ctx, c.get, c.base+"/offer-availability/catalog")
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
	if err := applyPricing(ctx, c, p); err != nil {
		return err
	}
	idx, err := applyCategories(ctx, c, p, tree)
	if err != nil {
		return err
	}
	if err := applyPrices(ctx, c, p, currency); err != nil {
		return err
	}
	return applyContents(ctx, c, p, idx)
}

// applyPricing publishes the tax table and the delivery charge schedule.
func applyPricing(ctx context.Context, c *client, p *Plan) error {
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
	return nil
}

// applyCategories creates the plan's categories, parents first, and returns the
// path index the contents step resolves category ids through.
func applyCategories(ctx context.Context, c *client, p *Plan, tree CategoryTree) (map[string]Category, error) {
	idx := treeIndex(tree)
	for _, cs := range p.Categories {
		body := struct {
			ParentID *string `json:"parentId,omitempty"`
			Name     string  `json:"name"`
		}{Name: cs.Name}
		if cs.ParentPath != "" {
			parent, ok := idx[strings.ToLower(cs.ParentPath)]
			if !ok {
				return nil, fmt.Errorf("category %q: parent %q was not created", cs.Path, cs.ParentPath)
			}
			body.ParentID = &parent.CategoryID
		}
		var created Category
		if err := c.write(ctx, "category", "/product-content/categories", body, nil, &created, http.StatusCreated); err != nil {
			return nil, fmt.Errorf("create category %q: %w", cs.Path, err)
		}
		if created.Path == "" {
			created.Path = cs.Path
		}
		idx[strings.ToLower(cs.Path)] = created
	}
	return idx, nil
}

// applyPrices creates the plan's new item prices and replaces the list price of
// the existing ones.
func applyPrices(ctx context.Context, c *client, p *Plan, currency string) error {
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
		// PUT replaces the list-price attribute: idempotent by method, so no Idempotency-Key and a
		// retry after a lost response re-applies the same state. The observed version is NOT sent
		// (it would turn that retry into a 409 concurrent-update); the plan is declarative.
		path := "/pricing/item-prices/" + url.PathEscape(pa.CatalogItemID) + "/list-price"
		if err := c.put(ctx, "item-price", path, struct {
			ListPrice Money `json:"listPrice"`
		}{price}, nil, http.StatusOK); err != nil {
			return fmt.Errorf("set list price of %s: %w", pa.SKU, err)
		}
	}
	return nil
}

// applyContents creates or revises the plan's product contents and publishes the
// ones the plan marks for publication.
func applyContents(ctx context.Context, c *client, p *Plan, idx map[string]Category) error {
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
			if err := c.write(ctx, "content-revise", base+"/revisions", body, material, &got, http.StatusOK); err != nil {
				return fmt.Errorf("revise content of %s: %w", ca.SKU, err)
			}
			revision = got.Revision
		}
		if ca.Publish {
			material := struct {
				Revision int `json:"revision"`
			}{revision}
			if err := c.write(ctx, "content-publish", base+"/publications", nil, material, nil, http.StatusOK); err != nil {
				return fmt.Errorf("publish content of %s (revision %d): %w", ca.SKU, revision, err)
			}
		}
	}
	return nil
}
