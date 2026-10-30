// Command seed-storefront provisions everything a shopper needs to buy a product in the storefront
// (http://storefront.localhost) and see its delivery promise, through the services' PUBLISHED REST
// APIs only (Kong), repeatably: a second run against an unchanged cluster writes nothing.
//
// Input: fixtures/storefront/catalogue.json (CURATED merchandising: currency, tax bands, delivery
// charges, departments, per product its department, subcategory and list price), the REAL product
// data of fixtures/products/fulfilment-products.json (dimensions, weight, handling tags) and the
// retail-network data file of warehouse-infra (scripts/data/retail-network-seed.json: SKU -> the
// catalogue item's title, its natural key in offer-availability). The WMS side of the promise comes
// from fixtures/storefront/fulfilment-site.json: the CPT schedule of the fulfilment site (site-1)
// in process-path-management, which must agree with the network data file's node profile.
//
//  1. CHECK   validates the fixtures against the real data (the CPT schedule against the network
//     node profile, its paths against process-path-management's Active paths), resolves every
//     SKU -> title -> catalogItemId (offer-availability GET /catalog, paged), and reads the site's
//     CPT schedule, the current tax table, charge schedule, category tree, item prices and
//     contents. A missing or DISCONTINUED catalogue item fails here, before any write: the
//     network seed must run first.
//  2. PLAN    a pure, deterministic diff of the desired against the observed state, printed per
//     kind with counts (BuildPlan).
//  3. APPLY   only the diffs, in dependency order: first the WMS step (PUT the site's CPT
//     schedule when its timezone or cutoffs differ), then the storefront tier, every POST under a
//     deterministic Idempotency-Key (seed-sf:<40 hex of sha256(op, path, canonical body)>).
//  4. VERIFY  network-fulfillment's capability offers of the storefront SKUs (warn only), every
//     product purchasable in product-discovery within -verify-timeout, one delivery quote through
//     the BFF (postal prefix 787 by default), one BFF search hit per department.
//
// Exit 0 only when VERIFY passes (or, with -dry-run, when CHECK and PLAN pass). Normally run
// through scripts/10-provision-storefront.sh (make provision-storefront).
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"
	_ "time/tzdata" // the CPT schedule's IANA timezone is validated wherever this runs
)

const (
	defaultBaseURL     = "http://localhost:8000/api"
	defaultBFFURL      = "http://storefront.localhost/api/storefront"
	defaultMediaBase   = "http://storefront.localhost/media/departments"
	defaultNetworkData = "../warehouse-infra/scripts/data/retail-network-seed.json"
)

func main() {
	log.SetFlags(0)
	var (
		baseURL   = flag.String("base-url", envOr("STOREFRONT_SEED_BASE_URL", defaultBaseURL), "Kong base URL; each context at <base-url>/<context> ($STOREFRONT_SEED_BASE_URL)")
		bffURL    = flag.String("storefront-url", envOr("STOREFRONT_BFF_URL", defaultBFFURL), "storefront BFF base URL, used by VERIFY ($STOREFRONT_BFF_URL)")
		fixture   = flag.String("fixture", "fixtures/storefront/catalogue.json", "storefront fixture (curated merchandising)")
		site      = flag.String("site", "fixtures/storefront/fulfilment-site.json", "fulfilment-site fixture: the CPT schedule of the site in process-path-management (WMS step)")
		products  = flag.String("products", "fixtures/products/fulfilment-products.json", "real product data (dimensions, weight, handling tags)")
		network   = flag.String("network-data", envOr("STOREFRONT_NETWORK_DATA", defaultNetworkData), "warehouse-infra retail-network data file: SKU -> catalogue title ($STOREFRONT_NETWORK_DATA; default relative to the repo root)")
		mediaBase = flag.String("media-base", envOr("STOREFRONT_MEDIA_BASE", defaultMediaBase), "absolute URL of the department images (<media-base>/<department-slug>.svg)")
		dryRun    = flag.Bool("dry-run", false, "CHECK and PLAN only: write nothing")
		verifyOn  = flag.Bool("verify-only", false, "CHECK, PLAN (printed) and VERIFY: write nothing")
		wmsOnly   = flag.Bool("wms-only", false, "the WMS step alone: CHECK, PLAN, APPLY and VERIFY of the fulfilment site's CPT schedule (combine with -dry-run / -verify-only)")
		timeout   = flag.Duration("verify-timeout", 3*time.Minute, "how long VERIFY waits for every product to become purchasable")
		interval  = flag.Duration("verify-interval", 2*time.Second, "VERIFY polling interval")
		prefix    = flag.String("quote-prefix", "787", "postal prefix of the VERIFY delivery quote (the first 3 characters of a ZIP code)")
	)
	flag.Parse()
	if *dryRun && *verifyOn {
		log.Fatal("-dry-run and -verify-only are exclusive")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	res, err := Run(ctx, Config{
		BaseURL: *baseURL, StorefrontURL: *bffURL, FixturePath: *fixture, SitePath: *site, ProductsPath: *products,
		NetworkDataPath: *network, MediaBase: *mediaBase, DryRun: *dryRun, VerifyOnly: *verifyOn, WMSOnly: *wmsOnly,
		VerifyTimeout: *timeout, VerifyInterval: *interval, QuotePrefix: *prefix,
		HTTP: &http.Client{Timeout: 30 * time.Second}, Out: os.Stdout,
	})
	mode := "provision"
	switch {
	case *dryRun:
		mode = "dry-run"
	case *verifyOn:
		mode = "verify-only"
	}
	if *wmsOnly {
		mode += " (wms-only)"
	}
	planned := -1
	if res != nil && res.Plan != nil {
		planned = res.Plan.Writes()
	}
	writes := 0
	if res != nil {
		writes = res.Writes
	}
	if err != nil {
		fmt.Printf("SUMMARY seed-storefront %s: FAIL, writes planned %d, writes sent %d\n", mode, planned, writes)
		log.Fatalf("seed-storefront: %v", err)
	}
	fmt.Printf("SUMMARY seed-storefront %s: OK, writes planned %d, writes sent %d\n", mode, planned, writes)
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
