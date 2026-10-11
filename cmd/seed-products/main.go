// Command seed-products registers a catalogue of REAL products (at least
// 2,000 by default) in product-master through its published REST API, then
// reads the catalogue back to prove every product landed.
//
// The products come from Open Food Facts (https://world.openfoodfacts.org,
// ODbL 1.0): each SKU is the product's GS1 GTIN with a verified check digit,
// and the description is its real brand, name and net quantity. They are
// committed in fixtures/products/openfoodfacts-products.json so a run is
// offline and deterministic; refresh that file with -fetch.
//
// Usage:
//
//	seed-products                       # register the fixture (PRODUCT_MASTER_BASE_URL)
//	seed-products -base-url http://localhost:8000/api/product-master   # via Kong (kind)
//	seed-products -fetch                # re-harvest the fixture from Open Food Facts
//
// Normally run through scripts/08-seed-products.sh. Exit status is non-zero
// when fewer than -min products are in the fixture, any request fails, or the
// read-back finds a missing or different product.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"time"
)

func main() {
	log.SetFlags(0)
	var (
		fixture     = flag.String("fixture", "fixtures/products/openfoodfacts-products.json", "fixture file (read in register mode, written by -fetch)")
		baseURL     = flag.String("base-url", envOr("PRODUCT_MASTER_BASE_URL", "http://localhost:8090"), "product-master base URL")
		workers     = flag.Int("workers", 8, "concurrent registration workers")
		minProducts = flag.Int("min", 2000, "fail unless at least this many products are registered")
		limit       = flag.Int("limit", 0, "register only the first N fixture products (0 = all; must still be >= -min)")
		classify    = flag.Bool("classify", true, "also PUT the temperature classification the fixture derives")
		doVerify    = flag.Bool("verify", true, "read the catalogue back with GET /products and check every product")
		fetch       = flag.Bool("fetch", false, "harvest a fresh fixture from Open Food Facts instead of registering")
		fetchPause  = flag.Duration("fetch-pause", 7*time.Second, "pause between Open Food Facts search requests (their limit is ~10/min)")
		fetchCache  = flag.String("fetch-cache", defaultCacheDir(), "cache of fetched Open Food Facts pages, so an interrupted fetch resumes (\"\" = none)")
	)
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	if *fetch {
		runFetch(ctx, *fixture, *minProducts, *fetchPause, *fetchCache)
		return
	}
	runRegister(ctx, *fixture, strings.TrimRight(*baseURL, "/"), *workers, *minProducts, *limit, *classify, *doVerify)
}

func runFetch(ctx context.Context, path string, minProducts int, pause time.Duration, cacheDir string) {
	log.Printf("harvesting at least %d products from Open Food Facts (pause %s between requests, cache %q)", minProducts, pause, cacheDir)
	products, err := fetchCatalog(ctx, newOFFClient(pause, cacheDir), defaultSlices, minProducts)
	if err != nil {
		log.Fatalf("fetch: %v", err)
	}
	f := Fixture{
		Source:    "Open Food Facts - https://world.openfoodfacts.org (search API v2, sorted by unique scans)",
		License:   "Open Database License (ODbL) 1.0 - https://opendatacommons.org/licenses/odbl/1-0/",
		FetchedAt: time.Now().UTC().Format(time.RFC3339),
		Products:  products,
	}
	if err := writeFixture(path, f); err != nil {
		log.Fatalf("write fixture: %v", err)
	}
	frozen, chilled := countTemps(products)
	log.Printf("wrote %s: %d products (%d frozen, %d chilled, %d unclassified)", path, len(products), frozen, chilled, len(products)-frozen-chilled)
}

func runRegister(ctx context.Context, path, base string, workers, minProducts, limit int, classify, doVerify bool) {
	f, err := loadFixture(path)
	if err != nil {
		log.Fatalf("fixture: %v", err)
	}
	products := f.Products
	if limit > 0 && limit < len(products) {
		products = products[:limit]
	}
	if len(products) < minProducts {
		log.Fatalf("fixture has %d products, want at least %d (-min)", len(products), minProducts)
	}

	c := &pmClient{base: base, http: &http.Client{Timeout: 15 * time.Second}}
	if err := health(ctx, c); err != nil {
		log.Fatalf("product-master at %s is not ready: %v", base, err)
	}

	frozen, chilled := countTemps(products)
	log.Printf("registering %d real products in product-master at %s (%d workers; %d frozen, %d chilled to classify)", len(products), base, workers, frozen, chilled)
	start := time.Now()
	step := len(products) / 10
	rep := c.register(ctx, products, workers, classify, func(done int) {
		if step > 0 && done%step == 0 {
			log.Printf("  %5d / %d", done, len(products))
		}
	})
	elapsed := time.Since(start)

	log.Printf("registered in %s (%.0f products/s): %d created, %d already present (description now identical)",
		elapsed.Round(time.Millisecond), float64(len(products))/elapsed.Seconds(), rep.created.Load(), rep.updated.Load())
	if classify {
		log.Printf("classified: %d newly, %d already identical", rep.classified.Load(), rep.classUnchanged.Load())
	}
	if n := len(rep.failures); n > 0 {
		for i, f := range rep.failures {
			if i == 20 {
				log.Printf("  ... and %d more", n-20)
				break
			}
			log.Printf("  FAIL %s", f)
		}
		log.Fatalf("%d of %d products failed", n, len(products))
	}

	if !doVerify {
		return
	}
	listed, err := c.listAll(ctx)
	if err != nil {
		log.Fatalf("verify: GET /products: %v", err)
	}
	problems := verify(products, listed, classify)
	if len(problems) > 0 {
		for i, p := range problems {
			if i == 20 {
				log.Printf("  ... and %d more", len(problems)-20)
				break
			}
			log.Printf("  MISMATCH %s", p)
		}
		log.Fatalf("verify: %d of %d products missing or different", len(problems), len(products))
	}
	log.Printf("verified: all %d products listed by GET /products with the expected description%s (catalogue total %d)",
		len(products), map[bool]string{true: " and classification", false: ""}[classify], len(listed))
}

// health is the preflight. /healthz (liveness) and /readyz (a shutdown
// drain gate) both answer 200 while the database is unreachable, so the
// only honest "can it serve" probe is a real read: GET /products?limit=1
// touches the store. Without it a dead database surfaces as 2,000 separate
// 500s instead of one clear line.
func health(ctx context.Context, c *pmClient) error {
	for _, path := range []string{"/healthz", "/products?limit=1"} {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+path, nil)
		if err != nil {
			return err
		}
		resp, err := c.http.Do(req)
		if err != nil {
			return err
		}
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("GET %s: %s %s", path, resp.Status, strings.TrimSpace(string(b)))
		}
	}
	return nil
}

func countTemps(ps []FixtureProduct) (frozen, chilled int) {
	for _, p := range ps {
		switch p.TemperatureClass {
		case "Frozen":
			frozen++
		case "Chilled":
			chilled++
		}
	}
	return frozen, chilled
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// defaultCacheDir keeps fetched pages under the user's cache dir (never in
// the repo): <UserCacheDir>/warehouse-e2e/openfoodfacts.
func defaultCacheDir() string {
	dir, err := os.UserCacheDir()
	if err != nil {
		return ""
	}
	return filepath.Join(dir, "warehouse-e2e", "openfoodfacts")
}
