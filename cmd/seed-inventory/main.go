// Command seed-inventory stocks the pilot sites' bins in inventory-storage with the products that
// are registered in product-master, through the services' published REST APIs only.
//
// It does three things, in this order:
//
//  1. CHECK   lists every product registered in product-master (dimensions, handling tags,
//     temperature class, DOT hazard class) and every Active storage slot of the pilot
//     sites in facility-layout (zone temperature + hazmat rating, volume and weight limits);
//  2. PLAN    decides, deterministically, which SKUs each site stocks, how many units, and into
//     which bins (assortment by site role, size class and handling class; bins filled to
//     80 % of their real volume and weight limits; hazmat in hazmat-rated zones, one SKU
//     per bin; cold chain in chilled and frozen zones). It refuses to continue below -min-units;
//  3. STOCK   PUT /bins/{binId} (capacity) and POST /stock/stow for every planned line, then
//     reads every bin back (GET /bins/{binId}) and a sample of SKUs (GET /inventory/{sku}/usable).
//
// Idempotent: a bin already holding its planned units is skipped, PUT /bins converges, and every
// stow carries a deterministic Idempotency-Key, so a re-run (or a retry after a dropped response)
// does not stow twice. Normally run through scripts/09-seed-inventory.sh.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"time"
)

// Pilot sites: every stocking role, with a second non-sortable and a second XL site because the
// oversized catalogue (about 2,500 SKUs) needs the room.
const defaultSites = "BFI4,BOS7,CVG1,ABE8,SCK4,CHA2,JAX3,AUS3,EWR8,JFK5"

func main() {
	log.SetFlags(0)
	var (
		pmURL    = flag.String("product-master-url", envOr("PRODUCT_MASTER_BASE_URL", "http://localhost:8000/api/product-master"), "product-master base URL")
		flURL    = flag.String("facility-url", envOr("FACILITY_LAYOUT_BASE_URL", "http://localhost:8000/api/facility-layout"), "facility-layout base URL")
		invURL   = flag.String("inventory-url", envOr("INVENTORY_STORAGE_BASE_URL", "http://localhost:8000/api/inventory-storage"), "inventory-storage base URL")
		sitesCSV = flag.String("sites", envOr("SEED_SITES", defaultSites), "comma-separated pilot site codes")
		minUnits = flag.Int("min-units", 100000, "fail unless the plan stocks at least this many units")
		fill     = flag.Float64("fill", 0.8, "fraction of a bin's volume and weight limit that may be used")
		workers  = flag.Int("workers", 6, "concurrent bin workers")
		dryRun   = flag.Bool("dry-run", false, "check and plan, write nothing")
		doVerify = flag.Bool("verify", true, "read every bin back after stocking")
		sample   = flag.Int("verify-skus", 300, "SKUs whose usable inventory is read back")
	)
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	hc := &http.Client{Timeout: 30 * time.Second}
	pm := &reader{base: strings.TrimRight(*pmURL, "/"), http: hc}
	fl := &reader{base: strings.TrimRight(*flURL, "/"), http: hc}
	inv := &invClient{base: strings.TrimRight(*invURL, "/"), http: hc}

	// 1. CHECK
	products, skipped, err := loadProducts(ctx, pm)
	if err != nil {
		log.Fatalf("check: %v", err)
	}
	var codes []string
	for _, c := range strings.Split(*sitesCSV, ",") {
		if c = strings.TrimSpace(c); c != "" {
			codes = append(codes, c)
		}
	}
	sites, err := loadSites(ctx, fl, codes)
	if err != nil {
		log.Fatalf("check: %v", err)
	}
	bins := 0
	for _, s := range sites {
		bins += len(s.Bins)
	}
	log.Printf("check: %d registered products stockable (%d skipped: no dimensions and not a grocery); %d pilot sites, %d storage bins",
		len(products), len(skipped), len(sites), bins)

	// 2. PLAN
	plan := BuildPlan(products, sites, Options{Fill: *fill})
	report(plan, sites)
	if plan.Units < *minUnits {
		log.Fatalf("plan: %d units is below -min-units %d; add pilot sites (-sites) or lower the bar", plan.Units, *minUnits)
	}
	if *dryRun {
		log.Printf("dry run: nothing written")
		return
	}

	// 3. STOCK
	if err := health(ctx, inv); err != nil {
		log.Fatalf("inventory-storage at %s is not ready: %v", inv.base, err)
	}
	start := time.Now()
	step := len(plan.Bins) / 10
	rep := inv.stock(ctx, plan.Bins, *workers, func(done int) {
		if step > 0 && done%step == 0 {
			log.Printf("  %5d / %d bins", done, len(plan.Bins))
		}
	})
	log.Printf("stocked in %s: %d bins created, %d resized, %d already stocked (skipped), %d completed after an interruption; %d stow lines, %d units",
		time.Since(start).Round(time.Millisecond), rep.binsCreated.Load(), rep.binsResized.Load(), rep.binsAlreadyStocked.Load(),
		rep.binsPartial.Load(), rep.lines.Load(), rep.units.Load())
	if n := len(rep.failures); n > 0 {
		for i, f := range rep.failures {
			if i == 20 {
				log.Printf("  ... and %d more", n-20)
				break
			}
			log.Printf("  FAIL %s", f)
		}
		log.Fatalf("%d of %d bins failed", n, len(plan.Bins))
	}
	if !*doVerify {
		return
	}
	problems, occupied := inv.verify(ctx, plan, *workers, *sample)
	if len(problems) > 0 {
		for i, p := range problems {
			if i == 20 {
				log.Printf("  ... and %d more", len(problems)-20)
				break
			}
			log.Printf("  MISMATCH %s", p)
		}
		log.Fatalf("verify: %d problems", len(problems))
	}
	log.Printf("verified: all %d planned bins hold their stock (%d units on hand across them, %d planned); usable inventory read back for a sample of SKUs",
		len(plan.Bins), occupied, plan.Units)
}

func health(ctx context.Context, c *invClient) error {
	_, err := c.do(ctx, http.MethodGet, "/bins/seed-inventory-preflight", nil, "", nil, http.StatusOK, http.StatusNotFound)
	return err
}

func report(plan *Plan, sites []Site) {
	log.Printf("plan: %s", plan.Summary())
	codes := make([]string, 0, len(sites))
	for _, s := range sites {
		codes = append(codes, s.Code)
	}
	sort.Strings(codes)
	for _, s := range sites {
		log.Printf("  %-5s %-24s %6d units in %5d of %5d bins", s.Code, s.Role, plan.UnitsBySite[s.Code], plan.BinsBySite[s.Code], len(s.Bins))
	}
	profiles := make([]string, 0, len(plan.SKUsByProfile))
	for k := range plan.SKUsByProfile {
		profiles = append(profiles, k)
	}
	sort.Strings(profiles)
	var parts []string
	for _, k := range profiles {
		parts = append(parts, fmt.Sprintf("%s %d", k, plan.SKUsByProfile[k]))
	}
	log.Printf("  SKUs by profile: %s", strings.Join(parts, ", "))
	if n := len(plan.SkippedSKUs); n > 0 {
		log.Printf("  %d SKUs have no eligible site/zone at the pilot sites and are not stocked", n)
	}
	if n := len(plan.Unplaced); n > 0 {
		total := 0
		for _, q := range plan.Unplaced {
			total += q
		}
		log.Printf("  %d SKUs could not place %d units (no bin with room left)", n, total)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
