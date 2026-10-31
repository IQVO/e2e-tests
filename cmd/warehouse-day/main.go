// Command warehouse-day simulates one compressed operating day of the
// warehouse estate, end to end, driven ONLY through the published REST
// contracts of every bounded context (see api.go) and verified against
// their published AsyncAPI events (see verify.go).
//
// The day, in simulated time (the real wall-clock length is DAY_DURATION):
//
//	05:30  opening      building map, slots, bins, stations, process
//	                    paths, CPT schedule, labor standards, inbound
//	                    stock, forecast, labor plan, shift plan
//	06:00  shift start  associates clock in, get assigned, check in
//	06:15  order intake a demand curve of customer orders (morning ramp,
//	                    lunch peak, evening tail), held orders, cancels,
//	                    an understocked SKU that backorders
//	       floor        WES release scheduler (pull-based), pickers,
//	                    rebin/put-wall, packers, SLAM operators, a
//	                    cross-trained flex associate, supervisor rounds,
//	                    inventory control (cycle counts, replenishment,
//	                    backorder retry), system sweeps
//	11:30  breaks       two staggered lunch groups
//	~20:00 drain        intake has stopped; the floor clears the backlog
//	22:00  close        associates check out and end their shifts
//	       audit        per-order audit across every context + Kafka tap
//
// The process exits non-zero when the audit finds the warehouse did not
// actually work (lost orders, 5xx, missing events, DLQ growth, ...).
package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type config struct {
	gateway      string
	brokers      []string
	runID        string
	day          time.Duration
	drainTimeout time.Duration
	orders       int
	pickers      int
	packers      int
	slammers     int
	rebinners    int
	flex         int
	pickSeconds  float64
	packSeconds  float64
	// siteCode is the simulated building's own site: the facility-layout
	// site its map lives in and the fulfillmentSiteId its orders carry.
	siteCode string
	// cptSiteID is the process-path-management site whose CPT schedule the
	// simulated trucks replace; it defaults to siteCode (see site.go).
	cptSiteID string
	// storefrontSiteFixture declares the storefront's live fulfilment site,
	// which siteGuard refuses to write unless allowLiveSite is set.
	storefrontSiteFixture string
	allowLiveSite         bool
	buildingID            string
	// wesPickPath is the WES work pool PICK work lands in: the pathId
	// order-management stamps on order lines (its DefaultPathId "pick"),
	// which the catalogue resolves to the PICK family. WES pools by the
	// raw path id, so charge/plan/release must target it, not "PICK".
	wesPickPath string
	reportDir   string
	seed        int64
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envDur(k string, def time.Duration) time.Duration {
	if d, err := time.ParseDuration(os.Getenv(k)); err == nil {
		return d
	}
	return def
}

func envInt(k string, def int) int {
	if n, err := strconv.Atoi(os.Getenv(k)); err == nil {
		return n
	}
	return def
}

func envFloat(k string, def float64) float64 {
	if f, err := strconv.ParseFloat(os.Getenv(k), 64); err == nil {
		return f
	}
	return def
}

func loadConfig() config {
	var c config
	flag.StringVar(&c.gateway, "gateway", envOr("WAREHOUSE_GATEWAY", ""), "API gateway base (e.g. http://localhost:8000/api); empty = per-service *_BASE_URL env (e2e harness ports)")
	brokers := flag.String("brokers", envOr("KAFKA_BROKERS", "localhost:9092"), "Kafka bootstrap brokers for the event audit (comma separated; empty disables the Kafka audit)")
	flag.StringVar(&c.runID, "run", envOr("RUN_ID", fmt.Sprintf("D%s", strings.ToUpper(strconv.FormatInt(time.Now().Unix()%1_000_000, 36)))), "run id (scopes SKUs, associates, shift id)")
	flag.DurationVar(&c.day, "day", envDur("DAY_DURATION", 8*time.Minute), "wall-clock length of the simulated 06:00-22:00 day")
	flag.DurationVar(&c.drainTimeout, "drain-timeout", envDur("DRAIN_TIMEOUT", 3*time.Minute), "extra wall time allowed after 22:00 for the floor to clear")
	flag.IntVar(&c.orders, "orders", envInt("DAY_ORDERS", 150), "customer orders over the day")
	flag.IntVar(&c.pickers, "pickers", envInt("DAY_PICKERS", 5), "pickers")
	flag.IntVar(&c.packers, "packers", envInt("DAY_PACKERS", 3), "packers")
	flag.IntVar(&c.slammers, "slam", envInt("DAY_SLAM", 2), "SLAM operators")
	flag.IntVar(&c.rebinners, "rebin", envInt("DAY_REBIN", 2), "rebin / put-wall associates")
	flag.IntVar(&c.flex, "flex", envInt("DAY_FLEX", 1), "cross-trained (pick+pack) flex associates")
	flag.Float64Var(&c.pickSeconds, "pick-seconds", envFloat("DAY_PICK_SECONDS", 1.5), "wall seconds a pick takes at standard")
	flag.Float64Var(&c.packSeconds, "pack-seconds", envFloat("DAY_PACK_SECONDS", 2.0), "wall seconds a pack takes at standard")
	flag.StringVar(&c.siteCode, "site", envOr("DAY_SITE", "SIM1"), "the simulated building's own site: facility-layout site code and the fulfillmentSiteId of every order (uppercase letters and digits)")
	flag.StringVar(&c.cptSiteID, "cpt-site", envOr("DAY_CPT_SITE", ""), "process-path-management site whose CPT schedule the simulated trucks replace (empty = -site)")
	flag.StringVar(&c.storefrontSiteFixture, "storefront-site-fixture", envOr("DAY_STOREFRONT_SITE_FIXTURE", defaultStorefrontSiteFixture), "fixture declaring the storefront's live fulfilment site (siteId); the run refuses to write that site")
	flag.BoolVar(&c.allowLiveSite, "allow-live-site", false, "allow -site/-cpt-site to be the storefront's fulfilment site (overwrites its CPT schedule; re-run make provision-storefront after)")
	flag.StringVar(&c.wesPickPath, "wes-pick-path", envOr("DAY_WES_PICK_PATH", "pick"), "WES work pool for PICK work (the pathId order-management stamps on order lines)")
	flag.StringVar(&c.buildingID, "building", envOr("DAY_BUILDING", "SIM1"), "workforce-management building id")
	flag.StringVar(&c.reportDir, "report-dir", envOr("DAY_REPORT_DIR", "run"), "where the JSON audit report is written")
	flag.Int64Var(&c.seed, "seed", int64(envInt("DAY_SEED", 0)), "random seed (0 = time based)")
	flag.Parse()
	if *brokers != "" {
		c.brokers = strings.Split(*brokers, ",")
	}
	if c.seed == 0 {
		c.seed = time.Now().UnixNano()
	}
	if c.cptSiteID == "" {
		c.cptSiteID = c.siteCode
	}
	return c
}

// ---------------------------------------------------------------------
// simulated clock
// ---------------------------------------------------------------------

const (
	dayOpenHour  = 6
	dayCloseHour = 22
)

type simClock struct {
	wallStart time.Time
	wallDay   time.Duration
	simStart  time.Time
	simDay    time.Duration
}

func newSimClock(wallDay time.Duration) *simClock {
	now := time.Now()
	d := time.Date(now.Year(), now.Month(), now.Day(), dayOpenHour, 0, 0, 0, time.Local)
	return &simClock{wallStart: now, wallDay: wallDay, simStart: d, simDay: (dayCloseHour - dayOpenHour) * time.Hour}
}

// ratio is wall seconds per simulated second.
func (c *simClock) ratio() float64 { return float64(c.wallDay) / float64(c.simDay) }

func (c *simClock) now() time.Time {
	el := time.Since(c.wallStart)
	return c.simStart.Add(time.Duration(float64(el) / c.ratio()))
}

func (c *simClock) hhmm() string { return c.now().Format("15:04") }

// wallAt returns the wall time at which the sim clock reads hh:mm.
func (c *simClock) wallAt(h, m int) time.Time {
	sim := time.Duration(h-dayOpenHour)*time.Hour + time.Duration(m)*time.Minute
	return c.wallStart.Add(time.Duration(float64(sim) * c.ratio()))
}

// simDur converts a simulated duration to wall time.
func (c *simClock) simDur(d time.Duration) time.Duration {
	return time.Duration(float64(d) * c.ratio())
}

func (c *simClock) after(h, m int) bool { return !time.Now().Before(c.wallAt(h, m)) }

// ---------------------------------------------------------------------
// logging
// ---------------------------------------------------------------------

var logMu sync.Mutex
var theClock atomic.Pointer[simClock]

func logf(who, format string, args ...any) {
	logMu.Lock()
	defer logMu.Unlock()
	sim := "05:30"
	if c := theClock.Load(); c != nil {
		sim = c.hhmm()
	}
	fmt.Printf("[%s %s] %-16s %s\n", time.Now().Format("15:04:05"), sim, who, fmt.Sprintf(format, args...))
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

type float64Source interface{ Float64() float64 }

var _ float64Source = (*rand.Rand)(nil)

func jitter(r float64Source, base float64, spread float64) time.Duration {
	f := base * (1 + (r.Float64()*2-1)*spread)
	if f < 0.05 {
		f = 0.05
	}
	return time.Duration(f * float64(time.Second))
}

func main() {
	cfg := loadConfig()
	// Before any call to any service: never write the storefront's site.
	warnings, err := siteGuard(cfg, func() (string, error) { return storefrontSiteID(cfg.storefrontSiteFixture) })
	for _, w := range warnings {
		fmt.Fprintln(os.Stderr, "warehouse-day: WARNING:", w)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "warehouse-day: refusing to run:", err)
		os.Exit(2)
	}
	fmt.Printf("warehouse-day: site %s (orders' fulfillmentSiteId), CPT schedule on %s\n", cfg.siteCode, cfg.cptSiteID)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	s := newSim(cfg)
	ok := s.run(ctx)
	if !ok {
		os.Exit(1)
	}
}
