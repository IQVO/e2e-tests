// Command retail-day is the one execution that simulates a warehouse, the retail network and the
// storefront together: shoppers buy on the storefront, the network turns the order into a purchase
// order, the warehouse floor picks, packs and ships it, and the network carries it to the doorstep
// (docs/adr/0001-retail-day.md, docs/retail-day-journey.md).
//
//	retail-day -preflight        read-only: the REST preflight, then exit
//	retail-day                   the preflight, then the day (internal/retailday/day), then the report
//
// The day is played by the REAL actors (internal/sim/{shopper,floor,outbound,transport,inbound}) behind
// the day package's phase interfaces (wire*.go), linked by internal/retailday/link and judged by the
// REST collector (internal/retailday/collect). It exits 0 only when every planned order is accounted
// for, no rule failed, every phase ran and nothing the evidence could not read was left unexplained.
// The dead letters of the run are counted from the Kafka `*.dlq` topics (internal/retailday/dlq): Prepare
// snapshots their end offsets before anything is written and the audit counts what carries the run id
// since. No REST route lists them, so the broker must be reachable (-brokers, default localhost:9092,
// which k3s-edges.sh does NOT forward: the preflight names the kubectl port-forward that does). A broker
// that cannot be read, or -brokers "", makes the count unknown: the day then passes only with
// -allow-unverified, which lists it as NOT VERIFIED instead of failing the day on it (nothing else is
// excused). A dead letter that carries the run id fails the day.
//
// The preflight only reads and fails closed: an answer it cannot read is a failed check. The day writes
// (it plays shoppers and a warehouse), but its readiness steps write nothing unless -provision is given
// (see wire.go: building.prepare).
//
// Exit codes: 0 the day passed (or, with -preflight, every check passed); 1 the preflight failed, a
// phase could not be built or run, or the verdict is FAILED; 2 usage, or the run refuses to start
// (outside the release window without -start-now or -wait-for-window).
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/claudioed/warehouse-e2e/internal/retailday"
	"github.com/claudioed/warehouse-e2e/internal/retailday/day"
	"github.com/claudioed/warehouse-e2e/internal/sim"
	"github.com/claudioed/warehouse-e2e/internal/sim/shopper"
)

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// The seams main_test.go replaces: the preflight and the release window read the cluster.
var (
	runPreflight = retailday.Preflight
	findWindow   = retailday.NextWindow
)

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

// runIDPattern is what the actors accept as a run id: short, upper-case, run-unique.
var runIDPattern = regexp.MustCompile(`^[A-Z0-9]{3,12}$`)

// options is the parsed command line of one run.
type options struct {
	gateway, storefront, netSite, building, vendor     *string
	minProducts                                        *int
	handling, ackBudget                                *time.Duration
	requireWindow, waitWindow                          *bool
	maxWait                                            *time.Duration
	preflightOnly, asJSON                              *bool
	seed                                               *int64
	shoppers, shelfSize, parallel                      *int
	startNow                                           *bool
	linkTimeout                                        *time.Duration
	allowUnverified, deliveryPoint, provision, verbose *bool
	brokers, runID, fixtures, networkData              *string
	doors, staging, stowBins                           *string
}

// parseFlags defines retail-day's flags on fs and returns their bindings.
func parseFlags(fs *flag.FlagSet) *options {
	o := &options{}
	o.gateway = fs.String("gateway", envOr("WAREHOUSE_GATEWAY", "http://localhost:8000/api"), "Kong base of the k3s cluster (k3s-edges.sh forwards it to localhost:8000)")
	o.storefront = fs.String("storefront", envOr("STOREFRONT_BASE_URL", "http://storefront.localhost/api/storefront"), "base URL of the storefront BFF")
	o.netSite = fs.String("network-site", envOr("RETAIL_DAY_NETWORK_SITE", "site-1"), "the id the network knows the fulfilment node by (vendorNodeCode, network-fulfillment SITE_ID)")
	o.building = fs.String("building", envOr("RETAIL_DAY_BUILDING", "AUS3"), "the facility-layout site that serves the node (ADR 0001 D1)")
	o.vendor = fs.String("vendor", envOr("RETAIL_DAY_VENDOR", "V-WS"), "vendor id of the fulfilment node")
	o.minProducts = fs.Int("min-products", 10, "purchasable products the storefront must list")
	o.handling = fs.Duration("handling", 0, "override the node's handling time (0 = read it from the node profile)")
	o.ackBudget = fs.Duration("ack-budget", 30*time.Minute, "order-sourcing's release budget before handling (a constant in its code)")
	o.requireWindow = fs.Bool("require-window", false, "fail the preflight outside the release window")
	o.waitWindow = fs.Bool("wait-for-window", false, "after a passing preflight, sleep until the release window opens")
	o.maxWait = fs.Duration("max-wait", 8*time.Hour, "longest -wait-for-window may sleep; a further window is refused")
	o.preflightOnly = fs.Bool("preflight", false, "run the read-only preflight and exit")
	o.asJSON = fs.Bool("json", false, "print the preflight (with -preflight) or the day's report as JSON")
	o.seed = fs.Int64("seed", 0, "the day's seed: the shelf, the buyers and their baskets are a function of it (0 = one from the clock, printed in the report)")
	o.shoppers = fs.Int("shoppers", 8, "buyers: at least 5 (one delivered order plus one each of declined, cancelled, not served and in doubt)")
	o.shelfSize = fs.Int("shelf", 10, "SKUs on the day's shelf, chosen by the seed from the purchasable products")
	o.parallel = fs.Int("parallel", 4, "buyers, orders on the floor, loads at the dock and parcels in transit at once; also sizes the floor's crew")
	o.startNow = fs.Bool("start-now", false, "play the day outside the release window: the purchase-order wait then lasts until the computed release time (ADR 0001 D3), bounded by -link-timeout")
	o.linkTimeout = fs.Duration("link-timeout", 2*time.Hour, "longest the day waits for ONE purchase order to be released, acknowledged and raised in order-management")
	o.allowUnverified = fs.Bool("allow-unverified", false, "list what the evidence could not read (the run's dead letters when the broker cannot be read or -brokers is empty) as NOT VERIFIED instead of failing the day on it (nothing else is excused)")
	o.brokers = fs.String("brokers", envOr("KAFKA_BROKERS", defaultBrokers), "the ONE Kafka broker address (host:port) the run's dead letters are counted through; the k3s broker is not forwarded by k3s-edges.sh, so this needs a `kubectl port-forward svc/kafka 9092:9092` (the preflight says so). Empty: dead letters are not counted")
	o.deliveryPoint = fs.Bool("delivery-point", false, "exercise the delivery-point chain (gap G67): the FIRST delivered order fails twice (the driver reports NOT_HOME), runs alone, and the day must observe DeliveryPointFlagged on last-mile-delivery's topic (read through -brokers; without it only the advisory on the order's tracking view) and the advisory on customer-ordering's tracking view")
	o.provision = fs.Bool("provision", false, "let the readiness steps WRITE setup data: register missing -doors and -staging slots and declare a never-declared dock calendar. Never the CPT schedule (make provision-storefront), never bins")
	o.runID = fs.String("run-id", "", "run id stamped on stations, plates, seals and ASNs: 3-12 upper-case letters or digits (default RD + the clock)")
	o.fixtures = fs.String("fixtures", envOr("RETAIL_DAY_FIXTURES", "fixtures/storefront"), "directory of catalogue.json and fulfilment-site.json (the SKUs and the building's CPT schedule)")
	o.networkData = fs.String("network-data", envOr("RETAIL_DAY_NETWORK_DATA", "../warehouse-infra/scripts/data/retail-network-seed.json"), "warehouse-infra's retail-network-seed.json (SKU -> catalogue title)")
	o.doors = fs.String("doors", "", "comma-separated facility-layout OUTBOUND dock slot codes -provision may register when missing (default: use the Active ones the building has)")
	o.staging = fs.String("staging", "", "comma-separated facility-layout Staging slot codes -provision may register when missing (default: use the Active ones the building has)")
	o.stowBins = fs.String("stow-bins", "", "comma-separated inventory-storage bins the restock may stow into (default: the building's Storage slots that are registered bins with room)")
	o.verbose = fs.Bool("v", false, "log every actor's own steps to stderr")
	return o
}

func run(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("retail-day", flag.ContinueOnError)
	fs.SetOutput(stderr)
	opts := parseFlags(fs)
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() > 0 {
		fmt.Fprintf(stderr, "retail-day: unexpected argument %q\n", fs.Arg(0))
		return 2
	}
	if msg := checkBrokers(*opts.brokers); msg != "" {
		fmt.Fprintf(stderr, "retail-day: %s\n", msg)
		return 2
	}
	id := *opts.runID
	if !*opts.preflightOnly {
		if id == "" {
			id = "RD" + strings.ToUpper(strconv.FormatInt(time.Now().Unix(), 36))
		}
		if msg := checkUsage(id, *opts.shoppers, *opts.shelfSize, *opts.parallel, *opts.linkTimeout); msg != "" {
			fmt.Fprintf(stderr, "retail-day: %s\n", msg)
			return 2
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	base := sim.GatewayBase(*opts.gateway, retailday.AllGateway())
	base[retailday.BFF] = *opts.storefront
	base[shopper.Service] = *opts.storefront // the shopper actor's own key for the BFF
	client := sim.NewClient(base, sim.NewCallStats())
	cfg := retailday.Config{
		NetworkSite: *opts.netSite, BuildingSite: *opts.building, VendorID: *opts.vendor, MinProducts: *opts.minProducts,
		RequireWindow: *opts.requireWindow, Handling: *opts.handling, AckBudget: *opts.ackBudget,
	}
	if stop, code := preflightStage(ctx, client, cfg, opts, stdout, stderr); stop {
		return code
	}

	var notes []string
	if code, note := gateWindow(ctx, client, cfg, windowGate{wait: *opts.waitWindow, maxWait: *opts.maxWait, startNow: *opts.startNow, linkTimeout: *opts.linkTimeout}, stderr); code != 0 {
		return code
	} else if note != "" {
		notes = append(notes, note)
	}
	return runDay(ctx, opts, base, id, notes, stdout, stderr)
}

// preflightStage runs the read-only preflight and reports it; stop says the
// run is over, and code is then its exit status.
func preflightStage(ctx context.Context, client *sim.Client, cfg retailday.Config, opts *options, stdout, stderr io.Writer) (stop bool, code int) {
	checks := runPreflight(ctx, client, cfg)
	if retailday.Failed(checks) == 0 {
		// the broker is read only once the cluster is known to answer: a day that cannot count its dead letters
		// stops here, not after the release window was waited for
		if c, ok := checkKafka(ctx, *opts.brokers); ok {
			checks = append(checks, c)
		}
	}
	failed := retailday.Failed(checks)
	switch {
	case *opts.asJSON && (*opts.preflightOnly || failed > 0):
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(map[string]any{"checks": checks, "failed": failed})
	case !*opts.asJSON:
		printChecks(stdout, checks)
	}
	if failed > 0 {
		fmt.Fprintf(stderr, "retail-day: preflight FAILED (%d of %d checks)\n", failed, len(checks))
		return true, 1
	}
	if *opts.preflightOnly {
		return true, 0
	}
	return false, 0
}

// runDay builds the day's phases, plays the day and returns the exit code.
func runDay(ctx context.Context, opts *options, base map[string]string, id string, notes []string, stdout, stderr io.Writer) int {
	var logMu sync.Mutex
	logf := func(format string, a ...any) {
		logMu.Lock()
		defer logMu.Unlock()
		fmt.Fprintf(stderr, "retail-day: "+format+"\n", a...)
	}
	var actorLog sim.LogFunc
	if *opts.verbose {
		actorLog = func(who, format string, a ...any) { logf("%s: "+format, append([]any{who}, a...)...) }
	}
	// the day has its own client and statistics: the preflight's calls are not the day's, and the BFF's 15 s
	// confirm budget needs a client that waits longer than 15 s
	dayClient := sim.NewClient(base, sim.NewCallStats())
	dayClient.HTTP = dayHTTPClient()
	phases, release, err := buildPhases(ctx, wireConfig{
		Client: dayClient, RunID: id, Building: *opts.building, NetworkSite: *opts.netSite, VendorID: *opts.vendor,
		FixtureDir: *opts.fixtures, NetworkData: *opts.networkData, LinkTimeout: *opts.linkTimeout, Parallel: *opts.parallel,
		Provision: *opts.provision, Doors: splitList(*opts.doors), Staging: splitList(*opts.staging), StowBins: splitList(*opts.stowBins),
		Brokers: *opts.brokers, DeliveryPoint: *opts.deliveryPoint, Log: logf, ActorLog: actorLog,
	})
	if release != nil {
		defer release()
	}
	if err != nil {
		fmt.Fprintf(stderr, "retail-day: the day cannot be built: %v\n", err)
		return 1
	}

	s := *opts.seed
	if s == 0 {
		s = time.Now().Unix()
	}
	runner := day.Runner{Phases: phases, Config: day.Config{
		Plan:     day.PlanConfig{RunID: id, Seed: s, Shoppers: *opts.shoppers, ShelfSize: *opts.shelfSize},
		Parallel: *opts.parallel, AllowUnverified: *opts.allowUnverified, Notes: notes, Log: logf,
	}}
	rep := runner.Run(ctx)
	if *opts.asJSON {
		if err := rep.JSON(stdout); err != nil {
			fmt.Fprintf(stderr, "retail-day: the report could not be written: %v\n", err)
			return 1
		}
	} else {
		rep.Text(stdout)
	}
	return exitCode(rep, stderr)
}

// checkUsage is what makes a run a usage error before anything is read from the cluster.
func checkUsage(runID string, shoppers, shelf, parallel int, linkTimeout time.Duration) string {
	switch {
	case !runIDPattern.MatchString(runID):
		return fmt.Sprintf("-run-id %q must be 3-12 upper-case letters or digits", runID)
	case shoppers < 5:
		return fmt.Sprintf("-shoppers %d cannot make the day's mix: need at least 5 (one delivered order plus one each of declined, cancelled, not served and in doubt)", shoppers)
	case shelf < 1:
		return fmt.Sprintf("-shelf %d: need at least 1 SKU", shelf)
	case parallel < 1:
		return fmt.Sprintf("-parallel %d: need at least 1", parallel)
	case linkTimeout <= 0:
		return fmt.Sprintf("-link-timeout %s: need a positive duration", linkTimeout)
	}
	return ""
}

// exitCode is the day's verdict as a process status: 0 only when the report passes by its facts AND
// says so. Both are required so that neither a report whose verdict string was edited, nor one whose
// rows were dropped, can make the command exit 0.
func exitCode(rep day.Report, stderr io.Writer) int {
	if why := rep.Reasons; !rep.Passed() || rep.Verdict != day.VerdictPassed {
		fmt.Fprintf(stderr, "retail-day: the day FAILED (verdict %q, %d reason(s))\n", rep.Verdict, len(why))
		return 1
	}
	return 0
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func printChecks(w io.Writer, cs []retailday.Check) {
	for _, c := range cs {
		mark := "PASS"
		if !c.OK {
			mark = "FAIL"
		}
		fmt.Fprintf(w, "%s  %s: %s\n", mark, c.Name, c.Detail)
		if !c.OK && c.Fix != "" {
			fmt.Fprintf(w, "      fix: %s\n", c.Fix)
		}
	}
	fmt.Fprintf(w, "%d checks, %d failed\n", len(cs), retailday.Failed(cs))
}

// windowGate says how a run that is not inside the release window is to behave.
type windowGate struct {
	wait        bool
	maxWait     time.Duration
	startNow    bool
	linkTimeout time.Duration
}

// gateWindow decides whether the day may start. Inside the window it always may. Outside it: with
// -wait-for-window it sleeps until the window opens; with -start-now it starts, and says in the report
// when the orders will be released (the PO wait lasts until then, so it refuses a release further away
// than -link-timeout, which could only fail); with neither it refuses (exit 2). A window that cannot be
// computed fails the run (exit 1). The note is "" when nothing needs saying.
func gateWindow(ctx context.Context, cl *sim.Client, cfg retailday.Config, g windowGate, stderr io.Writer) (code int, note string) {
	if g.wait {
		if err := waitForWindow(ctx, cl, cfg, g.maxWait, stderr); err != nil {
			fmt.Fprintf(stderr, "retail-day: %v\n", err)
			return 1, ""
		}
		return 0, ""
	}
	w, err := findWindow(ctx, cl, cfg)
	if err != nil {
		fmt.Fprintf(stderr, "retail-day: the release window cannot be computed: %v\n", err)
		return 1, ""
	}
	if w.InWindow {
		return 0, ""
	}
	switch {
	case !g.startNow:
		fmt.Fprintf(stderr, "retail-day: refusing to start outside the release window (%s): orders placed now are released later and the day would wait for them; pass -wait-for-window to sleep until it opens or -start-now to play the day anyway (ADR 0001 D3)\n", w)
		return 2, ""
	case w.Wait >= g.linkTimeout:
		fmt.Fprintf(stderr, "retail-day: -start-now: the orders would be released in %s, which is not before -link-timeout %s ends: raise -link-timeout past it or wait for the window (%s)\n", w.Wait.Round(time.Minute), g.linkTimeout, w)
		return 2, ""
	}
	return 0, fmt.Sprintf("started outside the release window with -start-now (%s): orders placed now are released in about %s, so the purchase-order wait lasts until then (-link-timeout %s)", w, w.Wait.Round(time.Minute), g.linkTimeout)
}

// waitForWindow sleeps until the release window opens; inside it, it returns at once.
// It re-reads the window after waking (a schedule can change while a run waits) and
// refuses to sleep longer than maxWait. Progress goes to w (stderr): it is not the report.
func waitForWindow(ctx context.Context, cl *sim.Client, cfg retailday.Config, maxWait time.Duration, w io.Writer) error {
	for range 3 {
		win, err := findWindow(ctx, cl, cfg)
		if err != nil {
			return err
		}
		if win.InWindow {
			fmt.Fprintf(w, "retail-day: %s\n", win)
			return nil
		}
		if win.Wait > maxWait {
			return fmt.Errorf("the next release window is in %s, more than -max-wait %s", win.Wait.Round(time.Minute), maxWait)
		}
		fmt.Fprintf(w, "retail-day: waiting: %s\n", win)
		t := time.NewTimer(win.Wait)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C:
		}
	}
	return fmt.Errorf("the release window did not open after three waits")
}
