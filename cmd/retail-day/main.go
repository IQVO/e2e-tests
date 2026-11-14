// Command retail-day is the one execution that simulates a warehouse, the retail
// network and the storefront together: shoppers buy on the storefront, the network
// turns the order into a purchase order, the warehouse floor picks, packs and ships
// it, and the network carries it to the doorstep (docs/adr/0001-retail-day.md,
// docs/retail-day-journey.md).
//
// STATUS: the preflight is complete; the day itself is not. It arrives with the
// shopper, outbound and transport actor packages (internal/sim/...). Until then the
// command refuses to run the day and says so, instead of printing a green report
// for work it did not do.
//
// The preflight only reads. It fails closed: an answer it cannot read is a failed
// check. Exit codes: 0 every check passed, 1 a check failed, 2 usage or not built.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/claudioed/warehouse-e2e/internal/retailday"
	"github.com/claudioed/warehouse-e2e/internal/sim"
)

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

func run(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("retail-day", flag.ContinueOnError)
	fs.SetOutput(stderr)
	gateway := fs.String("gateway", envOr("WAREHOUSE_GATEWAY", "http://localhost:8000/api"), "Kong base of the k3s cluster (k3s-edges.sh forwards it to localhost:8000)")
	storefront := fs.String("storefront", envOr("STOREFRONT_BASE_URL", "http://storefront.localhost/api/storefront"), "base URL of the storefront BFF")
	netSite := fs.String("network-site", envOr("RETAIL_DAY_NETWORK_SITE", "site-1"), "the id the network knows the fulfilment node by (vendorNodeCode, network-fulfillment SITE_ID)")
	building := fs.String("building", envOr("RETAIL_DAY_BUILDING", "AUS3"), "the facility-layout site that serves the node (ADR 0001 D1)")
	vendor := fs.String("vendor", envOr("RETAIL_DAY_VENDOR", "V-WS"), "vendor id of the fulfilment node")
	minProducts := fs.Int("min-products", 10, "purchasable products the storefront must list")
	handling := fs.Duration("handling", 0, "override the node's handling time (0 = read it from the node profile)")
	ackBudget := fs.Duration("ack-budget", 30*time.Minute, "order-sourcing's release budget before handling (a constant in its code)")
	requireWindow := fs.Bool("require-window", false, "fail the preflight outside the release window")
	waitWindow := fs.Bool("wait-for-window", false, "after a passing preflight, sleep until the release window opens")
	maxWait := fs.Duration("max-wait", 8*time.Hour, "longest -wait-for-window may sleep; a further window is refused")
	preflightOnly := fs.Bool("preflight", false, "run the read-only preflight and exit")
	asJSON := fs.Bool("json", false, "print the preflight as JSON")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	base := sim.GatewayBase(*gateway, retailday.AllGateway())
	base[retailday.BFF] = *storefront
	client := sim.NewClient(base, sim.NewCallStats())
	cfg := retailday.Config{
		NetworkSite: *netSite, BuildingSite: *building, VendorID: *vendor, MinProducts: *minProducts,
		RequireWindow: *requireWindow, Handling: *handling, AckBudget: *ackBudget,
	}
	checks := retailday.Preflight(ctx, client, cfg)
	if *asJSON {
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(map[string]any{"checks": checks, "failed": retailday.Failed(checks)})
	} else {
		printChecks(stdout, checks)
	}
	if n := retailday.Failed(checks); n > 0 {
		fmt.Fprintf(stderr, "retail-day: preflight FAILED (%d of %d checks)\n", n, len(checks))
		return 1
	}
	if *preflightOnly {
		return 0
	}
	if *waitWindow {
		if err := waitForWindow(ctx, client, cfg, *maxWait, stdout); err != nil {
			fmt.Fprintf(stderr, "retail-day: %v\n", err)
			return 1
		}
	}
	fmt.Fprintln(stderr, "retail-day: the day itself is not built yet (shopper, outbound and transport actors are separate packages still to be wired); nothing was run. Use -preflight.")
	return 2
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

// waitForWindow sleeps until the release window opens; inside it, it returns at once.
// It re-reads the window after waking (a schedule can change while a run waits) and
// refuses to sleep longer than maxWait.
func waitForWindow(ctx context.Context, cl *sim.Client, cfg retailday.Config, maxWait time.Duration, stdout io.Writer) error {
	for range 3 {
		w, err := retailday.NextWindow(ctx, cl, cfg)
		if err != nil {
			return err
		}
		if w.InWindow {
			fmt.Fprintf(stdout, "%s\n", w)
			return nil
		}
		if w.Wait > maxWait {
			return fmt.Errorf("the next release window is in %s, more than -max-wait %s", w.Wait.Round(time.Minute), maxWait)
		}
		fmt.Fprintf(stdout, "waiting: %s\n", w)
		t := time.NewTimer(w.Wait)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C:
		}
	}
	return fmt.Errorf("the release window did not open after three waits")
}
