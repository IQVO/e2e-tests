package main

import (
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strings"
)

// The simulated building is a site of its own (-site, default SIM1). Its
// customer orders carry that site as their caller-assigned
// fulfillmentSiteId (order-management ADR 0044) and its compressed truck
// times are published as THAT site's CPT schedule (-cpt-site, defaulting
// to -site), so order-management promises the simulated orders against
// the simulated trucks and never touches the storefront's fulfilment site.
//
// The storefront's fulfilment site has two ids (ADR 0001 D1, declared in
// the fixture):
//
//   - siteId (site-1): the network's id of the building, the node's
//     vendorNodeCode and network-fulfillment's SITE_ID. It is live and
//     network-connected: network-fulfillment caps every listing at the
//     process-path capacity before that site's NEXT cutoff. Writing the
//     simulator's UTC truck minutes onto it turns every capability offer
//     THROUGHPUT_CONSTRAINED 0 and the storefront shows every product
//     unavailable until make provision-storefront restores the schedule.
//   - warehouseSiteId (AUS3): the facility-layout site of the warehouse
//     building serving that node. network-fulfillment sends it as the
//     storefront orders' fulfillmentSiteId, order-management promises
//     against its CPT schedule and outbound-shipping cuts loads on it, and
//     its floor and stock are the shoppers'. Writing the simulator's
//     schedule there, or running the sim's orders there, mixes the two.
//
// siteGuard refuses both before the simulator writes anything.

// defaultStorefrontSiteFixture is where the storefront's fulfilment site is
// declared (cmd/seed-storefront's WMS step), relative to the e2e-tests root
// (scripts/07 runs the binary from there and also passes it absolute).
const defaultStorefrontSiteFixture = "fixtures/storefront/fulfilment-site.json"

// siteCodePattern is order-management's fulfillmentSiteId pattern
// (apis/openapi.yaml ReceiveOrderRequest.fulfillmentSiteId, ADR 0044) and
// facility-layout's site-code shape: uppercase letters and digits only.
var siteCodePattern = regexp.MustCompile(`^[A-Z0-9]+$`)

// storefrontSites are the two ids of the storefront's fulfilment site.
type storefrontSites struct {
	// NodeSiteID is the network's id of the building (siteId: the node's
	// vendorNodeCode, network-fulfillment's SITE_ID).
	NodeSiteID string
	// WarehouseSiteID is the facility-layout site of the warehouse building
	// serving the node (warehouseSiteId). Empty in a fixture that predates
	// ADR 0001 D1: the guard then protects the node id alone.
	WarehouseSiteID string
}

// readStorefrontSites reads the ids of the storefront's fulfilment-site
// fixture. A fixture without warehouseSiteId still loads; one without siteId
// does not.
func readStorefrontSites(path string) (storefrontSites, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return storefrontSites{}, err
	}
	var f struct {
		SiteID          string `json:"siteId"`
		WarehouseSiteID string `json:"warehouseSiteId"`
	}
	if err := json.Unmarshal(b, &f); err != nil {
		return storefrontSites{}, fmt.Errorf("decode %s: %w", path, err)
	}
	if strings.TrimSpace(f.SiteID) == "" {
		return storefrontSites{}, fmt.Errorf("%s has no siteId", path)
	}
	return storefrontSites{NodeSiteID: strings.TrimSpace(f.SiteID), WarehouseSiteID: strings.TrimSpace(f.WarehouseSiteID)}, nil
}

// siteGuard decides, before the first write, whether the run may proceed.
// liveSites reads the storefront's two site ids (the network's node and the
// warehouse building serving it). A non-nil error means refuse to run;
// warnings are printed and the run goes on.
func siteGuard(cfg config, liveSites func() (storefrontSites, error)) (warnings []string, err error) {
	if !siteCodePattern.MatchString(cfg.siteCode) {
		return nil, fmt.Errorf("-site %q is not a site code (uppercase letters and digits only): order-management rejects such a fulfillmentSiteId on every POST /orders (400 invalid-fulfillment-site-id, ADR 0044)", cfg.siteCode)
	}
	if strings.TrimSpace(cfg.cptSiteID) == "" {
		return nil, fmt.Errorf("-cpt-site is empty: it must name the site whose CPT schedule the simulated trucks replace (default: -site)")
	}
	if cfg.cptSiteID != cfg.siteCode {
		warnings = append(warnings, fmt.Sprintf("-cpt-site %q differs from -site %q: the orders carry fulfillmentSiteId %q, so order-management promises them against %s's CPT schedule, not the simulated trucks written to %s", cfg.cptSiteID, cfg.siteCode, cfg.siteCode, cfg.siteCode, cfg.cptSiteID))
	}
	if cfg.allowLiveSite {
		return append(warnings, "-allow-live-site: the storefront's fulfilment-site guard is OFF; if -site or -cpt-site is the storefront's node (siteId) its CPT schedule is overwritten and every storefront product shows unavailable, and if it is the storefront's warehouse building (warehouseSiteId) the storefront orders are promised and shipped against the simulator's trucks, until make provision-storefront runs again (one PUT per overwritten id)"), nil
	}
	live, err := liveSites()
	if err != nil {
		return nil, fmt.Errorf("cannot read the storefront's fulfilment sites from %s (%v), so the guard cannot tell whether -cpt-site %q is the storefront's node or its warehouse building: run from the e2e-tests root (scripts/07-run-warehouse-day.sh does), point -storefront-site-fixture at the fixture, or pass -allow-live-site", cfg.storefrontSiteFixture, err, cfg.cptSiteID)
	}
	for _, s := range []struct{ flag, value string }{{"-site", cfg.siteCode}, {"-cpt-site", cfg.cptSiteID}} {
		if live.NodeSiteID != "" && strings.EqualFold(s.value, live.NodeSiteID) {
			return nil, fmt.Errorf("%s %q is the storefront's fulfilment site (siteId of %s: the network node's id, network-fulfillment's SITE_ID): the simulator would replace its CPT schedule with compressed UTC truck times and promise its orders there, every capability offer would drop to THROUGHPUT_CONSTRAINED 0 and the storefront would show every product unavailable until make provision-storefront ran again. Run on the simulator's own site (-site, default SIM1; -cpt-site defaults to it), or pass -allow-live-site to do it on purpose", s.flag, s.value, cfg.storefrontSiteFixture)
		}
		if live.WarehouseSiteID != "" && strings.EqualFold(s.value, live.WarehouseSiteID) {
			return nil, fmt.Errorf("%s %q is the storefront's warehouse building (warehouseSiteId of %s: the facility-layout site that serves node %s, where network-fulfillment sends the storefront's orders): the simulator would replace its CPT schedule with compressed UTC truck times, so order-management would promise the shoppers' orders and outbound-shipping cut their loads against simulated trucks, and the simulated floor would share its map, stock and work pool with them until make provision-storefront ran again. Run on the simulator's own site (-site, default SIM1; -cpt-site defaults to it), or pass -allow-live-site to do it on purpose", s.flag, s.value, cfg.storefrontSiteFixture, live.NodeSiteID)
		}
	}
	return warnings, nil
}
