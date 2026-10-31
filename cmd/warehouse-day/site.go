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
// The storefront's fulfilment site (site-1, network-fulfillment's SITE_ID)
// is live and network-connected: network-fulfillment caps every listing at
// the process-path capacity before that site's NEXT cutoff. Writing the
// simulator's UTC truck minutes onto it turns every capability offer
// THROUGHPUT_CONSTRAINED 0 and the storefront shows every product
// unavailable until make provision-storefront restores the schedule.
// siteGuard refuses that before the simulator writes anything.

// defaultStorefrontSiteFixture is where the storefront's fulfilment site is
// declared (cmd/seed-storefront's WMS step), relative to the e2e-tests root
// (scripts/07 runs the binary from there and also passes it absolute).
const defaultStorefrontSiteFixture = "fixtures/storefront/fulfilment-site.json"

// siteCodePattern is order-management's fulfillmentSiteId pattern
// (apis/openapi.yaml ReceiveOrderRequest.fulfillmentSiteId, ADR 0044) and
// facility-layout's site-code shape: uppercase letters and digits only.
var siteCodePattern = regexp.MustCompile(`^[A-Z0-9]+$`)

// storefrontSiteID reads the siteId of the storefront's fulfilment-site
// fixture.
func storefrontSiteID(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	var f struct {
		SiteID string `json:"siteId"`
	}
	if err := json.Unmarshal(b, &f); err != nil {
		return "", fmt.Errorf("decode %s: %w", path, err)
	}
	if strings.TrimSpace(f.SiteID) == "" {
		return "", fmt.Errorf("%s has no siteId", path)
	}
	return strings.TrimSpace(f.SiteID), nil
}

// siteGuard decides, before the first write, whether the run may proceed.
// liveSite reads the storefront's fulfilment site id. A non-nil error means
// refuse to run; warnings are printed and the run goes on.
func siteGuard(cfg config, liveSite func() (string, error)) (warnings []string, err error) {
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
		return append(warnings, "-allow-live-site: the storefront's fulfilment-site guard is OFF; if -site or -cpt-site is the storefront's site, its CPT schedule is overwritten and every storefront product shows unavailable until make provision-storefront runs again"), nil
	}
	live, err := liveSite()
	if err != nil {
		return nil, fmt.Errorf("cannot read the storefront's fulfilment site from %s (%v), so the guard cannot tell whether -cpt-site %q is the live storefront site: run from the e2e-tests root (scripts/07-run-warehouse-day.sh does), point -storefront-site-fixture at the fixture, or pass -allow-live-site", cfg.storefrontSiteFixture, err, cfg.cptSiteID)
	}
	for _, s := range []struct{ flag, value string }{{"-cpt-site", cfg.cptSiteID}, {"-site", cfg.siteCode}} {
		if strings.EqualFold(s.value, live) {
			return nil, fmt.Errorf("%s %q is the storefront's fulfilment site (siteId of %s): the simulator would replace its CPT schedule with compressed UTC truck times and promise its orders there, every capability offer would drop to THROUGHPUT_CONSTRAINED 0 and the storefront would show every product unavailable until make provision-storefront ran again. Run on the simulator's own site (-site, default SIM1; -cpt-site defaults to it), or pass -allow-live-site to do it on purpose", s.flag, s.value, cfg.storefrontSiteFixture)
		}
	}
	return warnings, nil
}
