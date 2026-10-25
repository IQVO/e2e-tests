package main

// order-management ADR 0044: an order may carry an OPTIONAL, caller-assigned
// fulfillmentSiteId. The SiteSkuDemandChanged it projects goes to that site
// (assignment_version order-site-v1); an order without one goes to the static
// site (static-site-v1) exactly as before. These steps drive that through the
// real POST /orders and observe the real event on the order-management topic.

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/cucumber/godog"
)

const (
	orderSiteVersion  = "order-site-v1"
	staticSiteVersion = "static-site-v1"
	invalidSiteSlug   = "invalid-fulfillment-site-id"
)

// placeOrderAtSite issues POST /orders for one line. site "" omits the field
// (the body is then exactly what a pre-ADR-0044 caller sends).
func (w *world) placeOrderAtSite(qty int, sku, site string) error {
	body := map[string]any{
		"lines":                []map[string]any{{"sku": w.rs(sku), "quantity": qty}},
		"allowPartialShipment": false,
	}
	if site != "" {
		body["fulfillmentSiteId"] = w.rs(site)
	}
	if err := w.doJSON(http.MethodPost, orderBaseURL+"/orders", body); err != nil {
		return err
	}
	if w.last.status/100 == 2 {
		if id, _ := w.last.json()["id"].(string); id != "" {
			w.orderID = id
		}
	}
	return nil
}

// demandProjected waits for a SiteSkuDemandChanged for sku/units of this
// order and returns its payload; accept checks the site once it is found.
func (w *world) demandProjected(units int, sku string) (map[string]any, error) {
	sku = w.rs(sku)
	order := w.orderID
	return expectKafkaEvent(topicOrderEvents, typeSiteSkuDemandChanged, func(p map[string]any) bool {
		return p["source_order_id"] == order && p["sku"] == sku
	}, fmt.Sprintf("SiteSkuDemandChanged of order %s for %s", order, sku))
}

func registerPhase5OrderSiteSteps(sc *godog.ScenarioContext, w *world) {
	sc.Step(`^I place an order for (\d+) units? of SKU "([^"]*)" to be fulfilled from site "([^"]*)" in order-management$`,
		func(qty int, sku, site string) error { return w.placeOrderAtSite(qty, sku, site) })

	sc.Step(`^I place an order for (\d+) units? of SKU "([^"]*)" naming no site in order-management$`,
		func(qty int, sku string) error { return w.placeOrderAtSite(qty, sku, "") })

	sc.Step(`^I place an order for (\d+) units? of SKU "([^"]*)" naming the malformed site "([^"]*)" in order-management$`,
		func(qty int, sku, site string) error {
			// Sent verbatim: w.rs would not change it, and the point is that it is invalid.
			return w.doJSON(http.MethodPost, orderBaseURL+"/orders", map[string]any{
				"lines":                []map[string]any{{"sku": w.rs(sku), "quantity": qty}},
				"allowPartialShipment": false,
				"fulfillmentSiteId":    site,
			})
		})

	sc.Step(`^order-management projects (\d+) demanded units of SKU "([^"]*)" to site "([^"]*)" with assignment version "([^"]*)"$`,
		func(units int, sku, site, version string) error {
			p, err := w.demandProjected(units, sku)
			if err != nil {
				return err
			}
			if got, want := fmt.Sprint(p["site_id"]), w.rs(site); got != want {
				return fmt.Errorf("projected site_id = %q, want %q (payload %v)", got, want, p)
			}
			return assertDemandUnitsAndVersion(p, units, version)
		})

	sc.Step(`^order-management projects (\d+) demanded units of SKU "([^"]*)" to a site other than "([^"]*)" with assignment version "([^"]*)"$`,
		func(units int, sku, site, version string) error {
			p, err := w.demandProjected(units, sku)
			if err != nil {
				return err
			}
			if got := fmt.Sprint(p["site_id"]); got == "" || got == w.rs(site) {
				return fmt.Errorf("projected site_id = %q, want a site other than %q (payload %v)", got, w.rs(site), p)
			}
			return assertDemandUnitsAndVersion(p, units, version)
		})

	sc.Step(`^inventory-storage holds (\d+) loose units of SKU "([^"]*)" in a seeded forward bin at site "([^"]*)"$`,
		func(units int, sku, site string) error {
			// Demand is emitted only for lines that ALLOCATE (order-management
			// ADR 0035), so the order needs real, usable stock.
			if err := w.seededBins(site); err != nil {
				return err
			}
			bin := w.mcs().looseBin
			if err := w.receiveStock(units, w.rs(sku)); err != nil {
				return err
			}
			// The bin's handling unit reaches inventory-storage over Kafka; the
			// stow is refused until it lands, so retry for a short while.
			var err error
			for i := 0; i < 30; i++ {
				if err = w.stowStock(units, w.rs(sku), bin); err == nil {
					return nil
				}
				time.Sleep(time.Second)
			}
			return fmt.Errorf("stow %d of %s into %s: %w", units, w.rs(sku), bin, err)
		})

	sc.Step(`^the response is a problem of type "([^"]*)"$`, func(slug string) error {
		got, _ := w.last.json()["type"].(string)
		if !strings.HasSuffix(got, "/"+slug) {
			return fmt.Errorf("problem type = %q, want one ending /%s (body %s)", got, slug, w.last.body)
		}
		return nil
	})

	sc.Step(`^the order in order-management reports the fulfillment site "([^"]*)"$`, func(site string) error {
		if err := w.doJSON(http.MethodGet, orderBaseURL+"/orders/"+w.orderID, nil); err != nil {
			return err
		}
		if got, want := fmt.Sprint(w.last.json()["fulfillmentSiteId"]), w.rs(site); got != want {
			return fmt.Errorf("GET /orders/%s fulfillmentSiteId = %q, want %q (body %s)", w.orderID, got, want, w.last.body)
		}
		return nil
	})

	sc.Step(`^the order in order-management reports no fulfillment site$`, func() error {
		if err := w.doJSON(http.MethodGet, orderBaseURL+"/orders/"+w.orderID, nil); err != nil {
			return err
		}
		if v, present := w.last.json()["fulfillmentSiteId"]; present && v != nil {
			return fmt.Errorf("GET /orders/%s fulfillmentSiteId = %v, want absent or null", w.orderID, v)
		}
		return nil
	})
}

func assertDemandUnitsAndVersion(p map[string]any, units int, version string) error {
	if got, _ := p["demanded_units"].(float64); int(got) != units {
		return fmt.Errorf("demanded_units = %v, want %d (payload %v)", p["demanded_units"], units, p)
	}
	if got := fmt.Sprint(p["assignment_version"]); got != version {
		return fmt.Errorf("assignment_version = %q, want %q (payload %v)", got, version, p)
	}
	return nil
}
