package main

import (
	simlib "github.com/claudioed/warehouse-e2e/internal/sim"
)

// Service names double as the gateway path segment (Kong routes
// /api/<service>/...) and as the metrics prefix.
const (
	svcFacility    = "facility-layout"
	svcInventory   = "inventory-storage"
	svcWES         = "wes-work-planning"
	svcFulfillment = "fulfillment-execution"
	svcWorkforce   = "workforce-management"
	svcOrder       = "order-management"
	svcProcessPath = "process-path-management"
	svcLabor       = "labor-performance"
	// svcProductMaster owns SKU classification and the physical profile
	// (product-master ADR 0001/0003); inventory-storage and the readers
	// only keep local copies fed by its events.
	svcProductMaster = "product-master"
)

var allServices = []string{svcFacility, svcProductMaster, svcInventory, svcWES, svcFulfillment, svcWorkforce, svcOrder, svcProcessPath, svcLabor}

func newAPI(cfg config, stats *callStats) *api {
	var base map[string]string
	if cfg.gateway != "" {
		base = simlib.GatewayBase(cfg.gateway, allServices)
	} else {
		base = map[string]string{}
		base[svcFacility] = envOr("FACILITY_BASE_URL", "http://localhost:8081")
		base[svcInventory] = envOr("INVENTORY_BASE_URL", "http://localhost:8082")
		base[svcWES] = envOr("WES_BASE_URL", "http://localhost:8083")
		base[svcFulfillment] = envOr("FULFILLMENT_BASE_URL", "http://localhost:8084")
		base[svcWorkforce] = envOr("WORKFORCE_BASE_URL", "http://localhost:8085")
		base[svcOrder] = envOr("ORDER_BASE_URL", "http://localhost:8086")
		base[svcProcessPath] = envOr("PROCESS_PATH_BASE_URL", "http://localhost:8087")
		base[svcLabor] = envOr("LABOR_BASE_URL", "http://localhost:8088")
		base[svcProductMaster] = envOr("PRODUCT_MASTER_BASE_URL", "http://localhost:8090")
	}
	return simlib.NewClient(base, stats)
}
