package main

import (
	"slices"
	"testing"
)

// TestWarehousePlanningIsAHarnessService: the planner's morning calls warehouse-planning, so it is
// in the health sweep and routed like every other context (Kong /api/<service> in gateway mode,
// WAREHOUSE_PLANNING_BASE_URL from env.sh on the local harness).
func TestWarehousePlanningIsAHarnessService(t *testing.T) {
	if !slices.Contains(allServices, svcPlanning) {
		t.Fatalf("allServices %v does not list %s: the health sweep would not check it", allServices, svcPlanning)
	}
	if got := newAPI(config{gateway: "http://edge:8000/api/"}, nil).Base[svcPlanning]; got != "http://edge:8000/api/warehouse-planning" {
		t.Fatalf("gateway base = %q, want http://edge:8000/api/warehouse-planning", got)
	}
	t.Setenv("WAREHOUSE_PLANNING_BASE_URL", "")
	if got := newAPI(config{}, nil).Base[svcPlanning]; got != "http://localhost:8100" {
		t.Fatalf("local default = %q, want env.sh's http://localhost:8100", got)
	}
	t.Setenv("WAREHOUSE_PLANNING_BASE_URL", "http://planning:9000")
	if got := newAPI(config{}, nil).Base[svcPlanning]; got != "http://planning:9000" {
		t.Fatalf("local base = %q, want the WAREHOUSE_PLANNING_BASE_URL override", got)
	}
}
