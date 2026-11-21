package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

// fakeFacilityLayout answers the three facility-layout reads loadSites makes, in the service's
// camelCase list shape ({items, nextCursor}).
func fakeFacilityLayout(t *testing.T) *httptest.Server {
	t.Helper()
	slot := func(code, zoneID, locationType string) map[string]any {
		return map[string]any{
			"locationCode": code, "zoneId": zoneID, "aisleId": zoneID + "-A01",
			"coordinates":  map[string]any{"x": 1, "y": 2, "z": 0},
			"locationType": locationType, "role": "Storage", "status": "Active",
			"capacity": map[string]any{"maxWeightKg": 500, "maxVolumeM3": 1.2},
		}
	}
	routes := map[string]any{
		"/sites": map[string]any{"items": []any{
			map[string]any{"siteCode": "WH1", "siteRole": "FulfillmentCenter", "status": "Active"},
		}},
		"/sites/WH1/zones": map[string]any{"items": []any{
			map[string]any{"zoneId": "z-amb", "siteCode": "WH1", "areaCode": "STOR", "zoneCode": "AMB",
				"temperatureClass": "Ambient", "hazmat": false, "status": "Active"},
			map[string]any{"zoneId": "z-asrs", "siteCode": "WH1", "areaCode": "AUTO", "zoneCode": "ASRS",
				"temperatureClass": "Ambient", "hazmat": false, "status": "Active"},
		}},
		"/sites/WH1/locations": map[string]any{"items": []any{
			slot("WH1-AUTO-ASRS-ASRS1-00-00-S", "z-asrs", "AutomatedStorage"),
			slot("WH1-STOR-AMB-A01-01-01-A", "z-amb", "PalletRack"),
		}},
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, ok := routes[r.URL.Path]
		if !ok {
			writeJSON(w, http.StatusNotFound, map[string]any{"type": "https://errors.example/not-found", "status": 404})
			return
		}
		writeJSON(w, http.StatusOK, body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// carry (WCS 05a Task 1 review): a slot decoded from facility-layout's JSON with locationType
// AutomatedStorage (ADR 0048) is never a seeding bin; the ordinary storage slot still is.
func TestLoadSitesSkipsAnAutomatedStorageSlotFromFacilityLayout(t *testing.T) {
	srv := fakeFacilityLayout(t)
	sites, err := loadSites(context.Background(), &reader{base: srv.URL, http: srv.Client()}, []string{"WH1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(sites) != 1 {
		t.Fatalf("sites = %+v, want WH1 only", sites)
	}
	var ids []string
	for _, b := range sites[0].Bins {
		ids = append(ids, b.ID)
	}
	if len(ids) != 1 || ids[0] != "WH1-STOR-AMB-A01-01-01-A" {
		t.Fatalf("bins = %v, want only WH1-STOR-AMB-A01-01-01-A (the AS/RS slot must be skipped)", ids)
	}
}
