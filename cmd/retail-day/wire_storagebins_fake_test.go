package main

import (
	"context"
	"net/http"
	"reflect"
	"testing"
)

// facilitySlot is a facility-layout LocationSlot as GET /sites/{siteCode}/locations returns it
// (camelCase, the required properties of the LocationSlot schema).
func facilitySlot(code, zoneID, locationType, status string) map[string]any {
	return map[string]any{
		"locationCode": code, "zoneId": zoneID, "aisleId": zoneID + "-A01",
		"coordinates":  map[string]any{"x": 1, "y": 2, "z": 0},
		"locationType": locationType, "role": "Storage", "activities": []string{},
		"capacity": map[string]any{"maxWeightKg": 500, "maxVolumeM3": 1.2}, "status": status,
	}
}

// carry (WCS 05a Task 1 review): the Storage listing decoded from facility-layout's JSON through
// storageBins never offers the AS/RS logical location (locationType AutomatedStorage, facility-layout
// ADR 0048) as a restock bin; inventory-storage would refuse loose stock there (ADR 0054).
func TestStorageBinsSkipsAnAutomatedStorageSlotFromFacilityLayout(t *testing.T) {
	f := newFakeCluster(t, map[string]http.HandlerFunc{
		"GET facility-layout/sites/WH1/locations": respondJSON(page(
			facilitySlot("WH1-STOR-AMB-A01-01-01-B", "z-amb", "PalletRack", "Active"),
			facilitySlot("WH1-AUTO-ASRS-ASRS1-00-00-S", "z-asrs", "AutomatedStorage", "Active"),
			facilitySlot("WH1-STOR-AMB-A01-01-01-A", "z-amb", "PalletRack", "Active"),
			facilitySlot("WH1-STOR-AMB-A01-01-01-C", "z-amb", "PalletRack", "Decommissioned"),
		)),
	})
	got, err := storageBins(context.Background(), f.client(), "WH1")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"WH1-STOR-AMB-A01-01-01-A", "WH1-STOR-AMB-A01-01-01-B"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("storageBins = %v, want %v (requests %v)", got, want, f.requests())
	}
}
