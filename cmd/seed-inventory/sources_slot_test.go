package main

import "testing"

func TestIsStockSlotKeepsAnActiveStorageSlotWithVolume(t *testing.T) {
	if !isStockSlot("Active", "AMB", "PalletRack", 1.2) {
		t.Fatal("an Active PalletRack slot with volume must be a stock slot")
	}
}

func TestIsStockSlotSkipsAnAutomatedStorageLocation(t *testing.T) {
	if isStockSlot("Active", "ASRS", automatedStorageType, 40) {
		t.Fatal("an AutomatedStorage slot holds totes only (inventory-storage ADR 0054): never a loose-stock bin")
	}
}

func TestIsStockSlotKeepsTheExistingExclusions(t *testing.T) {
	cases := []struct {
		name, status, zone, typ string
		volume                  float64
	}{
		{"inactive", "Decommissioned", "AMB", "PalletRack", 1},
		{"amnesty zone", "Active", "AMN", "Amnesty", 1},
		{"no volume", "Active", "AMB", "PalletRack", 0},
	}
	for _, c := range cases {
		if isStockSlot(c.status, c.zone, c.typ, c.volume) {
			t.Fatalf("%s: must not be a stock slot", c.name)
		}
	}
}
