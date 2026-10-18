package main

import (
	"fmt"
	"strings"
	"testing"
)

func mkBins(site, zone, temp string, hazmat bool, n int, volL float64, wtG int64) []Bin {
	var out []Bin
	for i := 1; i <= n; i++ {
		out = append(out, Bin{ID: fmt.Sprintf("%s-STOR1-%s-A01-%02d-01-A", site, zone, i), ZoneCode: zone, Temp: temp, Hazmat: hazmat, MaxVolumeL: volL, MaxWeightG: wtG})
	}
	return out
}

func testSites() []Site {
	var robotics, nonsort, fresh, micro []Bin
	robotics = append(robotics, mkBins("R1", "POD", "Ambient", false, 40, 120, 30_000)...)
	robotics = append(robotics, mkBins("R1", "HAZ", "Ambient", true, 10, 400, 150_000)...)
	robotics = append(robotics, mkBins("R1", "RSV", "Ambient", false, 10, 2400, 1_200_000)...)
	nonsort = append(nonsort, mkBins("N1", "RSV", "Ambient", false, 20, 2400, 1_200_000)...)
	nonsort = append(nonsort, mkBins("N1", "BLK", "Ambient", false, 5, 6000, 2_500_000)...)
	fresh = append(fresh, mkBins("F1", "CHL", "Chilled", false, 10, 2400, 1_200_000)...)
	fresh = append(fresh, mkBins("F1", "FRZ", "Frozen", false, 10, 2400, 1_200_000)...)
	fresh = append(fresh, mkBins("F1", "SHF", "Ambient", false, 20, 400, 150_000)...)
	micro = append(micro, mkBins("M1", "SHF", "Ambient", false, 10, 400, 150_000)...)
	micro = append(micro, mkBins("M1", "CHL", "Chilled", false, 5, 2400, 1_200_000)...)
	return []Site{
		{Code: "R1", Role: "ROBOTICS_SORTABLE_FC", Bins: robotics},
		{Code: "N1", Role: "NON_SORTABLE_FC", Bins: nonsort},
		{Code: "F1", Role: "FRESH_GROCERY_FC", Bins: fresh},
		{Code: "M1", Role: "MICRO_FC", Bins: micro},
	}
}

func testProducts() []Product {
	return []Product{
		{SKU: "ABO-SMALL", HasDims: true, VolumeL: 0.5, WeightG: 300},
		{SKU: "ABO-TINY", HasDims: true, VolumeL: 0.05, WeightG: 20},
		{SKU: "WANDS-SOFA", HasDims: true, VolumeL: 1500, WeightG: 60_000, Tags: []string{"Oversized"}},
		{SKU: "ABO-BATT", HasDims: true, VolumeL: 0.02, WeightG: 40, Tags: []string{"Hazmat"}, DotClass: 9},
		{SKU: "ABO-BLEACH", HasDims: true, VolumeL: 1.2, WeightG: 1300, Tags: []string{"Hazmat"}, DotClass: 8},
		{SKU: "4006381333931", Temp: "Frozen", Tags: []string{"TemperatureSensitive"}},
		{SKU: "4006381333948", Temp: "Chilled", Tags: []string{"TemperatureSensitive"}},
		{SKU: "4006381333955"}, // ambient grocery without dimensions
	}
}

func binsOf(plan *Plan, sku string) []*BinPlan {
	var out []*BinPlan
	for _, bp := range plan.Bins {
		for _, l := range bp.Lines {
			if l.SKU == sku {
				out = append(out, bp)
				break
			}
		}
	}
	return out
}

func TestPlanIsDeterministic(t *testing.T) {
	a := BuildPlan(testProducts(), testSites(), Options{})
	b := BuildPlan(testProducts(), testSites(), Options{})
	if a.Units != b.Units || len(a.Bins) != len(b.Bins) {
		t.Fatalf("plans differ: %s vs %s", a.Summary(), b.Summary())
	}
	for i := range a.Bins {
		if a.Bins[i].Bin.ID != b.Bins[i].Bin.ID || a.Bins[i].Units != b.Bins[i].Units || a.Bins[i].Capacity != b.Bins[i].Capacity {
			t.Fatalf("bin %d differs", i)
		}
	}
}

func TestHandlingClassesLandInTheRightZones(t *testing.T) {
	plan := BuildPlan(testProducts(), testSites(), Options{})
	for _, tc := range []struct {
		sku, zone, temp string
		hazmat          bool
	}{
		{"ABO-BATT", "HAZ", "Ambient", true},
		{"ABO-BLEACH", "HAZ", "Ambient", true},
		{"4006381333931", "FRZ", "Frozen", false},
		{"4006381333948", "CHL", "Chilled", false},
	} {
		bins := binsOf(plan, tc.sku)
		if len(bins) == 0 {
			t.Fatalf("%s was not stocked", tc.sku)
		}
		for _, bp := range bins {
			if bp.Bin.ZoneCode != tc.zone || bp.Bin.Temp != tc.temp || bp.Bin.Hazmat != tc.hazmat {
				t.Errorf("%s in %s (%s, hazmat=%v), want zone %s %s", tc.sku, bp.Bin.ID, bp.Bin.ZoneCode, bp.Bin.Hazmat, tc.zone, tc.temp)
			}
		}
	}
}

func TestOversizedOnlyAtBulkSitesAndColdOnlyAtFreshOrMicro(t *testing.T) {
	plan := BuildPlan(testProducts(), testSites(), Options{})
	for _, bp := range binsOf(plan, "WANDS-SOFA") {
		if bp.Site != "N1" {
			t.Errorf("oversized sofa stocked at %s", bp.Site)
		}
	}
	for _, sku := range []string{"4006381333931", "4006381333948"} {
		for _, bp := range binsOf(plan, sku) {
			if bp.Site != "F1" && bp.Site != "M1" {
				t.Errorf("%s (cold chain) stocked at %s", sku, bp.Site)
			}
		}
	}
	for _, bp := range binsOf(plan, "ABO-SMALL") {
		if bp.Site == "N1" || bp.Site == "F1" {
			t.Errorf("a small non-grocery item stocked at %s", bp.Site)
		}
	}
}

func TestHazmatGetsABinOfItsOwn(t *testing.T) {
	plan := BuildPlan(testProducts(), testSites(), Options{})
	for _, bp := range plan.Bins {
		hz := 0
		for _, l := range bp.Lines {
			if l.SKU == "ABO-BATT" || l.SKU == "ABO-BLEACH" {
				hz++
			}
		}
		if hz > 0 && len(bp.Lines) != 1 {
			t.Errorf("bin %s mixes hazmat with %d other lines (same-bin DOT segregation)", bp.Bin.ID, len(bp.Lines)-1)
		}
	}
}

func TestBinsNeverExceedTheirPhysicalLimitsOrFill(t *testing.T) {
	products := []Product{}
	for i := 0; i < 400; i++ {
		products = append(products, Product{SKU: fmt.Sprintf("ABO-S%03d", i), HasDims: true, VolumeL: 0.3 + float64(i%7), WeightG: int64(200 + i*7)})
	}
	plan := BuildPlan(products, testSites(), Options{Fill: 0.8})
	for _, bp := range plan.Bins {
		var vol, wt float64
		byID := map[string]Product{}
		for _, p := range products {
			byID[p.SKU] = p
		}
		for _, l := range bp.Lines {
			p := byID[l.SKU]
			vol += float64(l.Qty) * p.VolumeL
			wt += float64(l.Qty) * float64(p.WeightG)
		}
		if vol > bp.Bin.MaxVolumeL*0.8+1e-6 || wt > float64(bp.Bin.MaxWeightG)*0.8+1e-6 {
			t.Errorf("bin %s over its 80%% fill: %.1f L of %.1f, %.0f g of %d", bp.Bin.ID, vol, bp.Bin.MaxVolumeL, wt, bp.Bin.MaxWeightG)
		}
		if bp.Capacity < bp.Units {
			t.Errorf("bin %s capacity %d below planned %d", bp.Bin.ID, bp.Capacity, bp.Units)
		}
	}
}

func TestQuantitiesAreCasePackScaleAndSizeOrdered(t *testing.T) {
	tiny := quantityFor(Product{SKU: "A", HasDims: true, VolumeL: 0.05, WeightG: 20}, small, "S")
	sofa := quantityFor(Product{SKU: "A", HasDims: true, VolumeL: 1500, WeightG: 60_000, Tags: []string{"Oversized"}}, large, "S")
	if tiny < 12 || tiny > 36 || sofa < 1 || sofa > 3 {
		t.Fatalf("tiny=%d sofa=%d out of band", tiny, sofa)
	}
	heavy := quantityFor(Product{SKU: "H", HasDims: true, VolumeL: 0.1, WeightG: 50_000}, small, "S")
	if int64(heavy)*50_000 > maxLineWeight {
		t.Fatalf("line of %d x 50 kg exceeds the line weight cap", heavy)
	}
}

func TestRegisteredButUnstockableProductsAreReportedNotStocked(t *testing.T) {
	// an XL product that no site role accepts: only a sortable site exists
	sites := []Site{{Code: "R1", Role: "ROBOTICS_SORTABLE_FC", Bins: mkBins("R1", "POD", "Ambient", false, 5, 120, 30_000)}}
	plan := BuildPlan([]Product{{SKU: "WANDS-X", HasDims: true, VolumeL: 900, WeightG: 70_000, Tags: []string{"Oversized"}}}, sites, Options{})
	if plan.Units != 0 || len(plan.SkippedSKUs) != 1 || plan.SkippedSKUs[0] != "WANDS-X" {
		t.Fatalf("plan %s skipped %v", plan.Summary(), plan.SkippedSKUs)
	}
}

func TestRunsOutOfRoomIsReportedAsUnplaced(t *testing.T) {
	sites := []Site{{Code: "R1", Role: "ROBOTICS_SORTABLE_FC", Bins: mkBins("R1", "POD", "Ambient", false, 1, 10, 1000)}}
	plan := BuildPlan([]Product{{SKU: "ABO-A", HasDims: true, VolumeL: 3, WeightG: 100}}, sites, Options{Fill: 0.8})
	if plan.Units == 0 || plan.Units > 2 { // 10 L x 0.8 / 3 L = 2 units
		t.Fatalf("placed %d units in a 10 L bin", plan.Units)
	}
	if plan.Unplaced["ABO-A"] == 0 && quantityFor(Product{SKU: "ABO-A", HasDims: true, VolumeL: 3, WeightG: 100}, small, "R1") > 2 {
		t.Fatal("the units that did not fit were not reported")
	}
}

func TestProfileNamesAreStable(t *testing.T) {
	if got := profileOf(Product{Temp: "Frozen"}, small); got != "frozen" {
		t.Fatal(got)
	}
	if got := profileOf(Product{HasDims: true, Tags: []string{"Hazmat"}}, small); got != "hazmat" {
		t.Fatal(got)
	}
	if !strings.Contains(profileOf(Product{}, small), "grocery") {
		t.Fatal("grocery profile")
	}
}
