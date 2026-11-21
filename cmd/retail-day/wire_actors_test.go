package main

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/claudioed/warehouse-e2e/internal/retailday/day"
	"github.com/claudioed/warehouse-e2e/internal/sim/floor"
	"github.com/claudioed/warehouse-e2e/internal/sim/inbound"
	"github.com/claudioed/warehouse-e2e/internal/sim/outbound"
)

// ---- floor ----

type fakeProcessor struct {
	ids      []string
	parallel int
	out      []floor.Result
}

func (f *fakeProcessor) ProcessAll(_ context.Context, ids []string, parallel int) []floor.Result {
	f.ids, f.parallel = append([]string(nil), ids...), parallel
	return f.out
}

func TestFloorResultMapsEveryFieldAndKeepsDivertedDiverted(t *testing.T) {
	src := floor.Result{
		OrderID: "om-1", PackageID: "pkg-1", Status: "LABELED", OrderRef: "ref-1", SourceOrderID: "src-1",
		SiteID: "AUS3", CPT: "2026-11-15T23:00:00Z", WeightG: 1234, Problems: []string{"a", "b"},
	}
	got := toFloorResult(src)
	want := day.FloorResult{
		OrderID: "om-1", PackageID: "pkg-1", Status: "LABELED", OrderRef: "ref-1", SourceOrderID: "src-1",
		SiteID: "AUS3", CPT: "2026-11-15T23:00:00Z", WeightG: 1234, Problems: []string{"a", "b"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("floor result\n got %+v\nwant %+v", got, want)
	}
	if !got.Labeled() {
		t.Error("a labelled package does not read as labelled")
	}
	src.Problems[0] = "changed"
	if got.Problems[0] != "a" {
		t.Error("Problems alias the floor's slice")
	}

	for _, status := range []string{"DIVERTED", "SEALED", ""} {
		d := toFloorResult(floor.Result{OrderID: "om-2", PackageID: "pkg-2", Status: status})
		if d.Status != status || d.Labeled() {
			t.Errorf("status %q became %q (labelled %v)", status, d.Status, d.Labeled())
		}
	}
}

func TestFloorAdapterKeepsTheOrderAndLengthOfTheActorsAnswer(t *testing.T) {
	fp := &fakeProcessor{out: []floor.Result{
		{OrderID: "om-b", PackageID: "p-b", Status: "LABELED"},
		{OrderID: "om-a", Status: "DIVERTED", PackageID: "p-a"},
		{OrderID: "om-c", Problems: []string{"order-management GET /orders/om-c: 404"}},
	}}
	got := floorAdapter{p: fp}.ProcessAll(context.Background(), []string{"om-b", "om-a", "om-c"}, 5)
	if !reflect.DeepEqual(fp.ids, []string{"om-b", "om-a", "om-c"}) || fp.parallel != 5 {
		t.Errorf("the actor was called with %v, parallel %d", fp.ids, fp.parallel)
	}
	if len(got) != 3 || got[0].OrderID != "om-b" || got[1].OrderID != "om-a" || got[2].OrderID != "om-c" {
		t.Fatalf("results %+v", got)
	}
	if !got[0].Labeled() || got[1].Labeled() || got[2].Labeled() {
		t.Errorf("labelled: %v %v %v", got[0].Labeled(), got[1].Labeled(), got[2].Labeled())
	}
	if len(got[2].Problems) != 1 {
		t.Errorf("problems %v", got[2].Problems)
	}
}

// ---- shipper ----

type fakeDock struct {
	got []outbound.Package
	out []outbound.Shipment
}

func (f *fakeDock) ShipAll(_ context.Context, pkgs []outbound.Package) []outbound.Shipment {
	f.got = append([]outbound.Package(nil), pkgs...)
	return f.out
}

func TestPackageMapsToTheFactsOfPackageManifested(t *testing.T) {
	got := toOutboundPackage(day.Package{
		OrderID: "customer-order", PackageID: "pkg-1", OrderRef: "ref-1", SourceOrderID: "src-1",
		SiteID: "AUS3", CPT: "17:00", WeightG: 900,
	})
	want := outbound.Package{ID: "pkg-1", OrderRef: "ref-1", SourceOrderID: "src-1", SiteID: "AUS3", CPT: "17:00", WeightG: 900}
	if got != want {
		t.Fatalf("package\n got %+v\nwant %+v", got, want)
	}
}

func TestShipmentIsConfirmedOnlyByShipConfirmed(t *testing.T) {
	for _, tc := range []struct {
		name         string
		in           outbound.Shipment
		departed, ok bool
		wantProblems int
	}{
		{"left and confirmed", outbound.Shipment{PackageID: "p", OrderRef: "r", LoadID: "L1", Departed: true, LoadState: outbound.LoadShipConfirmed}, true, true, 0},
		{"left, load still closed", outbound.Shipment{PackageID: "p", Departed: true, LoadState: outbound.LoadClosed}, true, false, 0},
		{"not left, confirmed load", outbound.Shipment{PackageID: "p", Departed: false, LoadState: outbound.LoadShipConfirmed}, false, true, 0},
		{"never loaded", outbound.Shipment{PackageID: "p", Problems: []string{"x", "y"}}, false, false, 2},
	} {
		got := toShipment(tc.in)
		if got.Departed != tc.departed || got.Confirmed != tc.ok || len(got.Problems) != tc.wantProblems {
			t.Errorf("%s: %+v", tc.name, got)
		}
		if got.PackageID != tc.in.PackageID || got.OrderRef != tc.in.OrderRef || got.LoadID != tc.in.LoadID {
			t.Errorf("%s: identity lost: %+v", tc.name, got)
		}
	}
}

func TestShipperAdapterPassesPackagesInOrderAndReturnsOneShipmentEach(t *testing.T) {
	fd := &fakeDock{out: []outbound.Shipment{
		{PackageID: "p1", Departed: true, LoadState: outbound.LoadShipConfirmed},
		{PackageID: "p2", Problems: []string{"load not closed"}},
	}}
	a := shipperAdapter{get: func() dock { return fd }}
	got := a.ShipAll(context.Background(), []day.Package{{PackageID: "p1", CPT: "11:00"}, {PackageID: "p2", CPT: "11:00"}})
	if len(fd.got) != 2 || fd.got[0].ID != "p1" || fd.got[1].ID != "p2" || fd.got[0].CPT != "11:00" {
		t.Errorf("the dock was handed %+v", fd.got)
	}
	if len(got) != 2 || !got[0].Confirmed || got[1].Confirmed || len(got[1].Problems) != 1 {
		t.Errorf("shipments %+v", got)
	}
}

func TestShipperAdapterBeforePrepareShipsNothingAndSaysSo(t *testing.T) {
	for name, a := range map[string]shipperAdapter{
		"no getter":   {},
		"not readied": {get: func() dock { return nil }},
	} {
		got := a.ShipAll(context.Background(), []day.Package{{PackageID: "p1", OrderRef: "r1"}, {PackageID: "p2"}})
		if len(got) != 2 {
			t.Fatalf("%s: %d shipments for 2 packages", name, len(got))
		}
		for i, s := range got {
			if s.Departed || s.Confirmed || len(s.Problems) != 1 || !strings.Contains(s.Problems[0], "never readied") {
				t.Errorf("%s: shipment %d %+v", name, i, s)
			}
		}
		if got[0].PackageID != "p1" || got[0].OrderRef != "r1" {
			t.Errorf("%s: %+v", name, got[0])
		}
	}
}

// ---- restocker ----

type fakeCrew struct {
	start inbound.Stock
	res   inbound.Result
}

func (f *fakeCrew) Restock(_ context.Context, start inbound.Stock, _ inbound.RestockOptions) inbound.Result {
	f.start = start
	return f.res
}

func TestRestockResultCountsWhatWasStowedAndKeepsEveryFinding(t *testing.T) {
	res := inbound.Result{
		Problems: []string{"restock: read failed"},
		Short:    map[string]int{"SKU-C": 3},
		Deliveries: []inbound.Delivery{
			{ASN: "ASN-1", Stowed: []inbound.Stowed{{SKU: "SKU-A", Qty: 4, Bin: "b1"}, {SKU: "SKU-A", Qty: 2, Bin: "b2"}, {SKU: "SKU-B", Qty: 5, Bin: "b1"}}, Received: map[string]int{"SKU-A": 99}},
			{ASN: "ASN-2", Stowed: []inbound.Stowed{{SKU: "SKU-A", Qty: 1, Bin: "b3"}}, Problems: []string{"door not released"}},
		},
	}
	got := toRestockResult(res)
	if want := map[string]int{"SKU-A": 7, "SKU-B": 5}; !reflect.DeepEqual(got.Restocked, want) {
		t.Errorf("restocked %v, want %v (stowed units, not received ones)", got.Restocked, want)
	}
	if want := map[string]int{"SKU-C": 3}; !reflect.DeepEqual(got.Short, want) {
		t.Errorf("short %v", got.Short)
	}
	if len(got.Problems) != 2 || !strings.Contains(got.Problems[0], "read failed") || !strings.Contains(got.Problems[1], "ASN-2") || !strings.Contains(got.Problems[1], "door not released") {
		t.Errorf("problems %v: the restock's and every truck's must reach the day", got.Problems)
	}
	res.Short["SKU-C"] = 99
	if got.Short["SKU-C"] != 3 {
		t.Error("Short aliases the crew's map")
	}
}

func TestRestockResultOfADayThatNeededNoRestock(t *testing.T) {
	got := toRestockResult(inbound.Result{})
	if got.Restocked == nil || len(got.Restocked) != 0 || len(got.Short) != 0 || len(got.Problems) != 0 {
		t.Errorf("%+v", got)
	}
}

func TestRestockAdapterHandsTheCrewTheStartStock(t *testing.T) {
	fc := &fakeCrew{res: inbound.Result{Deliveries: []inbound.Delivery{{Stowed: []inbound.Stowed{{SKU: "S", Qty: 2}}}}}}
	got, err := restockAdapter{get: func() crew { return fc }}.Restock(context.Background(), map[string]int{"S": 10, "T": 4})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(fc.start, inbound.Stock{"S": 10, "T": 4}) {
		t.Errorf("start %v", fc.start)
	}
	if got.Restocked["S"] != 2 {
		t.Errorf("restocked %v", got.Restocked)
	}
}

func TestRestockAdapterBeforePrepareIsAnErrorNotAnEmptyRestock(t *testing.T) {
	for name, a := range map[string]restockAdapter{"no getter": {}, "not readied": {get: func() crew { return nil }}} {
		if _, err := a.Restock(context.Background(), map[string]int{"S": 1}); err == nil || !strings.Contains(err.Error(), "never readied") {
			t.Errorf("%s: err %v", name, err)
		}
	}
}

func TestRestockAdapterReportsTheEndOfTheContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := (restockAdapter{get: func() crew { return &fakeCrew{} }}).Restock(ctx, map[string]int{"S": 1}); err == nil {
		t.Error("a cancelled run reported a clean restock")
	}
}
