package inbound

import (
	"context"
	"strings"
	"testing"

	"github.com/claudioed/warehouse-e2e/internal/sim"
)

func TestReadStockReadsTheUsableUnitsOfEverySKU(t *testing.T) {
	w := newWorld(t)
	c := w.crew(w.seedSite())
	w.with(func() { w.inv.reserved[skuB] = 30 })
	got, err := c.ReadStock(context.Background(), []string{skuA, skuB, skuC, "SKU-UNSTOCKED"})
	if err != nil {
		t.Fatalf("ReadStock: %v", err)
	}
	want := Stock{skuA: 100, skuB: 70, skuC: 100, "SKU-UNSTOCKED": 0}
	for sku, n := range want {
		if got[sku] != n {
			t.Errorf("usable of %s = %d, want %d", sku, got[sku], n)
		}
	}
}

func TestReadStockStopsAtATransportErrorNamingTheSKU(t *testing.T) {
	c := &Crew{C: sim.NewClient(map[string]string{}, sim.NewCallStats()), RunID: "T1"} // no inventory-storage at all
	_, err := c.ReadStock(context.Background(), []string{skuA})
	if err == nil || !strings.Contains(err.Error(), skuA) || !strings.Contains(err.Error(), "transport error") {
		t.Fatalf("err = %v, want a transport error naming %s", err, skuA)
	}
}

func TestDeficitIsWhatTheDayConsumedBelowTheStartAndNothingForSKUsAtOrAboveIt(t *testing.T) {
	start := Stock{skuA: 100, skuB: 100, skuC: 100, "SKU-D": 5}
	now := Stock{skuA: 90, skuB: 100, skuC: 130, "SKU-D": 0}
	got := Deficit(start, now)
	want := []Line{{skuA, 10}, {"SKU-D", 5}}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("Deficit = %v, want %v (sorted by SKU; B is at the start, C above it)", got, want)
	}
	if got := Deficit(start, Stock{}); len(got) != 4 {
		t.Errorf("a SKU missing from the current stock reads as 0: Deficit = %v", got)
	}
}

func TestRestockEndsTheDayAtOrAboveTheStockItStartedWith(t *testing.T) {
	w := newWorld(t)
	site := w.seedSite()
	site.Bins = []string{binA, binB}
	c := w.crew(site)
	skus := []string{skuA, skuB, skuC}
	start, err := c.ReadStock(context.Background(), skus)
	if err != nil {
		t.Fatal(err)
	}
	w.inv.consume(skuA, 15) // the day's picks
	w.inv.consume(skuB, 40)

	res := c.Restock(context.Background(), start, RestockOptions{})
	if !res.OK() {
		t.Fatalf("restock not OK: %v", res.Findings())
	}
	if len(res.Lines) != 2 || res.Lines[0] != (Line{skuA, 15}) || res.Lines[1] != (Line{skuB, 40}) {
		t.Errorf("restocked lines = %v, want A x15 and B x40 (C was untouched)", res.Lines)
	}
	if len(res.Deliveries) != 1 || !res.Deliveries[0].OK() {
		t.Fatalf("deliveries = %+v, want one OK truck", res.Deliveries)
	}
	for _, sku := range skus {
		if res.After[sku] < start[sku] {
			t.Errorf("%s ends at %d, below the start %d", sku, res.After[sku], start[sku])
		}
	}
	if res.Before[skuA] != 85 || res.Before[skuB] != 60 || res.After[skuA] != 100 || res.After[skuB] != 100 {
		t.Errorf("before %v after %v", res.Before, res.After)
	}
}

func TestRestockOfAShelfThatLostNothingWritesNothing(t *testing.T) {
	w := newWorld(t)
	c := w.crew(w.seedSite())
	start, _ := c.ReadStock(context.Background(), []string{skuA, skuB})
	writes := w.writes()
	res := c.Restock(context.Background(), start, RestockOptions{})
	if !res.OK() || len(res.Deliveries) != 0 || len(res.Lines) != 0 {
		t.Fatalf("result = %+v, want OK with no delivery", res)
	}
	if w.writes() != writes {
		t.Errorf("%d writes for a shelf at its start, want 0", w.writes()-writes)
	}
}

func TestRestockRunTwiceTheSecondRunAddsNoWrite(t *testing.T) {
	w := newWorld(t)
	site := w.seedSite()
	site.Bins = []string{binA, binB}
	c := w.crew(site)
	start, _ := c.ReadStock(context.Background(), []string{skuA, skuB, skuC})
	w.inv.consume(skuA, 10)
	if res := c.Restock(context.Background(), start, RestockOptions{}); !res.OK() {
		t.Fatalf("first run: %v", res.Findings())
	}
	writes := w.writes()
	res := c.Restock(context.Background(), start, RestockOptions{})
	if !res.OK() || len(res.Deliveries) != 0 {
		t.Fatalf("second run = %+v, want OK and no delivery", res)
	}
	if w.writes() != writes {
		t.Errorf("the second run sent %d POST/PUT, want 0: the stock was already back", w.writes()-writes)
	}
}

func TestRestockSplitsAFatShortfallIntoSeveralTrucksOneAfterTheOther(t *testing.T) {
	w := newWorld(t)
	site := w.seedSite()
	site.Bins = []string{binA, binB}
	w.with(func() {
		for _, sku := range []string{"SKU-004", "SKU-005"} {
			w.recv.skus[sku] = true
			w.inv.stock(sku, binA, 100)
		}
	})
	c := w.crew(site)
	skus := []string{skuA, skuB, skuC, "SKU-004", "SKU-005"}
	start, _ := c.ReadStock(context.Background(), skus)
	for _, sku := range skus {
		w.inv.consume(sku, 20)
	}
	res := c.Restock(context.Background(), start, RestockOptions{LinesPerASN: 2})
	if !res.OK() {
		t.Fatalf("restock not OK: %v", res.Findings())
	}
	if len(res.Deliveries) != 3 {
		t.Fatalf("%d trucks, want 3 (2+2+1 lines)", len(res.Deliveries))
	}
	asns, visits := map[string]bool{}, map[string]bool{}
	for _, d := range res.Deliveries {
		asns[d.ASN], visits[d.VisitID] = true, true
	}
	if len(asns) != 3 || len(visits) != 3 {
		t.Errorf("trucks shared an ASN or a visit: %v %v", asns, visits)
	}
	if n := w.served("yard-management POST /visits/{id}/check-outs"); n != 3 {
		t.Errorf("%d check-outs, want 3", n)
	}
}

func TestRestockSaysWhichSKUsStillEndBelowTheirStart(t *testing.T) {
	w := newWorld(t)
	site := w.seedSite()
	site.Bins = []string{binA}
	c := w.crew(site)
	start, _ := c.ReadStock(context.Background(), []string{skuA})
	w.inv.consume(skuA, 40)
	w.with(func() { w.inv.bins[binA].capacity = w.inv.bins[binA].occupied + 25 }) // only 25 of the 40 fit
	res := c.Restock(context.Background(), start, RestockOptions{})
	if res.OK() {
		t.Fatal("restock OK although the shelf ends below its start")
	}
	if res.Short[skuA] != 15 {
		t.Errorf("short = %v, want %s short by 15", res.Short, skuA)
	}
	if res.After[skuA] != 85 {
		t.Errorf("after = %v", res.After)
	}
	joined := strings.Join(res.Findings(), "|")
	for _, want := range []string{"only 25 of 40", skuA + " ends at 85", "100"} {
		if !strings.Contains(joined, want) {
			t.Errorf("findings %q lack %q", joined, want)
		}
	}
}

func TestRestockWithoutInventoryStorageIsAFindingAndSendsNothing(t *testing.T) {
	c := &Crew{C: sim.NewClient(map[string]string{}, sim.NewCallStats()), RunID: "T1"}
	res := c.Restock(context.Background(), Stock{skuA: 10}, RestockOptions{})
	if res.OK() || len(res.Deliveries) != 0 || len(res.Findings()) == 0 {
		t.Errorf("result = %+v", res)
	}
}

func TestRestockOfAnEmptyShelfIsOK(t *testing.T) {
	w := newWorld(t)
	c := w.crew(w.seedSite())
	if res := c.Restock(context.Background(), Stock{}, RestockOptions{}); !res.OK() {
		t.Errorf("result = %+v", res)
	}
}
