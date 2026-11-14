package inbound

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func stowsOf(t *testing.T, w *world) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, b := range w.bodies("inventory-storage POST /stowages") {
		var m map[string]any
		if err := json.Unmarshal(b, &m); err != nil {
			t.Fatal(err)
		}
		out = append(out, m)
	}
	return out
}

func TestStowPutsTheLineIntoABinOfTheSite(t *testing.T) {
	w := newWorld(t)
	site := w.seedSite()
	site.Bins = []string{binA}
	c := w.crew(site)
	got, err := c.Stow(context.Background(), Line{skuA, 30})
	if err != nil {
		t.Fatalf("Stow: %v", err)
	}
	if len(got) != 1 || got[0] != (Stowed{SKU: skuA, Qty: 30, Bin: binA}) {
		t.Fatalf("stowed = %v", got)
	}
	if u := w.inv.usable(skuA); u != 130 {
		t.Errorf("usable = %d, want 130", u)
	}
	s := stowsOf(t, w)
	if len(s) != 1 || s[0]["sku"] != skuA || s[0]["quantity"] != float64(30) || s[0]["binId"] != binA {
		t.Errorf("stow body = %v", s)
	}
}

func TestStowSplitsALineAcrossBinsWhenTheFirstHasLessRoom(t *testing.T) {
	w := newWorld(t)
	site := w.seedSite()
	site.Bins = []string{binA, binB}
	w.with(func() { w.inv.bins[binA].capacity = w.inv.bins[binA].occupied + 20 }) // room for 20 only
	c := w.crew(site)
	got, err := c.Stow(context.Background(), Line{skuA, 50})
	if err != nil {
		t.Fatalf("Stow: %v", err)
	}
	if len(got) != 2 || got[0] != (Stowed{skuA, 20, binA}) || got[1] != (Stowed{skuA, 30, binB}) {
		t.Fatalf("stowed = %v, want 20 in %s then 30 in %s", got, binA, binB)
	}
	if u := w.inv.usable(skuA); u != 150 {
		t.Errorf("usable = %d, want 150: every unit of the line stowed once", u)
	}
}

func TestStowMovesOnWhenABinRefusesTheSKUsClass(t *testing.T) {
	for _, tc := range []struct{ class, slug, goodBin string }{
		{"HAZ", "hazmat-zone-required", binHaz},
		{"CHL", "temperature-class-mismatch", binChill},
	} {
		t.Run(tc.class, func(t *testing.T) {
			w := newWorld(t)
			site := w.seedSite()
			site.Bins = []string{binA, tc.goodBin}
			w.with(func() { w.inv.class[skuA] = tc.class })
			c := w.crew(site)
			got, err := c.Stow(context.Background(), Line{skuA, 10})
			if err != nil {
				t.Fatalf("Stow: %v", err)
			}
			if len(got) != 1 || got[0].Bin != tc.goodBin {
				t.Fatalf("stowed = %v, want it in %s after %s refused it with %s", got, tc.goodBin, binA, tc.slug)
			}
			if n := len(stowsOf(t, w)); n != 2 {
				t.Errorf("%d stow attempts, want 2 (the refused bin, then the right one)", n)
			}
		})
	}
}

func TestStowSkipsABinThatDoesNotExist(t *testing.T) {
	w := newWorld(t)
	site := w.seedSite()
	site.Bins = []string{"AUS3-NOPE-BIN-A01-01-01-A", binA}
	c := w.crew(site)
	got, err := c.Stow(context.Background(), Line{skuA, 10})
	if err != nil || len(got) != 1 || got[0].Bin != binA {
		t.Fatalf("stowed = %v, %v; want it in %s", got, err, binA)
	}
}

func TestStowThatFindsNoRoomReportsEveryBinAndHowMuchWentIn(t *testing.T) {
	w := newWorld(t)
	site := w.seedSite()
	site.Bins = []string{binA, binB, "AUS3-NOPE-BIN-A01-01-01-A"}
	w.with(func() {
		w.inv.bins[binA].capacity = w.inv.bins[binA].occupied + 5
		w.inv.bins[binB].capacity = w.inv.bins[binB].occupied // full
	})
	c := w.crew(site)
	got, err := c.Stow(context.Background(), Line{skuA, 50})
	if err == nil {
		t.Fatal("Stow returned nil with 45 units unplaced")
	}
	if len(got) != 1 || got[0] != (Stowed{skuA, 5, binA}) {
		t.Errorf("stowed = %v, want the 5 units that fitted reported", got)
	}
	for _, want := range []string{"5 of 50", skuA, binA, binB, "AUS3-NOPE-BIN", "full", "bin-not-found"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q lacks %q", err, want)
		}
	}
}

func TestStowDoesNotRetryOrSkipAnUnexpectedRefusal(t *testing.T) {
	w := newWorld(t)
	site := w.seedSite()
	site.Bins = []string{binA, binB}
	w.with(func() { w.inv.failStow = 503 })
	c := w.crew(site)
	got, err := c.Stow(context.Background(), Line{skuA, 10})
	if err == nil || !strings.Contains(err.Error(), "503 dependency-unavailable") {
		t.Fatalf("err = %v, want 503 dependency-unavailable", err)
	}
	if len(got) != 0 {
		t.Errorf("stowed = %v", got)
	}
	if n := len(stowsOf(t, w)); n != 1 {
		t.Errorf("%d stow attempts, want 1: another bin cannot fix a 5xx", n)
	}
}

func TestStowWithNoBinsConfiguredSendsNothing(t *testing.T) {
	w := newWorld(t)
	site := w.seedSite()
	site.Bins = nil
	c := w.crew(site)
	if _, err := c.Stow(context.Background(), Line{skuA, 10}); err == nil || !strings.Contains(err.Error(), "no bin") {
		t.Fatalf("err = %v, want it to say the site has no bin configured", err)
	}
	if n := w.writes(); n != 0 {
		t.Errorf("%d writes, want 0", n)
	}
}

func TestStowChoosesTheFirstBinBySKUDeterministicallyButNotAlwaysTheSame(t *testing.T) {
	w := newWorld(t)
	site := w.seedSite()
	site.Bins = []string{binA, binB}
	c1, c2 := w.crew(site), w.crew(site)
	first := map[string]string{}
	distinct := map[string]bool{}
	for _, sku := range []string{"SKU-0001", "SKU-0002", "SKU-0003", "SKU-0004", "SKU-0005", "SKU-0006", "SKU-0007", "SKU-0008"} {
		if b1, b2 := c1.binsFor(sku)[0], c2.binsFor(sku)[0]; b1 != b2 {
			t.Fatalf("%s: two crews chose %s and %s", sku, b1, b2)
		} else {
			first[sku] = b1
			distinct[b1] = true
		}
		if len(c1.binsFor(sku)) != 2 {
			t.Fatalf("%s: binsFor = %v, want both bins in rotation order", sku, c1.binsFor(sku))
		}
	}
	if len(distinct) != 2 {
		t.Errorf("first bins = %v: every SKU went to the same bin, want chaotic storage", first)
	}
}

func TestStowReceiptStowsWhatTheReceiptCountedNotWhatWasAnnounced(t *testing.T) {
	w := newWorld(t)
	site := w.seedSite()
	site.Bins = []string{binA}
	c := w.crew(site)
	asn, a, _ := docked(t, c, Line{skuA, 10}, Line{skuB, 5})
	r, err := c.OpenReceipt(context.Background(), asn, a)
	if err != nil {
		t.Fatal(err)
	}
	if r, err = c.ReceiveLine(context.Background(), r, 1, 8); err != nil { // 8 of the 10 announced
		t.Fatal(err)
	}
	r, err = c.CloseReceipt(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	got, err := c.StowReceipt(context.Background(), r)
	if err != nil {
		t.Fatalf("StowReceipt: %v", err)
	}
	if len(got) != 1 || got[0] != (Stowed{skuA, 8, binA}) {
		t.Fatalf("stowed = %v, want only the 8 units counted of %s (line 2 counted nothing)", got, skuA)
	}
}
