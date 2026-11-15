package day

import (
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/claudioed/warehouse-e2e/internal/retailday"
)

// candidates returns n purchasable items with distinct catalogue ids and SKUs.
func candidates(n int) []Item {
	out := make([]Item, n)
	for i := range out {
		out[i] = Item{CatalogItemID: fmt.Sprintf("cat-%03d", i), SKU: fmt.Sprintf("SKU-%03d", i)}
	}
	return out
}

func cfgFor(seed int64) PlanConfig {
	return PlanConfig{RunID: "RD1", Seed: seed, Shoppers: 12, ShelfSize: 8}
}

func mustPlan(t *testing.T, cfg PlanConfig, items []Item) Plan {
	t.Helper()
	p, err := BuildPlan(cfg, items)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	return p
}

func TestPlanIsAPureFunctionOfTheSeed(t *testing.T) {
	items := candidates(30)
	a, b := mustPlan(t, cfgFor(7), items), mustPlan(t, cfgFor(7), items)
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("same seed, different plans:\n%+v\n%+v", a, b)
	}
	// the order the storefront happened to list the products in must not matter
	rev := slices.Clone(items)
	slices.Reverse(rev)
	if c := mustPlan(t, cfgFor(7), rev); !reflect.DeepEqual(a, c) {
		t.Fatalf("the plan depends on the candidates' order:\n%+v\n%+v", a, c)
	}
	if d := mustPlan(t, cfgFor(8), items); reflect.DeepEqual(a, d) {
		t.Fatal("two seeds gave the same plan")
	}
}

func TestPlanShelfIsASortedSeededSubsetOfTheCandidates(t *testing.T) {
	items := candidates(30)
	p := mustPlan(t, cfgFor(3), items)
	if len(p.Shelf) != 8 {
		t.Fatalf("shelf of %d, want 8", len(p.Shelf))
	}
	known := map[Item]bool{}
	for _, it := range items {
		known[it] = true
	}
	for i, it := range p.Shelf {
		if !known[it] {
			t.Errorf("shelf item %+v is not a candidate", it)
		}
		if i > 0 && p.Shelf[i-1].CatalogItemID >= it.CatalogItemID {
			t.Errorf("shelf not strictly sorted by catalogue id at %d: %v", i, p.Shelf)
		}
	}
	if got := p.ShelfSKUs(); len(got) != 8 || !slices.IsSorted(got) {
		t.Errorf("ShelfSKUs %v", got)
	}
}

func TestPlanMixHasExactlyOneOfEachExceptionAndTheRestDelivered(t *testing.T) {
	for seed := int64(1); seed <= 25; seed++ {
		p := mustPlan(t, cfgFor(seed), candidates(30))
		if len(p.Buyers) != 12 {
			t.Fatalf("seed %d: %d buyers, want 12", seed, len(p.Buyers))
		}
		count := map[retailday.Outcome]int{}
		for i, b := range p.Buyers {
			count[b.Expect]++
			if b.Index != i+1 {
				t.Errorf("seed %d: buyer %d has index %d", seed, i, b.Index)
			}
		}
		for _, o := range []retailday.Outcome{retailday.Declined, retailday.Cancelled, retailday.NotServed, retailday.InDoubt} {
			if count[o] != 1 {
				t.Errorf("seed %d: %d %s buyers, want exactly 1", seed, count[o], o)
			}
		}
		if count[retailday.Delivered] != 8 {
			t.Errorf("seed %d: %d delivered buyers, want 8", seed, count[retailday.Delivered])
		}
	}
}

func TestPlanBuyersAreShapedByWhatTheyMustProve(t *testing.T) {
	for seed := int64(1); seed <= 25; seed++ {
		p := mustPlan(t, cfgFor(seed), candidates(30))
		onShelf := map[string]bool{}
		for _, it := range p.Shelf {
			onShelf[it.SKU] = true
		}
		for _, b := range p.Buyers {
			if len(b.Lines) < 1 || len(b.Lines) > 3 {
				t.Errorf("seed %d buyer %d: %d lines, want 1-3", seed, b.Index, len(b.Lines))
			}
			seen := map[string]bool{}
			for _, l := range b.Lines {
				if !onShelf[l.Item.SKU] {
					t.Errorf("seed %d buyer %d buys %s, which is not on the shelf", seed, b.Index, l.Item.SKU)
				}
				if seen[l.Item.SKU] {
					t.Errorf("seed %d buyer %d buys %s on two lines", seed, b.Index, l.Item.SKU)
				}
				seen[l.Item.SKU] = true
				if l.Qty < 1 || l.Qty > 2 {
					t.Errorf("seed %d buyer %d: quantity %d, want 1-2", seed, b.Index, l.Qty)
				}
			}
			switch b.Expect {
			case retailday.Delivered:
				if b.Token != "tok_approve" || (b.Tier != "STANDARD" && b.Tier != "TWO_DAY") || (b.Prefix != "786" && b.Prefix != "787") || b.CancelAfterPlacement || b.ExpectOutOfArea {
					t.Errorf("seed %d: a delivered buyer is mis-shaped: %+v", seed, b)
				}
			case retailday.Declined:
				if b.Token != "tok_decline" || b.FallbackToken != "" || b.CancelAfterPlacement {
					t.Errorf("seed %d: a declined buyer must pay with tok_decline and have no fallback: %+v", seed, b)
				}
			case retailday.Cancelled:
				if b.Token != "tok_approve" || !b.CancelAfterPlacement {
					t.Errorf("seed %d: a cancelled buyer must pay and cancel after placement: %+v", seed, b)
				}
			case retailday.NotServed:
				if !b.ExpectOutOfArea || b.Prefix == "786" || b.Prefix == "787" || len(b.Prefix) != 3 {
					t.Errorf("seed %d: a not-served buyer needs a prefix outside 786/787: %+v", seed, b)
				}
			case retailday.InDoubt:
				if b.Token != "tok_timeout" {
					t.Errorf("seed %d: an in-doubt buyer pays with tok_timeout: %+v", seed, b)
				}
			}
		}
	}
}

func TestEveryBuyerCarriesItsPlacedFacts(t *testing.T) {
	p := mustPlan(t, cfgFor(5), candidates(30))
	for _, b := range p.Buyers {
		if b.Placed.RunID != "RD1" || b.Placed.Expect != b.Expect || b.Placed.OrderID != "" {
			t.Errorf("buyer %d: Placed %+v", b.Index, b.Placed)
		}
		want := map[string]int{}
		for _, l := range b.Lines {
			want[l.Item.SKU] += l.Qty
		}
		if !reflect.DeepEqual(b.Placed.SKUs, want) || b.Placed.Lines() != len(b.Lines) {
			t.Errorf("buyer %d: Placed.SKUs %v, lines %v", b.Index, b.Placed.SKUs, b.Lines)
		}
	}
}

func TestPlanRefusesWhatCannotBePlanned(t *testing.T) {
	items := candidates(10)
	dup := append(candidates(10), Item{CatalogItemID: "cat-new", SKU: "SKU-003"})
	dupCat := append(candidates(10), Item{CatalogItemID: "cat-003", SKU: "SKU-new"})
	cases := []struct {
		name  string
		cfg   PlanConfig
		items []Item
		want  string
	}{
		{"no run id", PlanConfig{Seed: 1, Shoppers: 8, ShelfSize: 4}, items, "run id"},
		{"too few shoppers for the mix", PlanConfig{RunID: "R", Seed: 1, Shoppers: 4, ShelfSize: 4}, items, "at least 5"},
		{"no shelf", PlanConfig{RunID: "R", Seed: 1, Shoppers: 8, ShelfSize: 0}, items, "shelf"},
		{"shelf larger than the catalogue", PlanConfig{RunID: "R", Seed: 1, Shoppers: 8, ShelfSize: 11}, items, "shelf of 11"},
		{"two products on one SKU", PlanConfig{RunID: "R", Seed: 1, Shoppers: 8, ShelfSize: 4}, dup, "SKU-003"},
		{"one product twice", PlanConfig{RunID: "R", Seed: 1, Shoppers: 8, ShelfSize: 4}, dupCat, "cat-003"},
		{"a product without a SKU", PlanConfig{RunID: "R", Seed: 1, Shoppers: 8, ShelfSize: 4}, append(candidates(10), Item{CatalogItemID: "x"}), "SKU"},
	}
	for _, c := range cases {
		if _, err := BuildPlan(c.cfg, c.items); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: err %v, want it to mention %q", c.name, err, c.want)
		}
	}
}

func TestDemandIsWhatCouldLeaveTheShelfAndRefusesAnOverdrawnShelf(t *testing.T) {
	p := mustPlan(t, cfgFor(2), candidates(30))
	demand := p.Demand()
	want := map[string]int{}
	for _, b := range p.Buyers {
		if b.Expect == retailday.Declined || b.Expect == retailday.NotServed {
			continue // these provably never place an order
		}
		for _, l := range b.Lines {
			want[l.Item.SKU] += l.Qty
		}
	}
	if !reflect.DeepEqual(demand, want) {
		t.Fatalf("demand %v, want %v", demand, want)
	}

	enough := map[string]int{}
	for _, sku := range p.ShelfSKUs() {
		enough[sku] = demand[sku] + 5
	}
	if err := p.CheckDemand(enough); err != nil {
		t.Fatalf("a shelf with room was refused: %v", err)
	}
	exact := map[string]int{}
	for _, sku := range p.ShelfSKUs() {
		exact[sku] = demand[sku]
	}
	if err := p.CheckDemand(exact); err != nil {
		t.Fatalf("a shelf with exactly the demand was refused: %v", err)
	}

	var victim string
	for sku, n := range demand {
		if n > 0 {
			victim = sku
			break
		}
	}
	short := maps(enough)
	short[victim] = demand[victim] - 1
	if err := p.CheckDemand(short); err == nil || !strings.Contains(err.Error(), victim) {
		t.Fatalf("an overdrawn %s was not refused: %v", victim, err)
	}
	missing := maps(enough)
	delete(missing, victim)
	if err := p.CheckDemand(missing); err == nil || !strings.Contains(err.Error(), victim) {
		t.Fatalf("a shelf SKU with no stock reading was not refused: %v", err)
	}
}

func maps(in map[string]int) map[string]int {
	out := make(map[string]int, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
