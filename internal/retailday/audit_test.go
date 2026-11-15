package retailday

import (
	"strings"
	"testing"
	"time"
)

func healthy() OrderFacts {
	end := time.Date(2026, 11, 16, 21, 0, 0, 0, time.UTC)
	return OrderFacts{
		OrderID: "ord-1", Expect: Delivered, Lines: 2,
		Status: "DELIVERED", TotalMinor: 4299, AuthorizedMinor: 4299, CapturedMinor: 4299,
		POs:       []PO{{Number: "PO-1", State: "SHIPPED"}},
		OMOrders:  []OMOrder{{ID: "om-1", Status: "Shipped"}},
		WorkUnits: 2, PickTasksDone: 2, PackTasks: 1,
		Packages:              []Package{{ID: "pkg-1", Outcome: "LABELED"}},
		ShipmentConfirmations: 1,
		TrackingNumbers:       []string{"RN0000000000000001"},
		DeliveredAt:           end.Add(-30 * time.Hour), // a compressed run delivers long before the window ends
		PromiseEnd:            end,
	}
}

func rules(fs []Finding) []string {
	var out []string
	for _, f := range fs {
		if !f.Warn {
			out = append(out, f.Rule)
		}
	}
	return out
}

func TestAHealthyDeliveredOrderHasNoFindings(t *testing.T) {
	if fs := AuditOrder(healthy()); len(fs) != 0 {
		t.Fatalf("findings on a healthy order: %+v", fs)
	}
}

// Each case breaks exactly one fact and must be caught by exactly the rule that guards it: a
// rule that never fires proves nothing.
func TestEveryDeliveredRuleCatchesItsOwnBreakage(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*OrderFacts)
		want string
	}{
		{"authorised less than the total", func(f *OrderFacts) { f.AuthorizedMinor = 4000 }, "money/authorised"},
		{"captured less than the total", func(f *OrderFacts) { f.CapturedMinor = 0 }, "money/captured"},
		{"captured twice", func(f *OrderFacts) { f.CapturedMinor = 8598 }, "money/captured"},
		{"a hold was voided on a delivered order", func(f *OrderFacts) { f.VoidedMinor = 100 }, "money/voided"},
		{"no total", func(f *OrderFacts) { f.TotalMinor = 0; f.AuthorizedMinor = 0; f.CapturedMinor = 0 }, "money/total"},
		{"a duplicated purchase order", func(f *OrderFacts) { f.POs = append(f.POs, PO{Number: "PO-2", State: "ISSUED"}) }, "network/one-po"},
		{"no purchase order", func(f *OrderFacts) { f.POs = nil }, "network/one-po"},
		{"the purchase order expired", func(f *OrderFacts) { f.POs[0].State = "EXPIRED" }, "network/po-state"},
		{"two order-management orders for one PO", func(f *OrderFacts) { f.OMOrders = append(f.OMOrders, OMOrder{ID: "om-2", Status: "Held"}) }, "warehouse/one-order"},
		{"the order never shipped", func(f *OrderFacts) { f.OMOrders[0].Status = "Released" }, "warehouse/order-shipped"},
		{"a line without a work unit", func(f *OrderFacts) { f.WorkUnits = 1 }, "warehouse/work-units"},
		{"a pick not done", func(f *OrderFacts) { f.PickTasksDone = 1 }, "warehouse/picks"},
		{"two pack tasks", func(f *OrderFacts) { f.PackTasks = 2 }, "warehouse/one-pack"},
		{"no package", func(f *OrderFacts) { f.Packages = nil; f.TrackingNumbers = nil }, "warehouse/package"},
		{"a diverted package", func(f *OrderFacts) { f.Packages[0].Outcome = "DIVERTED" }, "warehouse/labeled"},
		{"shipment confirmed twice", func(f *OrderFacts) { f.ShipmentConfirmations = 2 }, "warehouse/shipment-confirmed"},
		{"shipment never confirmed", func(f *OrderFacts) { f.ShipmentConfirmations = 0 }, "warehouse/shipment-confirmed"},
		{"a tracking number short", func(f *OrderFacts) { f.TrackingNumbers = nil }, "network/tracking"},
		{"the shopper still sees SHIPPED", func(f *OrderFacts) { f.Status = "SHIPPED" }, "status"},
		{"never delivered", func(f *OrderFacts) { f.DeliveredAt = time.Time{} }, "delivered"},
		{"delivered after the window ended", func(f *OrderFacts) { f.DeliveredAt = f.PromiseEnd.Add(time.Minute) }, "promise"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := healthy()
			c.mut(&f)
			got := rules(AuditOrder(f))
			// "status" and the like may ride along with others; the guarding rule must be present
			// and nothing unrelated may have fired.
			found := false
			for _, r := range got {
				if r == c.want {
					found = true
				}
			}
			if !found {
				t.Fatalf("breaking %q: rules fired %v, want %s among them", c.name, got, c.want)
			}
			if len(got) > 2 {
				t.Fatalf("breaking %q fired %d rules %v: a single breakage should not light up the whole audit", c.name, len(got), got)
			}
		})
	}
}

func TestDeliveringEarlyIsNotLateAndAMissingWindowOnlyWarns(t *testing.T) {
	f := healthy()
	if got := rules(AuditOrder(f)); len(got) != 0 {
		t.Fatalf("early delivery must pass: %v", got)
	}
	f.PromiseEnd = time.Time{}
	fs := AuditOrder(f)
	if len(rules(fs)) != 0 || len(fs) != 1 || !fs[0].Warn || fs[0].Rule != "promise" {
		t.Fatalf("a missing window must warn once and not fail: %+v", fs)
	}
}

func TestRefusedAndCancelledOrdersLeaveNothingBehind(t *testing.T) {
	clean := OrderFacts{OrderID: "o", Expect: Declined}
	if fs := AuditOrder(clean); len(fs) != 0 {
		t.Fatalf("a declined order with nothing downstream must pass: %+v", fs)
	}
	for _, c := range []struct {
		name string
		f    OrderFacts
		want string
	}{
		{"declined but a PO exists", OrderFacts{OrderID: "o", Expect: Declined, POs: []PO{{Number: "PO-9", State: "ISSUED"}}}, "network/no-po"},
		{"not served but an OM order exists", OrderFacts{OrderID: "o", Expect: NotServed, OMOrders: []OMOrder{{ID: "om"}}}, "warehouse/no-order"},
		{"declined but money was captured", OrderFacts{OrderID: "o", Expect: Declined, CapturedMinor: 5}, "money/untouched"},
		{"declined, authorised and not voided", OrderFacts{OrderID: "o", Expect: Declined, AuthorizedMinor: 5}, "money/untouched"},
		{"cancelled but a work unit exists", OrderFacts{OrderID: "o", Expect: Cancelled, Status: "CANCELLED", WorkUnits: 1}, "warehouse/no-work"},
		{"cancelled, hold not released", OrderFacts{OrderID: "o", Expect: Cancelled, Status: "CANCELLED", AuthorizedMinor: 100, VoidedMinor: 40}, "money/released"},
		{"cancelled but captured", OrderFacts{OrderID: "o", Expect: Cancelled, Status: "CANCELLED", CapturedMinor: 1}, "money/not-captured"},
		{"cancelled but the shopper sees PLACED", OrderFacts{OrderID: "o", Expect: Cancelled, Status: "PLACED"}, "status"},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := rules(AuditOrder(c.f))
			if len(got) == 0 || got[0] != c.want {
				t.Fatalf("rules %v, want %s first", got, c.want)
			}
		})
	}
	ok := OrderFacts{OrderID: "o", Expect: Cancelled, Status: "CANCELLED", AuthorizedMinor: 100, VoidedMinor: 100}
	if fs := AuditOrder(ok); len(fs) != 0 {
		t.Fatalf("a cancelled order whose hold was released in full must pass: %+v", fs)
	}
}

func TestInDoubtIsRecordedNotAssertedGreen(t *testing.T) {
	fs := AuditOrder(OrderFacts{OrderID: "o", Expect: InDoubt})
	if len(fs) != 1 || !fs[0].Warn || !strings.Contains(fs[0].Detail, "G8") {
		t.Fatalf("want one G8 warning, got %+v", fs)
	}
	if fs := AuditOrder(OrderFacts{OrderID: "o", Expect: InDoubt, CapturedMinor: 10}); FailedFindings(fs) != 1 {
		t.Fatalf("money moved on an in-doubt order must fail: %+v", fs)
	}
}

func TestAnUnknownExpectationFailsInsteadOfPassingSilently(t *testing.T) {
	if got := rules(AuditOrder(OrderFacts{OrderID: "o", Expect: "teleported"})); len(got) != 1 || got[0] != "expectation" {
		t.Fatalf("got %v", got)
	}
}

func TestEstateStockMustBeConservedAndTheStorefrontMustFollow(t *testing.T) {
	good := []StockFacts{{SKU: "B", UsableBefore: 10, Shipped: 3, Restocked: 5, UsableAfter: 12, Purchasable: true}, {SKU: "A", UsableBefore: 3, Shipped: 3, UsableAfter: 0, Purchasable: false}}
	if fs := AuditEstate(good, 0, 0, 0); len(fs) != 0 {
		t.Fatalf("conserved stock must pass: %+v", fs)
	}
	for name, c := range map[string]struct {
		stock []StockFacts
		dlq   int
		errs  int
		want  string
	}{
		"stock lost":               {[]StockFacts{{SKU: "A", UsableBefore: 10, Shipped: 3, UsableAfter: 6, Purchasable: true}}, 0, 0, "stock/conserved"},
		"stock appeared":           {[]StockFacts{{SKU: "A", UsableBefore: 10, Shipped: 3, UsableAfter: 8, Purchasable: true}}, 0, 0, "stock/conserved"},
		"still sold at zero":       {[]StockFacts{{SKU: "A", UsableBefore: 1, Shipped: 1, UsableAfter: 0, Purchasable: true}}, 0, 0, "storefront/follows-stock"},
		"not sold with stock":      {[]StockFacts{{SKU: "A", UsableBefore: 5, UsableAfter: 5, Purchasable: false}}, 0, 0, "storefront/follows-stock"},
		"a dead letter of the run": {nil, 2, 0, "kafka/dead-letters"},
		"a 5xx":                    {nil, 0, 1, "http/5xx"},
	} {
		got := rules(AuditEstate(c.stock, c.dlq, c.errs, 0))
		if len(got) != 1 || got[0] != c.want {
			t.Errorf("%s: rules %v, want [%s]", name, got, c.want)
		}
	}
}

func TestEstateFindingsAreOrderedBySKU(t *testing.T) {
	fs := AuditEstate([]StockFacts{{SKU: "Z", UsableBefore: 5, UsableAfter: 1, Purchasable: true}, {SKU: "A", UsableBefore: 5, UsableAfter: 1, Purchasable: true}}, 0, 0, 0)
	if len(fs) != 2 || !strings.HasPrefix(fs[0].Detail, "A:") || !strings.HasPrefix(fs[1].Detail, "Z:") {
		t.Fatalf("report order must be deterministic: %+v", fs)
	}
}
