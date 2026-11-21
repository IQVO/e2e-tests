package retailday

import (
	"context"
	"testing"
)

// mapEvidence is the smallest Evidence: it pins the interface so a change to the seam
// breaks the build of both the collector and the runner at once.
type mapEvidence struct {
	orders map[string]OrderFacts
	shelf  map[string]Shelf
}

func (m mapEvidence) Order(_ context.Context, p Placed) (OrderFacts, []string) {
	f, ok := m.orders[p.OrderID]
	if !ok {
		return OrderFacts{OrderID: p.OrderID, Expect: p.Expect}, []string{"order not found"}
	}
	return f, nil
}
func (m mapEvidence) Shelf(_ context.Context, skus []string) (map[string]Shelf, error) {
	out := map[string]Shelf{}
	for _, s := range skus {
		out[s] = m.shelf[s]
	}
	return out, nil
}
func (m mapEvidence) DeadLetters(context.Context, string) (int, bool, error) { return 0, false, nil }

var _ Evidence = mapEvidence{}

func TestPlacedLinesIsTheNumberOfSKUs(t *testing.T) {
	if n := (Placed{SKUs: map[string]int{"A": 2, "B": 1}}).Lines(); n != 2 {
		t.Fatalf("lines %d, want 2 (a line is a SKU, not a unit)", n)
	}
	if n := (Placed{}).Lines(); n != 0 {
		t.Fatalf("an order that was never placed has no lines, got %d", n)
	}
}

func TestEvidenceDoubleSeparatesNotFoundFromNone(t *testing.T) {
	var e Evidence = mapEvidence{}
	f, notes := e.Order(context.Background(), Placed{OrderID: "x", Expect: Delivered})
	if len(notes) != 1 || f.OrderID != "x" {
		t.Fatalf("a missing order must come back with a note, got %+v %v", f, notes)
	}
	if _, known, _ := e.DeadLetters(context.Background(), "r"); known {
		t.Fatal("the double cannot count dead letters and must say so")
	}
}
