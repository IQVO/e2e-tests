package sim

import "testing"

// TestTotalOnCountsTheGivenTopicsOnly: Total keys on the short type across every tapped topic, so
// two producers of the same short name add up; TotalOn counts the producer's own topics.
func TestTotalOnCountsTheGivenTopicsOnly(t *testing.T) {
	tap := &EventTap{Counts: map[string]map[string]int{
		"warehouse.work-planning.events": {"ShiftPlanCommitted": 2},
		"warehouse.workforce.events":     {"ShiftPlanCommitted": 1},
		"warehouse.workforce.analytics":  {"ShiftPlanCommitted": 3, "AssociateShiftStarted": 5},
	}}
	if got := tap.Total("ShiftPlanCommitted"); got != 6 {
		t.Fatalf("Total = %d, want 6 (every topic)", got)
	}
	if got := tap.TotalOn("ShiftPlanCommitted", "warehouse.workforce.events", "warehouse.workforce.analytics"); got != 4 {
		t.Fatalf("TotalOn workforce topics = %d, want 4", got)
	}
	if got := tap.TotalOn("ShiftPlanCommitted", "warehouse.untapped.events"); got != 0 {
		t.Fatalf("TotalOn an untapped topic = %d, want 0", got)
	}
	if got := tap.TotalOn("ShiftPlanCommitted"); got != 0 {
		t.Fatalf("TotalOn no topic = %d, want 0", got)
	}
}
