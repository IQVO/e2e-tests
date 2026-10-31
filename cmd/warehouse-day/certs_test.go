package main

import (
	"math/rand"
	"testing"
)

// Every staffed associate must hold exactly the capabilities of the path it is
// assigned to: workforce-management refuses an assignment otherwise (409).
func TestStaffedAssociatesHoldTheirPathsRequiredCapability(t *testing.T) {
	s := &sim{cfg: config{pickers: 2, packers: 1, slammers: 1, rebinners: 1, flex: 1, runID: "T"}, rnd: rand.New(rand.NewSource(1))}
	s.staffTheFloor()
	if len(s.associates) != 6 {
		t.Fatalf("staffed %d associates, want 6", len(s.associates))
	}
	for _, a := range s.associates {
		want := capabilityOf(a.path)
		found := false
		for _, c := range a.certs {
			if c == want {
				found = true
			}
		}
		if !found {
			t.Errorf("%s assigned to %s holds %v, needs %q", a.id, a.path, a.certs, want)
		}
	}
}

func TestCapabilityOfIsTheDefinedRequiredCapability(t *testing.T) {
	for _, p := range processPaths {
		if got := capabilityOf(p.id); got != p.capability {
			t.Errorf("capabilityOf(%s) = %q, want %q", p.id, got, p.capability)
		}
	}
}
