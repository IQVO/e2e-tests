package main

import "testing"

func TestIsStowCandidate(t *testing.T) {
	cases := []struct {
		status, code, typ string
		want              bool
	}{
		{"Active", "WH1-STOR-AMB-A01-01-01-A", "PalletRack", true},
		{"Active", "WH1-AUTO-ASRS-ASRS1-00-00-S", automatedStorageType, false},
		{"Decommissioned", "WH1-STOR-AMB-A01-01-01-B", "PalletRack", false},
		{"Active", "", "PalletRack", false},
	}
	for _, c := range cases {
		if got := isStowCandidate(c.status, c.code, c.typ); got != c.want {
			t.Fatalf("isStowCandidate(%q, %q, %q) = %v, want %v", c.status, c.code, c.typ, got, c.want)
		}
	}
}
