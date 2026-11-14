package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

// An unreachable gateway is the failure the command exists to catch: every check must
// fail closed, each with a fix, and the exit code must say so, with or without -preflight.
const dead = "http://127.0.0.1:1"

func TestPreflightFailsClosedOnAnUnreachableCluster(t *testing.T) {
	var out, errb bytes.Buffer
	code := run([]string{"-preflight", "-gateway", dead + "/api", "-storefront", dead + "/api/storefront"}, &out, &errb)
	if code != 1 {
		t.Fatalf("exit %d, want 1; stderr %s", code, errb.String())
	}
	text := out.String()
	if strings.Contains(text, "PASS  health") {
		t.Errorf("a context behind a dead gateway passed:\n%s", text)
	}
	if !strings.Contains(text, "FAIL  health order-management") || !strings.Contains(text, "fix: kubectl -n warehouse-systems get pods") {
		t.Errorf("no failing health line with its fix:\n%s", text)
	}
	if !strings.Contains(errb.String(), "preflight FAILED") {
		t.Errorf("stderr: %s", errb.String())
	}
}

func TestADayIsNeverStartedOnAFailingPreflight(t *testing.T) {
	var out, errb bytes.Buffer
	if code := run([]string{"-gateway", dead + "/api", "-storefront", dead}, &out, &errb); code != 1 {
		t.Fatalf("exit %d, want 1 (a failed preflight), stderr %s", code, errb.String())
	}
	if strings.Contains(errb.String(), "not built yet") {
		t.Errorf("reached the day: %s", errb.String())
	}
}

func TestJSONOutputIsMachineReadable(t *testing.T) {
	var out, errb bytes.Buffer
	if code := run([]string{"-preflight", "-json", "-gateway", dead + "/api", "-storefront", dead}, &out, &errb); code != 1 {
		t.Fatalf("exit %d", code)
	}
	var doc struct {
		Checks []struct {
			Name string `json:"name"`
			OK   bool   `json:"ok"`
		} `json:"checks"`
		Failed int `json:"failed"`
	}
	if err := json.Unmarshal(out.Bytes(), &doc); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, out.String())
	}
	if doc.Failed == 0 || doc.Failed != len(doc.Checks) {
		t.Errorf("failed %d of %d checks; a dead gateway fails every one", doc.Failed, len(doc.Checks))
	}
}

func TestUsageErrorExitsTwo(t *testing.T) {
	var out, errb bytes.Buffer
	if code := run([]string{"-no-such-flag"}, &out, &errb); code != 2 {
		t.Fatalf("exit %d, want 2", code)
	}
}
