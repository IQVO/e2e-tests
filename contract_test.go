package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// Capture the actual step payload, rather than a parallel test-only DTO.
func TestProcessPathCycleTimeContract(t *testing.T) {
	for _, operation := range []string{"define", "revise"} {
		t.Run(operation, func(t *testing.T) {
			var body map[string]any
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Error(err)
				}
				w.WriteHeader(http.StatusCreated)
			}))
			defer server.Close()
			old := processPathBaseURL
			processPathBaseURL = server.URL
			defer func() { processPathBaseURL = old }()
			world := newWorld()
			var err error
			if operation == "define" {
				err = world.definePath("contract-path", "contract", "pick", "90s")
			} else {
				err = world.revisePath("contract-path", "contract", "pick", "90s")
			}
			if err != nil {
				t.Fatal(err)
			}
			duration, _ := body["cycleTimeP95"].(string)
			d, err := time.ParseDuration(duration)
			if err != nil || d <= 0 {
				t.Fatalf("%s payload cycleTimeP95 = %q; want explicit positive duration (payload=%v)", operation, duration, body)
			}
		})
	}
}

func TestRunIdentityFromEnvironment(t *testing.T) {
	t.Setenv("E2E_RUN_ID", "123456789012")
	for _, world := range []*world{newWorld(), newWorld()} {
		if got := world.rs("NET-SKU-<run>"); got != "NET-SKU-123456789012" {
			t.Errorf("shared run identity = %q", got)
		}
		if got := world.resolvePathID("propagation-<run>"); got != "propagation-123456789012" {
			t.Errorf("path scope lost prefix: %q", got)
		}
	}
}

func TestDefaultTagsExcludeSetupAndSoak(t *testing.T) {
	if got := defaultGodogTags(); got != "~@soak && ~@setup" {
		t.Fatalf("default tags = %q; setup must run only during service preparation", got)
	}
}

