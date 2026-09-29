// soak_test.go — steps for features/soak_backlog_ramp.feature (@soak).
//
// A long-running, ramping load scenario: two injector goroutines enqueue
// and release work units into the "pick-soak"/"pack-soak" process paths
// at an interval that ramps from SOAK_RAMP_START_INTERVAL down to
// SOAK_RAMP_END_INTERVAL over SOAK_DURATION, while a configurable pool of
// picker/packer station goroutines continuously claim-next and complete
// the resulting fulfillment-execution tasks. See the feature file's own
// header comment for why this is observational (no backlog/queue-depth
// assertions) rather than pass/fail like every other scenario in this
// repo, and why it is excluded from the default `go test` run.
//
// Deliberately does NOT reuse world.doJSON/world.last: multiple goroutines
// hit the same *world concurrently here, and world.last is not safe for
// concurrent access (every other feature's steps run single-scenario,
// single-goroutine, so that was never a problem before this file).
// soakPostJSON below is the same request/response shape but returns its
// result directly instead of mutating shared state.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// soakMetrics is the running counter set printed by soakSummaryIsPrinted.
// atomic.Int64 fields: written concurrently by every injector/worker
// goroutine, read once at the end from the scenario goroutine.
type soakMetrics struct {
	enqueued, enqueueErrors                  atomic.Int64
	released, releaseRejected, releaseErrors atomic.Int64
	claimed, claimRejected, claimErrors      atomic.Int64
	completed, completeErrors                atomic.Int64
}

// soakState carries state across this scenario's four steps (seed pools ->
// register stations -> run ramp -> print summary).
type soakState struct {
	pickPathID, packPathID string
	duration               time.Duration
	wipLimit               int
	pickers, packers       int
	metrics                soakMetrics
}

func envDurationOrDefault(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}

func envIntOrDefault(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

// soakPostJSON is doJSON's concurrency-safe sibling: performs a POST
// against url with an optional JSON body and returns the result directly
// rather than recording it on a shared *world. http.Client is safe for
// concurrent use (net/http docs), so sharing w.client across goroutines is
// fine as long as nothing also touches w.last from those goroutines.
func soakPostJSON(client *http.Client, url string, body any) (int, []byte, error) {
	var reader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return 0, nil, err
		}
		reader = bytes.NewReader(b)
	}
	req, err := http.NewRequest(http.MethodPost, url, reader)
	if err != nil {
		return 0, nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, fmt.Errorf("HTTP POST %s: %w", url, err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, nil, err
	}
	return resp.StatusCode, b, nil
}

// sleepCtx sleeps for d unless ctx is cancelled first — used by the
// worker/injector loops so the whole scenario stops promptly once the
// soak duration elapses instead of finishing whatever sleep is in flight.
func sleepCtx(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

// ---------------------------------------------------------------------
// step: seed both soak work pools
// ---------------------------------------------------------------------

// soakSeedWorkPools seeds release-fed work pools for both soak process
// paths (same direct-DB-seed pattern as wesHasReleaseFedWorkPoolWithWIPLimit,
// reused here for two paths) and initializes w.soak.
func (w *world) soakSeedWorkPools(pickPathID, packPathID string) error {
	w.soak = &soakState{
		pickPathID: pickPathID,
		packPathID: packPathID,
		wipLimit:   envIntOrDefault("SOAK_WIP_LIMIT", 20),
	}
	if err := w.wesHasReleaseFedWorkPoolWithWIPLimit(pickPathID, w.soak.wipLimit); err != nil {
		return fmt.Errorf("seeding pick-soak work pool: %w", err)
	}
	if err := w.wesHasReleaseFedWorkPoolWithWIPLimit(packPathID, w.soak.wipLimit); err != nil {
		return fmt.Errorf("seeding pack-soak work pool: %w", err)
	}
	return nil
}

// ---------------------------------------------------------------------
// step: register the configured picker/packer stations
// ---------------------------------------------------------------------

func (w *world) soakRegisterStations() error {
	if w.soak == nil {
		return fmt.Errorf("soak state not initialized — the work-pool seeding step must run first")
	}
	w.soak.pickers = envIntOrDefault("SOAK_PICKERS", 3)
	w.soak.packers = envIntOrDefault("SOAK_PACKERS", 2)

	for i := 0; i < w.soak.pickers; i++ {
		if err := w.registerStation(fmt.Sprintf("soak-picker-%d", i), "pick"); err != nil {
			return fmt.Errorf("registering picker station %d: %w", i, err)
		}
	}
	for i := 0; i < w.soak.packers; i++ {
		if err := w.registerStation(fmt.Sprintf("soak-packer-%d", i), "pack"); err != nil {
			return fmt.Errorf("registering packer station %d: %w", i, err)
		}
	}
	return nil
}

// ---------------------------------------------------------------------
// step: run the ramp
// ---------------------------------------------------------------------

// soakInjectorLoop enqueues a new work unit onto pathID then immediately
// attempts to release it, on an interval that ramps LINEARLY from start
// down to end over the soak's configured duration (so the last minute of
// an hour-long run injects far faster than the first). A 409 from
// /release (WIP limit reached, or nothing pending) is an EXPECTED steady
// -state outcome once pickers/packers can't keep up with the ramp — see
// the feature file's header comment — so it is counted, not treated as an
// error.
func (w *world) soakInjectorLoop(ctx context.Context, pathID string, start, end time.Duration, wg *sync.WaitGroup) {
	defer wg.Done()
	m := &w.soak.metrics
	loopStart := time.Now()
	startNano := loopStart.UnixNano()
	var seq int64

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		frac := float64(time.Since(loopStart)) / float64(w.soak.duration)
		if frac > 1 {
			frac = 1
		}
		interval := time.Duration(float64(start) + (float64(end)-float64(start))*frac)
		if interval < 10*time.Millisecond {
			interval = 10 * time.Millisecond
		}

		seq++
		workUnitID := fmt.Sprintf("soak-%s-%d-%d", pathID, startNano, seq)
		status, _, err := soakPostJSON(w.client, fmt.Sprintf("%s/paths/%s/work-units", wesBaseURL, pathID), map[string]any{
			"workUnitId": workUnitID,
			"cpt":        time.Now().UTC().Add(time.Hour).Format(time.RFC3339),
			"reference":  workUnitID,
		})
		switch {
		case err != nil, status != http.StatusCreated:
			m.enqueueErrors.Add(1)
		default:
			m.enqueued.Add(1)
			rstatus, _, rerr := soakPostJSON(w.client, fmt.Sprintf("%s/paths/%s/release", wesBaseURL, pathID), nil)
			switch {
			case rerr != nil:
				m.releaseErrors.Add(1)
			case rstatus == http.StatusOK:
				m.released.Add(1)
			case rstatus == http.StatusConflict:
				m.releaseRejected.Add(1)
			default:
				m.releaseErrors.Add(1)
			}
		}

		sleepCtx(ctx, interval)
	}
}

// soakWorkerLoop is one station continuously pulling work: claim-next,
// complete on success, and back off briefly on 409 (no claimable task —
// expected whenever this station's queue is temporarily drained) or on a
// transport/unexpected-status error (to avoid hot-looping against a
// genuinely broken dependency).
func (w *world) soakWorkerLoop(ctx context.Context, stationID, taskType string, wg *sync.WaitGroup) {
	defer wg.Done()
	m := &w.soak.metrics

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		status, body, err := soakPostJSON(w.client, fmt.Sprintf("%s/stations/%s/claim-next", fulfillmentBaseURL, stationID), map[string]any{
			"taskType": taskType,
		})
		switch {
		case err != nil:
			m.claimErrors.Add(1)
			sleepCtx(ctx, 500*time.Millisecond)
		case status == http.StatusOK:
			m.claimed.Add(1)
			var parsed map[string]any
			_ = json.Unmarshal(body, &parsed)
			taskID, _ := parsed["id"].(string)
			if taskID == "" {
				m.completeErrors.Add(1)
				continue
			}
			cstatus, _, cerr := soakPostJSON(w.client, fmt.Sprintf("%s/tasks/%s/complete", fulfillmentBaseURL, taskID), map[string]any{
				"stationId": stationID,
			})
			if cerr != nil || cstatus < 200 || cstatus >= 300 {
				m.completeErrors.Add(1)
			} else {
				m.completed.Add(1)
			}
		case status == http.StatusConflict:
			m.claimRejected.Add(1)
			sleepCtx(ctx, 300*time.Millisecond)
		default:
			m.claimErrors.Add(1)
			sleepCtx(ctx, 500*time.Millisecond)
		}
	}
}

// soakRampBacklog is the main step: spins up both injectors and every
// configured picker/packer worker, lets them all run concurrently for
// SOAK_DURATION (default 1h), then joins. Fails the scenario ONLY on a
// hard setup/connectivity problem (zero work units ever enqueued across
// the whole run) — everything else (WIP-limit 409s, no-claimable-task
// 409s) is expected ramp behavior, reported in the summary, not a
// failure. See the feature file's header comment.
func (w *world) soakRampBacklog(pickPathID, packPathID string) error {
	if w.soak == nil {
		return fmt.Errorf("soak state not initialized — earlier soak steps must run first")
	}

	w.soak.duration = envDurationOrDefault("SOAK_DURATION", time.Hour)
	startInterval := envDurationOrDefault("SOAK_RAMP_START_INTERVAL", 2*time.Second)
	endInterval := envDurationOrDefault("SOAK_RAMP_END_INTERVAL", 200*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), w.soak.duration)
	defer cancel()

	var wg sync.WaitGroup

	wg.Add(2)
	go w.soakInjectorLoop(ctx, pickPathID, startInterval, endInterval, &wg)
	go w.soakInjectorLoop(ctx, packPathID, startInterval, endInterval, &wg)

	for i := 0; i < w.soak.pickers; i++ {
		wg.Add(1)
		go w.soakWorkerLoop(ctx, fmt.Sprintf("soak-picker-%d", i), "PICK", &wg)
	}
	for i := 0; i < w.soak.packers; i++ {
		wg.Add(1)
		go w.soakWorkerLoop(ctx, fmt.Sprintf("soak-packer-%d", i), "PACK", &wg)
	}

	wg.Wait()

	if w.soak.metrics.enqueued.Load() == 0 {
		return fmt.Errorf(
			"soak run never successfully enqueued a single work unit on %q or %q — check connectivity (enqueue errors: %d)",
			pickPathID, packPathID, w.soak.metrics.enqueueErrors.Load(),
		)
	}
	return nil
}

// ---------------------------------------------------------------------
// step: print the summary
// ---------------------------------------------------------------------

func (w *world) soakSummaryIsPrinted() error {
	if w.soak == nil {
		return fmt.Errorf("soak state not initialized")
	}
	m := &w.soak.metrics
	fmt.Printf(`
=== soak run summary (paths %q / %q, duration %s, %d pickers, %d packers, WIP limit %d) ===
  enqueue:  %6d ok   %6d errors
  release:  %6d ok   %6d rejected(409: WIP limit/empty)   %6d errors
  claim:    %6d ok   %6d rejected(409: nothing claimable) %6d errors
  complete: %6d ok   %6d errors
===============================================================================
`,
		w.soak.pickPathID, w.soak.packPathID, w.soak.duration, w.soak.pickers, w.soak.packers, w.soak.wipLimit,
		m.enqueued.Load(), m.enqueueErrors.Load(),
		m.released.Load(), m.releaseRejected.Load(), m.releaseErrors.Load(),
		m.claimed.Load(), m.claimRejected.Load(), m.claimErrors.Load(),
		m.completed.Load(), m.completeErrors.Load(),
	)
	return nil
}
