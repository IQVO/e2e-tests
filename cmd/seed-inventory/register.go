package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// invClient drives inventory-storage's PUBLISHED REST contract only (apis/openapi.yaml):
// PUT /bins/{binId} (declarative, idempotent), GET /bins/{binId} and POST /stock/stow.
type invClient struct {
	base string
	http *http.Client
}

func retryable(err error) bool {
	if ae, ok := err.(*apiError); ok {
		return ae.Status == http.StatusConflict || ae.Status == http.StatusTooManyRequests || ae.Status >= 500
	}
	return err != nil
}

// do sends one request, retrying transport errors, 5xx, 429 and 409 concurrent-modification
// (a stow racing another on the same bin). out, when non-nil, receives a 2xx JSON body.
func (c *invClient) do(ctx context.Context, method, path string, body any, key string, out any, ok ...int) (int, error) {
	var raw []byte
	if body != nil {
		raw, _ = json.Marshal(body)
	}
	var last error
	backoff := 150 * time.Millisecond
	for attempt := 1; attempt <= 6; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, c.base+path, bytes.NewReader(raw))
		if err != nil {
			return 0, err
		}
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		if key != "" {
			req.Header.Set("Idempotency-Key", key)
		}
		resp, err := c.http.Do(req)
		if err == nil {
			b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
			_ = resp.Body.Close()
			for _, s := range ok {
				if resp.StatusCode == s {
					if out != nil {
						_ = json.Unmarshal(b, out)
					}
					return s, nil
				}
			}
			err = &apiError{Status: resp.StatusCode, Body: strings.TrimSpace(string(b))}
		}
		last = err
		if !retryable(err) {
			return 0, err
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(backoff):
		}
		backoff *= 2
	}
	return 0, fmt.Errorf("after 6 attempts: %w", last)
}

type binState struct {
	BinID    string `json:"binId"`
	Capacity int    `json:"capacity"`
	Occupied int    `json:"occupied"`
}

// getBin returns nil, nil for a bin that does not exist yet.
func (c *invClient) getBin(ctx context.Context, id string) (*binState, error) {
	var b binState
	if _, err := c.do(ctx, http.MethodGet, "/bins/"+url.PathEscape(id), nil, "", &b, http.StatusOK); err != nil {
		if ae, ok := err.(*apiError); ok && ae.Status == http.StatusNotFound {
			return nil, nil
		}
		return nil, err
	}
	return &b, nil
}

type stockReport struct {
	binsCreated, binsResized, binsAlreadyStocked, binsPartial atomic.Int64
	units, lines                                              atomic.Int64
	mu                                                        sync.Mutex
	failures                                                  []string
}

func (r *stockReport) fail(format string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failures = append(r.failures, fmt.Sprintf(format, args...))
}

// stockBin registers the bin and stows its lines. It is safe to re-run:
//   - a bin already holding at least the planned units is skipped (a re-run adds nothing);
//   - PUT /bins converges to the planned capacity without touching occupancy;
//   - every stow carries a deterministic Idempotency-Key (bin, SKU, quantity), so a retry after
//     a dropped response replays instead of stowing twice.
func (c *invClient) stockBin(ctx context.Context, bp *BinPlan, rep *stockReport) {
	id := bp.Bin.ID
	cur, err := c.getBin(ctx, id)
	if err != nil {
		rep.fail("read bin %s: %v", id, err)
		return
	}
	if cur != nil && cur.Occupied >= bp.Units {
		rep.binsAlreadyStocked.Add(1)
		return
	}
	if cur != nil && cur.Occupied > 0 {
		rep.binsPartial.Add(1) // an interrupted earlier run: the idempotency keys complete it
	}
	status, err := c.do(ctx, http.MethodPut, "/bins/"+url.PathEscape(id), map[string]int{"capacity": bp.Capacity}, "", nil, http.StatusOK, http.StatusCreated)
	if err != nil {
		rep.fail("register bin %s (capacity %d): %v", id, bp.Capacity, err)
		return
	}
	if status == http.StatusCreated {
		rep.binsCreated.Add(1)
	} else if cur == nil || cur.Capacity != bp.Capacity {
		rep.binsResized.Add(1)
	}
	for _, l := range bp.Lines {
		key := fmt.Sprintf("seed-inventory-v1:%s:%s:%d", id, l.SKU, l.Qty)
		if len(key) > 255 {
			key = key[:255]
		}
		body := map[string]any{"sku": l.SKU, "quantity": l.Qty, "binId": id}
		if _, err := c.do(ctx, http.MethodPost, "/stock/stow", body, key, nil, http.StatusCreated, http.StatusOK); err != nil {
			rep.fail("stow %d x %s into %s: %v", l.Qty, l.SKU, id, err)
			return
		}
		rep.lines.Add(1)
		rep.units.Add(int64(l.Qty))
	}
}

// stock runs stockBin over every planned bin with a bounded worker pool.
func (c *invClient) stock(ctx context.Context, bins []*BinPlan, workers int, progress func(done int)) *stockReport {
	rep := &stockReport{}
	jobs := make(chan *BinPlan)
	var done atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for bp := range jobs {
				c.stockBin(ctx, bp, rep)
				if n := done.Add(1); progress != nil {
					progress(int(n))
				}
			}
		}()
	}
	for _, bp := range bins {
		jobs <- bp
	}
	close(jobs)
	wg.Wait()
	return rep
}

// verify reads every planned bin back and checks it holds at least the planned units and the
// planned capacity; it also reads GET /inventory/{sku}/usable for a sample of SKUs. It returns the
// problems and the total occupied units across the planned bins.
func (c *invClient) verify(ctx context.Context, plan *Plan, workers, sample int) (problems []string, occupied int) {
	var mu sync.Mutex
	var wg sync.WaitGroup
	jobs := make(chan *BinPlan)
	var total atomic.Int64
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for bp := range jobs {
				got, err := c.getBin(ctx, bp.Bin.ID)
				mu.Lock()
				switch {
				case err != nil:
					problems = append(problems, fmt.Sprintf("%s: %v", bp.Bin.ID, err))
				case got == nil:
					problems = append(problems, fmt.Sprintf("%s: bin missing", bp.Bin.ID))
				default:
					total.Add(int64(got.Occupied))
					if got.Occupied < bp.Units {
						problems = append(problems, fmt.Sprintf("%s: holds %d units, planned %d", bp.Bin.ID, got.Occupied, bp.Units))
					}
					if got.Capacity < bp.Capacity {
						problems = append(problems, fmt.Sprintf("%s: capacity %d, planned %d", bp.Bin.ID, got.Capacity, bp.Capacity))
					}
				}
				mu.Unlock()
			}
		}()
	}
	for _, bp := range plan.Bins {
		jobs <- bp
	}
	close(jobs)
	wg.Wait()

	// sample SKUs: usable inventory must cover the units planned for them
	planned := map[string]int{}
	var order []string
	for _, bp := range plan.Bins {
		for _, l := range bp.Lines {
			if _, seen := planned[l.SKU]; !seen {
				order = append(order, l.SKU)
			}
			planned[l.SKU] += l.Qty
		}
	}
	step := 1
	if sample > 0 && len(order) > sample {
		step = len(order) / sample
	}
	for i := 0; i < len(order) && sample > 0; i += step {
		sku := order[i]
		var u struct {
			Usable int `json:"usable"`
		}
		if _, err := c.do(ctx, http.MethodGet, "/inventory/"+url.PathEscape(sku)+"/usable", nil, "", &u, http.StatusOK); err != nil {
			problems = append(problems, fmt.Sprintf("usable %s: %v", sku, err))
		} else if u.Usable < planned[sku] {
			problems = append(problems, fmt.Sprintf("usable %s: %d, planned %d", sku, u.Usable, planned[sku]))
		}
	}
	return problems, int(total.Load())
}
