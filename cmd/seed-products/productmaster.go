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

// pmClient drives product-master's PUBLISHED REST contract only
// (apis/openapi.yaml): PUT /products/{sku}, PUT
// /products/{sku}/classification and GET /products. Every write there is an
// idempotent PUT, so a re-run against a dirty database is safe: an
// identical body answers 200 and changes nothing (no version bump, no
// event). That is what makes this seeder fit the suite's idempotency rule
// without run-scoping the SKUs: the GTINs are shared reference data.
type pmClient struct {
	base string
	http *http.Client
}

type apiError struct {
	Status int
	Body   string
}

func (e *apiError) Error() string { return fmt.Sprintf("HTTP %d: %s", e.Status, e.Body) }

// retryable: a concurrent write won (409 concurrent-modification), the
// service is shedding load, or the transport failed. A 400 is a contract
// violation in our request and never retried.
func retryable(err error) bool {
	if ae, ok := err.(*apiError); ok {
		return ae.Status == http.StatusConflict || ae.Status == http.StatusTooManyRequests || ae.Status >= 500
	}
	return err != nil
}

func (c *pmClient) put(ctx context.Context, path string, body any, wantStatus ...int) (int, error) {
	raw, err := json.Marshal(body)
	if err != nil {
		return 0, err
	}
	var lastErr error
	backoff := 200 * time.Millisecond
	for attempt := 1; attempt <= 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.base+path, bytes.NewReader(raw))
		if err != nil {
			return 0, err
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := c.http.Do(req)
		if err == nil {
			b, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
			_ = resp.Body.Close()
			for _, s := range wantStatus {
				if resp.StatusCode == s {
					return s, nil
				}
			}
			err = &apiError{Status: resp.StatusCode, Body: strings.TrimSpace(string(b))}
		}
		lastErr = err
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
	return 0, fmt.Errorf("after 5 attempts: %w", lastErr)
}

type registerReport struct {
	created, updated, classified, classUnchanged, dimensioned atomic.Int64
	mu                                                        sync.Mutex
	failures                                                  []string
}

func (r *registerReport) fail(format string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failures = append(r.failures, fmt.Sprintf(format, args...))
}

// register registers (and, where the fixture says so, classifies) every
// product with a bounded worker pool.
func (c *pmClient) register(ctx context.Context, products []FixtureProduct, workers int, classify bool, progress func(done int)) *registerReport {
	rep := &registerReport{}
	jobs := make(chan FixtureProduct)
	var done atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for p := range jobs {
				c.registerOne(ctx, p, classify, rep)
				if n := done.Add(1); progress != nil {
					progress(int(n))
				}
			}
		}()
	}
	for _, p := range products {
		jobs <- p
	}
	close(jobs)
	wg.Wait()
	return rep
}

func (c *pmClient) registerOne(ctx context.Context, p FixtureProduct, classify bool, rep *registerReport) {
	sku := url.PathEscape(p.SKU)
	status, err := c.put(ctx, "/products/"+sku, map[string]string{"description": p.Description}, http.StatusCreated, http.StatusOK)
	if err != nil {
		rep.fail("register %s: %v", p.SKU, err)
		return
	}
	if status == http.StatusCreated {
		rep.created.Add(1)
	} else {
		rep.updated.Add(1)
	}
	if p.Dimensions != nil {
		d := p.Dimensions
		body := map[string]int64{"lengthMm": d.LengthMm, "widthMm": d.WidthMm, "heightMm": d.HeightMm, "weightG": d.WeightG}
		if _, err = c.put(ctx, "/products/"+sku+"/dimensions/declared", body, http.StatusOK, http.StatusCreated); err != nil {
			rep.fail("dimensions %s: %v", p.SKU, err)
			return
		}
		rep.dimensioned.Add(1)
	}
	tags, temp, dot, ok := p.classification()
	if !classify || !ok {
		return
	}
	body := map[string]any{"handlingTags": tags}
	if temp != "" {
		body["temperatureClass"] = temp
	}
	if dot != 0 {
		body["dotHazardClass"] = dot
	}
	status, err = c.put(ctx, "/products/"+sku+"/classification", body, http.StatusCreated, http.StatusOK)
	if err != nil {
		rep.fail("classify %s: %v", p.SKU, err)
		return
	}
	if status == http.StatusCreated {
		rep.classified.Add(1)
	} else {
		rep.classUnchanged.Add(1)
	}
}

type listedProduct struct {
	SKU            string `json:"sku"`
	Description    string `json:"description"`
	Classification *struct {
		HandlingTags     []string `json:"handlingTags"`
		TemperatureClass string   `json:"temperatureClass"`
		DotHazardClass   *int     `json:"dotHazardClass"`
	} `json:"classification"`
	PhysicalProfile struct {
		Declared *struct {
			LengthMm int64 `json:"lengthMm"`
			WidthMm  int64 `json:"widthMm"`
			HeightMm int64 `json:"heightMm"`
			WeightG  int64 `json:"weightG"`
		} `json:"declared"`
	} `json:"physicalProfile"`
}

// listAll pages through GET /products (max page size 500).
func (c *pmClient) listAll(ctx context.Context) (map[string]listedProduct, error) {
	out := map[string]listedProduct{}
	cursor := ""
	for {
		u := c.base + "/products?limit=500"
		if cursor != "" {
			u += "&cursor=" + url.QueryEscape(cursor)
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			return nil, err
		}
		resp, err := c.http.Do(req)
		if err != nil {
			return nil, err
		}
		var page struct {
			Items      []listedProduct `json:"items"`
			NextCursor string          `json:"nextCursor"`
		}
		if resp.StatusCode != http.StatusOK {
			b, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
			_ = resp.Body.Close()
			return nil, &apiError{Status: resp.StatusCode, Body: string(b)}
		}
		err = json.NewDecoder(resp.Body).Decode(&page)
		_ = resp.Body.Close()
		if err != nil {
			return nil, err
		}
		for _, it := range page.Items {
			out[it.SKU] = it
		}
		if page.NextCursor == "" {
			return out, nil
		}
		cursor = page.NextCursor
	}
}

// verify reads the catalogue back over the API and checks every fixture
// product is there with the exact description and, when classified, the
// exact temperature class. Reading back through the list endpoint (not the
// write responses) proves the data is durable and queryable.
func verify(products []FixtureProduct, listed map[string]listedProduct, classify bool) []string {
	var problems []string
	for _, p := range products {
		got, ok := listed[p.SKU]
		if !ok {
			problems = append(problems, fmt.Sprintf("%s: not listed by GET /products", p.SKU))
			continue
		}
		if got.Description != p.Description {
			problems = append(problems, fmt.Sprintf("%s: description %q, want %q", p.SKU, got.Description, p.Description))
		}
		if d := p.Dimensions; d != nil {
			g := got.PhysicalProfile.Declared
			if g == nil || g.LengthMm != d.LengthMm || g.WidthMm != d.WidthMm || g.HeightMm != d.HeightMm || g.WeightG != d.WeightG {
				problems = append(problems, fmt.Sprintf("%s: declared dimensions %+v, want %+v", p.SKU, g, *d))
			}
		}
		tags, temp, dot, ok := p.classification()
		if !classify || !ok {
			continue
		}
		c := got.Classification
		if c == nil || c.TemperatureClass != temp || !sameSet(c.HandlingTags, tags) || (dot != 0 && (c.DotHazardClass == nil || *c.DotHazardClass != dot)) {
			problems = append(problems, fmt.Sprintf("%s: classification %+v, want %v/%s/dot %d", p.SKU, c, tags, temp, dot))
		}
	}
	return problems
}

func sameSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for _, x := range a {
		if !contains(b, x) {
			return false
		}
	}
	return true
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}
