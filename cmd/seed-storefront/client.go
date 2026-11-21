package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"
)

// apiError is a non-expected HTTP answer; Type is the RFC 9457 problem slug when there is one.
type apiError struct {
	Method, URL string
	Status      int
	Type        string
	Body        string
}

func (e *apiError) Error() string {
	return fmt.Sprintf("%s %s: HTTP %d %s", e.Method, e.URL, e.Status, e.Body)
}

func isStatus(err error, status int) bool {
	var ae *apiError
	return errors.As(err, &ae) && ae.Status == status
}

// errDryRun guards the write path: a dry run or a verify-only run never reaches it.
var errDryRun = errors.New("internal: write attempted in a read-only run")

// client speaks the PUBLISHED REST contracts only (through Kong) and counts every write by kind.
type client struct {
	base     string // e.g. http://localhost:8000/api (each context at <base>/<context>)
	sf       string // storefront BFF, e.g. http://storefront.localhost/api/storefront
	http     *http.Client
	readOnly bool
	writes   map[string]int
	backoff  time.Duration
}

func newClient(base, sf string, hc *http.Client, readOnly bool) *client {
	return &client{
		base: strings.TrimRight(base, "/"), sf: strings.TrimRight(sf, "/"), http: hc, readOnly: readOnly,
		writes: map[string]int{}, backoff: 200 * time.Millisecond,
	}
}

// idempotencyKey is deterministic: "seed-sf:" + the first 40 hex digits of
// sha256(op + path + canonical body). For body-less commands (publish) and commands whose effect
// depends on the aggregate version (revise), the material also carries the revision it applies
// to, so a later, different command on the same path never replays an old stored response.
func idempotencyKey(op, path string, material any) string {
	raw, _ := json.Marshal(material) // struct fields in declaration order, map keys sorted: canonical
	sum := sha256.Sum256([]byte(op + "\n" + path + "\n" + string(raw)))
	return "seed-sf:" + hex.EncodeToString(sum[:])[:40]
}

// get reads base+path into out. 404 is returned as an *apiError the caller can test.
func (c *client) get(ctx context.Context, url string, out any) error {
	_, err := c.do(ctx, http.MethodGet, url, nil, "", out, http.StatusOK)
	return err
}

// write POSTs body (nil: no body) under a deterministic Idempotency-Key and counts it under kind.
func (c *client) write(ctx context.Context, kind, path string, body, keyMaterial, out any, ok ...int) error {
	if c.readOnly {
		return errDryRun
	}
	if keyMaterial == nil {
		keyMaterial = body
	}
	key := idempotencyKey(kind, path, keyMaterial)
	if _, err := c.do(ctx, http.MethodPost, c.base+path, body, key, out, ok...); err != nil {
		return err
	}
	c.writes[kind]++
	return nil
}

// put sends an idempotent PUT (a wholesale replacement: no Idempotency-Key in the contract, a
// retry re-applies the same state) and counts it under kind.
func (c *client) put(ctx context.Context, kind, path string, body, out any, ok ...int) error {
	if c.readOnly {
		return errDryRun
	}
	if _, err := c.do(ctx, http.MethodPut, c.base+path, body, "", out, ok...); err != nil {
		return err
	}
	c.writes[kind]++
	return nil
}

// do sends one request, retrying transport errors, 429 and 5xx with the SAME key (a retry after a
// dropped response replays the stored answer instead of writing twice).
func (c *client) do(ctx context.Context, method, url string, body any, key string, out any, ok ...int) (int, error) {
	var raw []byte
	if body != nil {
		raw, _ = json.Marshal(body)
	}
	var last error
	wait := c.backoff
	for attempt := 1; attempt <= 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(raw))
		if err != nil {
			return 0, err
		}
		req.Header.Set("Accept", "application/json")
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		if key != "" {
			req.Header.Set("Idempotency-Key", key)
		}
		resp, err := c.http.Do(req)
		if err == nil {
			b, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
			_ = resp.Body.Close()
			for _, s := range ok {
				if resp.StatusCode == s {
					if out != nil && len(b) > 0 {
						if err := json.Unmarshal(b, out); err != nil {
							return s, fmt.Errorf("%s %s: decode: %w", method, url, err)
						}
					}
					return s, nil
				}
			}
			ae := &apiError{Method: method, URL: url, Status: resp.StatusCode, Body: strings.TrimSpace(string(b))}
			var prob struct {
				Type string `json:"type"`
			}
			if json.Unmarshal(b, &prob) == nil {
				ae.Type = strings.TrimPrefix(prob.Type, "/problems/")
			}
			err = ae
			if resp.StatusCode != http.StatusTooManyRequests && resp.StatusCode < 500 {
				return resp.StatusCode, err
			}
		}
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		last = err
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(wait):
		}
		wait *= 2
	}
	return 0, fmt.Errorf("after 5 attempts: %w", last)
}

// totalWrites sums the writes of every kind.
func (c *client) totalWrites() int {
	n := 0
	for _, v := range c.writes {
		n += v
	}
	return n
}

// writeSummary renders the write counters in a stable order.
func (c *client) writeSummary() string {
	kinds := []string{"cpt-schedule", "tax-table", "charge-schedule", "category", "item-price", "content-create", "content-revise", "content-publish"}
	seen := map[string]bool{}
	var parts []string
	for _, k := range kinds {
		seen[k] = true
		parts = append(parts, fmt.Sprintf("%s %d", k, c.writes[k]))
	}
	var extra []string
	for k := range c.writes {
		if !seen[k] {
			extra = append(extra, k)
		}
	}
	sort.Strings(extra)
	for _, k := range extra {
		parts = append(parts, fmt.Sprintf("%s %d", k, c.writes[k]))
	}
	return strings.Join(parts, ", ")
}
