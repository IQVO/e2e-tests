package collect

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/claudioed/warehouse-e2e/internal/paging"
	"github.com/claudioed/warehouse-e2e/internal/retailday"
	"github.com/claudioed/warehouse-e2e/internal/sim"
)

// Every request the collector makes goes through Collector.get, and get can only issue a
// GET: the collector is evidence, never an actor (ADR 0001 D5, retailday.Evidence). A test
// proves the fakes never received another verb.

// outcome is what one answer means to a fact.
type outcome int

const (
	// found: a 2xx answer.
	found outcome = iota
	// none: the context answered 404, so what was asked for does not exist. That is an
	// answer, not a failure: the fact is zero and nothing is noted.
	none
	// failed: a 5xx, a transport error, a timeout, a refusal that is not 404. The fact could
	// not be read, so it stays zero AND is noted.
	failed
)

func classify(r sim.Resp) outcome {
	switch {
	case r.OK():
		return found
	case r.Status == http.StatusNotFound:
		return none
	default:
		return failed
	}
}

// problemSlug is the last segment of an RFC 9457 problem's type, or "" when the body is not one.
func problemSlug(body []byte) string {
	var p struct {
		Type string `json:"type"`
		Code string `json:"code"`
	}
	if json.Unmarshal(body, &p) != nil {
		return ""
	}
	if p.Code != "" {
		return p.Code
	}
	t := strings.TrimRight(p.Type, "/")
	if i := strings.LastIndex(t, "/"); i >= 0 {
		return t[i+1:]
	}
	return t
}

// describe says what a failed answer was, short enough for a note.
func describe(svc, route string, r sim.Resp) string {
	if r.Status == 0 {
		return fmt.Sprintf("%s %s: no usable answer: %s", svc, route, sim.Truncate(string(r.Body), 120))
	}
	s := fmt.Sprintf("%s %s: HTTP %d", svc, route, r.Status)
	if slug := problemSlug(r.Body); slug != "" {
		s += " " + slug
	}
	return s
}

// get performs one GET, bounded by the collector's concurrency limit. A service the client has
// no base URL for answers status 0 without a request: it is a configuration gap, never a "none
// exists".
func (c *Collector) get(ctx context.Context, svc, route, path string) sim.Resp {
	if c.c.Base[svc] == "" {
		return sim.Resp{Status: 0, Body: []byte("no base URL configured for " + svc)}
	}
	select {
	case c.sem <- struct{}{}:
	case <-ctx.Done():
		return sim.Resp{Status: 0, Body: []byte(ctx.Err().Error())}
	}
	defer func() { <-c.sem }()
	return c.c.Call(ctx, svc, http.MethodGet, route, path, nil)
}

// maxPages bounds one collection walk so a provider that keeps returning a cursor cannot loop.
const maxPages = 1000

// listAll reads a paginated collection ({items, nextCursor}) at the collector's page size,
// following nextCursor to the end. path carries the filters (repeated on every page). The returned
// Resp is the last page's: !OK() means the walk stopped and the items are what was read so far.
func listAll[T any](ctx context.Context, c *Collector, svc, route, path string) ([]T, sim.Resp) {
	items, _, r := listUpTo[T](ctx, c, svc, route, path, 0)
	return items, r
}

// listUpTo is listAll that stops once bound items are in hand (bound <= 0: no bound). more reports
// that the collection had items beyond the bound, so the caller knows what it did not see.
func listUpTo[T any](ctx context.Context, c *Collector, svc, route, path string, bound int) (all []T, more bool, last sim.Resp) {
	sep := "?"
	if strings.Contains(path, "?") {
		sep = "&"
	}
	path = fmt.Sprintf("%s%slimit=%d", path, sep, c.set.PageSize)
	cursor := ""
	for i := 0; i < maxPages; i++ {
		r := c.get(ctx, svc, route, paging.With(path, cursor))
		if !r.OK() {
			return all, false, r
		}
		items, next, err := paging.Decode[T](r.Body)
		if err != nil {
			return all, false, sim.Resp{Status: 0, Body: []byte(err.Error())}
		}
		all = append(all, items...)
		if bound > 0 && len(all) >= bound {
			return all[:bound], len(all) > bound || next != "", r
		}
		if next == "" {
			return all, false, r
		}
		if next == cursor {
			return all, false, sim.Resp{Status: 0, Body: []byte(fmt.Sprintf("provider repeated cursor %q", cursor))}
		}
		cursor = next
	}
	return all, false, sim.Resp{Status: 0, Body: []byte("paging did not end")}
}

// query builds a query string with its keys sorted, so the same request is the same bytes.
func query(kv ...string) string {
	v := url.Values{}
	for i := 0; i+1 < len(kv); i += 2 {
		v.Set(kv[i], kv[i+1])
	}
	if len(v) == 0 {
		return ""
	}
	return "?" + v.Encode()
}

func esc(s string) string { return url.PathEscape(s) }

// stamp renders an instant the one way every note does.
func stamp(t time.Time) string { return t.UTC().Format(time.RFC3339) }

// run is the state of one Order call: the placed order and the notes the readers raise. Readers
// return their facts; only the notes are shared, so the merge order of the facts is fixed and
// the result does not depend on which goroutine finished first.
type run struct {
	c   *Collector
	ctx context.Context
	p   retailday.Placed

	mu    sync.Mutex
	notes []string
}

// note records a line for the caller: what could not be read, or what two reads disagreed on.
func (r *run) note(format string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.notes = append(r.notes, fmt.Sprintf(format, args...))
}

// unread notes a fact that could not be read, naming the answer that stopped it.
func (r *run) unread(what, svc, route string, resp sim.Resp) {
	r.note("unread %s: %s", what, describe(svc, route, resp))
}

// sortedNotes returns the notes without duplicates, in a fixed order.
func (r *run) sortedNotes() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	seen := map[string]bool{}
	var out []string
	for _, n := range r.notes {
		if !seen[n] {
			seen[n] = true
			out = append(out, n)
		}
	}
	sort.Strings(out)
	return out
}

// par runs the readers at the same time and returns when every one has finished. A reader that
// panics is a note, never a crash: the evidence must still come back.
func (r *run) par(fns ...func()) {
	var wg sync.WaitGroup
	for _, f := range fns {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if e := recover(); e != nil {
					r.note("unread order: a reader failed unexpectedly: %v", e)
				}
			}()
			f()
		}()
	}
	wg.Wait()
}

// fetchOne reads one resource and decodes a 2xx body into T. none is a 404 (the thing does not
// exist: nothing is noted); failed is every other refusal, a transport error or a body that is
// not a T (noted under what, the facts that stay zero).
func fetchOne[T any](r *run, what, svc, route, path string) (T, outcome) {
	var v, zero T
	resp := r.c.get(r.ctx, svc, route, path)
	switch classify(resp) {
	case found:
		if err := resp.Decode(&v); err != nil {
			r.note("unread %s: %s %s: the body is not what the contract says: %s", what, svc, route, sim.Truncate(err.Error(), 160))
			return zero, failed
		}
		return v, found
	case none:
		return zero, none
	default:
		r.unread(what, svc, route, resp)
		return zero, failed
	}
}

// fetchList is fetchOne for a paginated collection: the whole collection, or none/failed.
func fetchList[T any](r *run, what, svc, route, path string) ([]T, outcome) {
	items, resp := listAll[T](r.ctx, r.c, svc, route, path)
	switch classify(resp) {
	case found:
		return items, found
	case none:
		return nil, none
	default:
		r.unread(what, svc, route, resp)
		return nil, failed
	}
}
