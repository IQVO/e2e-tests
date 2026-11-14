package outbound

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/claudioed/warehouse-e2e/internal/paging"
	"github.com/claudioed/warehouse-e2e/internal/sim"
)

// Slug is the problem slug of a refusal: the fleet `code` when the body has one
// (outbound-shipping), else the last path segment of the problem `type`
// (every service: https://errors.<service>.warehouse-systems.dev/<slug>).
func Slug(r sim.Resp) string {
	var p struct {
		Type string `json:"type"`
		Code string `json:"code"`
	}
	if json.Unmarshal(r.Body, &p) != nil {
		return ""
	}
	if p.Code != "" {
		return p.Code
	}
	if i := strings.LastIndex(p.Type, "/"); i >= 0 {
		return p.Type[i+1:]
	}
	return p.Type
}

// Transient is one exact consumer-lag refusal: the status AND the slug. A call
// that depends on an event another service has not consumed yet is refused with
// one of these until the consumer catches up; nothing else is retried.
type Transient struct {
	Status int
	Slug   string
}

// The consumer-lag refusals, each one read from the owning service's contract.
var (
	// outbound-shipping has not consumed PackageManifested yet (or ignored it).
	packageNotFound = Transient{http.StatusNotFound, "package-not-found"}
	// dock-scheduling has not consumed facility-layout's dock slot yet.
	unknownDoor = Transient{http.StatusUnprocessableEntity, "unknown-door"}
	// dock-scheduling has not consumed process-path-management's CPT schedule yet.
	missingOutboundRef = Transient{http.StatusUnprocessableEntity, "missing-outbound-ref"}
	// yard-management has not consumed DockAppointmentBooked yet.
	unknownAppointment = Transient{http.StatusUnprocessableEntity, "unknown-appointment"}
)

func (t Transient) matches(r sim.Resp) bool { return r.Status == t.Status && Slug(r) == t.Slug }

// poll repeats check every w.Interval until it reports done, fails, or w.Timeout
// elapses. check returns ("", nil) when done, (reason, nil) while it is still
// waiting (reason says for what), and ("", err) for a failure that must not be
// retried. A timeout is an error that names the last reason: a bounded wait that
// expires reads like a service bug, so it says what never arrived.
func poll(ctx context.Context, w Waits, what string, check func() (string, error)) error {
	w = w.orDefault()
	deadline := time.Now().Add(w.Timeout)
	for {
		reason, err := check()
		if err != nil {
			return err
		}
		if reason == "" {
			return nil
		}
		if !time.Now().Add(w.Interval).Before(deadline) {
			return fmt.Errorf("%s: still waiting after %s: %s", what, w.Timeout, reason)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%s: %w (last: %s)", what, ctx.Err(), reason)
		case <-time.After(w.Interval):
		}
	}
}

// refusal is the error of a non-2xx answer the caller does not tolerate.
func refusal(what string, r sim.Resp) error {
	if r.Status == 0 {
		return fmt.Errorf("%s: transport error: %s", what, sim.Truncate(string(r.Body), 200))
	}
	return fmt.Errorf("%s -> %d %s: %s", what, r.Status, Slug(r), sim.Truncate(string(r.Body), 200))
}

// callTransient performs do until it answers 2xx. An answer matching one of
// transients is retried (a fresh request, hence a fresh Idempotency-Key: the
// services store a 4xx under the key, so re-sending the key would replay the
// refusal for ever); any other refusal, and a transport error, is returned at
// once.
func callTransient(ctx context.Context, w Waits, what string, transients []Transient, do func() sim.Resp) (sim.Resp, error) {
	var last sim.Resp
	err := poll(ctx, w, what, func() (string, error) {
		last = do()
		if last.OK() {
			return "", nil
		}
		for _, t := range transients {
			if t.matches(last) {
				return fmt.Sprintf("%d %s", last.Status, t.Slug), nil
			}
		}
		return "", refusal(what, last)
	})
	return last, err
}

// pageLimit is the explicit page size of every collection read here. The
// shared sim.ListAll asks for 500, the guideline maximum, but outbound-shipping
// caps `limit` at 200 and answers 400 invalid-parameter above it, so this
// package pages at the smallest maximum of the services it reads.
const pageLimit = 200

// listAll reads a paginated collection, following nextCursor to the end. The
// returned Resp is the last page read: !OK() means the walk failed.
func listAll[T any](ctx context.Context, c *sim.Client, svc, route, path string) ([]T, sim.Resp) {
	var all []T
	cursor := ""
	sep := "?"
	if strings.Contains(path, "?") {
		sep = "&"
	}
	for pages := 0; pages < 10000; pages++ {
		p := fmt.Sprintf("%s%slimit=%d", path, sep, pageLimit)
		if cursor != "" {
			p += "&cursor=" + url.QueryEscape(cursor)
		}
		r := c.Call(ctx, svc, http.MethodGet, route, p, nil)
		if !r.OK() {
			return all, r
		}
		items, next, err := paging.Decode[T](r.Body)
		if err != nil {
			return all, sim.Resp{Status: 0, Body: []byte(err.Error())}
		}
		all = append(all, items...)
		if next == "" || next == cursor {
			return all, r
		}
		cursor = next
	}
	return all, sim.Resp{Status: 0, Body: []byte("paging did not end")}
}
