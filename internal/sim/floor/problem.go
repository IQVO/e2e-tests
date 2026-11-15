package floor

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/claudioed/warehouse-e2e/internal/sim"
)

// Slug is the problem slug of a refusal: the fleet `code` when the body has one,
// else the last path segment of the problem `type`
// (https://errors.<service>.warehouse-systems.dev/<slug>).
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

// Transient is one exact refusal that means "not yet": the status AND the slug.
// A call that depends on an event another service has not consumed yet, or on a
// pool another person has not freed, is refused with one of these until it has;
// nothing else is retried.
type Transient struct {
	Status int
	Slug   string
}

// The "not yet" refusals, each read from the owning service's contract
// (apis/openapi.yaml and its error catalogue on origin/develop).
var (
	// fulfillment-execution: the station's capabilities fit no pending task of the type. The
	// pool has nothing to offer right now: the WorkReleased event is not consumed yet, or the
	// pending work is held by another station.
	noClaimableTask = Transient{http.StatusConflict, "no-claimable-task"}
	// wes-work-planning: a release-fed pool is at its WIP limit; room appears when work completes.
	wipLimitReached = Transient{http.StatusConflict, "wip-limit-reached"}
	// wes-work-planning: the pool changed under the call; the next release is a new request.
	poolConcurrent = Transient{http.StatusConflict, "concurrent-modification"}
	// wes-work-planning: the pool is empty, which for a unit this floor saw Pending means
	// somebody released it between the read and the release.
	workPoolEmpty = Transient{http.StatusConflict, "work-pool-empty"}
	// inventory-storage: a racing request modified the reservation, stock unit or bin first.
	stockConcurrent = Transient{http.StatusConflict, "concurrent-modification"}
)

func (t Transient) matches(r sim.Resp) bool { return r.Status == t.Status && Slug(r) == t.Slug }

// refusal is the error of a non-2xx answer the caller does not tolerate.
func refusal(what string, r sim.Resp) error {
	if r.Status == 0 {
		return fmt.Errorf("%s: transport error: %s", what, sim.Truncate(string(r.Body), 200))
	}
	return fmt.Errorf("%s -> %d %s: %s", what, r.Status, Slug(r), sim.Truncate(string(r.Body), 200))
}

// callTransient performs do until it answers 2xx. An answer matching one of
// transients is retried (do builds a fresh request, hence a fresh
// Idempotency-Key: the services store a 4xx under the key, so re-sending the key
// would replay the refusal for ever); any other refusal, and a transport error,
// is returned at once.
func callTransient(ctx context.Context, clock Clock, w Waits, what string, transients []Transient, do func() sim.Resp) (sim.Resp, error) {
	var last sim.Resp
	err := await(ctx, clock, w, what, func(context.Context) (verdict, string, error) {
		last = do()
		if last.OK() {
			return vDone, "", nil
		}
		for _, t := range transients {
			if t.matches(last) {
				return vWait, fmt.Sprintf("%d %s", last.Status, t.Slug), nil
			}
		}
		return 0, "", refusal(what, last)
	})
	return last, err
}

// pageLimit is the explicit page size of every collection read here: below the
// smallest maximum (100 is the guideline default, 500 the cap) of the services
// this package reads, so no service answers 400 invalid-limit.
const pageLimit = 100

// listAll reads a paginated collection, following nextCursor to the end. path is
// the path with its filters; route the templated metrics key. The returned Resp
// is the last page read: !OK() means the walk failed and the items are what was
// read so far.
func listAll[T any](ctx context.Context, c *sim.Client, svc, route, path string) ([]T, sim.Resp) {
	sep := "?"
	if strings.Contains(path, "?") {
		sep = "&"
	}
	return sim.ListAll[T](ctx, c, svc, route, fmt.Sprintf("%s%slimit=%d", path, sep, pageLimit))
}
