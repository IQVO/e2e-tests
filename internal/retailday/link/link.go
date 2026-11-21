// Package link follows a storefront order to the order-management order raised for its purchase
// order, over published REST only.
//
// The floor, the dock and inventory work on order-management's id; the shopper, customer-ordering
// and the carrier's tracking view know the storefront's. No single route maps one to the other, so
// the chain is read link by link:
//
//	order-sourcing        GET /assignments?orderId=X          assignmentId and releases[].releaseNo
//	(derived)             poNumber = "PO-" + hex(sha256(assignmentId + ":" + releaseNo))[:16]
//	                                                           vendor-connectivity ADR 0002 A1
//	network-fulfillment   GET /purchase-orders/{poNumber}      state and localOrderId
//
// The last route is a keyed read added to network-fulfillment for this purpose: its other routes
// list only unanswered demand (ADR 0026), and the network reference it mints (NR-<uuid>) is known
// to nobody outside. localOrderId appears when the demand is acknowledged ("work must never exist
// for demand we have not committed to"), so waiting for it is waiting for the acknowledgement.
//
// Two ways in, because two callers need different things:
//
//   - OrderIDs waits. The runner calls it right after the shoppers, when the purchase order has
//     not been released yet (about cutoff minus handling minus a 30 minute budget, ADR 0001 D3),
//     network-fulfillment has not polled it (every minute) and nothing is acknowledged. It polls,
//     bounded, and a refusal that cannot change (the node rejected the order, the assignment is
//     unsourceable) ends it at once.
//   - ForPurchaseOrders reads once. The collector calls it after the day, when everything that
//     will exist exists; a missing order is a fact there, not something to wait for.
//
// It only reads. Every failure is an error that names what was last seen; nothing panics.
package link

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/claudioed/warehouse-e2e/internal/retailday"
	"github.com/claudioed/warehouse-e2e/internal/sim"
	"github.com/claudioed/warehouse-e2e/internal/sim/transport"
)

// The services read, by their sim.Client base key.
const (
	SvcSourcing = "order-sourcing"
	SvcNetwork  = "network-fulfillment"
)

// Settings bounds the wait. A zero field takes its default.
type Settings struct {
	// Timeout bounds the wait for ONE order. Default 2h: an order placed inside the release window is
	// released at once and linked in a few minutes, but a run started outside it waits for the
	// computed release time.
	Timeout time.Duration
	// Interval is the pause between two reads. Default 15s (order-sourcing's release sweep runs
	// every 15s, network-fulfillment polls every minute).
	Interval time.Duration
	// PageSize is the limit sent on the assignments read. Default 200.
	PageSize int
	// Clock is the only source of time; tests give it a manual one. Default the wall clock.
	Clock transport.Clock
}

func (s Settings) withDefaults() Settings {
	if s.Timeout <= 0 {
		s.Timeout = 2 * time.Hour
	}
	if s.Interval <= 0 {
		s.Interval = 15 * time.Second
	}
	if s.PageSize <= 0 {
		s.PageSize = 200
	}
	if s.Clock == nil {
		s.Clock = transport.SystemClock{}
	}
	return s
}

// Linker reads the chain. It holds no state between calls.
type Linker struct {
	c   *sim.Client
	set Settings
}

// New returns a linker reading through client, which needs a Base entry for SvcSourcing and
// SvcNetwork.
func New(client *sim.Client, s Settings) *Linker {
	return &Linker{c: client, set: s.withDefaults()}
}

// PurchaseOrderNumber derives the poNumber of a release (vendor-connectivity ADR 0002 A1). It is
// deterministic in (assignmentId, releaseNo) and is mapped, never parsed.
func PurchaseOrderNumber(assignmentID string, releaseNo int) string {
	sum := sha256.Sum256([]byte(assignmentID + ":" + strconv.Itoa(releaseNo)))
	return "PO-" + hex.EncodeToString(sum[:])[:16]
}

// Release is one release of an assignment: one purchase order.
type Release struct {
	AssignmentID string
	GroupNo      int
	ReleaseNo    int
	Outcome      string // ACCEPTED, REJECTED, EXPIRED, WITHDRAWN, PENDING
}

// PO is the poNumber of the release.
func (r Release) PO() string { return PurchaseOrderNumber(r.AssignmentID, r.ReleaseNo) }

// OrderIDs waits for the order-management order of every purchase order released for the storefront
// order p and returns their ids, sorted and without repeats (more than one is a defect the caller
// reports). It returns an error when the wait ends without all of them, naming what it last saw.
func (l *Linker) OrderIDs(ctx context.Context, p retailday.Placed) ([]string, error) {
	if p.OrderID == "" {
		return nil, errors.New("link: the order has no storefront id")
	}
	deadline := l.set.Clock.Now().Add(l.set.Timeout)
	var last string
	for {
		ids, state, err := l.once(ctx, p.OrderID)
		switch {
		case err != nil:
			return nil, fmt.Errorf("order %s: %w", p.OrderID, err)
		case state == "":
			return ids, nil
		}
		last = state
		if !l.set.Clock.Now().Before(deadline) {
			return nil, fmt.Errorf("order %s: no order-management order after %s: %s", p.OrderID, l.set.Timeout, last)
		}
		if err := l.set.Clock.Sleep(ctx, l.set.Interval); err != nil {
			return nil, fmt.Errorf("order %s: %w (last: %s)", p.OrderID, err, last)
		}
	}
}

// ForPurchaseOrders reads once the order-management order of each purchase order and returns the
// ids found, sorted and without repeats. A purchase order the network-fulfillment never received,
// or has not acknowledged, contributes nothing and is not an error ("none exists"); a refusal or a
// server error is.
func (l *Linker) ForPurchaseOrders(ctx context.Context, p retailday.Placed, poNumbers []string) ([]string, error) {
	var ids []string
	var errs []error
	for _, po := range poNumbers {
		o, found, err := l.purchaseOrder(ctx, po)
		switch {
		case err != nil:
			errs = append(errs, err)
		case found && o.LocalOrderID != "":
			ids = append(ids, o.LocalOrderID)
		}
	}
	return unique(ids), errors.Join(errs...)
}

// once reads the whole chain one time. state is empty when every release is resolved, otherwise what
// is still missing; err is a refusal that waiting cannot change.
func (l *Linker) once(ctx context.Context, orderID string) (ids []string, state string, err error) {
	rels, state, err := l.releases(ctx, orderID)
	if err != nil || state != "" {
		return nil, state, err
	}
	var missing []string
	var transient []string
	for _, r := range rels {
		switch r.Outcome {
		case "REJECTED", "EXPIRED", "WITHDRAWN":
			return nil, "", fmt.Errorf("the node did not take the purchase order %s (assignment %s release %d): outcome %s", r.PO(), r.AssignmentID, r.ReleaseNo, r.Outcome)
		}
		o, found, perr := l.purchaseOrder(ctx, r.PO())
		switch {
		case perr != nil && isRefusal(perr):
			return nil, "", perr
		case perr != nil:
			transient = append(transient, perr.Error())
			missing = append(missing, r.PO())
		case !found:
			missing = append(missing, r.PO()+" (network-fulfillment has not received it)")
		case o.State == "REJECTED":
			return nil, "", fmt.Errorf("network-fulfillment rejected the purchase order %s (assignment %s release %d)", r.PO(), r.AssignmentID, r.ReleaseNo)
		case o.LocalOrderID == "":
			missing = append(missing, fmt.Sprintf("%s (state %s, not acknowledged)", r.PO(), o.State))
		default:
			ids = append(ids, o.LocalOrderID)
		}
	}
	if len(missing) > 0 {
		s := "waiting for " + strings.Join(missing, "; ")
		if len(transient) > 0 {
			s += " [" + strings.Join(transient, "; ") + "]"
		}
		return nil, s, nil
	}
	return unique(ids), "", nil
}

// releases reads the order's assignments and the purchase orders released for them. state is non-empty
// while the order has no assignment, or an assignment has no release, yet.
func (l *Linker) releases(ctx context.Context, orderID string) (rels []Release, state string, err error) {
	path := "/assignments?orderId=" + url.QueryEscape(orderID) + "&limit=" + strconv.Itoa(l.set.PageSize)
	as, last := sim.ListAll[assignment](ctx, l.c, SvcSourcing, "GET /assignments", path)
	if !last.OK() {
		if refused(last) {
			return nil, "", fmt.Errorf("order-sourcing GET /assignments: %s", describe(last))
		}
		return nil, "order-sourcing GET /assignments: " + describe(last), nil
	}
	if len(as) == 0 {
		return nil, "order-sourcing has no assignment for the order yet", nil
	}
	sort.Slice(as, func(i, j int) bool {
		return as[i].GroupNo < as[j].GroupNo || as[i].GroupNo == as[j].GroupNo && as[i].ID < as[j].ID
	})
	var unreleased []string
	for _, a := range as {
		switch a.State {
		case "UNSOURCEABLE", "CANCELLED":
			return nil, "", fmt.Errorf("order-sourcing assignment %s (group %d) is %s%s: no purchase order will be released", a.ID, a.GroupNo, a.State, reason(a))
		}
		if len(a.Releases) == 0 {
			unreleased = append(unreleased, fmt.Sprintf("%s (group %d, %s)", a.ID, a.GroupNo, a.State))
			continue
		}
		for _, r := range a.Releases {
			rels = append(rels, Release{AssignmentID: a.ID, GroupNo: a.GroupNo, ReleaseNo: r.ReleaseNo, Outcome: r.Outcome})
		}
	}
	if len(unreleased) > 0 {
		return nil, "no purchase order released yet for assignment " + strings.Join(unreleased, ", "), nil
	}
	return rels, "", nil
}

// purchaseOrder reads one purchase order of network-fulfillment. found is false for a 404 (never
// received); a 4xx other than 404 is a refusal (isRefusal), a 5xx or a transport error is an error
// that a retry may clear.
func (l *Linker) purchaseOrder(ctx context.Context, po string) (o networkOrder, found bool, err error) {
	r := l.c.Call(ctx, SvcNetwork, http.MethodGet, "GET /purchase-orders/{poNumber}", "/purchase-orders/"+url.PathEscape(po), nil)
	switch {
	case r.Status == http.StatusNotFound && isProblem(r.Body):
		// the service answered: it never received this purchase order (yet)
		return networkOrder{}, false, nil
	case r.Status == http.StatusNotFound:
		// nobody answered: the gateway has no route, which is what a release without
		// GET /purchase-orders/{poNumber} looks like. Waiting would not change it.
		return networkOrder{}, false, &refusal{fmt.Sprintf("network-fulfillment GET /purchase-orders/%s: %s: is the network-fulfillment release that adds this route deployed?", po, describe(r))}
	case r.OK():
		if derr := r.Decode(&o); derr != nil {
			return networkOrder{}, false, fmt.Errorf("network-fulfillment GET /purchase-orders/%s: unreadable answer: %v", po, derr)
		}
		return o, true, nil
	case refused(r):
		return networkOrder{}, false, &refusal{fmt.Sprintf("network-fulfillment GET /purchase-orders/%s: %s", po, describe(r))}
	default:
		return networkOrder{}, false, fmt.Errorf("network-fulfillment GET /purchase-orders/%s: %s", po, describe(r))
	}
}

type assignment struct {
	ID           string `json:"assignmentId"`
	OrderID      string `json:"orderId"`
	GroupNo      int    `json:"groupNo"`
	State        string `json:"state"`
	CancelReason string `json:"cancelReason"`
	LockReason   string `json:"lockReason"`
	Releases     []struct {
		ReleaseNo int    `json:"releaseNo"`
		Outcome   string `json:"outcome"`
	} `json:"releases"`
}

func reason(a assignment) string {
	switch {
	case a.CancelReason != "":
		return " (" + a.CancelReason + ")"
	case a.LockReason != "":
		return " (" + a.LockReason + ")"
	}
	return ""
}

type networkOrder struct {
	NetworkRef   string `json:"networkRef"`
	SiteID       string `json:"siteId"`
	State        string `json:"state"`
	LocalOrderID string `json:"localOrderId"`
}

// refusal marks a 4xx that no retry changes.
type refusal struct{ msg string }

func (r *refusal) Error() string { return r.msg }

func isRefusal(err error) bool {
	var r *refusal
	return errors.As(err, &r)
}

// refused reports a 4xx the caller cannot fix by waiting: a malformed request or an unknown route.
// A rate limit and a request timeout are not. A 404 is a refusal here too; the one place where it
// means "not yet" (a purchase order network-fulfillment has not received) decides that itself.
func refused(r sim.Resp) bool {
	return r.Status >= 400 && r.Status < 500 &&
		r.Status != http.StatusTooManyRequests && r.Status != http.StatusRequestTimeout
}

// isProblem reports an RFC 9457 problem document: a service answered, as opposed to a gateway that
// found no route and says so in its own words.
func isProblem(body []byte) bool {
	var p struct {
		Type   string `json:"type"`
		Title  string `json:"title"`
		Status int    `json:"status"`
	}
	return json.Unmarshal(body, &p) == nil && (p.Type != "" || p.Title != "" || p.Status != 0)
}

func describe(r sim.Resp) string {
	if r.Status == 0 {
		return "transport error: " + strings.TrimSpace(string(r.Body))
	}
	body := strings.TrimSpace(string(r.Body))
	if len(body) > 160 {
		body = body[:160] + "..."
	}
	return fmt.Sprintf("HTTP %d %s", r.Status, body)
}

func unique(in []string) []string {
	out := slices.Clone(in)
	slices.Sort(out)
	return slices.Compact(out)
}
