package collect

import (
	"context"
	"net/http"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/claudioed/warehouse-e2e/internal/retailday"
)

// The reads that do not depend on each other are made together. A gate holds each of them until all
// have arrived, so a collector that read them one after the other never gets past it (and the
// test fails after the gate's timeout); a collector that reads them together always does, however
// the scheduler orders it.
func TestReadsThatDoNotDependOnEachOtherAreMadeAtTheSameTime(t *testing.T) {
	t.Run("the shopper, the money and the sourcing", func(t *testing.T) {
		w := newWorld(t)
		fx := seedDelivered(w, "1")
		w.gate = newGate(4, func(svc, path, _ string) bool {
			return svc == "customer-ordering" && (path == "/orders/ord-1" || path == "/orders/ord-1/tracking") ||
				svc == "customer-payments" && path == "/payment-authorizations" ||
				svc == "order-sourcing" && path == "/assignments"
		})
		fx.collect(t, newCollector(w))
		if !w.gate.joined() {
			t.Fatal("the order, its tracking, its payments and its assignments were not read at the same time")
		}
	})
	t.Run("the order-management order, the work, the packages and the loads", func(t *testing.T) {
		w := newWorld(t)
		fx := seedDelivered(w, "1")
		w.gate = newGate(5, func(svc, path, rawQuery string) bool {
			return svc == "order-management" && path == "/orders/om-1" ||
				svc == "wes-work-planning" && path == "/work-units" ||
				svc == "fulfillment-execution" && path == "/tasks" && strings.Contains(rawQuery, "orderRef=om-1&") ||
				svc == "fulfillment-execution" && path == "/packages" ||
				svc == "outbound-shipping" && path == "/loads"
		})
		fx.collect(t, newCollector(w))
		if !w.gate.joined() {
			t.Fatal("the warehouse side was not read at the same time")
		}
	})
	t.Run("the SKUs of the shelf", func(t *testing.T) {
		w := stockWorld(t)
		w.gate = newGate(2, func(svc, path, _ string) bool {
			return svc == "inventory-storage" && path == "/inventory/SKU-A/usable" || svc == "inventory-storage" && path == "/inventory/SKU-B/usable"
		})
		if _, err := newCollector(w).Shelf(t.Context(), []string{"SKU-A", "SKU-B"}); err != nil {
			t.Fatal(err)
		}
		if !w.gate.joined() {
			t.Fatal("two SKUs were not read at the same time")
		}
	})
}

// Parallel bounds the requests in flight at once: with every request lingering, an unbounded
// collector overlaps four at the first step alone. The bound can never fail a bounded one.
func TestRequestsInFlightNeverExceedTheBound(t *testing.T) {
	for _, parallel := range []int{1, 2, 3} {
		w := newWorld(t)
		fx := seedDelivered(w, "1")
		fx.addOM("1b")
		w.linger = 5 * time.Millisecond
		c := newCollector(w, func(s *Settings) { s.Parallel = parallel })
		f, notes := fx.collect(t, c)
		if _, err := c.Shelf(t.Context(), []string{"SKU-A", "SKU-B"}); err != nil {
			t.Fatal(err)
		}
		wantNotes(t, notes)
		if f.Status != "DELIVERED" || len(f.OMOrders) != 2 {
			t.Fatalf("parallel %d: facts %+v", parallel, f)
		}
		w.mu.Lock()
		peak := w.peak
		w.mu.Unlock()
		if peak > parallel || peak < 1 {
			t.Fatalf("parallel %d: %d requests were in flight at once", parallel, peak)
		}
	}
}

// A call is bounded in time: what is not answered by then is zero and noted, and the call returns.
func TestACallThatTakesTooLongIsCutOffAndNoted(t *testing.T) {
	w := newWorld(t)
	fx := seedDelivered(w, "1")
	w.linger = time.Minute
	start := time.Now()
	f, notes := fx.collect(t, newCollector(w, func(s *Settings) { s.Timeout = 150 * time.Millisecond }))
	if d := time.Since(start); d > 10*time.Second {
		t.Fatalf("the call took %s", d)
	}
	want := retailday.OrderFacts{OrderID: "ord-1", Expect: retailday.Delivered, Lines: 2}
	sameFacts(t, f, want)
	if len(notes) == 0 {
		t.Fatal("a call that read nothing said nothing")
	}
	for _, n := range notes {
		if !strings.HasPrefix(n, "unread ") || !strings.Contains(n, "no usable answer") {
			t.Fatalf("note %q", n)
		}
	}
	if _, err := newCollector(w, func(s *Settings) { s.Timeout = 150 * time.Millisecond }).Shelf(t.Context(), []string{"SKU-A"}); err == nil {
		t.Fatal("a shelf that was never read came back without an error")
	}
}

func TestACancelledCallReturnsAtOnceWithNotes(t *testing.T) {
	w := newWorld(t)
	fx := seedDelivered(w, "1")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	f, notes := newCollector(w).Order(ctx, fx.placed)
	if len(notes) == 0 || f.Status != "" || f.AuthorizedMinor != 0 {
		t.Fatalf("facts %+v notes %q", f, notes)
	}
	if got := rules(retailday.AuditOrder(f)); len(got) == 0 {
		t.Fatal("an order that was not read passed the audit")
	}
}

// Many calls on one collector at once (the memory of loads, the bound and the notes are shared) give
// each order what it gives alone, and the same notes in the same order.
func TestConcurrentCallsGiveWhatEachGivesAlone(t *testing.T) {
	w := newWorld(t)
	orders := []*fixture{seedDelivered(w, "1"), seedDelivered(w, "2"), seedCancelled(w, "3"), seedDeclined(w, "4")}
	type answer struct {
		f     retailday.OrderFacts
		notes []string
	}
	alone := make([]answer, len(orders))
	for i, fx := range orders {
		f, n := fx.collect(t, newCollector(w))
		alone[i] = answer{f, n}
	}
	c := newCollector(w, func(s *Settings) { s.Parallel = 3 })
	var wg sync.WaitGroup
	errs := make(chan string, 64)
	for rep := 0; rep < 6; rep++ {
		for i, fx := range orders {
			wg.Add(1)
			go func() {
				defer wg.Done()
				f, n := c.Order(context.Background(), fx.placed)
				if !reflect.DeepEqual(f, alone[i].f) || !slices.Equal(n, alone[i].notes) {
					errs <- "order " + fx.placed.OrderID + " differs when read alongside others"
				}
			}()
		}
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Error(e)
	}
}

// Every context down: the call still returns, every fact is zero, every one is named, the notes are
// sorted and without a repeat, and the audit does not pass it.
func TestEveryContextDownIsEveryFactUnreadAndNeverAPass(t *testing.T) {
	for _, mode := range []string{"500", "dropped"} {
		t.Run(mode, func(t *testing.T) {
			w := newWorld(t)
			fx := seedDelivered(w, "1")
			for _, svc := range append(retailday.AllGateway(), retailday.BFF) {
				if mode == "500" {
					w.fail(svc, 500)
				} else {
					w.drop(svc)
				}
			}
			f, notes := fx.collect(t, newCollector(w))
			sameFacts(t, f, retailday.OrderFacts{OrderID: "ord-1", Expect: retailday.Delivered, Lines: 2})
			if len(notes) == 0 || !slices.IsSorted(notes) || len(slices.Compact(slices.Clone(notes))) != len(notes) {
				t.Fatalf("notes %q", notes)
			}
			joined := strings.Join(notes, "\n")
			for _, fact := range []string{"Status, PromiseEnd", "TrackingNumbers", "AuthorizedMinor, CapturedMinor, VoidedMinor", "POs"} {
				if !strings.Contains(joined, "unread "+fact) {
					t.Fatalf("no note for %s in %q", fact, notes)
				}
			}
			if fs := retailday.AuditOrder(f); retailday.FailedFindings(fs) == 0 {
				t.Fatal("an order that could not be read passed the audit")
			}
		})
	}
}

// A body that is not what the contract says is "could not look", not "none exists".
func TestABodyThatIsNotTheContractIsNotedNotReadAsNone(t *testing.T) {
	for _, tc := range []struct {
		name, svc, pattern, body, note string
	}{
		{"a status that is not a string", "customer-ordering", "/orders/{orderId}", `{"status": 12}`,
			"unread Status, PromiseEnd: customer-ordering /orders/{orderId}: the body is not what the contract says"},
		{"a page without items", "order-sourcing", "/assignments", `{"nextCursor":""}`,
			"unread POs: order-sourcing /assignments?orderId: no usable answer"},
		{"a promise that is not a date", "customer-ordering", "/orders/{orderId}", `{"status":"DELIVERED","groups":[{"status":"DELIVERED","deliverByLatest":"tomorrow"}]}`,
			"unread Status, PromiseEnd: customer-ordering /orders/{orderId}: the body is not what the contract says"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newWorld(t)
			fx := seedDelivered(w, "1")
			w.rawBody(tc.svc, tc.pattern, tc.body)
			f, notes := fx.collect(t, newCollector(w))
			if !slices.ContainsFunc(notes, func(n string) bool { return strings.HasPrefix(n, tc.note) }) {
				t.Fatalf("notes %q lack %q", notes, tc.note)
			}
			if tc.svc == "customer-ordering" && (f.Status != "" || !f.PromiseEnd.IsZero()) {
				t.Fatalf("status %q promise %s", f.Status, f.PromiseEnd)
			}
		})
	}
}

// A provider that keeps answering the same cursor does not hold the collector: the walk stops and
// says so.
func TestAProviderThatRepeatsItsCursorIsNotWalkedForever(t *testing.T) {
	w := newWorld(t)
	fx := seedDelivered(w, "1")
	w.rawBody("order-sourcing", "/assignments", `{"items":[],"nextCursor":"again"}`)
	f, notes := fx.collect(t, newCollector(w))
	hasNote(t, notes, "unread POs", "order-sourcing /assignments?orderId", `provider repeated cursor "again"`)
	_ = f
	if n := w.requests("order-sourcing", "/assignments"); n != 2 {
		t.Fatalf("%d requests: the walk should have stopped at the first repeat", n)
	}
}

// Every collection is read at the page size every service accepts (outbound-shipping caps limit at
// 200 and answers 400 above it), and a smaller page changes nothing but the number of requests.
func TestCollectionsAreReadAtTheConfiguredPageSizeAndTheAnswerDoesNotDependOnIt(t *testing.T) {
	w := newWorld(t)
	fx := seedDelivered(w, "1")
	fx.addAssignment("asg-1b", "ACCEPTED")
	base, notes := fx.collect(t, newCollector(w))
	wantNotes(t, notes)
	for _, c := range w.snapshot() {
		if c.Status == http.StatusBadRequest {
			t.Fatalf("a request the fake refused: %+v", c)
		}
		if strings.Contains(c.Path, "/loads") || strings.Contains(c.Path, "/assignments") {
			if !strings.Contains(c.Query, "limit=200") {
				t.Fatalf("a collection read without the default page size: %+v", c)
			}
		}
	}
	w.reset()
	one, notes := fx.collect(t, newCollector(w, func(s *Settings) { s.PageSize = 1 }))
	wantNotes(t, notes)
	sameFacts(t, one, base)
	if n := w.requests("order-sourcing", "/assignments"); n != 2 {
		t.Fatalf("%d assignment pages of one item for two assignments", n)
	}
}

func TestDeadLettersAreNotKnownNeverZero(t *testing.T) {
	w := newWorld(t)
	n, known, err := newCollector(w).DeadLetters(t.Context(), "RUN1")
	if n != 0 || known || err != nil {
		t.Fatalf("n=%d known=%v err=%v: no published route lists dead letters, so the report must show not verified", n, known, err)
	}
	if len(w.snapshot()) != 0 {
		t.Fatal("dead letters were looked for over REST")
	}
}

// Whatever happened, the collector only asked.
func TestNoRequestOtherThanAGETIsEverMade(t *testing.T) {
	w := newWorld(t)
	a := seedDelivered(w, "1")
	b := seedCancelled(w, "2")
	d := seedDeclined(w, "3")
	c := newCollector(w)
	a.collect(t, c)
	b.collect(t, c)
	d.collect(t, c)
	w.fail("wes-work-planning", 500)
	w.drop("shipping-and-tracking")
	a.collect(t, c)
	if _, err := c.Shelf(t.Context(), []string{"SKU-A", "SKU-B"}); err != nil {
		t.Fatal(err)
	}
	if len(w.snapshot()) < 30 {
		t.Fatalf("only %d requests: the test did not exercise the collector", len(w.snapshot()))
	}
	noGets(t, w)
	for _, call := range w.snapshot() {
		if call.Method != http.MethodGet {
			t.Fatalf("%+v", call)
		}
	}
}

// The caller's client is not modified, and a service with no base URL is a note, never a request.
func TestAServiceWithoutABaseURLIsNotedAndNeverRequested(t *testing.T) {
	w := newWorld(t)
	fx := seedDelivered(w, "1")
	client := w.client()
	delete(client.Base, "checkout")
	statsBefore := client.Stats
	c := New(client, Settings{Catalogue: catalogue(), BuildingSite: site, OrderIDs: w.orderIDs})
	f, notes := c.Order(t.Context(), fx.placed)
	hasNote(t, notes, "unread TotalMinor: checkout /checkout-sessions/{sessionId}: no usable answer: no base URL configured for checkout")
	if f.TotalMinor != 0 || f.AuthorizedMinor != 4500 {
		t.Fatalf("facts %+v", f)
	}
	if n := w.requests("checkout", ""); n != 0 {
		t.Fatalf("%d requests to a service with no base URL", n)
	}
	if client.Stats != statsBefore || client.Base["checkout"] != "" {
		t.Fatal("the caller's client was modified")
	}
}
