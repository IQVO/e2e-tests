package collect

import (
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/warehouse-e2e/internal/retailday"
	"github.com/claudioed/warehouse-e2e/internal/sim"
)

// A date a service leaves out, nulls or leaves empty is "no date", not a body that is not the contract.
func TestAnAbsentNullOrEmptyDateIsNoDate(t *testing.T) {
	for _, date := range []string{`""`, `null`} {
		t.Run(date, func(t *testing.T) {
			w := newWorld(t)
			fx := seedDelivered(w, "1")
			w.rawBody("customer-ordering", "/orders/{orderId}", `{"status":"IN_FULFILLMENT","lines":[{"lineNo":1,"catalogItemId":"CAT-A","quantity":2},{"lineNo":2,"catalogItemId":"CAT-B","quantity":1}],"groups":[{"status":"PROMISED","deliverByLatest":`+date+`}]}`)
			w.rawBody("shipping-and-tracking", "/journeys/{trackingNumber}", `{"state":"IN_TRANSIT","poNumber":"`+fx.po+`","orderId":"ord-1","deliveredAt":`+date+`}`)
			f, notes := fx.collect(t, newCollector(w))
			wantNotes(t, notes)
			if f.Status != "IN_FULFILLMENT" || !f.PromiseEnd.IsZero() || !f.DeliveredAt.IsZero() {
				t.Fatalf("status %q promise %s delivered %s", f.Status, f.PromiseEnd, f.DeliveredAt)
			}
		})
	}
}

// The caller's client is the caller's: it is copied, never changed (it may have no call statistics).
func TestTheCallersClientIsCopiedNeverChanged(t *testing.T) {
	w := newWorld(t)
	fx := seedDelivered(w, "1")
	client := &sim.Client{HTTP: http.DefaultClient, Base: w.base()}
	c := New(client, Settings{Catalogue: catalogue(), BuildingSite: site, OrderIDs: w.orderIDs})
	f, notes := c.Order(t.Context(), fx.placed)
	wantNotes(t, notes)
	if f.Status != "DELIVERED" {
		t.Fatalf("facts %+v", f)
	}
	if client.Stats != nil {
		t.Fatal("the caller's client was given call statistics by the collector")
	}
}

// The tracking numbers are a set: one number named by two groups is one package.
func TestATrackingNumberNamedTwiceIsOnePackage(t *testing.T) {
	w := newWorld(t)
	fx := seedDelivered(w, "1")
	w.mu.Lock()
	g := &w.ordering.tracking[fx.placed.OrderID].Groups
	*g = append(*g, foTrackGroup{GroupNo: 2, PromiseID: "prm-1b", Status: "DELIVERED", Packages: []foPackage{{TrackingNumber: fx.tn, DestinationKind: "SHIP_TO"}}})
	w.mu.Unlock()
	f, _ := fx.collect(t, newCollector(w))
	if want := []string{fx.tn}; !slices.Equal(f.TrackingNumbers, want) {
		t.Fatalf("tracking %v want %v", f.TrackingNumbers, want)
	}
}

// One load whose packages cannot be read makes the count unknown: the other loads' answer is not it.
func TestOneUnreadableLoadMakesTheConfirmationCountUnknown(t *testing.T) {
	w := newWorld(t)
	fx := seedDelivered(w, "1") // load-old-1 carries only others' packages; load-1 carries the order
	w.failWhen("outbound-shipping", func(r *http.Request) bool { return strings.HasSuffix(r.URL.Path, "/load-old-1/packages") }, 500)
	f, notes := fx.collect(t, newCollector(w))
	hasNote(t, notes, "unread ShipmentConfirmations: outbound-shipping /loads/{loadId}/packages: HTTP 500")
	if f.ShipmentConfirmations != 0 {
		t.Fatalf("%d confirmations from a scan that could not read every load", f.ShipmentConfirmations)
	}
}

// Packages are reported in id order whatever order their orders came in.
func TestPackagesAreInIdOrderAcrossOrders(t *testing.T) {
	w := newWorld(t)
	fx := seedDelivered(w, "1") // om-1, package pkg-1
	fx.addOM("0a")              // om-0a sorts first, but its package sorts last
	w.mu.Lock()
	w.fe.packages[len(w.fe.packages)-1].ID = "pkg-zz"
	w.mu.Unlock()
	f, notes := fx.collect(t, newCollector(w))
	wantNotes(t, notes)
	want := []retailday.Package{{ID: "pkg-1", Outcome: "LABELED"}, {ID: "pkg-zz", Outcome: "LABELED"}}
	if !slices.Equal(f.Packages, want) {
		t.Fatalf("packages %v want %v", f.Packages, want)
	}
	if ids := []string{f.OMOrders[0].ID, f.OMOrders[1].ID}; !slices.Equal(ids, []string{"om-0a", "om-1"}) {
		t.Fatalf("orders %v", ids)
	}
}

// The shelf is bounded in time too.
func TestAShelfThatTakesTooLongIsCutOff(t *testing.T) {
	w := stockWorld(t)
	w.linger = time.Minute
	start := time.Now()
	got, err := newCollector(w, func(s *Settings) { s.Timeout = 150 * time.Millisecond }).Shelf(t.Context(), []string{"SKU-A", "SKU-B"})
	if err == nil || got != nil {
		t.Fatalf("shelf %v err %v", got, err)
	}
	if d := time.Since(start); d > 10*time.Second {
		t.Fatalf("the shelf took %s", d)
	}
}
