package main

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/warehouse-e2e/internal/sim/transport"
)

// ---- the node book: origin and delivery station, read where the transport expects them ----

func nodeRoutes() map[string]http.HandlerFunc {
	return map[string]http.HandlerFunc{
		"GET vendor-connectivity/vendor/nodes": func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Query().Get("vendorId") != "V-WS" {
				problem(w, 404, "vendor-not-found") // a request for another vendor, or for none, finds nothing
				return
			}
			respondJSON(page(
				map[string]any{"nodeId": "node-other", "vendorNodeCode": "site-9", "status": "ACTIVE"},
				map[string]any{"nodeId": "node-site1", "vendorNodeCode": "site-1", "status": "ACTIVE"},
			))(w, r)
		},
		"GET node-topology/service-areas/786": respondJSON(map[string]any{"postalPrefix": "786", "deliveryStationNodeId": "node-ds-a"}),
		"GET node-topology/service-areas/787": respondJSON(map[string]any{"postalPrefix": "787", "deliveryStationNodeId": "node-ds-b"}),
	}
}

func newNodeBook(f *fakeCluster) *nodeBook {
	return &nodeBook{c: f.client(), vendorID: "V-WS", networkSite: "site-1", prefixes: &orderPrefixes{}}
}

func TestNodeBookReadsTheOriginByVendorNodeCodeAndAStationPerPrefix(t *testing.T) {
	f := newFakeCluster(t, nodeRoutes())
	n := newNodeBook(f)
	if err := n.resolve(context.Background()); err != nil {
		t.Fatal(err)
	}
	n.prefixes.set("ord-a", "786")
	n.prefixes.set("ord-b", "787")
	origin, dest, err := n.route("ord-a")
	if err != nil || origin != "node-site1" || dest != "node-ds-a" {
		t.Errorf("order a: %q %q %v", origin, dest, err)
	}
	origin, dest, err = n.route("ord-b")
	if err != nil || origin != "node-site1" || dest != "node-ds-b" {
		t.Errorf("order b: %q %q %v (the station follows the order's prefix)", origin, dest, err)
	}
	if w := f.writes(); len(w) != 0 {
		t.Errorf("resolving the nodes wrote: %v", w)
	}
}

func TestNodeBookFailsClosed(t *testing.T) {
	cases := map[string]struct {
		patch map[string]http.HandlerFunc
		want  string
	}{
		"no node with the network's code": {map[string]http.HandlerFunc{
			"GET vendor-connectivity/vendor/nodes": respondJSON(page(map[string]any{"nodeId": "n", "vendorNodeCode": "site-9", "status": "ACTIVE"})),
		}, "none with vendorNodeCode site-1"},
		"the node is not ACTIVE": {map[string]http.HandlerFunc{
			"GET vendor-connectivity/vendor/nodes": respondJSON(page(map[string]any{"nodeId": "n", "vendorNodeCode": "site-1", "status": "SUSPENDED"})),
		}, "SUSPENDED"},
		"the node has no id": {map[string]http.HandlerFunc{
			"GET vendor-connectivity/vendor/nodes": respondJSON(page(map[string]any{"vendorNodeCode": "site-1", "status": "ACTIVE"})),
		}, "without a node id"},
		"vendor-connectivity refuses": {map[string]http.HandlerFunc{
			"GET vendor-connectivity/vendor/nodes": func(w http.ResponseWriter, _ *http.Request) { problem(w, 503, "unavailable") },
		}, "vendor-connectivity GET"},
		"a served prefix has no area": {map[string]http.HandlerFunc{
			"GET node-topology/service-areas/787": func(w http.ResponseWriter, _ *http.Request) { problem(w, 404, "not-found") },
		}, "no service area for the postal prefix 787"},
		"a service area names no station": {map[string]http.HandlerFunc{
			"GET node-topology/service-areas/786": respondJSON(map[string]any{"postalPrefix": "786"}),
		}, "no deliveryStationNodeId"},
		"node-topology fails": {map[string]http.HandlerFunc{
			"GET node-topology/service-areas/786": func(w http.ResponseWriter, _ *http.Request) { problem(w, 500, "boom") },
		}, "node-topology GET /service-areas/786"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			routes := nodeRoutes()
			for k, v := range tc.patch {
				routes[k] = v
			}
			n := newNodeBook(newFakeCluster(t, routes))
			err := n.resolve(context.Background())
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err %v, want it to mention %q", err, tc.want)
			}
			if _, _, rerr := n.route("ord"); rerr == nil {
				t.Error("a book that failed to resolve still routes")
			}
		})
	}
}

func TestNodeBookRouteRefusesWhatItDoesNotKnow(t *testing.T) {
	n := newNodeBook(newFakeCluster(t, nodeRoutes()))
	n.prefixes.set("ord-a", "786")
	if _, _, err := n.route("ord-a"); err == nil || !strings.Contains(err.Error(), "never resolved") {
		t.Errorf("before resolve: %v", err)
	}
	if err := n.resolve(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, _, err := n.route("never-bought"); err == nil || !strings.Contains(err.Error(), "no postal prefix was recorded") {
		t.Errorf("unknown order: %v", err)
	}
	n.prefixes.set("ord-far", "331")
	if _, _, err := n.route("ord-far"); err == nil || !strings.Contains(err.Error(), "331") {
		t.Errorf("an unserved prefix: %v", err)
	}
}

// ---- the carrier ----

type fakeParcels struct {
	called       bool
	orderID      string
	packages     int
	origin, dest string
	opt          transport.Options
	out          transport.Delivery
}

func (f *fakeParcels) DeliverOrder(_ context.Context, orderID string, packages int, origin, dest string, o transport.Options) transport.Delivery {
	f.called, f.orderID, f.packages, f.origin, f.dest, f.opt = true, orderID, packages, origin, dest, o
	return f.out
}

func TestCarrierAdapterDeliversFromTheOriginToTheOrdersStation(t *testing.T) {
	at := time.Date(2026, 11, 15, 20, 0, 0, 0, time.UTC)
	fp := &fakeParcels{out: transport.Delivery{DeliveredAt: at, Problems: []string{"scan stored late"}}}
	a := carrierAdapter{t: fp, route: func(id string) (string, string, error) { return "node-o-" + id, "node-d-" + id, nil }}
	got := a.DeliverOrder(context.Background(), "ord-1", 2)
	if !fp.called || fp.orderID != "ord-1" || fp.packages != 2 || fp.origin != "node-o-ord-1" || fp.dest != "node-d-ord-1" {
		t.Errorf("the transport was asked %+v", fp)
	}
	if !got.Delivered() || !got.DeliveredAt.Equal(at) || len(got.Problems) != 1 || got.Problems[0] != "scan stored late" {
		t.Errorf("delivery %+v", got)
	}
}

func TestCarrierAdapterNeverDeliversWhatItCannotRoute(t *testing.T) {
	fp := &fakeParcels{out: transport.Delivery{DeliveredAt: time.Now()}}
	a := carrierAdapter{t: fp, route: func(string) (string, string, error) { return "", "", context.DeadlineExceeded }}
	got := a.DeliverOrder(context.Background(), "ord-1", 1)
	if fp.called {
		t.Error("the transport ran without an origin and a station")
	}
	if got.Delivered() || len(got.Problems) != 1 {
		t.Errorf("delivery %+v", got)
	}
}

func TestDeliveryIsNotDeliveredWithoutADeliveryTime(t *testing.T) {
	if toDelivery(transport.Delivery{Problems: []string{"route refused"}}).Delivered() {
		t.Error("a parcel with no DeliveredAt reads as delivered")
	}
}
