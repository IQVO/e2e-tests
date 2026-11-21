package main

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"

	"github.com/claudioed/warehouse-e2e/internal/retailday/day"
	"github.com/claudioed/warehouse-e2e/internal/sim"
	"github.com/claudioed/warehouse-e2e/internal/sim/transport"
)

// The Carrier phase: transport.Transport.DeliverOrder with the origin node and the delivery station.
//
// Where the two ids are read from. The linehaul and last-mile actors take node-topology node ids
// (linehaul GET /departures?origin=&destination=, node-topology GET /nodes/{nodeId}); node-topology
// generates them ("node-<uuid>"), so neither "site-1" nor "DS-SIM-1" is one. Both are READ, never
// written:
//
//	origin       vendor-connectivity GET /vendor/nodes?vendorId=<vendor>: the node whose vendorNodeCode is
//	             the network's id of the building (site-1); its nodeId is node-topology's. Must be ACTIVE.
//	destination  node-topology GET /service-areas/{postalPrefix}: deliveryStationNodeId of the prefix the
//	             order ships to (the shopper adapter records the prefix of every order it places).
const (
	svcVendor = "vendor-connectivity"
	svcNodes  = "node-topology"
)

// servedPrefixes are the postal prefixes the network delivers to (journey page, section 2); they
// are the ones the day's delivered buyers use (internal/retailday/day/plan.go).
var servedPrefixes = []string{"786", "787"}

// nodeBook holds the ids the transport legs need. resolve reads them once, in Prepare.
type nodeBook struct {
	c           *sim.Client
	vendorID    string
	networkSite string
	prefixes    *orderPrefixes

	mu       sync.Mutex
	origin   string
	stations map[string]string // postal prefix -> delivery station node id
}

// resolve reads the origin node and the delivery station of every served prefix. Anything it cannot
// read is an error: the day would otherwise discover it after the floor had worked.
func (n *nodeBook) resolve(ctx context.Context) error {
	origin, err := n.readOrigin(ctx)
	if err != nil {
		return err
	}
	stations := map[string]string{}
	for _, p := range servedPrefixes {
		id, err := n.readStation(ctx, p)
		if err != nil {
			return err
		}
		stations[p] = id
	}
	n.mu.Lock()
	n.origin, n.stations = origin, stations
	n.mu.Unlock()
	return nil
}

type vendorNode struct {
	NodeID         string `json:"nodeId"`
	VendorNodeCode string `json:"vendorNodeCode"`
	Status         string `json:"status"`
}

func (n *nodeBook) readOrigin(ctx context.Context) (string, error) {
	path := "/vendor/nodes?vendorId=" + url.QueryEscape(n.vendorID)
	nodes, last := sim.ListAll[vendorNode](ctx, n.c, svcVendor, "GET /vendor/nodes", path)
	if !last.OK() {
		return "", fmt.Errorf("the origin node: vendor-connectivity GET %s: %d %s", path, last.Status, sim.Truncate(string(last.Body), 160))
	}
	for _, v := range nodes {
		if v.VendorNodeCode != n.networkSite {
			continue
		}
		switch {
		case v.NodeID == "":
			return "", fmt.Errorf("the origin node: vendor %s lists %s without a node id", n.vendorID, n.networkSite)
		case v.Status != "ACTIVE":
			return "", fmt.Errorf("the origin node %s (%s) is %s, not ACTIVE: linehaul plans no departure from it", v.NodeID, n.networkSite, v.Status)
		}
		return v.NodeID, nil
	}
	return "", fmt.Errorf("the origin node: vendor %s has %d node(s), none with vendorNodeCode %s", n.vendorID, len(nodes), n.networkSite)
}

func (n *nodeBook) readStation(ctx context.Context, prefix string) (string, error) {
	r := n.c.Call(ctx, svcNodes, http.MethodGet, "GET /service-areas/{postalPrefix}", "/service-areas/"+url.PathEscape(prefix), nil)
	var got struct {
		Station string `json:"deliveryStationNodeId"`
	}
	switch {
	case r.Status == http.StatusNotFound:
		return "", fmt.Errorf("the delivery station: node-topology has no service area for the postal prefix %s", prefix)
	case !r.OK():
		return "", fmt.Errorf("the delivery station: node-topology GET /service-areas/%s: %d %s", prefix, r.Status, sim.Truncate(string(r.Body), 160))
	case r.Decode(&got) != nil || got.Station == "":
		return "", fmt.Errorf("the delivery station: node-topology GET /service-areas/%s answered no deliveryStationNodeId: %s", prefix, sim.Truncate(string(r.Body), 160))
	}
	return got.Station, nil
}

// route is the origin node and the delivery station of one placed order.
func (n *nodeBook) route(orderID string) (origin, dest string, err error) {
	n.mu.Lock()
	origin = n.origin
	stations := n.stations
	n.mu.Unlock()
	if origin == "" {
		return "", "", fmt.Errorf("order %s: the origin node was never resolved: the prepare phase did not complete", orderID)
	}
	prefix, ok := n.prefixes.get(orderID)
	if !ok {
		return "", "", fmt.Errorf("order %s: no postal prefix was recorded for it, so its delivery station is unknown", orderID)
	}
	dest, ok = stations[prefix]
	if !ok || dest == "" {
		return "", "", fmt.Errorf("order %s ships to prefix %s, which no delivery station was resolved for (served: %s)", orderID, prefix, strings.Join(servedPrefixes, ", "))
	}
	return origin, dest, nil
}

// parcels is the one method of *transport.Transport the adapter uses.
type parcels interface {
	DeliverOrder(ctx context.Context, orderID string, packages int, origin, destStation string, o transport.Options) transport.Delivery
}

type carrierAdapter struct {
	t     parcels
	route func(orderID string) (origin, dest string, err error)
	opt   transport.Options
	// point, when set (-delivery-point), gives the first order delivered the delivery-point journey.
	point *pointJourney
}

var _ day.Carrier = carrierAdapter{}

func (a carrierAdapter) DeliverOrder(ctx context.Context, orderID string, packages int) day.Delivery {
	origin, dest, err := a.route(orderID)
	if err != nil {
		return day.Delivery{Problems: []string{err.Error()}}
	}
	opt := a.opt
	var journey bool
	if a.point != nil {
		var done func()
		opt, journey, done = a.point.begin(a.opt)
		defer done()
	}
	d := a.t.DeliverOrder(ctx, orderID, packages, origin, dest, opt)
	if journey {
		a.point.report(orderID, d)
	}
	return toDelivery(d)
}

// toDelivery: DeliveredAt is set by the transport only when every package was delivered, and the
// day reads "delivered" from it, so it is copied as it is.
func toDelivery(d transport.Delivery) day.Delivery {
	return day.Delivery{DeliveredAt: d.DeliveredAt, Problems: slices.Clone(d.Problems)}
}
