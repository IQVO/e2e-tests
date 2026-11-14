package retailday

// The three tiers of the retail day, by the name each context answers to behind
// the gateway (Kong routes /api/<name>/...). A name here is also the key of
// sim.Client.Base.

// Warehouse is the WMS/WES tier the floor, the dock and the yard run on.
var Warehouse = []string{
	"facility-layout", "product-master", "inventory-storage", "wes-work-planning",
	"fulfillment-execution", "workforce-management", "order-management",
	"process-path-management", "labor-performance", "inbound-receiving",
	"dock-scheduling", "yard-management", "outbound-shipping",
}

// Network is the retail network: purchase orders, tracking, transport, delivery,
// and the one fleet-side seam, network-fulfillment.
var Network = []string{
	"customer-ordering", "delivery-promise", "order-sourcing", "vendor-connectivity",
	"node-topology", "node-performance", "offer-availability", "shipping-and-tracking",
	"linehaul-transportation", "last-mile-delivery", "network-fulfillment",
}

// Storefront is the shopper tier behind the BFF. The BFF itself is reached on
// its own base URL (Storefront key below), not through /api/<name>.
var Storefront = []string{
	"checkout", "shopping-cart", "pricing", "product-content", "product-discovery",
	"customer-payments", "payment-provider-simulator",
}

// BFF is the sim.Client.Base key of the storefront-bff base URL.
const BFF = "storefront-bff"

// AllGateway returns every context routed through the gateway.
func AllGateway() []string {
	out := make([]string, 0, len(Warehouse)+len(Network)+len(Storefront))
	out = append(out, Warehouse...)
	out = append(out, Network...)
	out = append(out, Storefront...)
	return out
}
