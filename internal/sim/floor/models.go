package floor

import (
	"fmt"
	"sort"
	"sync"
)

// The read models of the routes the floor reads, in the services' wire names.

// orderDTO is order-management GET /orders/{id}.
type orderDTO struct {
	ID                string      `json:"id"`
	Status            string      `json:"status"`
	FulfillmentSiteID string      `json:"fulfillmentSiteId"`
	PromiseDate       string      `json:"promiseDate"`
	Lines             []orderLine `json:"lines"`
}

type orderLine struct {
	LineNo        int    `json:"lineNo"`
	SKU           string `json:"sku"`
	Quantity      int    `json:"quantity"`
	GiftWrap      bool   `json:"giftWrap"`
	Status        string `json:"status"`
	ReservationID string `json:"reservationId"`
}

// workUnit is wes-work-planning's work unit: one per order line, `<orderId>-line-<n>`, reference = the order id.
type workUnit struct {
	ID        string `json:"id"`
	PathID    string `json:"pathId"`
	CPT       string `json:"cpt"`
	Reference string `json:"reference"`
	State     string `json:"state"`
	GiftWrap  bool   `json:"giftWrap"`
	SKU       string `json:"sku"`
	LineNo    int    `json:"lineNo"`
}

// task is fulfillment-execution's task. For a PICK task orderRef is the work unit's id; for the
// PACK task rebin creates it is the order id.
type task struct {
	ID             string `json:"id"`
	Type           string `json:"type"`
	Status         string `json:"status"`
	CPT            string `json:"cpt"`
	OrderRef       string `json:"orderRef"`
	Fragile        bool   `json:"fragile"`
	GiftWrap       bool   `json:"giftWrap"`
	LeaseStationID string `json:"leaseStationId"`
}

// pkgDTO is fulfillment-execution's package, as POST /tasks/{id}/package-seals and GET /packages/{id} answer.
type pkgDTO struct {
	ID              string   `json:"id"`
	OrderRef        string   `json:"orderRef"`
	Status          string   `json:"status"`
	ScannedContents []string `json:"scannedContents"`
	SortLane        string   `json:"sortLane"`
}

// reservationDTO is inventory-storage's reservation.
type reservationDTO struct {
	ID        string `json:"id"`
	SKU       string `json:"sku"`
	Quantity  int    `json:"quantity"`
	DemandRef string `json:"demandRef"`
	LineNo    int    `json:"lineNo"`
	Status    string `json:"status"`
}

// Result is what became of one order. Package is set once a package was sealed; Status is the
// package's state as fulfillment-execution last answered it (LABELED when SLAM applied the label).
// Problems are the findings, every refusal and every timeout in order: a Result with Problems did
// not complete cleanly, and nothing here panics. The fields from PackageID on are what
// outbound-shipping's PackageManifested carries (package_id, order_ref, source_order_id, site_id,
// cpt, weight_g): hand them to the outbound crew as its Package.
type Result struct {
	OrderID string
	// Units are the order's WES work unit ids, by line.
	Units []string
	// PackTaskID is the PACK task that rebin created.
	PackTaskID string

	PackageID string
	Status    string
	SortLane  string
	// OrderRef is the manifest's order_ref (the PACK task's orderRef: the order id) and
	// SourceOrderID the order-management id ShipmentConfirmed lists.
	OrderRef      string
	SourceOrderID string
	// SiteID is the order's fulfillmentSiteId.
	SiteID string
	// CPT is the packCpt posted at rebin: the order's promise date, which the PACK task keeps and
	// PackageManifested publishes (RFC 3339, UTC).
	CPT string
	// WeightG is what the SLAM scale read, in grams.
	WeightG int
	// Contents are the SKUs sealed in the package, one per unit.
	Contents []string

	Problems []string
}

// Labeled reports a package that SLAM labeled.
func (r Result) Labeled() bool { return r.PackageID != "" && r.Status == PackageLabeled }

// OK reports an order that reached a LABELED package without a single finding.
func (r Result) OK() bool { return r.Labeled() && len(r.Problems) == 0 }

// station is one installed station and the person at it. A station works one task at a time.
type station struct {
	ID         string
	Occupant   string
	Capability string
	busy       bool
}

// lineState is one order line: what order-management says, the work unit wes-work-planning made
// of it and how far the line got.
type lineState struct {
	order *orderState
	No    int
	SKU   string
	Qty   int
	Gift  bool
	// Reservation is the reservation id order-management holds for the line, if any.
	Reservation string
	Unit        workUnit

	picked  bool
	fragile bool
	cpt     string
}

func (l *lineState) lineID() string { return fmt.Sprintf("line-%d", l.No) }

// orderState is one order in flight. Several goroutines touch it (the one running Process, and a
// picker or packer of ANOTHER order that was handed one of its tasks), so every field is behind mu.
type orderState struct {
	id string

	mu       sync.Mutex
	site     string
	promise  string
	lines    []*lineState
	cpt      string // the packCpt override or the first picked task's cpt
	failure  error
	pkg      *pkgDTO
	packTask string

	releasedEvery bool // every unit was seen past Pending
	foreign       int  // units of others the release operator admitted on this order's behalf
}

func (o *orderState) fail(err error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.failure == nil {
		o.failure = err
	}
}

func (o *orderState) failed() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.failure
}

func (o *orderState) allPicked() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, l := range o.lines {
		if !l.picked {
			return false
		}
	}
	return true
}

func (o *orderState) unpicked() []int {
	o.mu.Lock()
	defer o.mu.Unlock()
	var out []int
	for _, l := range o.lines {
		if !l.picked {
			out = append(out, l.No)
		}
	}
	return out
}

func (o *orderState) markPicked(l *lineState, taskCPT string, fragile bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	l.picked, l.fragile, l.cpt = true, fragile, taskCPT
	if o.cpt == "" {
		o.cpt = taskCPT
	}
}

// packHints are the PACK task's hints: fragile when any picked line was, gift wrap when any line asks.
func (o *orderState) packHints() (cpt string, fragile, gift bool, required []string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, l := range o.lines {
		fragile = fragile || l.fragile
		gift = gift || l.Gift
		required = append(required, l.lineID())
	}
	sort.Strings(required)
	return o.cpt, fragile, gift, required
}

// contents is one SKU per unit of every line, in line order.
func (o *orderState) contents() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	var out []string
	for _, l := range o.lines {
		for i := 0; i < l.Qty; i++ {
			out = append(out, l.SKU)
		}
	}
	return out
}

func (o *orderState) sealed() *pkgDTO {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.pkg
}

func (o *orderState) releasedAll() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.releasedEvery
}

func (o *orderState) markReleasedAll() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.releasedEvery = true
}

func (o *orderState) addForeignRelease() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.foreign++
	return o.foreign
}

// syncUnitStates records the units' states as wes-work-planning last answered them and returns the
// units still Pending, by line.
func (o *orderState) syncUnitStates(state map[string]string) []workUnit {
	o.mu.Lock()
	defer o.mu.Unlock()
	var pending []workUnit
	for _, l := range o.lines {
		if s, found := state[l.Unit.ID]; found {
			l.Unit.State = s
		}
		if l.Unit.State == UnitPending {
			pending = append(pending, l.Unit)
		}
	}
	return pending
}

func (o *orderState) line(no int) *lineState {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, l := range o.lines {
		if l.No == no {
			return l
		}
	}
	return nil
}

func (o *orderState) unitState(no int) string {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, l := range o.lines {
		if l.No == no {
			return l.Unit.State
		}
	}
	return ""
}

func (o *orderState) setPackage(p *pkgDTO, taskID string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.pkg, o.packTask = p, taskID
}
