package inbound

import "strconv"

// ---------------------------------------------------------------------
// inventory-storage
// ---------------------------------------------------------------------

type bin struct {
	id, zone           string // zone: AMB (ambient), HAZ (hazmat-rated), CHL (chilled)
	capacity, occupied int
}

type stockUnit struct {
	id, sku, bin string
	qty          int
}

type fakeInventory struct {
	*fake
	bins     map[string]*bin
	units    []*stockUnit
	reserved map[string]int // units bound to reservations by someone else
	class    map[string]string
	// staged is what ReceiptLineReceived booked as staged stock: not usable until it is stowed.
	staged map[string]int
	// failStow, when set, makes POST /stowages answer that status with the slug "dependency-unavailable".
	failStow int
}

func newFakeInventory(w *world) *fakeInventory {
	f := &fakeInventory{fake: newFake(w, SvcInventory), bins: map[string]*bin{}, reserved: map[string]int{}, class: map[string]string{}, staged: map[string]int{}}
	f.handle("PUT", "/bins/{id}", func(c *fctx) reply {
		m, bad := c.obj()
		if bad != nil {
			return *bad
		}
		capacity, _ := intOf(m["capacity"])
		if capacity < 1 {
			return refuse(422, "invalid-capacity")
		}
		b := f.bins[c.params["id"]]
		if b == nil {
			b = &bin{id: c.params["id"], zone: "AMB", capacity: capacity}
			f.bins[b.id] = b
			return created(f.binJSON(b))
		}
		if capacity < b.occupied {
			return refuse(409, "capacity-below-occupancy")
		}
		b.capacity = capacity
		return ok(f.binJSON(b))
	})
	f.handle("GET", "/bins/{id}", func(c *fctx) reply {
		b := f.bins[c.params["id"]]
		if b == nil {
			return refuse(404, "bin-not-found")
		}
		return ok(f.binJSON(b))
	})
	f.handle("POST", "/stowages", f.stow)
	f.handle("GET", "/inventory/{sku}/usable", func(c *fctx) reply {
		sku := c.params["sku"]
		u := f.usable(sku)
		return ok(map[string]any{"sku": sku, "usable": u, "looseUsable": u, "reservableCases": 0})
	})
	return f
}

func (f *fakeInventory) binJSON(b *bin) map[string]any {
	return map[string]any{"binId": b.id, "capacity": b.capacity, "occupied": b.occupied, "available": b.capacity - b.occupied}
}

func (f *fakeInventory) onHand(sku string) int {
	n := 0
	for _, u := range f.units {
		if u.sku == sku {
			n += u.qty
		}
	}
	return n
}

// usable is on-hand minus the units bound to reservations; staged stock is not usable.
func (f *fakeInventory) usable(sku string) int { return f.onHand(sku) - f.reserved[sku] }

// stock puts units straight into a bin (the seeded stock a day starts with). The caller holds the world lock or
// is the single-threaded setup.
func (f *fakeInventory) stock(sku, binID string, qty int) {
	b := f.bins[binID]
	b.occupied += qty
	f.units = append(f.units, &stockUnit{id: "su-seed-" + strconv.Itoa(len(f.units)), sku: sku, bin: binID, qty: qty})
}

// consume removes units from the bins, first bin first: what the day's picks do.
func (f *fakeInventory) consume(sku string, qty int) {
	f.w.mu.Lock()
	defer f.w.mu.Unlock()
	for _, u := range f.units {
		if u.sku != sku || qty == 0 {
			continue
		}
		take := min(u.qty, qty)
		u.qty -= take
		f.bins[u.bin].occupied -= take
		qty -= take
	}
}

// onLineReceived is ReceiptLineReceived (Good) reaching inventory-storage: the units are staged, and StockReceived
// is raised on the analytics topic.
func (f *fakeInventory) onLineReceived(sku string, qty int) {
	f.staged[sku] += qty
	f.w.tap.publish("ReceiptLineReceived", `{"sku":"`+sku+`","quantity":`+strconv.Itoa(qty)+`}`)
	f.w.tap.publish("StockReceived", `{"sku":"`+sku+`","quantity":`+strconv.Itoa(qty)+`}`)
}

func (f *fakeInventory) stow(c *fctx) reply {
	m, bad := c.obj()
	if bad != nil {
		return *bad
	}
	sku, binID := str(m["sku"]), str(m["binId"])
	qty, _ := intOf(m["quantity"])
	if f.failStow != 0 {
		return refuse(f.failStow, "dependency-unavailable")
	}
	if sku == "" || binID == "" {
		return refuse(400, "stow-requires-item-and-location")
	}
	if qty < 1 {
		return refuse(422, "invalid-quantity")
	}
	b := f.bins[binID]
	if b == nil {
		return refuse(404, "bin-not-found")
	}
	if b.occupied+qty > b.capacity {
		return refuse(409, "bin-full")
	}
	switch f.class[sku] {
	case "HAZ":
		if b.zone != "HAZ" {
			return refuse(409, "hazmat-zone-required")
		}
	case "CHL":
		if b.zone != "CHL" {
			return refuse(409, "temperature-class-mismatch")
		}
	}
	b.occupied += qty
	u := &stockUnit{id: f.w.id("su"), sku: sku, bin: binID, qty: qty}
	f.units = append(f.units, u)
	return created(map[string]any{"id": u.id, "sku": sku, "binId": binID, "quantity": qty, "reserved": 0, "state": "AVAILABLE"})
}
