package inbound

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/claudioed/warehouse-e2e/internal/sim/outbound"
	"github.com/claudioed/warehouse-e2e/internal/sim/transport"
)

// Service names: the keys of sim.Client.Base (and the Kong path segment). The
// three services inbound shares with the outbound crew reuse its constants.
const (
	SvcInbound   = "inbound-receiving"
	SvcInventory = "inventory-storage"
	SvcDock      = outbound.SvcDock
	SvcYard      = outbound.SvcYard
	SvcFacility  = outbound.SvcFacility
)

// What is reused from the outbound and transport actor packages instead of being written twice:
// the clock (so every wait runs on an injected time), the wait bounds, the carrier, the appointment
// window, the dock calendar settings and the readiness report types.
type (
	// Clock is the actors' only source of time (Now stamps windows, Sleep is every wait).
	Clock = transport.Clock
	// Waits bounds an eventually-style loop: Timeout of clock time, a check every Interval.
	// Zero values mean the suite's own 30 s and 500 ms.
	Waits = outbound.Waits
	// Carrier is the carrier the simulated trucks belong to.
	Carrier = outbound.Carrier
	// Window is an appointment window.
	Window = outbound.Window
)

func withDefaultWaits(w Waits) Waits {
	if w.Timeout <= 0 {
		w.Timeout = 30 * time.Second
	}
	if w.Interval <= 0 {
		w.Interval = 500 * time.Millisecond
	}
	return w
}

// Site is the building the restock crew works in. Doors are the facility-layout location codes of its
// INBOUND-flow dock slots (Inbound or Both); Bins are the inventory-storage bins the stower may put stock
// into, in preference order. Both may be left empty and filled from a Readiness (Readiness.Apply).
type Site struct {
	Code  string
	Doors []string
	Bins  []string
}

// Line is one SKU and a quantity of eaches: what a supplier announces and what a stower puts away.
type Line struct {
	SKU string
	Qty int
}

// ASN is a registered advance ship notice. Lines are numbered 1..n in this order.
type ASN struct {
	Number string
	Lines  []Line
}

// Appointment is dock-scheduling's booked INBOUND appointment.
type Appointment struct {
	ID     string
	SiteID string
	Door   string
	ASN    string
	Seal   string // the seal the carrier declared when booking; the gate check-in carries the same one
	Window Window
	State  string
}

// Visit is the carrier visit the gate checked in, with the one trailer it brought.
type Visit struct {
	ID            string
	Plate         string
	Unit          string
	TrailerID     string
	AppointmentID string
	Door          string
	MoveID        string // the TO_DOOR move that docked the trailer
}

// Receipt is inbound-receiving's receipt as the receiver last saw it.
type Receipt struct {
	ID            string
	ASN           string
	AppointmentID string
	Door          string
	State         string // Open, Closed
	Lines         []ReceiptLine
	Discrepancies []string // "<kind> line <n> <sku>: expected E, received R, damaged D"
}

// ReceiptLine is one line of a receipt.
type ReceiptLine struct {
	LineNo          int
	SKU             string
	Expected        int
	ReceivedGood    int
	ReceivedDamaged int
}

// Stowed is one stow: units of a SKU put into a bin of inventory-storage.
type Stowed struct {
	SKU string
	Qty int
	Bin string
}

// Delivery is what became of one truck: the ASN, the appointment, the visit, the receipt, the stows and the
// release of the door. Problems are the findings, every refusal and every timeout in order; a delivery with
// Problems did not complete, and nothing here panics.
type Delivery struct {
	ASN           string
	AppointmentID string
	Door          string
	VisitID       string
	TrailerID     string
	ReceiptID     string
	Lines         []Line
	Received      map[string]int // SKU -> Good units the receipt counted
	Stowed        []Stowed
	Released      bool // the visit was checked out with its trailer
	Completed     bool // dock-scheduling shows the appointment COMPLETED
	Problems      []string
}

// OK reports a delivery whose goods were received and stowed and whose truck left, with no finding.
func (d Delivery) OK() bool {
	return d.Released && d.Completed && len(d.Problems) == 0 && len(d.Stowed) > 0
}

func (d *Delivery) problem(format string, args ...any) {
	d.Problems = append(d.Problems, fmt.Sprintf(format, args...))
}

// StowedUnits is the total units stowed per SKU.
func (d Delivery) StowedUnits() map[string]int {
	out := map[string]int{}
	for _, s := range d.Stowed {
		out[s.SKU] += s.Qty
	}
	return out
}

// normalise merges duplicate SKUs, drops non-positive quantities and sorts by SKU, so the same demand always
// makes the same ASN (the day's shelf is deterministic, ADR 0001 D4).
func normalise(lines []Line) []Line {
	sum := map[string]int{}
	for _, l := range lines {
		if l.Qty > 0 && l.SKU != "" {
			sum[l.SKU] += l.Qty
		}
	}
	out := make([]Line, 0, len(sum))
	for sku, q := range sum {
		out = append(out, Line{SKU: sku, Qty: q})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SKU < out[j].SKU })
	return out
}

func describeLines(lines []Line) string {
	parts := make([]string, len(lines))
	for i, l := range lines {
		parts[i] = fmt.Sprintf("%s x%d", l.SKU, l.Qty)
	}
	return strings.Join(parts, ", ")
}
