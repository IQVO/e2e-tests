// Package outbound simulates the people who get a SLAMMED package out of the
// building, over the published REST contracts only: the dock planner (books a
// door for a cutoff), the gate and yard clerk (checks the carrier in, moves the
// trailer to the door, completes the door work, records the seal, checks the
// visit out), the stager, the loader and the load supervisor.
//
// What it does NOT do is just as deliberate: no database, no injected Kafka
// event. The PackageManifested that starts the chain is raised by
// fulfillment-execution's SLAM; the TrailerDocked that starts loading and the
// TrailerDeparted that confirms the shipment are raised by yard-management when
// the clerk completes the move and checks the visit out. ShipmentConfirmed is
// only OBSERVED (see Confirmations), never produced here.
//
// The simulators' rules apply: every creating POST carries a fresh
// Idempotency-Key (sim.Client does it), a refusal is a finding and never a
// panic, and the one tolerated failure is the consumer-lag refusal of a call
// that depends on an event another service has not consumed yet, retried inside
// a bounded loop (retry.go). Anything else fails at once.
//
// It was written against the services' published contracts (apis/openapi.yaml,
// the ADRs) and exercised against contract fakes (fakes_test.go); it has never
// run against the cluster.
package outbound

import (
	"fmt"
	"time"
)

// Service names: the keys of sim.Client.Base (and the Kong path segment).
const (
	SvcOutbound    = "outbound-shipping"
	SvcYard        = "yard-management"
	SvcDock        = "dock-scheduling"
	SvcFacility    = "facility-layout"
	SvcProcessPath = "process-path-management"
)

// Load and package states of outbound-shipping that the actors branch on.
const (
	LoadPlanned       = "PLANNED"
	LoadLoading       = "LOADING"
	LoadClosed        = "CLOSED"
	LoadShipConfirmed = "SHIP_CONFIRMED"

	PackageExpected = "EXPECTED"
	PackageStaged   = "STAGED"
	PackageLoaded   = "LOADED"
	PackageShipped  = "SHIPPED"
)

// Site is the building the floor works in. Doors are the facility-layout
// location codes of its OUTBOUND-flow dock slots, StagingLocations those of its
// Staging slots; Timezone is the IANA zone its CPT schedule is declared in.
type Site struct {
	Code             string
	Timezone         string
	Doors            []string
	StagingLocations []string
}

// Package is what SLAM manifested: the facts of PackageManifested the floor
// needs to find the package again in outbound-shipping.
type Package struct {
	ID            string // package_id: outbound-shipping's package id
	OrderRef      string // order_ref (the work unit's order reference)
	SourceOrderID string // source_order_id: order-management's order id (what ShipmentConfirmed lists)
	SiteID        string // site_id; empty means the floor's own site
	CPT           string // cpt: the cutoff's local time of the manifest, e.g. "17:00"
	WeightG       int
}

// Window is an appointment window.
type Window struct{ Start, End time.Time }

// Appointment is dock-scheduling's booked OUTBOUND appointment.
type Appointment struct {
	ID     string
	SiteID string
	Door   string
	CptID  string
	Window Window
	State  string
}

// Visit is the carrier visit the clerk checked in at the gate, with the one
// trailer it brought.
type Visit struct {
	ID            string
	SiteID        string
	Plate         string
	Unit          string
	TrailerID     string
	AppointmentID string
	Door          string
	MoveID        string // the TO_DOOR move that docked the trailer
}

// Shipment is what became of one package. Departed is true when the visit was
// checked out with its trailer; LoadState is the load's state as outbound-shipping
// last answered (SHIP_CONFIRMED when the departure was consumed). Problems are
// the findings: every refusal and every timeout, in order. A shipment with
// Problems did not complete; nothing here panics.
type Shipment struct {
	PackageID string
	OrderRef  string
	LoadID    string
	Door      string
	VisitID   string
	TrailerID string
	Seal      string
	Departed  bool
	LoadState string
	Problems  []string
}

// OK reports a shipment that left the building and was confirmed by outbound-shipping.
func (s Shipment) OK() bool {
	return s.Departed && s.LoadState == LoadShipConfirmed && len(s.Problems) == 0
}

func (s *Shipment) problem(format string, args ...any) {
	s.Problems = append(s.Problems, fmt.Sprintf(format, args...))
}

// Waits bounds an eventually-style loop: the call is repeated every Interval
// until it succeeds or Timeout elapses. Zero values mean the suite's own 30 s
// and 500 ms.
type Waits struct{ Timeout, Interval time.Duration }

func (w Waits) orDefault() Waits {
	if w.Timeout <= 0 {
		w.Timeout = 30 * time.Second
	}
	if w.Interval <= 0 {
		w.Interval = 500 * time.Millisecond
	}
	return w
}

// loadDTO, packageDTO: the read models of outbound-shipping's GET routes.
type loadDTO struct {
	ID            string `json:"id"`
	SiteID        string `json:"site_id"`
	CptID         string `json:"cpt_id"`
	OperatingDate string `json:"operating_date"`
	ShipMethod    string `json:"ship_method"`
	State         string `json:"state"`
	DoorCode      string `json:"door_code"`
	TrailerID     string `json:"trailer_id"`
	AppointmentID string `json:"appointment_id"`
	PackageCount  int    `json:"package_count"`
}

type packageDTO struct {
	ID              string `json:"id"`
	SiteID          string `json:"site_id"`
	SourceOrderID   string `json:"source_order_id"`
	WorkUnitRef     string `json:"work_unit_ref"`
	LoadID          string `json:"load_id"`
	StagingLocation string `json:"staging_location"`
	State           string `json:"state"`
}

// Load is outbound-shipping's outbound load, as the floor reads it.
type Load = loadDTO

// Cutoff is one cutoff (CPT) of a site's schedule, in process-path-management's wire names.
type Cutoff struct {
	CptID           string   // e.g. "aus-1700"
	LocalTime       string   // "HH:MM" in the schedule's timezone: what a package manifest's cpt equals
	DaysOfWeek      []string // Mon..Sun
	ShipMethod      string
	EligiblePathIDs []string // process paths the cutoff serves; required by the PUT
}
