package main

// Step definitions for the inbound dock chain: dock-scheduling (appointment
// booking, since inbound-receiving ADR 0019), yard-management (the gate
// check-in, docking, door work and departure) and inbound-receiving's LOCAL
// COPY of the appointment, used by features/inbound_receiving.feature (the
// appointment scenario) and features/inbound_journey.feature.
//
// Black-box and REST-only: the only Kafka access is read-only scans of the
// owners' integration topics. Nothing is injected; every fact is produced by
// the services themselves. The dock door is real: a Dock location type and a
// Dock slot (dockFlow Inbound/Outbound) declared in facility-layout, which
// dock-scheduling, yard-management and inbound-receiving all learn the door
// from over Kafka.

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/cucumber/godog"
)

var (
	dockSchedulingBaseURL = envOrDefault("DOCK_SCHEDULING_BASE_URL", "http://localhost:8112")
	yardBaseURL           = envOrDefault("YARD_MANAGEMENT_BASE_URL", "http://localhost:8113")
)

const (
	topicYardEvents = "warehouse.yard-management.events"

	typeVisitCheckedIn      = "com.warehouse.wms.yard-management.yardvisit.VisitCheckedIn"
	typeTrailerDocked       = "com.warehouse.wms.yard-management.trailer.TrailerDocked"
	typeDoorWorkCompleted   = "com.warehouse.wms.yard-management.trailer.DoorWorkCompleted"
	typeSealExceptionRaised = "com.warehouse.wms.yard-management.trailer.SealExceptionRaised"
	typeReceiptStowDone     = "com.warehouse.wms.inbound-receiving.receipt.ReceiptStowCompleted"
	typeContainerStowed     = "com.warehouse.wms.inventory-storage.container.ContainerStowed"

	// dockSlotMinutes is the slot granularity the scenarios declare on the
	// site's dock calendar; every appointment window starts and ends on it.
	dockSlotMinutes = 5
	// dockToleranceMinutes is how early a carrier may arrive and still be
	// sent to the appointment door (the appointment's arrival tolerance).
	dockToleranceMinutes = 60
	// dockLeadMinutes is how far ahead of "now" the window anchor sits, so a
	// booking made a few minutes into the scenario is never in the past.
	dockLeadMinutes = 10

	dockDoorType = "DockDoor"
)

// dockChainState is the per-process memory of the dock chain, keyed by the
// first ASN number of an appointment (or by the shipment reference of an
// outbound one). Scenarios own distinct run-scoped keys, like inboundState.
type dockChainState struct {
	appt    map[string]map[string]any // key -> dock-scheduling's appointment (last answer)
	checkIn map[string]map[string]any // ASN -> yard-management's CheckIn answer
	arrived map[string]time.Time      // ASN -> the visit's arrivedAt
}

func (w *world) dock() *dockChainState {
	in := w.ib()
	if in.dock == nil {
		in.dock = &dockChainState{
			appt:    map[string]map[string]any{},
			checkIn: map[string]map[string]any{},
			arrived: map[string]time.Time{},
		}
	}
	return in.dock
}

// ---------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------

func (w *world) dockChainServicesAreHealthy() error {
	for name, base := range map[string]string{
		"dock-scheduling": dockSchedulingBaseURL,
		"yard-management": yardBaseURL,
		"facility-layout": facilityBaseURL,
	} {
		if err := w.doJSON(http.MethodGet, base+"/healthz", nil); err != nil {
			return fmt.Errorf("%s not reachable: %w", name, err)
		}
		if w.last.status != http.StatusOK {
			return fmt.Errorf("%s /healthz returned %d, body=%s", name, w.last.status, w.last.body)
		}
	}
	return nil
}

// problemSlug is the last path segment of the problem type of the last
// answer ("" when it is not a problem document).
func (w *world) problemSlug() string {
	typ, _ := w.last.json()["type"].(string)
	return typ[strings.LastIndex(typ, "/")+1:]
}

// retryWhileProblem runs call until it answers something other than one of
// the transient problem slugs (a fact the owner has not consumed yet), then
// leaves that final answer on w.last.
func (w *world) retryWhileProblem(call func() error, transient ...string) error {
	var final error
	err := eventually(func() error {
		if err := call(); err != nil {
			final = err
			return nil
		}
		if w.last.status >= 400 {
			slug := w.problemSlug()
			for _, t := range transient {
				if slug == t {
					return fmt.Errorf("%d %s: %s", w.last.status, slug, w.last.body)
				}
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return final
}

// windowAt returns the half-open window [anchor+startMin, anchor+startMin+durMin).
// The anchor is fixed by the dock calendar step (dockLeadMinutes ahead, on the
// slot granularity), so "starts when the other one ends" is exact.
func (w *world) windowAt(startMin, durMin int) (time.Time, time.Time) {
	in := w.ib()
	if in.anchor.IsZero() || in.anchor.Before(time.Now()) {
		in.anchor = slotAnchor(time.Now())
	}
	start := in.anchor.Add(time.Duration(startMin) * time.Minute)
	return start, start.Add(time.Duration(durMin) * time.Minute)
}

// slotAnchor is now + dockLeadMinutes rounded UP to the next slot boundary.
func slotAnchor(now time.Time) time.Time {
	slot := time.Duration(dockSlotMinutes) * time.Minute
	return now.UTC().Add(time.Duration(dockLeadMinutes) * time.Minute).Truncate(slot).Add(slot)
}

func siteOfDoor(door string) string { return strings.SplitN(door, "-", 2)[0] }

func parseTime(v any) (time.Time, error) {
	s, _ := v.(string)
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("%v is not an RFC 3339 time", v)
	}
	return t, nil
}

func (w *world) dockAppointment(key string) (map[string]any, error) {
	a, ok := w.dock().appt[key]
	if !ok {
		return nil, fmt.Errorf("no dock-scheduling appointment has been booked for %s in this process", key)
	}
	return a, nil
}

func (w *world) readDockAppointment(id string) (map[string]any, error) {
	if err := w.doJSON(http.MethodGet, dockSchedulingBaseURL+"/appointments/"+id, nil); err != nil {
		return nil, err
	}
	if w.last.status != http.StatusOK {
		return nil, fmt.Errorf("GET dock-scheduling appointment %s = %d (%s)", id, w.last.status, w.last.body)
	}
	return w.last.json(), nil
}

// ---------------------------------------------------------------------
// facility-layout: a dock site with real dock doors
// ---------------------------------------------------------------------

// dockDoorExists declares, in facility-layout, the site, a Dock location type
// (role Dock, no capacity), the dock zone and aisle the door code names, and
// the door itself as a Dock slot of the given flow. Every declaration
// tolerates a 409, so a re-run against a long-lived database is harmless.
func (w *world) dockDoorExists(flow, door string) error {
	door = w.rs(door)
	parts := strings.Split(door, "-")
	if len(parts) != 7 {
		return fmt.Errorf("door %q is not a 7-segment location code", door)
	}
	site, area, zone, aisle := parts[0], parts[1], parts[2], parts[3]
	if err := w.ensureSite(site, "Dock chain E2E "+site); err != nil {
		return fmt.Errorf("site %s: %w", site, err)
	}
	if err := w.expectOKOr409(w.doJSON(http.MethodPost, facilityBaseURL+"/location-types",
		map[string]any{"name": dockDoorType, "role": "Dock"})); err != nil {
		return fmt.Errorf("location type %s: %w", dockDoorType, err)
	}
	zoneID := strings.Join(parts[:3], "-")
	if err := w.ensureZone(area, zone, site, "Ambient", false); err != nil {
		return fmt.Errorf("zone %s: %w", zoneID, err)
	}
	if err := w.ensureAisle(aisle, zoneID, 1, "TwoWay"); err != nil {
		return fmt.Errorf("aisle %s of zone %s: %w", aisle, zoneID, err)
	}
	if err := w.expectOKOr409(w.doJSON(http.MethodPost, facilityBaseURL+"/locations",
		map[string]any{"locationCode": door, "locationType": dockDoorType, "dockFlow": flow})); err != nil {
		return fmt.Errorf("dock slot %s: %w", door, err)
	}
	return nil
}

// dockCalendarDeclared declares the booking settings of the site on
// dock-scheduling's calendar, retrying while the site is not yet known there
// (it learns sites and doors from facility-layout over Kafka), and re-anchors
// the scenario's appointment windows.
func (w *world) dockCalendarDeclared(site string) error {
	site = w.rs(site)
	w.ib().anchor = slotAnchor(time.Now())
	return eventually(func() error {
		if err := w.doJSON(http.MethodPut, fmt.Sprintf("%s/sites/%s/dock-calendar/settings", dockSchedulingBaseURL, site),
			map[string]any{
				"maxWindowMinutes": 240, "slotGranularityMinutes": dockSlotMinutes,
				"arrivalToleranceMinutes": dockToleranceMinutes, "noShowGraceMinutes": 240,
			}); err != nil {
			return err
		}
		if w.last.status != http.StatusOK {
			return fmt.Errorf("declare calendar settings of %s = %d (%s)", site, w.last.status, w.last.body)
		}
		return nil
	})
}

// ---------------------------------------------------------------------
// dock-scheduling: book, refuse, reschedule, cancel
// ---------------------------------------------------------------------

func (w *world) postBooking(body map[string]any) error {
	// dock-scheduling learns doors (facility-layout) and ASNs (inbound-receiving)
	// over Kafka: a booking that names one it has not consumed yet is refused
	// 422 and persists nothing, so retrying is exactly what a planner would do.
	return w.retryWhileProblem(func() error {
		return w.doJSON(http.MethodPost, dockSchedulingBaseURL+"/appointments", body)
	}, "unknown-door", "unknown-asn")
}

func (w *world) inboundBody(door, carrier, asn string, startMin, durMin int) map[string]any {
	start, end := w.windowAt(startMin, durMin)
	return map[string]any{
		"direction": "INBOUND", "doorCode": door, "carrier": carrier, "carrierScac": "ACMF",
		"windowStart": start.Format(time.RFC3339), "windowEnd": end.Format(time.RFC3339),
		"asnNumbers": []string{asn}, "expectedSealNumber": expectedSeal(asn),
	}
}

func expectedSeal(asn string) string { return "SEAL-" + asn }

func (w *world) dockBooked(door, carrier, asn string, startMin, durMin int) error {
	door, asn = w.rs(door), w.rs(asn)
	if err := w.postBooking(w.inboundBody(door, carrier, asn, startMin, durMin)); err != nil {
		return err
	}
	got := w.last.json()
	id, _ := got["appointmentId"].(string)
	v, _ := intOf(got["version"])
	if w.last.status != http.StatusCreated || id == "" || got["state"] != "BOOKED" || v != 1 ||
		got["doorCode"] != door || got["siteId"] != siteOfDoor(door) || got["direction"] != "INBOUND" {
		return fmt.Errorf("book %s at %s: status %d, want 201 BOOKED v1 INBOUND at site %s (body: %s)",
			asn, door, w.last.status, siteOfDoor(door), w.last.body)
	}
	w.dock().appt[asn] = got
	w.ib().appointmentByASN[asn] = id
	w.ib().doorByASN[asn] = door
	return nil
}

func (w *world) dockBookingRefused(door, asn string, startMin, durMin, status int, slug string) error {
	door, asn = w.rs(door), w.rs(asn)
	if err := w.postBooking(w.inboundBody(door, "ACME Freight", asn, startMin, durMin)); err != nil {
		return err
	}
	return w.problemIs(status, slug)
}

// dockOutboundBooked books an OUTBOUND appointment (it names a shipment
// reference, not an ASN) and remembers it under the reference.
func (w *world) dockOutboundBooked(ref, door, carrier string, startMin, durMin int) error {
	ref, door = w.rs(ref), w.rs(door)
	start, end := w.windowAt(startMin, durMin)
	if err := w.postBooking(map[string]any{
		"direction": "OUTBOUND", "doorCode": door, "carrier": carrier,
		"windowStart": start.Format(time.RFC3339), "windowEnd": end.Format(time.RFC3339),
		"shipmentRefs": []string{ref},
	}); err != nil {
		return err
	}
	got := w.last.json()
	id, _ := got["appointmentId"].(string)
	if w.last.status != http.StatusCreated || id == "" || got["state"] != "BOOKED" || got["direction"] != "OUTBOUND" {
		return fmt.Errorf("book outbound %s at %s: status %d, want 201 BOOKED OUTBOUND (body: %s)", ref, door, w.last.status, w.last.body)
	}
	w.dock().appt[ref] = got
	w.ib().appointmentByASN[ref] = id
	return nil
}

func (w *world) dockRescheduled(asn, door string, startMin, durMin int) error {
	asn, door = w.rs(asn), w.rs(door)
	a, err := w.dockAppointment(asn)
	if err != nil {
		return err
	}
	start, end := w.windowAt(startMin, durMin)
	prev, _ := intOf(a["version"])
	id, _ := a["appointmentId"].(string)
	if err := w.expectOK2xx(w.doJSON(http.MethodPost, fmt.Sprintf("%s/appointments/%s/reschedules", dockSchedulingBaseURL, id),
		map[string]any{"doorCode": door, "windowStart": start.Format(time.RFC3339), "windowEnd": end.Format(time.RFC3339)})); err != nil {
		return fmt.Errorf("reschedule %s to %s: %w", id, door, err)
	}
	got := w.last.json()
	v, _ := intOf(got["version"])
	if w.last.status != http.StatusOK || got["state"] != "BOOKED" || got["doorCode"] != door || v != prev+1 {
		return fmt.Errorf("reschedule %s: status %d, want 200 BOOKED at %s version %d (body: %s)", id, w.last.status, door, prev+1, w.last.body)
	}
	w.dock().appt[asn] = got
	w.ib().doorByASN[asn] = door
	return nil
}

func (w *world) dockCancelled(asn string) error {
	asn = w.rs(asn)
	a, err := w.dockAppointment(asn)
	if err != nil {
		return err
	}
	id, _ := a["appointmentId"].(string)
	if err := w.expectOK2xx(w.doJSON(http.MethodPost, fmt.Sprintf("%s/appointments/%s/cancellations", dockSchedulingBaseURL, id),
		map[string]any{"reason": "CARRIER_REQUEST"})); err != nil {
		return fmt.Errorf("cancel %s: %w", id, err)
	}
	got := w.last.json()
	if w.last.status != http.StatusOK || got["state"] != "CANCELLED" || got["cancellationReason"] != "CARRIER_REQUEST" {
		return fmt.Errorf("cancel %s: status %d, want 200 CANCELLED CARRIER_REQUEST (body: %s)", id, w.last.status, w.last.body)
	}
	w.dock().appt[asn] = got
	return nil
}

// dockStateIs polls dock-scheduling for the appointment's state: the gate
// check-in and the completion reach it as events from the other owners.
func (w *world) dockStateIs(asn, want string) error {
	asn = w.rs(asn)
	a, err := w.dockAppointment(asn)
	if err != nil {
		return err
	}
	id, _ := a["appointmentId"].(string)
	return eventually(func() error {
		got, err := w.readDockAppointment(id)
		if err != nil {
			return err
		}
		if got["state"] != want {
			return fmt.Errorf("dock-scheduling appointment %s state = %v, want %s (body: %s)", id, got["state"], want, w.last.body)
		}
		switch want {
		case "CHECKED_IN":
			if got["arrival"] == nil {
				return fmt.Errorf("appointment %s is CHECKED_IN without the arrival fact (body: %s)", id, w.last.body)
			}
		case "COMPLETED":
			if got["completedAt"] == nil {
				return fmt.Errorf("appointment %s is COMPLETED without completedAt (body: %s)", id, w.last.body)
			}
		}
		return nil
	})
}

// ---------------------------------------------------------------------
// inbound-receiving: the local copy of the appointment
// ---------------------------------------------------------------------

// copyIsInState polls inbound-receiving's (deprecated) read of its local copy.
func (w *world) copyIsInState(asn, want string) error {
	asn = w.rs(asn)
	id, err := w.appointmentIDFor(asn)
	if err != nil {
		return err
	}
	return eventually(func() error {
		if err := w.doJSON(http.MethodGet, fmt.Sprintf("%s/appointments/%s", inboundBaseURL, id), nil); err != nil {
			return err
		}
		if w.last.status != http.StatusOK {
			return fmt.Errorf("GET inbound-receiving appointment %s = %d (%s)", id, w.last.status, w.last.body)
		}
		if got := w.last.json()["state"]; got != want {
			return fmt.Errorf("inbound-receiving's copy of %s is %v, want %s (body: %s)", id, got, want, w.last.body)
		}
		return nil
	})
}

// copyIsStill reads the copy ONCE: inbound-receiving's copy holds only Booked and
// Cancelled (ADR 0019 point 4); the arrival and the completion are not copied,
// they stay in yard-management and dock-scheduling.
func (w *world) copyIsStill(asn, want string) error {
	asn = w.rs(asn)
	id, err := w.appointmentIDFor(asn)
	if err != nil {
		return err
	}
	if err := w.doJSON(http.MethodGet, fmt.Sprintf("%s/appointments/%s", inboundBaseURL, id), nil); err != nil {
		return err
	}
	if w.last.status != http.StatusOK {
		return fmt.Errorf("GET inbound-receiving appointment %s = %d (%s)", id, w.last.status, w.last.body)
	}
	if got := w.last.json()["state"]; got != want {
		return fmt.Errorf("inbound-receiving's copy of %s is %v, want it still %s (body: %s)", id, got, want, w.last.body)
	}
	return nil
}

// dockStateIsNow reads dock-scheduling ONCE: used to prove a state has NOT moved yet.
func (w *world) dockStateIsNow(asn, want string) error {
	asn = w.rs(asn)
	a, err := w.dockAppointment(asn)
	if err != nil {
		return err
	}
	got, err := w.readDockAppointment(a["appointmentId"].(string))
	if err != nil {
		return err
	}
	if got["state"] != want {
		return fmt.Errorf("dock-scheduling appointment %v is %v, want it still %s (body: %s)", a["appointmentId"], got["state"], want, w.last.body)
	}
	return nil
}

// copyMirrors requires inbound-receiving's copy to carry exactly what
// dock-scheduling holds: door, window, ASN and the version (the copy's
// ordering guard, ADR 0020).
func (w *world) copyMirrors(asn string) error {
	asn = w.rs(asn)
	a, err := w.dockAppointment(asn)
	if err != nil {
		return err
	}
	id, _ := a["appointmentId"].(string)
	wantStart, _ := parseTime(a["windowStart"])
	wantEnd, _ := parseTime(a["windowEnd"])
	wantVersion, _ := intOf(a["version"])
	return eventually(func() error {
		if err := w.doJSON(http.MethodGet, fmt.Sprintf("%s/appointments/%s", inboundBaseURL, id), nil); err != nil {
			return err
		}
		if w.last.status != http.StatusOK {
			return fmt.Errorf("GET inbound-receiving appointment %s = %d (%s)", id, w.last.status, w.last.body)
		}
		c := w.last.json()
		gs, _ := parseTime(c["windowStart"])
		ge, _ := parseTime(c["windowEnd"])
		gv, _ := intOf(c["version"])
		asns, _ := c["asnNumbers"].([]any)
		if c["doorCode"] != a["doorCode"] || !gs.Equal(wantStart) || !ge.Equal(wantEnd) || gv != wantVersion ||
			len(asns) != 1 || asns[0] != asn {
			return fmt.Errorf("inbound-receiving's copy = %s, want door %v window %s..%s version %d asn %s",
				w.last.body, a["doorCode"], wantStart.Format(time.RFC3339), wantEnd.Format(time.RFC3339), wantVersion, asn)
		}
		return nil
	})
}

// copyNeverAppears proves an appointment is absent from the copy for the
// whole absence window (an OUTBOUND appointment is never copied, ADR 0019).
func (w *world) copyNeverAppears(key string) error {
	key = w.rs(key)
	id, err := w.appointmentIDFor(key)
	if err != nil {
		return err
	}
	deadline := time.Now().Add(kafkaAbsenceWindow)
	for time.Now().Before(deadline) {
		if err := w.doJSON(http.MethodGet, fmt.Sprintf("%s/appointments/%s", inboundBaseURL, id), nil); err != nil {
			return err
		}
		if w.last.status != http.StatusNotFound {
			return fmt.Errorf("inbound-receiving's copy of the outbound appointment %s answered %d, want 404 (body: %s)", id, w.last.status, w.last.body)
		}
		time.Sleep(eventualWaitPoll)
	}
	return nil
}

// openReceiptRefused tries to open the ASN's receipt from the appointment of
// `key` and requires the given problem.
func (w *world) openReceiptRefused(asn, key string, status int, slug string) error {
	asn, key = w.rs(asn), w.rs(key)
	id, err := w.appointmentIDFor(key)
	if err != nil {
		return err
	}
	if err := w.doJSON(http.MethodPost, inboundBaseURL+"/receipts", map[string]any{"asnNumber": asn, "appointmentId": id}); err != nil {
		return err
	}
	return w.problemIs(status, slug)
}

// ---------------------------------------------------------------------
// yard-management: gate, dock, door work, departure
// ---------------------------------------------------------------------

// yardKnowsDoor waits until yard-management's board lists the door: its
// check-in sends the trailer to a parking spot, not the appointment door, when
// the door has not reached its facility copy yet, and that is not refused.
func (w *world) yardKnowsDoor(site, door string) error {
	return eventually(func() error {
		if err := w.doJSON(http.MethodGet, fmt.Sprintf("%s/sites/%s/yard-board", yardBaseURL, site), nil); err != nil {
			return err
		}
		if w.last.status != http.StatusOK {
			return fmt.Errorf("GET yard-board of %s = %d (%s)", site, w.last.status, w.last.body)
		}
		doors, _ := w.last.json()["doors"].([]any)
		for _, d := range doors {
			if d.(map[string]any)["code"] == door {
				return nil
			}
		}
		return fmt.Errorf("door %s is not on yard-management's board of %s yet (%d doors)", door, site, len(doors))
	})
}

func tail(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[len(s)-n:]
}

// carrierChecksInAtGate records the carrier's arrival at the gate: one
// trailer, the seal the booking declared, naming the appointment. The visit
// must go straight to the appointment door as a VISITING_DRIVER move.
func (w *world) carrierChecksInAtGate(asn string) error {
	asn = w.rs(asn)
	a, err := w.dockAppointment(asn)
	if err != nil {
		return err
	}
	id, _ := a["appointmentId"].(string)
	door, _ := a["doorCode"].(string)
	site := siteOfDoor(door)
	if err := w.yardKnowsDoor(site, door); err != nil {
		return err
	}
	tag := tail(asn, 20)
	// yard-management learns the appointment from dock-scheduling over Kafka: a
	// check-in naming one it has not consumed yet is refused 422 and persists
	// nothing, so retrying is what a gate clerk would do.
	if err := w.retryWhileProblem(func() error {
		return w.doJSON(http.MethodPost, yardBaseURL+"/visits", map[string]any{
			"siteId": site, "carrier": "ACME Freight", "carrierScac": "ACMF",
			"tractorPlate": "TRK-" + tag, "driverRef": "drv-" + tag, "purpose": "LIVE_UNLOAD",
			"appointmentId": id,
			"trailers": []map[string]any{{"unitNumber": "TR-" + tag, "kind": "DRY_VAN", "loadState": "LOADED",
				"sealNumber": expectedSeal(asn)}},
		})
	}, "unknown-appointment"); err != nil {
		return err
	}
	if w.last.status != http.StatusCreated {
		return fmt.Errorf("gate check-in of %s: status %d, want 201 (body: %s)", id, w.last.status, w.last.body)
	}
	got := w.last.json()
	visit, _ := got["visit"].(map[string]any)
	trailers, _ := got["trailers"].([]any)
	moves, _ := got["moves"].([]any)
	if visit["state"] != "CHECKED_IN" || visit["appointmentId"] != id || visit["siteId"] != site || len(trailers) != 1 || len(moves) != 1 {
		return fmt.Errorf("gate check-in of %s = %s, want a CHECKED_IN visit with 1 trailer and 1 move", id, w.last.body)
	}
	mv := moves[0].(map[string]any)
	to, _ := mv["to"].(map[string]any)
	if mv["performer"] != "VISITING_DRIVER" || mv["reason"] != "TO_DOOR" || mv["state"] != "CLAIMED" ||
		to["kind"] != "DOOR" || to["code"] != door || mv["appointmentId"] != id {
		return fmt.Errorf("gate check-in move = %v, want a CLAIMED VISITING_DRIVER TO_DOOR move to door %s for %s", mv, door, id)
	}
	// SealCheckAtGate: the seal found equals the one the booking declared.
	tr := trailers[0].(map[string]any)
	seal, _ := tr["seal"].(map[string]any)
	if seal["number"] != expectedSeal(asn) {
		return fmt.Errorf("trailer seal = %v, want %s", seal, expectedSeal(asn))
	}
	arrived, err := parseTime(visit["arrivedAt"])
	if err != nil {
		return fmt.Errorf("visit arrivedAt: %w", err)
	}
	w.dock().checkIn[asn] = got
	w.dock().arrived[asn] = arrived
	return nil
}

func (w *world) checkInOf(asn string) (visit, trailer, move map[string]any, err error) {
	got, ok := w.dock().checkIn[asn]
	if !ok {
		return nil, nil, nil, fmt.Errorf("no gate check-in has been recorded for ASN %s in this process", asn)
	}
	visit, _ = got["visit"].(map[string]any)
	trailer = got["trailers"].([]any)[0].(map[string]any)
	move = got["moves"].([]any)[0].(map[string]any)
	return visit, trailer, move, nil
}

func (w *world) readTrailer(id string) (map[string]any, error) {
	if err := w.doJSON(http.MethodGet, yardBaseURL+"/trailers/"+id, nil); err != nil {
		return nil, err
	}
	if w.last.status != http.StatusOK {
		return nil, fmt.Errorf("GET trailer %s = %d (%s)", id, w.last.status, w.last.body)
	}
	return w.last.json(), nil
}

// visitCheckedInPublished requires yard-management's VisitCheckedIn on its
// integration topic to carry the appointment, the site and the SAME arrival
// instant the REST visit reports: inbound-receiving's dock-to-stock KPI starts
// from it (inbound-receiving ADR 0021).
func (w *world) visitCheckedInPublished(asn string) error {
	asn = w.rs(asn)
	visit, trailer, _, err := w.checkInOf(asn)
	if err != nil {
		return err
	}
	a, _ := w.dockAppointment(asn)
	apptID, _ := a["appointmentId"].(string)
	p, err := expectKafkaEvent(topicYardEvents, typeVisitCheckedIn,
		func(p map[string]any) bool { return p["appointment_id"] == apptID }, "appointment "+apptID)
	if err != nil {
		return err
	}
	at, err := parseTime(p["arrived_at"])
	if err != nil || !at.Equal(w.dock().arrived[asn]) {
		return fmt.Errorf("VisitCheckedIn arrived_at = %v, want the visit's %s (payload: %v)", p["arrived_at"], w.dock().arrived[asn].Format(time.RFC3339Nano), p)
	}
	ts, _ := p["trailers"].([]any)
	if p["visit_id"] != visit["visitId"] || p["site_id"] != siteOfDoor(a["doorCode"].(string)) || p["purpose"] != "LIVE_UNLOAD" || len(ts) != 1 ||
		ts[0].(map[string]any)["trailer_id"] != trailer["trailerId"] {
		return fmt.Errorf("VisitCheckedIn payload = %v, want visit %v at the door's site, LIVE_UNLOAD, 1 trailer %v", p, visit["visitId"], trailer["trailerId"])
	}
	return nil
}

// trailerIsDocked completes the visiting driver's move to the door: the
// trailer is DOCKED on exactly that door with its door work WAITING, and
// TrailerDocked names the door, the appointment and the ASN.
func (w *world) trailerIsDocked(asn string) error {
	asn = w.rs(asn)
	visit, trailer, move, err := w.checkInOf(asn)
	if err != nil {
		return err
	}
	a, _ := w.dockAppointment(asn)
	door, _ := a["doorCode"].(string)
	if err := w.expectOK2xx(w.doJSON(http.MethodPost, fmt.Sprintf("%s/moves/%s/completions", yardBaseURL, move["moveId"]), map[string]any{})); err != nil {
		return fmt.Errorf("complete move %v: %w", move["moveId"], err)
	}
	if mv := w.last.json(); w.last.status != http.StatusOK || mv["state"] != "COMPLETED" {
		return fmt.Errorf("complete move: status %d state %v (body: %s)", w.last.status, mv["state"], w.last.body)
	}
	got, err := w.readTrailer(trailer["trailerId"].(string))
	if err != nil {
		return err
	}
	loc, _ := got["location"].(map[string]any)
	if got["state"] != "DOCKED" || loc["kind"] != "DOOR" || loc["code"] != door || got["doorWork"] != "WAITING" {
		return fmt.Errorf("trailer after the dock move = %s, want DOCKED at door %s with door work WAITING", w.last.body, door)
	}
	p, err := expectKafkaEvent(topicYardEvents, typeTrailerDocked,
		func(p map[string]any) bool { return p["trailer_id"] == trailer["trailerId"] }, "trailer "+fmt.Sprint(trailer["trailerId"]))
	if err != nil {
		return err
	}
	asns, _ := p["asn_numbers"].([]any)
	if p["door_code"] != door || p["appointment_id"] != a["appointmentId"] || p["visit_id"] != visit["visitId"] ||
		len(asns) != 1 || asns[0] != asn {
		return fmt.Errorf("TrailerDocked = %v, want door %s, appointment %v, visit %v, ASN [%s]", p, door, a["appointmentId"], visit["visitId"], asn)
	}
	return nil
}

func (w *world) doorWorkStarted(asn string) error {
	asn = w.rs(asn)
	_, trailer, _, err := w.checkInOf(asn)
	if err != nil {
		return err
	}
	if err := w.expectOK2xx(w.doJSON(http.MethodPost, fmt.Sprintf("%s/trailers/%s/door-work-starts", yardBaseURL, trailer["trailerId"]), nil)); err != nil {
		return fmt.Errorf("start door work: %w", err)
	}
	if got := w.last.json(); w.last.status != http.StatusOK || got["doorWork"] != "IN_PROGRESS" {
		return fmt.Errorf("door work after the start = %s, want IN_PROGRESS", w.last.body)
	}
	return nil
}

func (w *world) checkOut(asn string) error {
	visit, trailer, _, err := w.checkInOf(asn)
	if err != nil {
		return err
	}
	return w.doJSON(http.MethodPost, fmt.Sprintf("%s/visits/%s/check-outs", yardBaseURL, visit["visitId"]),
		map[string]any{"trailersOut": []map[string]any{{"trailerId": trailer["trailerId"]}}})
}

// checkOutRefusedWhileUnloading proves the trailer is NOT released while its
// door work is IN_PROGRESS: only the receipt's close may release it.
func (w *world) checkOutRefusedWhileUnloading(asn string) error {
	if err := w.checkOut(w.rs(asn)); err != nil {
		return err
	}
	return w.problemIs(http.StatusConflict, "trailer-not-released")
}

// yardCompletesUnload waits for yard-management's AutoCompleteUnload after
// inbound-receiving's ReceiptClosed: door work DONE, load state EMPTY.
func (w *world) yardCompletesUnload(asn string) error {
	asn = w.rs(asn)
	a, _ := w.dockAppointment(asn)
	_, trailer, _, err := w.checkInOf(asn)
	if err != nil {
		return err
	}
	tid, _ := trailer["trailerId"].(string)
	if err := eventually(func() error {
		got, err := w.readTrailer(tid)
		if err != nil {
			return err
		}
		if got["doorWork"] != "DONE" || got["loadState"] != "EMPTY" || got["state"] != "DOCKED" {
			return fmt.Errorf("trailer %s = %s, want DOCKED, door work DONE, load EMPTY", tid, w.last.body)
		}
		return nil
	}); err != nil {
		return err
	}
	p, err := expectKafkaEvent(topicYardEvents, typeDoorWorkCompleted,
		func(p map[string]any) bool { return p["trailer_id"] == tid }, "trailer "+tid)
	if err != nil {
		return err
	}
	if p["door_code"] != a["doorCode"] || p["appointment_id"] != a["appointmentId"] || p["load_state"] != "EMPTY" {
		return fmt.Errorf("DoorWorkCompleted = %v, want door %v, appointment %v, EMPTY", p, a["doorCode"], a["appointmentId"])
	}
	return nil
}

// visitChecksOutAndDoorIsReleased checks the visit out with its trailer and
// requires the trailer DEPARTED and the door free on the yard board.
func (w *world) visitChecksOutAndDoorIsReleased(asn string) error {
	asn = w.rs(asn)
	a, _ := w.dockAppointment(asn)
	door, _ := a["doorCode"].(string)
	visit, trailer, _, err := w.checkInOf(asn)
	if err != nil {
		return err
	}
	if err := w.expectOK2xx(w.checkOut(asn)); err != nil {
		return fmt.Errorf("check out visit %v: %w", visit["visitId"], err)
	}
	if got := w.last.json(); w.last.status != http.StatusOK || got["state"] != "CHECKED_OUT" {
		return fmt.Errorf("check-out answered %d state %v, want 200 CHECKED_OUT (body: %s)", w.last.status, got["state"], w.last.body)
	}
	tr, err := w.readTrailer(trailer["trailerId"].(string))
	if err != nil {
		return err
	}
	if tr["state"] != "DEPARTED" {
		return fmt.Errorf("trailer after the check-out = %s, want DEPARTED", w.last.body)
	}
	return eventually(func() error {
		if err := w.doJSON(http.MethodGet, fmt.Sprintf("%s/sites/%s/yard-board", yardBaseURL, siteOfDoor(door)), nil); err != nil {
			return err
		}
		doors, _ := w.last.json()["doors"].([]any)
		for _, d := range doors {
			dm := d.(map[string]any)
			if dm["code"] == door {
				if dm["trailer"] != nil {
					return fmt.Errorf("door %s still holds a trailer: %v", door, dm["trailer"])
				}
				return nil
			}
		}
		return fmt.Errorf("door %s is not on the yard board", door)
	})
}

// noSealException proves the matching seal raised no SealExceptionRaised.
func (w *world) noSealException(asn string) error {
	asn = w.rs(asn)
	a, _ := w.dockAppointment(asn)
	return expectNoKafkaEvent(topicYardEvents, typeSealExceptionRaised,
		func(p map[string]any) bool { return p["appointment_id"] == a["appointmentId"] }, "appointment "+fmt.Sprint(a["appointmentId"]))
}

// ---------------------------------------------------------------------
// inbound-receiving: dock-to-stock (ADR 0021)
// ---------------------------------------------------------------------

// receiptStowCompleted requires ReceiptStowCompleted to carry exactly the
// facts ADR 0021 defines: the yard arrival, the LATEST ContainerStowed `at`
// of the receipt's cartons, their difference in whole seconds, the carton
// count and the appointment, and to be raised exactly once.
func (w *world) receiptStowCompleted(asn string, cartons int) error {
	asn = w.rs(asn)
	rec, err := w.readReceipt(asn)
	if err != nil {
		return err
	}
	rid, _ := rec["receiptId"].(string)
	a, _ := w.dockAppointment(asn)
	lpns := map[string]bool{}
	for _, s := range w.mcs().ssccs[asn] {
		lpns[s] = true
	}
	if len(lpns) != cartons {
		return fmt.Errorf("ASN %s has %d cartons, the step says %d", asn, len(lpns), cartons)
	}
	// The stow instants, from inventory-storage's own ContainerStowed.
	stowed := map[string]time.Time{}
	if _, _, err := scanTopic(topicInventoryEvents, kafkaScanWindow, func(m ceMessage) bool {
		if m.Type != typeContainerStowed {
			return false
		}
		p := m.payload()
		if lpn, _ := p["lpn"].(string); lpns[lpn] {
			if at, err := parseTime(p["at"]); err == nil && at.After(stowed[lpn]) {
				stowed[lpn] = at
			}
		}
		return len(stowed) == cartons
	}); err != nil {
		return err
	}
	if len(stowed) != cartons {
		return fmt.Errorf("inventory-storage published ContainerStowed for %d of the %d cartons of %s", len(stowed), cartons, asn)
	}
	var last time.Time
	for _, at := range stowed {
		if at.After(last) {
			last = at
		}
	}
	p, err := expectKafkaEvent(topicInboundEvents, typeReceiptStowDone,
		func(p map[string]any) bool { return p["receipt_id"] == rid }, "receipt "+rid)
	if err != nil {
		return err
	}
	arrivedAt, _ := parseTime(p["arrived_at"])
	stowedAt, _ := parseTime(p["stowed_at"])
	closedAt, _ := parseTime(p["closed_at"])
	wantClosed, _ := parseTime(rec["closedAt"])
	secs, _ := intOf(p["dock_to_stock_seconds"])
	n, _ := intOf(p["carton_count"])
	if p["asn_number"] != asn || p["appointment_id"] != a["appointmentId"] || n != cartons {
		return fmt.Errorf("ReceiptStowCompleted = %v, want ASN %s, appointment %v, %d cartons", p, asn, a["appointmentId"], cartons)
	}
	if !arrivedAt.Equal(w.dock().arrived[asn]) {
		return fmt.Errorf("ReceiptStowCompleted arrived_at = %v, want the gate arrival %s", p["arrived_at"], w.dock().arrived[asn].Format(time.RFC3339Nano))
	}
	if !stowedAt.Equal(last) {
		return fmt.Errorf("ReceiptStowCompleted stowed_at = %v, want the latest ContainerStowed %s", p["stowed_at"], last.Format(time.RFC3339Nano))
	}
	if !closedAt.Equal(wantClosed) {
		return fmt.Errorf("ReceiptStowCompleted closed_at = %v, want the receipt's closedAt %v", p["closed_at"], rec["closedAt"])
	}
	if want := int(last.Sub(arrivedAt) / time.Second); secs != want || secs <= 0 {
		return fmt.Errorf("dock_to_stock_seconds = %d, want stowed_at - arrived_at = %d (> 0)", secs, want)
	}
	// Exactly once, whatever order the facts arrived in.
	count := 0
	if _, _, err := scanTopic(topicInboundEvents, kafkaAbsenceWindow, func(m ceMessage) bool {
		if m.Type == typeReceiptStowDone && m.payload()["receipt_id"] == rid {
			count++
		}
		return false
	}); err != nil {
		return err
	}
	if count != 1 {
		return fmt.Errorf("ReceiptStowCompleted was raised %d times for receipt %s, want exactly 1", count, rid)
	}
	return nil
}

// ---------------------------------------------------------------------
// registration
// ---------------------------------------------------------------------

func registerDockChainSteps(sc *godog.ScenarioContext, w *world) {
	sc.Step(`^dock-scheduling, yard-management and facility-layout are healthy$`, w.dockChainServicesAreHealthy)
	sc.Step(`^an? (Inbound|Outbound) dock door "([^"]*)" exists in facility-layout$`, w.dockDoorExists)
	sc.Step(`^the dock calendar of site "([^"]*)" is declared in dock-scheduling with `+fmt.Sprint(dockSlotMinutes)+` minute slots and `+fmt.Sprint(dockToleranceMinutes)+` minutes of arrival tolerance$`, w.dockCalendarDeclared)

	sc.Step(`^a dock appointment at door "([^"]*)" for carrier "([^"]*)" and ASN "([^"]*)" is booked in dock-scheduling to start in (\d+) minutes for (\d+) minutes$`, w.dockBooked)
	sc.Step(`^booking a dock appointment at door "([^"]*)" for ASN "([^"]*)" in dock-scheduling to start in (\d+) minutes for (\d+) minutes is refused with (\d+) "([^"]*)"$`, w.dockBookingRefused)
	sc.Step(`^an outbound dock appointment for shipment "([^"]*)" at door "([^"]*)" for carrier "([^"]*)" is booked in dock-scheduling to start in (\d+) minutes for (\d+) minutes$`, w.dockOutboundBooked)
	sc.Step(`^the appointment of ASN "([^"]*)" is rescheduled in dock-scheduling to door "([^"]*)" starting in (\d+) minutes for (\d+) minutes$`, w.dockRescheduled)
	sc.Step(`^the appointment of ASN "([^"]*)" is cancelled in dock-scheduling$`, w.dockCancelled)
	sc.Step(`^dock-scheduling eventually shows the appointment of ASN "([^"]*)" as (BOOKED|CHECKED_IN|COMPLETED|CANCELLED)$`, w.dockStateIs)

	sc.Step(`^inbound-receiving's copy of the appointment of ASN "([^"]*)" is eventually "([^"]*)"$`, w.copyIsInState)
	sc.Step(`^inbound-receiving's copy of the appointment of ASN "([^"]*)" is still "([^"]*)"$`, w.copyIsStill)
	sc.Step(`^dock-scheduling still shows the appointment of ASN "([^"]*)" as (CHECKED_IN)$`, w.dockStateIsNow)
	sc.Step(`^inbound-receiving's copy of the appointment of ASN "([^"]*)" eventually mirrors dock-scheduling's door, window, ASN and version$`, w.copyMirrors)
	sc.Step(`^inbound-receiving never holds a copy of the outbound appointment for shipment "([^"]*)"$`, w.copyNeverAppears)
	sc.Step(`^opening the receipt of ASN "([^"]*)" in inbound-receiving from the appointment of "([^"]*)" is refused with (\d+) "([^"]*)"$`, w.openReceiptRefused)

	sc.Step(`^the carrier of ASN "([^"]*)" checks in at the gate of yard-management$`, w.carrierChecksInAtGate)
	sc.Step(`^yard-management published VisitCheckedIn for the appointment of ASN "([^"]*)" with the arrival of the visit$`, w.visitCheckedInPublished)
	sc.Step(`^the gate clerk docks the trailer of ASN "([^"]*)" at its appointment door in yard-management$`, w.trailerIsDocked)
	sc.Step(`^door work starts on the trailer of ASN "([^"]*)" in yard-management$`, w.doorWorkStarted)
	sc.Step(`^yard-management refuses to check out the visit of ASN "([^"]*)" while its trailer is still being unloaded$`, w.checkOutRefusedWhileUnloading)
	sc.Step(`^yard-management eventually completes the unload of the trailer of ASN "([^"]*)" after the receipt closed$`, w.yardCompletesUnload)
	sc.Step(`^the visit of ASN "([^"]*)" checks out in yard-management and its door is released$`, w.visitChecksOutAndDoorIsReleased)
	sc.Step(`^yard-management raised no seal exception for the appointment of ASN "([^"]*)"$`, w.noSealException)

	sc.Step(`^inbound-receiving published ReceiptStowCompleted for ASN "([^"]*)" with the gate arrival, the last of its (\d+) stows and the exact dock-to-stock seconds$`, w.receiptStowCompleted)
}
