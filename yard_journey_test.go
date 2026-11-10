package main

// Step definitions for features/yard_journey.feature: the yard journey across
// facility-layout, inbound-receiving, dock-scheduling and yard-management.
//
// Everything is black-box over the wire and nothing is injected: every fact
// travels as a real CloudEvent between real processes, which is the point. The
// contracts under test are
//
//	facility-layout   -> dock-scheduling, yard-management (zones, doors, spots)
//	inbound-receiving -> dock-scheduling (ASN), yard-management (ASN, receipts)
//	dock-scheduling   -> yard-management (appointment), inbound-receiving (copy)
//	yard-management   -> dock-scheduling (arrival, door release, turn-away)
//
// REST is the main surface. Kafka is READ (never written) in exactly one place:
// the integration topic of yard-management, to prove which event types reach
// it (see yardTopicCarries).

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/cucumber/godog"
	"github.com/segmentio/kafka-go"
)

var (
	dockSchedulingBaseURL = envOrDefault("DOCK_SCHEDULING_BASE_URL", "http://localhost:8112")
	yardBaseURL           = envOrDefault("YARD_BASE_URL", "http://localhost:8113")
	outboundBaseURL       = envOrDefault("OUTBOUND_SHIPPING_BASE_URL", "http://localhost:8115")
)

const (
	topicYardEvents        = "warehouse.yard-management.events"
	topicOutboundEvents    = "warehouse.outbound-shipping.events"
	topicProcessPathEvents = "warehouse.process-path-management.events"

	typeCPTScheduleChanged = "com.warehouse.wes.process-path-management.cptschedule.CPTScheduleChanged"
	typeShipmentConfirmed  = "com.warehouse.wms.outbound-shipping.outboundload.ShipmentConfirmed"

	yardTypePrefix = "com.warehouse.wms.yard-management."

	// Two location types shared by every run (reference data: registering one
	// again answers 409, which the setup steps tolerate). Their ROLE is what the
	// consumers key on: a Dock slot is a door, a Yard slot is a spot.
	yardSpotLocationType = "E2E-YardSpot"
	yardDoorLocationType = "E2E-DockDoor"
	// The staging lane the outbound leg stages packages at (role Staging, which
	// facility-layout requires a capacity for).
	yardStagingLocationType = "E2E-Staging"
)

// yardEventTypes maps the short event name a feature file uses to the exact
// CloudEvents type of yard-management's catalogue (its ADR 0004): the entity
// segment differs per aggregate, so a typo or a renamed type fails loudly.
var yardEventTypes = map[string]string{
	"VisitCheckedIn":       yardTypePrefix + "yardvisit.VisitCheckedIn",
	"VisitRejected":        yardTypePrefix + "yardvisit.VisitRejected",
	"TrailerDocked":        yardTypePrefix + "trailer.TrailerDocked",
	"TrailerUndocked":      yardTypePrefix + "trailer.TrailerUndocked",
	"DoorWorkStarted":      yardTypePrefix + "trailer.DoorWorkStarted",
	"DoorWorkCompleted":    yardTypePrefix + "trailer.DoorWorkCompleted",
	"TrailerDeparted":      yardTypePrefix + "trailer.TrailerDeparted",
	"SealExceptionRaised":  yardTypePrefix + "trailer.SealExceptionRaised",
	"YardMoveRequested":    yardTypePrefix + "yardmove.YardMoveRequested",
	"TrailerDwellExceeded": yardTypePrefix + "trailer.TrailerDwellExceeded",
}

// yardSite is the layout one scenario registered in facility-layout.
type yardSite struct {
	code    string
	door    string
	parking []string
	holding string
	staging string // facility-layout Staging slot; empty when the site has no lane
}

func (s *yardSite) spots() []string {
	out := append([]string{}, s.parking...)
	out = append(out, s.holding)
	sort.Strings(out)
	return out
}

// yardJourneyState remembers the ids the services minted. The world is shared
// by every scenario of a process, so every key carries a run-scoped token and
// scenarios never collide.
type yardJourneyState struct {
	sites    map[string]*yardSite // site code -> layout
	appts    map[string]string    // appointment label -> dock-scheduling id
	apptDoor map[string]string    // appointment label -> its door code
	visits   map[string]string    // tractor plate -> visit id
	trailers map[string]string    // unit number -> trailer id
	moves    map[string]string    // unit number -> move id of its check-in
	sealOf   map[string]string    // unit number -> seal it arrived with
	loads    map[string]string    // site code -> outbound-shipping load id
	siteOf   map[string]string    // unit number or plate -> site code
}

func (w *world) yj() *yardJourneyState {
	if w.yard == nil {
		w.yard = &yardJourneyState{
			sites: map[string]*yardSite{}, appts: map[string]string{}, apptDoor: map[string]string{},
			visits: map[string]string{}, trailers: map[string]string{}, moves: map[string]string{},
			sealOf: map[string]string{}, siteOf: map[string]string{}, loads: map[string]string{},
		}
	}
	return w.yard
}

func (w *world) yardSiteOf(code string) (*yardSite, error) {
	s, ok := w.yj().sites[code]
	if !ok {
		return nil, fmt.Errorf("site %s was not registered by this process", code)
	}
	return s, nil
}

func (w *world) trailerID(unit string) (string, error) {
	id, ok := w.yj().trailers[unit]
	if !ok {
		return "", fmt.Errorf("no trailer %s has been admitted at the gate in this process", unit)
	}
	return id, nil
}

// ---------------------------------------------------------------------
// Background
// ---------------------------------------------------------------------

func (w *world) yardJourneyServicesAreHealthy() error {
	for name, base := range map[string]string{
		"facility-layout":   facilityBaseURL,
		"product-master":    productMasterBaseURL,
		"inbound-receiving": inboundBaseURL,
		"dock-scheduling":   dockSchedulingBaseURL,
		"yard-management":   yardBaseURL,
	} {
		if err := w.doJSON(http.MethodGet, base+"/healthz", nil); err != nil {
			return fmt.Errorf("%s not reachable: %w", name, err)
		}
		if w.last.status < 200 || w.last.status >= 300 {
			return fmt.Errorf("%s /healthz returned %d, body=%s", name, w.last.status, w.last.body)
		}
	}
	return nil
}

// ---------------------------------------------------------------------
// facility-layout: the site, its door and its spots
// ---------------------------------------------------------------------

// siteIsRegistered registers a run-scoped site with ONE dock door (Inbound or
// Outbound), an optional Staging lane (the outbound leg) and
// the given yard spots in facility-layout (zones first, then slots), and
// remembers its codes. yard-management and dock-scheduling learn all of it ONLY
// from warehouse.facility.events. The site is run-scoped because the yard board
// and the yard policy are per site: a shared site would carry the trailers and
// the policy of an earlier run.
func (w *world) siteIsRegistered(site, flow, parkingBays, holdingBay, lane string) error {
	site = w.rs(site)
	doorZone, spotZone := site+"-DOCK-DR", site+"-YARD-PRK"
	st := &yardSite{code: site, door: doorZone + "-A01-01-01-A"}
	for _, bay := range strings.Split(parkingBays, ",") {
		st.parking = append(st.parking, fmt.Sprintf("%s-A01-%s-01-A", spotZone, strings.TrimSpace(bay)))
	}
	st.holding = fmt.Sprintf("%s-A01-%s-01-A", spotZone, holdingBay)

	steps := []struct {
		what string
		call func() error
	}{
		{"site", func() error {
			return w.expectOKOr409(w.doJSON(http.MethodPost, facilityBaseURL+"/sites", map[string]any{"siteCode": site, "name": "Yard journey " + site}))
		}},
		{"yard spot location type", func() error {
			return w.expectOKOr409(w.doJSON(http.MethodPost, facilityBaseURL+"/location-types", map[string]any{"name": yardSpotLocationType, "role": "Yard"}))
		}},
		{"dock door location type", func() error {
			return w.expectOKOr409(w.doJSON(http.MethodPost, facilityBaseURL+"/location-types", map[string]any{"name": yardDoorLocationType, "role": "Dock"}))
		}},
		{"dock zone", func() error {
			return w.expectOKOr409(w.doJSON(http.MethodPost, fmt.Sprintf("%s/sites/%s/zones", facilityBaseURL, site),
				map[string]any{"areaCode": "DOCK", "zoneCode": "DR", "temperatureClass": "Ambient"}))
		}},
		{"yard zone", func() error {
			return w.expectOKOr409(w.doJSON(http.MethodPost, fmt.Sprintf("%s/sites/%s/zones", facilityBaseURL, site),
				map[string]any{"areaCode": "YARD", "zoneCode": "PRK", "temperatureClass": "Ambient"}))
		}},
	}
	for _, zone := range []string{doorZone, spotZone} {
		zone := zone
		steps = append(steps, struct {
			what string
			call func() error
		}{"aisle of " + zone, func() error {
			return w.expectOKOr409(w.doJSON(http.MethodPost, fmt.Sprintf("%s/zones/%s/aisles", facilityBaseURL, zone),
				map[string]any{"aisleCode": "A01", "sequenceHint": 1, "direction": "TwoWay"}))
		}})
	}
	// Slots only after their zones: the consumers resolve a slot's site through
	// its zone, and a slot that arrives first is held pending.
	steps = append(steps, struct {
		what string
		call func() error
	}{"dock door", func() error {
		return w.expectOKOr409(w.doJSON(http.MethodPost, facilityBaseURL+"/locations",
			map[string]any{"locationCode": st.door, "locationType": yardDoorLocationType, "dockFlow": flow}))
	}})
	if lane != "" {
		stagingZone := site + "-STAG-LN"
		st.staging = stagingZone + "-A01-01-01-A"
		steps = append(steps, []struct {
			what string
			call func() error
		}{
			{"staging location type", func() error {
				return w.expectOKOr409(w.doJSON(http.MethodPost, facilityBaseURL+"/location-types", map[string]any{
					"name": yardStagingLocationType, "role": "Staging", "defaultCapacity": map[string]any{"maxWeightKg": 1000, "maxVolumeM3": 2},
				}))
			}},
			{"staging zone", func() error {
				return w.expectOKOr409(w.doJSON(http.MethodPost, fmt.Sprintf("%s/sites/%s/zones", facilityBaseURL, site),
					map[string]any{"areaCode": "STAG", "zoneCode": "LN", "temperatureClass": "Ambient"}))
			}},
			{"staging aisle", func() error {
				return w.expectOKOr409(w.doJSON(http.MethodPost, fmt.Sprintf("%s/zones/%s/aisles", facilityBaseURL, stagingZone),
					map[string]any{"aisleCode": "A01", "sequenceHint": 1, "direction": "TwoWay"}))
			}},
			{"staging lane", func() error {
				return w.expectOKOr409(w.doJSON(http.MethodPost, facilityBaseURL+"/locations",
					map[string]any{"locationCode": st.staging, "locationType": yardStagingLocationType}))
			}},
		}...)
	}
	for _, spot := range append(append([]string{}, st.parking...), st.holding) {
		spot := spot
		steps = append(steps, struct {
			what string
			call func() error
		}{"yard spot " + spot, func() error {
			return w.expectOKOr409(w.doJSON(http.MethodPost, facilityBaseURL+"/locations",
				map[string]any{"locationCode": spot, "locationType": yardSpotLocationType}))
		}})
	}
	for _, s := range steps {
		if err := s.call(); err != nil {
			return fmt.Errorf("register %s in facility-layout: %w", s.what, err)
		}
	}
	w.yj().sites[site] = st
	return nil
}

// yardKnowsLayout proves the layout reached yard-management through
// warehouse.facility.events alone: the yard board lists exactly the door and
// the spots facility-layout registered (and nothing else, the site is ours).
func (w *world) yardKnowsLayout(site string) error {
	site = w.rs(site)
	st, err := w.yardSiteOf(site)
	if err != nil {
		return err
	}
	return eventually(func() error {
		board, err := w.yardBoard(site)
		if err != nil {
			return err
		}
		var doors, spots []string
		for _, d := range asList(board["doors"]) {
			doors = append(doors, fmt.Sprint(asMap(d)["code"]))
		}
		for _, s := range asList(board["spots"]) {
			spots = append(spots, fmt.Sprint(asMap(s)["code"]))
		}
		sort.Strings(doors)
		sort.Strings(spots)
		if strings.Join(doors, ",") != st.door || strings.Join(spots, ",") != strings.Join(st.spots(), ",") {
			return fmt.Errorf("yard board of %s lists doors %v and spots %v, want door [%s] and spots %v", site, doors, spots, st.door, st.spots())
		}
		return nil
	})
}

func (w *world) yardBoard(site string) (map[string]any, error) {
	if err := w.doJSON(http.MethodGet, fmt.Sprintf("%s/sites/%s/yard-board", yardBaseURL, site), nil); err != nil {
		return nil, err
	}
	if w.last.status != http.StatusOK {
		return nil, fmt.Errorf("GET yard-board of %s: status %d (body: %s)", site, w.last.status, w.last.body)
	}
	return w.last.json(), nil
}

func asMap(v any) map[string]any {
	m, _ := v.(map[string]any)
	if m == nil {
		return map[string]any{}
	}
	return m
}

func asList(v any) []any {
	l, _ := v.([]any)
	return l
}

// ---------------------------------------------------------------------
// dock-scheduling: calendar and appointments
// ---------------------------------------------------------------------

// calendarAllowsEarlyArrival declares a 5-minute slot grid and a 30-minute
// arrival tolerance for the site. The grid lets a booking start a few minutes
// from now (the default 15-minute grid could put the window up to a quarter of
// an hour away), and the tolerance rides on DockAppointmentBooked into
// yard-management's appointment copy: a carrier who arrives inside it is sent
// straight to the door instead of to a holding spot (yard ADR 0003, A17).
func (w *world) calendarAllowsEarlyArrival(site string) error {
	site = w.rs(site)
	return eventually(func() error {
		if err := w.doJSON(http.MethodPut, fmt.Sprintf("%s/sites/%s/dock-calendar/settings", dockSchedulingBaseURL, site), map[string]any{
			"maxWindowMinutes": 240, "slotGranularityMinutes": 5, "arrivalToleranceMinutes": 30, "noShowGraceMinutes": 60,
		}); err != nil {
			return err
		}
		if w.last.status != http.StatusOK {
			return fmt.Errorf("PUT dock-calendar settings of %s: status %d (body: %s)", site, w.last.status, w.last.body)
		}
		return nil
	})
}

// windowFrom returns an RFC 3339 window starting at the next 5-minute boundary
// at least startIn minutes from now and lasting length minutes.
func windowFrom(startIn, length int) (string, string) {
	const grid = 5 * 60
	earliest := time.Now().Add(time.Duration(startIn) * time.Minute).Unix()
	start := (earliest + grid - 1) / grid * grid
	s := time.Unix(start, 0).UTC()
	return s.Format(time.RFC3339), s.Add(time.Duration(length) * time.Minute).Format(time.RFC3339)
}

// appointmentEventuallyBooked books an INBOUND appointment on the site's door
// covering the ASN.
func (w *world) appointmentEventuallyBooked(label, site, asn, seal string, startIn, length int) error {
	return w.bookDockAppointment(label, site, "INBOUND", startIn, length, map[string]any{
		"asnNumbers": []string{w.rs(asn)}, "expectedSealNumber": w.rs(seal),
	})
}

// outboundAppointmentEventuallyBooked books an OUTBOUND appointment on the
// site's door for a cutoff: dock-scheduling accepts it only for a cutoff it
// knows from the CPT schedule, and outbound-shipping takes the door of its load
// from the same cutoff id.
func (w *world) outboundAppointmentEventuallyBooked(label, site, cpt string, startIn, length int) error {
	return w.bookDockAppointment(label, site, "OUTBOUND", startIn, length, map[string]any{"cptId": w.rs(cpt)})
}

// bookDockAppointment books an appointment on the site's door. Two refusals are
// the expected transients and are retried: unknown-door (dock-scheduling has
// not consumed the facility events yet) and unknown-asn / unknown-cpt (it has
// not consumed ASNRegistered / the CPT schedule yet). Anything else is final.
func (w *world) bookDockAppointment(label, site, direction string, startIn, length int, refs map[string]any) error {
	label, site = w.rs(label), w.rs(site)
	st, err := w.yardSiteOf(site)
	if err != nil {
		return err
	}
	var final error
	err = eventually(func() error {
		start, end := windowFrom(startIn, length)
		body := map[string]any{
			"direction": direction, "doorCode": st.door, "carrier": "ACME Freight", "carrierScac": "ACME",
			"windowStart": start, "windowEnd": end,
		}
		for k, v := range refs {
			body[k] = v
		}
		if err := w.doJSON(http.MethodPost, dockSchedulingBaseURL+"/appointments", body); err != nil {
			final = err
			return nil
		}
		if w.last.status == http.StatusUnprocessableEntity {
			typ, _ := w.last.json()["type"].(string)
			for _, transient := range []string{"/unknown-asn", "/unknown-door", "/missing-outbound-ref"} {
				if strings.HasSuffix(typ, transient) {
					return fmt.Errorf("dock-scheduling does not know the %s yet (body: %s)", typ[strings.LastIndex(typ, "/")+1:], w.last.body)
				}
			}
		}
		final = w.expectOK2xx(nil)
		return nil
	})
	if err != nil {
		return err
	}
	if final != nil {
		return fmt.Errorf("book appointment %s: %w", label, final)
	}
	got := w.last.json()
	id, _ := got["appointmentId"].(string)
	if w.last.status != http.StatusCreated || id == "" || got["state"] != "BOOKED" || got["doorCode"] != st.door || got["siteId"] != site || got["direction"] != direction {
		return fmt.Errorf("book appointment %s: status %d state %v door %v site %v direction %v, want 201 BOOKED %s on %s at %s (body: %s)",
			label, w.last.status, got["state"], got["doorCode"], got["siteId"], got["direction"], direction, st.door, site, w.last.body)
	}
	w.yj().appts[label], w.yj().apptDoor[label] = id, st.door
	return nil
}

func (w *world) readAppointment(label string) (map[string]any, error) {
	label = w.rs(label)
	id, ok := w.yj().appts[label]
	if !ok {
		return nil, fmt.Errorf("no appointment %s has been booked in this process", label)
	}
	if err := w.doJSON(http.MethodGet, fmt.Sprintf("%s/appointments/%s", dockSchedulingBaseURL, id), nil); err != nil {
		return nil, err
	}
	if w.last.status != http.StatusOK {
		return nil, fmt.Errorf("GET appointment %s: status %d (body: %s)", id, w.last.status, w.last.body)
	}
	return w.last.json(), nil
}

// appointmentIs reads the appointment once. Used for the BOOKED baseline: an
// "eventually CHECKED_IN" is only evidence of the yard hop if the appointment
// provably started BOOKED.
func (w *world) appointmentIs(label, want string) error {
	got, err := w.readAppointment(label)
	if err != nil {
		return err
	}
	if got["state"] != want {
		return fmt.Errorf("dock-scheduling's appointment %s is %v, want %s (body: %s)", label, got["state"], want, w.last.body)
	}
	return nil
}

func (w *world) appointmentEventuallyIs(label, want string) error {
	return eventually(func() error { return w.appointmentIs(label, want) })
}

func (w *world) appointmentEventuallyCancelledAs(label, reason string) error {
	return eventually(func() error {
		got, err := w.readAppointment(label)
		if err != nil {
			return err
		}
		if got["state"] != "CANCELLED" || got["cancellationReason"] != reason {
			return fmt.Errorf("dock-scheduling's appointment %s is %v (reason %v), want CANCELLED as %s", label, got["state"], got["cancellationReason"], reason)
		}
		return nil
	})
}

// ---------------------------------------------------------------------
// yard-management: policy, gate, moves, door work
// ---------------------------------------------------------------------

func (w *world) yardPolicyDeclared(site string, requireAppointment bool, sealAction, holdingBay string) error {
	site = w.rs(site)
	st, err := w.yardSiteOf(site)
	if err != nil {
		return err
	}
	holding := st.holding
	if holdingBay == "" || !strings.Contains(holding, "-"+holdingBay+"-") {
		return fmt.Errorf("site %s has no holding spot at bay %q (holding spot is %s)", site, holdingBay, st.holding)
	}
	if err := w.expectOK2xx(w.doJSON(http.MethodPut, fmt.Sprintf("%s/sites/%s/yard-policy", yardBaseURL, site), map[string]any{
		"requireAppointment": requireAppointment, "sealMismatchAction": sealAction, "leaseSeconds": 300,
		"holdingSpotCodes": []string{holding},
	})); err != nil {
		return fmt.Errorf("declare yard policy of %s: %w", site, err)
	}
	got := w.last.json()
	if got["requireAppointment"] != requireAppointment || got["sealMismatchAction"] != sealAction || got["declared"] != true {
		return fmt.Errorf("yard policy of %s answered %s, want requireAppointment=%v and %s", site, w.last.body, requireAppointment, sealAction)
	}
	return nil
}

// gateChecksIn is POST /visits. It records what the gate answered but asserts
// nothing: the following Then step says whether the check-in was admitted or
// refused. The trailer's seal and the appointment are optional.
func (w *world) gateChecksIn(site, plate, purpose, unit, kind, load, appt, seal string) error {
	site, plate, unit, seal = w.rs(site), w.rs(plate), w.rs(unit), w.rs(seal)
	trailer := map[string]any{"unitNumber": unit, "kind": kind, "loadState": load}
	if seal != "" {
		trailer["sealNumber"] = seal
	}
	body := map[string]any{
		"siteId": site, "carrier": "ACME Freight", "carrierScac": "ACME", "tractorPlate": plate, "purpose": purpose,
		"trailers": []map[string]any{trailer},
	}
	if appt != "" {
		id, ok := w.yj().appts[w.rs(appt)]
		if !ok {
			return fmt.Errorf("no appointment %s has been booked in this process", appt)
		}
		body["appointmentId"] = id
	}
	// A check-in that names an appointment is refused 422 unknown-appointment
	// until yard-management's copy has consumed DockAppointmentBooked (the booking
	// answered a moment ago). That refusal is the one expected transient and is
	// retried; the last answer, whatever it is, is what the Then step judges.
	var sendErr error
	if err := eventually(func() error {
		if sendErr = w.doJSON(http.MethodPost, yardBaseURL+"/visits", body); sendErr != nil {
			return nil
		}
		if appt != "" && w.last.status == http.StatusUnprocessableEntity {
			if typ, _ := w.last.json()["type"].(string); strings.HasSuffix(typ, "/unknown-appointment") {
				return fmt.Errorf("yard-management does not know appointment %s yet (body: %s)", appt, w.last.body)
			}
		}
		return nil
	}); err != nil && sendErr == nil && w.last.status != http.StatusUnprocessableEntity {
		return err
	}
	if sendErr != nil {
		return sendErr
	}
	y := w.yj()
	y.siteOf[plate], y.siteOf[unit] = site, site
	y.sealOf[unit] = seal
	if w.last.status != http.StatusCreated {
		return nil
	}
	got := w.last.json()
	if id, _ := asMap(got["visit"])["visitId"].(string); id != "" {
		y.visits[plate] = id
	}
	if trs := asList(got["trailers"]); len(trs) == 1 {
		y.trailers[unit], _ = asMap(trs[0])["trailerId"].(string)
	}
	if mvs := asList(got["moves"]); len(mvs) == 1 {
		y.moves[unit], _ = asMap(mvs[0])["moveId"].(string)
	}
	return nil
}

// gateAdmitsWithMove asserts the last check-in was admitted and that the
// directed move is exactly the one the scenario expects: where it goes, why and
// who performs it. A walk-in with the holding spot at the highest bay code is
// the discriminator for "goes to the policy's holding spot": the default choice
// (first free spot by code) would be another spot.
func (w *world) gateAdmitsWithMove(reason, unit, performer, place, site string) error {
	unit, site = w.rs(unit), w.rs(site)
	st, err := w.yardSiteOf(site)
	if err != nil {
		return err
	}
	if w.last.status != http.StatusCreated {
		return fmt.Errorf("gate check-in of %s: status %d, want 201 (body: %s)", unit, w.last.status, w.last.body)
	}
	got := w.last.json()
	if state := asMap(got["visit"])["state"]; state != "CHECKED_IN" {
		return fmt.Errorf("gate check-in of %s: visit state %v, want CHECKED_IN (body: %s)", unit, state, w.last.body)
	}
	mvs := asList(got["moves"])
	if len(mvs) != 1 {
		return fmt.Errorf("gate check-in of %s created %d moves, want 1 (body: %s)", unit, len(mvs), w.last.body)
	}
	mv := asMap(mvs[0])
	wantCode, wantKind := st.door, "DOOR"
	if place == "holding spot" {
		wantCode, wantKind = st.holding, "SPOT"
	}
	to := asMap(mv["to"])
	if mv["reason"] != reason || mv["performer"] != performer || to["code"] != wantCode || to["kind"] != wantKind {
		return fmt.Errorf("move of %s is %v by %v to %v %v, want %s by %s to %s %s (body: %s)",
			unit, mv["reason"], mv["performer"], to["kind"], to["code"], reason, performer, wantKind, wantCode, w.last.body)
	}
	return nil
}

func (w *world) gateRefuses(status int, slug string) error { return w.problemIs(status, slug) }

func (w *world) moveCompleted(unit string) error {
	unit = w.rs(unit)
	id, ok := w.yj().moves[unit]
	if !ok {
		return fmt.Errorf("no move was created for trailer %s in this process", unit)
	}
	if err := w.expectOK2xx(w.doJSON(http.MethodPost, fmt.Sprintf("%s/moves/%s/completions", yardBaseURL, id), map[string]any{})); err != nil {
		return fmt.Errorf("complete move %s of %s: %w", id, unit, err)
	}
	if got := w.last.json()["state"]; got != "COMPLETED" {
		return fmt.Errorf("move %s of %s is %v, want COMPLETED (body: %s)", id, unit, got, w.last.body)
	}
	return nil
}

func (w *world) readTrailer(unit string) (map[string]any, error) {
	id, err := w.trailerID(unit)
	if err != nil {
		return nil, err
	}
	if err := w.doJSON(http.MethodGet, fmt.Sprintf("%s/trailers/%s", yardBaseURL, id), nil); err != nil {
		return nil, err
	}
	if w.last.status != http.StatusOK {
		return nil, fmt.Errorf("GET trailer %s: status %d (body: %s)", id, w.last.status, w.last.body)
	}
	return w.last.json(), nil
}

// trailerIsAt asserts state, place and (for a door) the door-work and load state.
func (w *world) trailerDockedAtDoor(unit, site, doorWork, load string) error {
	unit, site = w.rs(unit), w.rs(site)
	st, err := w.yardSiteOf(site)
	if err != nil {
		return err
	}
	tr, err := w.readTrailer(unit)
	if err != nil {
		return err
	}
	loc := asMap(tr["location"])
	if tr["state"] != "DOCKED" || loc["kind"] != "DOOR" || loc["code"] != st.door || tr["doorWork"] != doorWork || tr["loadState"] != load {
		return fmt.Errorf("trailer %s is %v at %v %v with door work %v and load %v, want DOCKED at DOOR %s with door work %s and load %s (body: %s)",
			unit, tr["state"], loc["kind"], loc["code"], tr["doorWork"], tr["loadState"], st.door, doorWork, load, w.last.body)
	}
	return nil
}

func (w *world) trailerParkedAtHolding(unit, site string) error {
	unit, site = w.rs(unit), w.rs(site)
	st, err := w.yardSiteOf(site)
	if err != nil {
		return err
	}
	tr, err := w.readTrailer(unit)
	if err != nil {
		return err
	}
	loc := asMap(tr["location"])
	if tr["state"] != "PARKED" || loc["kind"] != "SPOT" || loc["code"] != st.holding {
		return fmt.Errorf("trailer %s is %v at %v %v, want PARKED at SPOT %s (body: %s)", unit, tr["state"], loc["kind"], loc["code"], st.holding, w.last.body)
	}
	return nil
}

// trailerEventuallyHasDoorWork is the AutoCompleteUnload assertion: nothing
// here calls the door-work completion endpoint, so DONE and EMPTY can only come
// from inbound-receiving's ReceiptClosed reaching yard-management.
func (w *world) trailerEventuallyHasDoorWork(unit, doorWork, load string) error {
	unit = w.rs(unit)
	return eventually(func() error {
		tr, err := w.readTrailer(unit)
		if err != nil {
			return err
		}
		if tr["doorWork"] != doorWork || tr["loadState"] != load {
			return fmt.Errorf("trailer %s has door work %v and load %v, want %s and %s", unit, tr["doorWork"], tr["loadState"], doorWork, load)
		}
		return nil
	})
}

func (w *world) trailerDeparted(unit string) error {
	unit = w.rs(unit)
	tr, err := w.readTrailer(unit)
	if err != nil {
		return err
	}
	if tr["state"] != "DEPARTED" || tr["location"] != nil {
		return fmt.Errorf("trailer %s is %v at %v, want DEPARTED and nowhere (body: %s)", unit, tr["state"], tr["location"], w.last.body)
	}
	return nil
}

func (w *world) doorWorkStarted(unit string) error {
	unit = w.rs(unit)
	id, err := w.trailerID(unit)
	if err != nil {
		return err
	}
	if err := w.expectOK2xx(w.doJSON(http.MethodPost, fmt.Sprintf("%s/trailers/%s/door-work-starts", yardBaseURL, id), nil)); err != nil {
		return fmt.Errorf("start door work on %s: %w", unit, err)
	}
	if got := w.last.json()["doorWork"]; got != "IN_PROGRESS" {
		return fmt.Errorf("door work on %s is %v, want IN_PROGRESS (body: %s)", unit, got, w.last.body)
	}
	return nil
}

func (w *world) visitChecksOut(plate, unit string) error {
	plate, unit = w.rs(plate), w.rs(unit)
	visit, ok := w.yj().visits[plate]
	if !ok {
		return fmt.Errorf("no visit of plate %s has been admitted in this process", plate)
	}
	id, err := w.trailerID(unit)
	if err != nil {
		return err
	}
	if err := w.expectOK2xx(w.doJSON(http.MethodPost, fmt.Sprintf("%s/visits/%s/check-outs", yardBaseURL, visit),
		map[string]any{"trailersOut": []map[string]any{{"trailerId": id}}})); err != nil {
		return fmt.Errorf("check out visit %s: %w", visit, err)
	}
	got := w.last.json()
	if got["state"] != "CHECKED_OUT" || len(asList(got["trailersOut"])) != 1 {
		return fmt.Errorf("visit %s answered state %v with %d trailers out, want CHECKED_OUT with 1 (body: %s)", visit, got["state"], len(asList(got["trailersOut"])), w.last.body)
	}
	return nil
}

func (w *world) gateRecordsTurnAway(plate, site, reason string) error {
	plate, site = w.rs(plate), w.rs(site)
	if err := w.expectOK2xx(w.doJSON(http.MethodPost, yardBaseURL+"/visit-rejections", map[string]any{
		"siteId": site, "carrier": "ACME Freight", "carrierScac": "ACME", "tractorPlate": plate, "reason": reason,
	})); err != nil {
		return fmt.Errorf("record turn-away of %s: %w", plate, err)
	}
	got := w.last.json()
	if w.last.status != http.StatusCreated || got["state"] != "REJECTED" || got["rejectionReason"] != reason {
		return fmt.Errorf("turn-away of %s: status %d state %v reason %v, want 201 REJECTED for %s (body: %s)", plate, w.last.status, got["state"], got["rejectionReason"], reason, w.last.body)
	}
	id, _ := got["visitId"].(string)
	w.yj().visits[plate] = id
	return nil
}

// checkInIsRejectedAsSealMismatch asserts a REJECT-policy seal mismatch: the
// visit is REJECTED in the same call and nothing was admitted.
func (w *world) checkInIsRejectedAsSealMismatch(plate string) error {
	plate = w.rs(plate)
	if w.last.status != http.StatusCreated {
		return fmt.Errorf("check-in of %s: status %d, want 201 with a REJECTED visit (body: %s)", plate, w.last.status, w.last.body)
	}
	got := w.last.json()
	visit := asMap(got["visit"])
	if visit["state"] != "REJECTED" || visit["rejectionReason"] != "SEAL_MISMATCH" || len(asList(got["trailers"])) != 0 || len(asList(got["moves"])) != 0 {
		return fmt.Errorf("check-in of %s: state %v reason %v with %d trailers and %d moves, want REJECTED SEAL_MISMATCH with none (body: %s)",
			plate, visit["state"], visit["rejectionReason"], len(asList(got["trailers"])), len(asList(got["moves"])), w.last.body)
	}
	w.yj().visits[plate], _ = visit["visitId"].(string)
	return nil
}

// refusedCheckInPersistedNothing asserts the refused unit is not on site: the
// 409 left no trailer behind (the visit list is not asserted: the turn-away that
// follows legitimately records a REJECTED visit).
func (w *world) refusedCheckInPersistedNothing(unit, site string) error {
	unit, site = w.rs(unit), w.rs(site)
	if err := w.doJSON(http.MethodGet, fmt.Sprintf("%s/trailers?siteId=%s&unitNumber=%s", yardBaseURL, site, unit), nil); err != nil {
		return err
	}
	if w.last.status != http.StatusOK {
		return fmt.Errorf("GET trailers of %s: status %d (body: %s)", site, w.last.status, w.last.body)
	}
	if n := len(asList(w.last.json()["items"])); n != 0 {
		return fmt.Errorf("yard-management holds %d trailer(s) %s at %s after a refused check-in, want none (body: %s)", n, unit, site, w.last.body)
	}
	return nil
}

func (w *world) siteHoldsTrailers(site string, want int) error {
	site = w.rs(site)
	if err := w.doJSON(http.MethodGet, fmt.Sprintf("%s/trailers?siteId=%s", yardBaseURL, site), nil); err != nil {
		return err
	}
	if n := len(asList(w.last.json()["items"])); w.last.status != http.StatusOK || n != want {
		return fmt.Errorf("yard-management holds %d trailers at %s (status %d), want %d (body: %s)", n, site, w.last.status, want, w.last.body)
	}
	return nil
}

// yardBoardShowsAtDoor asserts the board puts the trailer on the site's door.
func (w *world) yardBoardShowsAtDoor(site, unit string) error {
	site, unit = w.rs(site), w.rs(unit)
	st, err := w.yardSiteOf(site)
	if err != nil {
		return err
	}
	id, err := w.trailerID(unit)
	if err != nil {
		return err
	}
	board, err := w.yardBoard(site)
	if err != nil {
		return err
	}
	for _, d := range asList(board["doors"]) {
		door := asMap(d)
		if door["code"] == st.door && asMap(door["trailer"])["trailerId"] == id {
			return nil
		}
	}
	return fmt.Errorf("yard board of %s does not show trailer %s at door %s (body: %s)", site, id, st.door, w.last.body)
}

// yardBoardIsEmpty asserts no spot, door or open move of the site holds ANY
// trailer. The site is this scenario's own, so "empty" is exact.
func (w *world) yardBoardIsEmpty(site string) error {
	site = w.rs(site)
	board, err := w.yardBoard(site)
	if err != nil {
		return err
	}
	var busy []string
	for _, key := range []string{"spots", "doors"} {
		for _, p := range asList(board[key]) {
			if m := asMap(p); m["trailer"] != nil {
				busy = append(busy, fmt.Sprintf("%v holds %v", m["code"], asMap(m["trailer"])["trailerId"]))
			}
		}
	}
	if n := len(asList(board["inMove"])) + len(asList(board["openMoves"])); n != 0 || len(busy) > 0 {
		return fmt.Errorf("yard board of %s is not empty: %v, %d trailer(s) in move or open moves (body: %s)", site, busy, n, w.last.body)
	}
	return nil
}

// ---------------------------------------------------------------------
// inbound-receiving: the receipt
// ---------------------------------------------------------------------

// receiptIsOpenedOnAppointment opens the receipt against the dock-scheduling
// appointment. inbound-receiving validates it against ITS copy of
// dock-scheduling's appointments (its ADR 0019) and takes the door from it, so
// the receipt carrying the facility-layout door of the booking proves
// facility -> dock-scheduling -> inbound-receiving over the wire. The copy may
// lag the booking by a moment: a refusal is retried.
func (w *world) receiptIsOpenedOnAppointment(asn, label string) error {
	asn, label = w.rs(asn), w.rs(label)
	apptID, ok := w.yj().appts[label]
	if !ok {
		return fmt.Errorf("no appointment %s has been booked in this process", label)
	}
	var got map[string]any
	err := eventually(func() error {
		var err error
		got, err = w.openReceipt(asn, map[string]any{"asnNumber": asn, "appointmentId": apptID})
		return err
	})
	if err != nil {
		return err
	}
	if got["appointmentId"] != apptID || got["doorCode"] != w.yj().apptDoor[label] {
		return fmt.Errorf("receipt of ASN %s carries appointment %v at door %v, want %s at %s (body: %s)", asn, got["appointmentId"], got["doorCode"], apptID, w.yj().apptDoor[label], w.last.body)
	}
	return nil
}

// ---------------------------------------------------------------------
// outbound-shipping: the ship-confirm leg
// ---------------------------------------------------------------------

func (w *world) outboundIsHealthy() error {
	if err := w.doJSON(http.MethodGet, outboundBaseURL+"/healthz", nil); err != nil {
		return fmt.Errorf("outbound-shipping not reachable: %w", err)
	}
	if w.last.status < 200 || w.last.status >= 300 {
		return fmt.Errorf("outbound-shipping /healthz returned %d, body=%s", w.last.status, w.last.body)
	}
	return nil
}

// cptScheduleIsPublished publishes the site's CPT schedule on
// warehouse.process-path-management.events.
//
// INJECTED, NOT REAL, AND NOT THE REAL SHAPE: process-path-management owns this
// fact (PUT /sites/{siteId}/cpt-schedule) and its CPTScheduleChanged carries no
// `version`; outbound-shipping's consumer requires `version >= 1` and
// dead-letters the real event ("cpt schedule change needs site_id, timezone,
// version and time"), so no load can ever be planned from the real producer. The
// fact is therefore published here in the shape outbound-shipping accepts (with
// version 1). dock-scheduling reads the same event and ignores the extra field.
// The mismatch is a defect between those two contexts, reported in the PR that
// added this scenario, not something this suite hides by agreeing with the
// consumer: when it is fixed this step should publish the producer's real shape
// (or call the real endpoint).
func (w *world) cptScheduleIsPublished(site, cpt string) error {
	site, cpt = w.rs(site), w.rs(cpt)
	return publishInjectedCloudEvent(topicProcessPathEvents, "/warehouse/process-path-management", typeCPTScheduleChanged,
		"urn:warehouse:process-path-management:events:CPTScheduleChanged:v1", site, time.Now(), map[string]any{
			"site_id": site, "timezone": "UTC", "version": 1,
			"cutoffs": []map[string]any{{
				"cpt_id": cpt, "local_time": "16:00", "ship_method": "STD-2DAY",
				"days_of_week": []string{"Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"},
			}},
		})
}

// packageIsManifestedUntilPlanned publishes fulfillment-execution's
// PackageManifested for the package until outbound-shipping has planned the
// load holding it.
//
// INJECTED, NOT REAL: no pick/pack run happens here, so fulfillment-execution
// never manifests anything; the payload carries the additive correlation of
// outbound-shipping's ADR 0003 (site_id, source_order_id, cpt), which the real
// producer fills only partly today. A manifest that arrives before
// outbound-shipping has applied the CPT schedule is IGNORED (not retried by the
// service), so the same package is published again, with a new event id, until a
// load holds it: re-publishing a package the service already knows is a no-op.
func (w *world) packageIsManifestedUntilPlanned(pkg, order, site string) error {
	pkg, order, site = w.rs(pkg), w.rs(order), w.rs(site)
	cutoff := time.Now().UTC().Truncate(24 * time.Hour).Add(16 * time.Hour)
	return eventually(func() error {
		if id, err := w.outboundLoadOf(site); err == nil {
			if err := w.doJSON(http.MethodGet, fmt.Sprintf("%s/loads/%s/packages", outboundBaseURL, id), nil); err == nil && w.last.status == http.StatusOK {
				for _, p := range asList(w.last.json()["items"]) {
					if asMap(p)["id"] == pkg {
						return nil
					}
				}
			}
		}
		if err := publishInjectedCloudEvent(topicFulfillmentEvents, "/warehouse/fulfillment-execution", typePackageManifestedFul,
			"urn:warehouse:fulfillment-execution:events:PackageManifested:v1", pkg, time.Now(), map[string]any{
				"package_id": pkg, "order_ref": order + "-line-1", "site_id": site, "source_order_id": order,
				"cpt": cutoff.Format(time.RFC3339), "weight_g": 1200,
			}); err != nil {
			return err
		}
		return fmt.Errorf("outbound-shipping has planned no load holding package %s at %s yet", pkg, site)
	})
}

// outboundLoadOf returns the id of the one load of the (run-scoped) site.
func (w *world) outboundLoadOf(site string) (string, error) {
	if id, ok := w.yj().loads[site]; ok {
		return id, nil
	}
	if err := w.doJSON(http.MethodGet, fmt.Sprintf("%s/loads?site_id=%s", outboundBaseURL, site), nil); err != nil {
		return "", err
	}
	items := asList(w.last.json()["items"])
	if w.last.status != http.StatusOK || len(items) != 1 {
		return "", fmt.Errorf("outbound-shipping lists %d loads of %s (status %d), want exactly 1 (body: %s)", len(items), site, w.last.status, w.last.body)
	}
	id, _ := asMap(items[0])["id"].(string)
	w.yj().loads[site] = id
	return id, nil
}

func (w *world) readLoad(site string) (map[string]any, error) {
	id, err := w.outboundLoadOf(site)
	if err != nil {
		return nil, err
	}
	if err := w.doJSON(http.MethodGet, fmt.Sprintf("%s/loads/%s", outboundBaseURL, id), nil); err != nil {
		return nil, err
	}
	if w.last.status != http.StatusOK {
		return nil, fmt.Errorf("GET load %s: status %d (body: %s)", id, w.last.status, w.last.body)
	}
	return w.last.json(), nil
}

func (w *world) readPackage(pkg, site string) (map[string]any, error) {
	id, err := w.outboundLoadOf(site)
	if err != nil {
		return nil, err
	}
	if err := w.doJSON(http.MethodGet, fmt.Sprintf("%s/loads/%s/packages", outboundBaseURL, id), nil); err != nil {
		return nil, err
	}
	for _, p := range asList(w.last.json()["items"]) {
		if asMap(p)["id"] == pkg {
			return asMap(p), nil
		}
	}
	return nil, fmt.Errorf("load %s holds no package %s (body: %s)", id, pkg, w.last.body)
}

func (w *world) loadIs(site, state string) error {
	site = w.rs(site)
	got, err := w.readLoad(site)
	if err != nil {
		return err
	}
	if got["state"] != state {
		return fmt.Errorf("outbound-shipping's load of %s is %v, want %s (body: %s)", site, got["state"], state, w.last.body)
	}
	return nil
}

func (w *world) loadIsAndPackageIs(site, state, pkg, pkgState string) error {
	if err := w.loadIs(site, state); err != nil {
		return err
	}
	p, err := w.readPackage(w.rs(pkg), w.rs(site))
	if err != nil {
		return err
	}
	if p["state"] != pkgState {
		return fmt.Errorf("package %s is %v, want %s (body: %s)", pkg, p["state"], pkgState, w.last.body)
	}
	return nil
}

func (w *world) loadEventuallyHasDoorOf(site, label string) error {
	site, label = w.rs(site), w.rs(label)
	return eventually(func() error {
		got, err := w.readLoad(site)
		if err != nil {
			return err
		}
		if got["door_code"] != w.yj().apptDoor[label] || got["appointment_id"] != w.yj().appts[label] {
			return fmt.Errorf("load of %s has door %v from appointment %v, want %s from %s", site, got["door_code"], got["appointment_id"], w.yj().apptDoor[label], w.yj().appts[label])
		}
		return nil
	})
}

// loadEventuallyLoadingOn is the TrailerDocked contract: yard-management's
// event, with the trailer id the yard minted, puts the load on its door to
// LOADING (there is no REST route that starts loading).
func (w *world) loadEventuallyLoadingOn(site, unit string) error {
	site, unit = w.rs(site), w.rs(unit)
	id, err := w.trailerID(unit)
	if err != nil {
		return err
	}
	return eventually(func() error {
		got, err := w.readLoad(site)
		if err != nil {
			return err
		}
		if got["state"] != "LOADING" || got["trailer_id"] != id {
			return fmt.Errorf("load of %s is %v on trailer %v, want LOADING on %s", site, got["state"], got["trailer_id"], id)
		}
		return nil
	})
}

func (w *world) loadEventuallyShipConfirmed(site, unit, pkg string) error {
	site, unit, pkg = w.rs(site), w.rs(unit), w.rs(pkg)
	id, err := w.trailerID(unit)
	if err != nil {
		return err
	}
	return eventually(func() error {
		got, err := w.readLoad(site)
		if err != nil {
			return err
		}
		if got["state"] != "SHIP_CONFIRMED" || got["trailer_id"] != id {
			return fmt.Errorf("load of %s is %v on trailer %v, want SHIP_CONFIRMED on %s", site, got["state"], got["trailer_id"], id)
		}
		p, err := w.readPackage(pkg, site)
		if err != nil {
			return err
		}
		if p["state"] != "SHIPPED" {
			return fmt.Errorf("package %s is %v, want SHIPPED", pkg, p["state"])
		}
		return nil
	})
}

func (w *world) packageStagesAtLane(pkg, site string) error {
	pkg, site = w.rs(pkg), w.rs(site)
	st, err := w.yardSiteOf(site)
	if err != nil {
		return err
	}
	if st.staging == "" {
		return fmt.Errorf("site %s has no staging lane", site)
	}
	if err := w.expectOK2xx(w.doJSON(http.MethodPost, fmt.Sprintf("%s/packages/%s/stagings", outboundBaseURL, pkg),
		map[string]any{"staging_location": st.staging})); err != nil {
		return fmt.Errorf("stage package %s: %w", pkg, err)
	}
	if got := w.last.json()["state"]; got != "STAGED" {
		return fmt.Errorf("package %s is %v after staging, want STAGED (body: %s)", pkg, got, w.last.body)
	}
	return nil
}

func (w *world) packageLoadsThroughDoor(pkg, site string) error {
	pkg, site = w.rs(pkg), w.rs(site)
	st, err := w.yardSiteOf(site)
	if err != nil {
		return err
	}
	id, err := w.outboundLoadOf(site)
	if err != nil {
		return err
	}
	if err := w.expectOK2xx(w.doJSON(http.MethodPost, fmt.Sprintf("%s/packages/%s/loadings", outboundBaseURL, pkg),
		map[string]any{"load_id": id, "door_code": st.door})); err != nil {
		return fmt.Errorf("load package %s: %w", pkg, err)
	}
	return nil
}

func (w *world) loadIsClosed(site string) error {
	site = w.rs(site)
	id, err := w.outboundLoadOf(site)
	if err != nil {
		return err
	}
	if err := w.expectOK2xx(w.doJSON(http.MethodPost, fmt.Sprintf("%s/loads/%s/closures", outboundBaseURL, id), map[string]any{})); err != nil {
		return fmt.Errorf("close load %s: %w", id, err)
	}
	return nil
}

func (w *world) doorWorkCompletedLoaded(unit string) error {
	unit = w.rs(unit)
	id, err := w.trailerID(unit)
	if err != nil {
		return err
	}
	if err := w.expectOK2xx(w.doJSON(http.MethodPost, fmt.Sprintf("%s/trailers/%s/door-work-completions", yardBaseURL, id), map[string]any{"loadState": "LOADED"})); err != nil {
		return fmt.Errorf("complete door work on %s: %w", unit, err)
	}
	if tr := w.last.json(); tr["doorWork"] != "DONE" || tr["loadState"] != "LOADED" {
		return fmt.Errorf("trailer %s has door work %v and load %v, want DONE and LOADED (body: %s)", unit, tr["doorWork"], tr["loadState"], w.last.body)
	}
	return nil
}

func (w *world) sealRecorded(unit, seal string) error {
	unit, seal = w.rs(unit), w.rs(seal)
	id, err := w.trailerID(unit)
	if err != nil {
		return err
	}
	if err := w.expectOK2xx(w.doJSON(http.MethodPost, fmt.Sprintf("%s/trailers/%s/seals", yardBaseURL, id), map[string]any{"state": "INTACT", "sealNumber": seal})); err != nil {
		return fmt.Errorf("record seal on %s: %w", unit, err)
	}
	return nil
}

// shipmentConfirmedPublished reads outbound-shipping's integration topic for the
// ShipmentConfirmed of THIS load: it must name the trailer the yard minted, the
// seal recorded on that trailer and the order of the manifested package.
func (w *world) shipmentConfirmedPublished(site, unit, seal, order string) error {
	site, unit, seal, order = w.rs(site), w.rs(unit), w.rs(seal), w.rs(order)
	load, err := w.outboundLoadOf(site)
	if err != nil {
		return err
	}
	trailer, err := w.trailerID(unit)
	if err != nil {
		return err
	}
	_, err = expectKafkaEvent(topicOutboundEvents, typeShipmentConfirmed, func(d map[string]any) bool {
		orders := asList(d["orders"])
		return d["load_id"] == load && d["trailer_id"] == trailer && d["seal_number"] == seal &&
			len(orders) == 1 && asMap(orders[0])["order_id"] == order
	}, fmt.Sprintf("load %s, trailer %s, seal %s, order %s", load, trailer, seal, order))
	return err
}

// ---------------------------------------------------------------------
// Kafka: which event types reach the integration topic
// ---------------------------------------------------------------------

type wantedYardEvent struct {
	name, fullType, subject string
}

// yardTopicCarries consumes warehouse.yard-management.events from its FIRST
// offset on a FRESH consumer group (nobody else's committed offsets are
// involved, so the read sees the whole topic) and requires every named event
// type to be present for the given subject. It is the regression guard for the
// integration/analytics split of yard-management's catalogue (its ADR 0004,
// ADR 0010): DoorWorkCompleted and TrailerDeparted once reached only
// warehouse.yard-management.analytics, so a consumer of the integration topic
// (dock-scheduling's CompleteOnDoorRelease, outbound-shipping's ship-confirm)
// never saw them while every consumer's own tests, built on fixtures, passed.
//
// The subject is a trailer id (or a visit id), minted by the service in THIS
// run: an event of an earlier run on the same topic cannot satisfy it. A name
// that is not an integration type (DoorWorkStarted is analytics-only) is never
// found, and the step fails after the scan window.
func (w *world) yardTopicCarries(names, subjectKind, key string) error {
	key = w.rs(key)
	subject, err := w.subjectOf(subjectKind, key)
	if err != nil {
		return err
	}
	var wanted []wantedYardEvent
	for _, name := range strings.FieldsFunc(names, func(r rune) bool { return r == ',' || r == ' ' }) {
		if name == "and" {
			continue
		}
		full, ok := yardEventTypes[name]
		if !ok {
			return fmt.Errorf("%q is not a yard-management event this suite knows", name)
		}
		wanted = append(wanted, wantedYardEvent{name: name, fullType: full, subject: subject})
	}
	missing, seen, err := awaitCloudEvents(topicYardEvents, kafkaScanWindow, wanted)
	if err != nil {
		return err
	}
	if len(missing) > 0 {
		var names []string
		for _, m := range missing {
			names = append(names, m.fullType)
		}
		return fmt.Errorf("%s has no %s for subject %s within %s (read %d messages from the first offset on a fresh group)",
			topicYardEvents, strings.Join(names, ", "), subject, kafkaScanWindow, seen)
	}
	return nil
}

func (w *world) subjectOf(kind, key string) (string, error) {
	switch kind {
	case "trailer":
		return w.trailerID(key)
	case "visit":
		id, ok := w.yj().visits[key]
		if !ok {
			return "", fmt.Errorf("no visit of plate %s has been recorded in this process", key)
		}
		return id, nil
	}
	return "", fmt.Errorf("unknown subject kind %q", kind)
}

// awaitCloudEvents reads topic from its first offset on a brand-new consumer
// group until every wanted (type, subject) pair was seen or the window ends; it
// returns the pairs still missing and how many messages it read.
func awaitCloudEvents(topic string, window time.Duration, wanted []wantedYardEvent) (missing []wantedYardEvent, seen int, err error) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: kafkaBrokerList(), Topic: topic,
		GroupID:        scanGroupID(topic),
		StartOffset:    kafka.FirstOffset,
		CommitInterval: time.Second,
		MinBytes:       1, MaxBytes: 10 << 20, MaxWait: 250 * time.Millisecond,
	})
	defer func() { _ = r.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), window)
	defer cancel()
	left := append([]wantedYardEvent{}, wanted...)
	for len(left) > 0 {
		msg, rerr := r.ReadMessage(ctx)
		if rerr != nil {
			if ctx.Err() != nil || errors.Is(rerr, context.DeadlineExceeded) {
				return left, seen, nil
			}
			return left, seen, fmt.Errorf("kafka read %s: %w", topic, rerr)
		}
		seen++
		var m ceMessage
		if json.Unmarshal(msg.Value, &m) != nil {
			continue
		}
		rest := left[:0]
		for _, want := range left {
			if m.Type == want.fullType && m.Subject == want.subject {
				continue
			}
			rest = append(rest, want)
		}
		left = rest
	}
	return nil, seen, nil
}

// ---------------------------------------------------------------------
// godog wiring
// ---------------------------------------------------------------------

func registerYardJourneySteps(sc *godog.ScenarioContext, w *world) {
	sc.Step(`^facility-layout, product-master, inbound-receiving, dock-scheduling and yard-management are healthy$`, w.yardJourneyServicesAreHealthy)

	sc.Step(`^site "([^"]*)" with one (Inbound|Outbound) dock door, parking bays "([^"]*)" and holding bay "([^"]*)"( and a staging lane)? is registered in facility-layout$`, w.siteIsRegistered)
	sc.Step(`^yard-management eventually knows the layout of site "([^"]*)" from facility-layout$`, w.yardKnowsLayout)
	sc.Step(`^dock-scheduling's calendar of site "([^"]*)" allows arrival 30 minutes early on a 5-minute slot grid$`, w.calendarAllowsEarlyArrival)
	sc.Step(`^an INBOUND appointment "([^"]*)" on the door of site "([^"]*)" for ASN "([^"]*)" with expected seal "([^"]*)" is eventually booked in dock-scheduling to start in (\d+) minutes for (\d+) minutes$`, w.appointmentEventuallyBooked)
	sc.Step(`^an OUTBOUND appointment "([^"]*)" on the door of site "([^"]*)" for cutoff "([^"]*)" is eventually booked in dock-scheduling to start in (\d+) minutes for (\d+) minutes$`, w.outboundAppointmentEventuallyBooked)
	sc.Step(`^dock-scheduling's appointment "([^"]*)" is "([A-Z_]+)"$`, w.appointmentIs)
	sc.Step(`^dock-scheduling's appointment "([^"]*)" is eventually "([A-Z_]+)"$`, w.appointmentEventuallyIs)
	sc.Step(`^dock-scheduling's appointment "([^"]*)" is eventually CANCELLED as ([A-Z_]+)$`, w.appointmentEventuallyCancelledAs)

	sc.Step(`^the yard policy of site "([^"]*)" requires an appointment, flags a seal mismatch and holds walk-ins at bay "([^"]*)"$`, func(site, bay string) error {
		return w.yardPolicyDeclared(site, true, "FLAG", bay)
	})
	sc.Step(`^the yard policy of site "([^"]*)" does not require an appointment, flags a seal mismatch and holds walk-ins at bay "([^"]*)"$`, func(site, bay string) error {
		return w.yardPolicyDeclared(site, false, "FLAG", bay)
	})
	sc.Step(`^the yard policy of site "([^"]*)" does not require an appointment, rejects a seal mismatch and holds walk-ins at bay "([^"]*)"$`, func(site, bay string) error {
		return w.yardPolicyDeclared(site, false, "REJECT", bay)
	})

	sc.Step(`^the carrier checks in at the gate of site "([^"]*)" with plate "([^"]*)" for purpose ([A-Z_]+) and trailer "([^"]*)" \(([A-Z_]+), ([A-Z]+)\)(?: on appointment "([^"]*)")?(?: sealed "([^"]*)")?$`, w.gateChecksIn)
	sc.Step(`^the gate admits the visit with a ([A-Z_]+) move of trailer "([^"]*)" by the ([A-Z_]+) to the (door|holding spot) of site "([^"]*)"$`, w.gateAdmitsWithMove)
	sc.Step(`^the gate refuses the check-in with (\d+) "([^"]*)"$`, w.gateRefuses)
	sc.Step(`^the gate refused check-in of trailer "([^"]*)" at site "([^"]*)" left no trailer behind$`, w.refusedCheckInPersistedNothing)
	sc.Step(`^yard-management holds (\d+) trailers? at site "([^"]*)"$`, func(n int, site string) error { return w.siteHoldsTrailers(site, n) })
	sc.Step(`^the gate records the turn-away of plate "([^"]*)" at site "([^"]*)" because ([A-Z_]+)$`, w.gateRecordsTurnAway)
	sc.Step(`^the check-in of plate "([^"]*)" is rejected as SEAL_MISMATCH with nothing admitted$`, w.checkInIsRejectedAsSealMismatch)

	sc.Step(`^the move of trailer "([^"]*)" is completed in yard-management$`, w.moveCompleted)
	sc.Step(`^trailer "([^"]*)" is DOCKED at the door of site "([^"]*)" with door work ([A-Z_]+) and load state ([A-Z]+)$`, w.trailerDockedAtDoor)
	sc.Step(`^trailer "([^"]*)" is PARKED at the holding spot of site "([^"]*)"$`, w.trailerParkedAtHolding)
	sc.Step(`^trailer "([^"]*)" eventually has door work ([A-Z_]+) and load state ([A-Z]+)$`, w.trailerEventuallyHasDoorWork)
	sc.Step(`^trailer "([^"]*)" is DEPARTED$`, w.trailerDeparted)
	sc.Step(`^the dock operator starts door work on trailer "([^"]*)" in yard-management$`, w.doorWorkStarted)
	sc.Step(`^the visit of plate "([^"]*)" checks out taking trailer "([^"]*)" in yard-management$`, w.visitChecksOut)
	sc.Step(`^the yard board of site "([^"]*)" shows trailer "([^"]*)" at the door$`, w.yardBoardShowsAtDoor)
	sc.Step(`^the yard board of site "([^"]*)" is empty$`, w.yardBoardIsEmpty)

	sc.Step(`^inbound-receiving opens a receipt for ASN "([^"]*)" on appointment "([^"]*)" at the booked door$`, w.receiptIsOpenedOnAppointment)

	sc.Step(`^outbound-shipping is healthy$`, w.outboundIsHealthy)
	sc.Step(`^the CPT schedule of site "([^"]*)" with cutoff "([^"]*)" at 16:00 UTC is published to warehouse\.process-path-management\.events$`, w.cptScheduleIsPublished)
	sc.Step(`^PackageManifested of package "([^"]*)" for order "([^"]*)" at site "([^"]*)" is published until outbound-shipping has planned a load holding it$`, w.packageIsManifestedUntilPlanned)
	sc.Step(`^outbound-shipping's load of site "([^"]*)" is "([A-Z_]+)"$`, w.loadIs)
	sc.Step(`^outbound-shipping's load of site "([^"]*)" is "([A-Z_]+)" and package "([^"]*)" is "([A-Z_]+)"$`, w.loadIsAndPackageIs)
	sc.Step(`^outbound-shipping eventually takes the door of appointment "([^"]*)" for the load of site "([^"]*)"$`, func(label, site string) error { return w.loadEventuallyHasDoorOf(site, label) })
	sc.Step(`^package "([^"]*)" stages at the staging lane of site "([^"]*)" in outbound-shipping$`, w.packageStagesAtLane)
	sc.Step(`^outbound-shipping eventually has the load of site "([^"]*)" LOADING on trailer "([^"]*)"$`, w.loadEventuallyLoadingOn)
	sc.Step(`^package "([^"]*)" loads through the door of site "([^"]*)" in outbound-shipping$`, w.packageLoadsThroughDoor)
	sc.Step(`^the load of site "([^"]*)" is closed in outbound-shipping$`, w.loadIsClosed)
	sc.Step(`^the dock operator completes door work on trailer "([^"]*)" as LOADED in yard-management$`, w.doorWorkCompletedLoaded)
	sc.Step(`^the dock operator records an INTACT seal "([^"]*)" on trailer "([^"]*)" in yard-management$`, func(seal, unit string) error { return w.sealRecorded(unit, seal) })
	sc.Step(`^outbound-shipping eventually has the load of site "([^"]*)" SHIP_CONFIRMED on trailer "([^"]*)" with package "([^"]*)" SHIPPED$`, w.loadEventuallyShipConfirmed)
	sc.Step(`^outbound-shipping published ShipmentConfirmed for the load of site "([^"]*)" on trailer "([^"]*)" sealed "([^"]*)" for order "([^"]*)"$`, w.shipmentConfirmedPublished)

	sc.Step(`^warehouse\.yard-management\.events, read from the first offset on a fresh consumer group, carries (.+) of (trailer|visit) "([^"]*)"$`, w.yardTopicCarries)
}
