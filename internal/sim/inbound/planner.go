package inbound

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/claudioed/warehouse-e2e/internal/sim"
	"github.com/claudioed/warehouse-e2e/internal/sim/outbound"
)

// sealOf is the seal the carrier declares when booking the ASN's appointment; the gate check-in carries the same
// one, so yard-management raises no SealExceptionRaised.
func sealOf(asn string) string { return "SEAL-" + asn }

// ---------------------------------------------------------------------
// dock planner (dock-scheduling)
// ---------------------------------------------------------------------

// BookDoor books an INBOUND appointment for the ASN on one of the site's inbound doors: the first that is free
// in the window. The appointment declares the ASN's seal. dock-scheduling learns ASNs and doors from other
// services over Kafka, so 422 unknown-asn (ASNRegistered not consumed yet) and 422 unknown-door (facility-layout's
// slot not consumed yet) are consumer lag and are retried in a bounded loop; 409 door-double-booked moves on to
// the next door; any other refusal (a window in the past, an outbound-only door, an ASN already scheduled) fails
// at once.
func (c *Crew) BookDoor(ctx context.Context, asnNumber string, w Window) (Appointment, error) {
	if len(c.Site.Doors) == 0 {
		return Appointment{}, fmt.Errorf("book a door for ASN %s: site %s has no INBOUND door configured", asnNumber, c.Site.Code)
	}
	var busy []string
	for _, door := range c.Site.Doors {
		body := map[string]any{
			"direction": "INBOUND", "doorCode": door, "carrier": c.Carrier.Name,
			"windowStart": w.Start.UTC().Format(time.RFC3339), "windowEnd": w.End.UTC().Format(time.RFC3339),
			"asnNumbers": []string{asnNumber}, "expectedSealNumber": sealOf(asnNumber),
		}
		if c.Carrier.SCAC != "" {
			body["carrierScac"] = c.Carrier.SCAC
		}
		what := fmt.Sprintf("book door %s for ASN %s", door, asnNumber)
		r, err := c.callTransient(ctx, what, []outbound.Transient{unknownDoor, unknownASN}, func() sim.Resp {
			return c.call(ctx, SvcDock, http.MethodPost, "/appointments", "/appointments", body)
		})
		if err != nil {
			if r.Status == http.StatusConflict && outbound.Slug(r) == "door-double-booked" {
				busy = append(busy, door)
				continue
			}
			return Appointment{}, err
		}
		var got struct {
			AppointmentID string   `json:"appointmentId"`
			SiteID        string   `json:"siteId"`
			Direction     string   `json:"direction"`
			DoorCode      string   `json:"doorCode"`
			State         string   `json:"state"`
			ASNNumbers    []string `json:"asnNumbers"`
		}
		if err := r.Decode(&got); err != nil {
			return Appointment{}, fmt.Errorf("%s: %w", what, err)
		}
		if r.Status != http.StatusCreated || got.AppointmentID == "" || got.State != "BOOKED" || got.Direction != "INBOUND" || got.DoorCode != door ||
			len(got.ASNNumbers) != 1 || got.ASNNumbers[0] != asnNumber {
			return Appointment{}, fmt.Errorf("%s: status %d state %q direction %q door %q asns %v, want 201 BOOKED INBOUND on %s for [%s] (body: %s)",
				what, r.Status, got.State, got.Direction, got.DoorCode, got.ASNNumbers, door, asnNumber, sim.Truncate(string(r.Body), 200))
		}
		c.logf("dock-planner", "booked inbound door %s for %s: %s", door, asnNumber, got.AppointmentID)
		return Appointment{ID: got.AppointmentID, SiteID: got.SiteID, Door: door, ASN: asnNumber, Seal: sealOf(asnNumber), Window: w, State: got.State}, nil
	}
	return Appointment{}, fmt.Errorf("book a door for ASN %s: every inbound door of %s is double-booked in the window (%v)", asnNumber, c.Site.Code, busy)
}
