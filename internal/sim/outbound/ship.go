package outbound

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/claudioed/warehouse-e2e/internal/sim"
)

// Confirmations is what the floor needs from the Kafka tap to see the one thing
// it must never produce: ShipmentConfirmed. *sim.EventTap satisfies it (the tap
// must be tracking the ref: EventTap.Track).
type Confirmations interface {
	// Seen is how many integration events of the short type carried ref.
	Seen(ref, typ string) int
}

// The event types the floor looks for.
const (
	TypeShipmentConfirmed = "ShipmentConfirmed"
	TypePackageMissedLoad = "PackageMissedLoad"
)

var _ Confirmations = (*sim.EventTap)(nil)

// AwaitShipmentConfirmed waits (bounded by Floor.Wait) until the tap has seen a
// ShipmentConfirmed carrying ref. ShipmentConfirmed lists the order-management
// order ids (orders[].order_id): pass Package.SourceOrderID, tracked on the tap.
// A PackageMissedLoad carrying the same ref ends the wait with an error at once:
// the truck left before the load was closed, so no confirmation will come.
func (f *Floor) AwaitShipmentConfirmed(ctx context.Context, c Confirmations, ref string) error {
	return poll(ctx, f.Wait, "await ShipmentConfirmed of "+ref, func() (string, error) {
		if n := c.Seen(ref, TypePackageMissedLoad); n > 0 {
			return "", fmt.Errorf("await ShipmentConfirmed of %s: PackageMissedLoad was raised %d time(s) for it: the trailer departed before its load was closed", ref, n)
		}
		if c.Seen(ref, TypeShipmentConfirmed) > 0 {
			return "", nil
		}
		return "no ShipmentConfirmed seen on the tap yet", nil
	})
}

// listLoads reads the site's loads in one state, newest first.
func (f *Floor) listLoads(ctx context.Context, site, state string) ([]Load, error) {
	path := "/loads?site_id=" + url.QueryEscape(site) + "&state=" + url.QueryEscape(state)
	loads, r := listAll[Load](ctx, f.C, SvcOutbound, "/loads?site_id&state", path)
	if !r.OK() {
		return nil, refusal("list "+state+" loads of "+site, r)
	}
	return loads, nil
}

func (f *Floor) listLoadPackages(ctx context.Context, loadID string) ([]packageDTO, error) {
	pkgs, r := listAll[packageDTO](ctx, f.C, SvcOutbound, "/loads/{id}/packages", "/loads/"+loadID+"/packages")
	if !r.OK() {
		return nil, refusal("list the packages of load "+loadID, r)
	}
	return pkgs, nil
}

func (p Package) matches(d packageDTO) bool {
	if p.ID != "" {
		return d.ID == p.ID
	}
	return (p.SourceOrderID != "" && d.SourceOrderID == p.SourceOrderID) || (p.OrderRef != "" && d.WorkUnitRef == p.OrderRef)
}

// AwaitLoadOf waits (bounded) until outbound-shipping knows the package on a
// PLANNED or LOADING load of the site and returns that load. outbound-shipping
// has no route that reads a package by itself: the load is found by walking the
// site's open loads and their packages, matching the package id (or, when the
// package has none, its source order id or order ref).
//
// A manifest is IGNORED, never retried, when it carries no site_id or cpt, when
// the site has no CPT schedule copy yet, or when its cpt is not a cutoff's local
// time on a listed day: the timeout message says so, because that is what an
// absent package almost always means.
func (f *Floor) AwaitLoadOf(ctx context.Context, p Package) (Load, error) {
	site := p.SiteID
	if site == "" {
		site = f.Site.Code
	}
	var found Load
	err := poll(ctx, f.Wait, fmt.Sprintf("find the load of package %s at %s", p.ID, site), func() (string, error) {
		for _, state := range []string{LoadPlanned, LoadLoading} {
			loads, err := f.listLoads(ctx, site, state)
			if err != nil {
				return "", err
			}
			for _, l := range loads {
				pkgs, err := f.listLoadPackages(ctx, l.ID)
				if err != nil {
					return "", err
				}
				for _, d := range pkgs {
					if p.matches(d) {
						found = l
						return "", nil
					}
				}
			}
		}
		return "no open load holds it: PackageManifested may not be consumed yet, or was ignored (no site_id/cpt, no CPT schedule for the site, or cpt " + p.CPT + " is not a cutoff's local time on a listed day)", nil
	})
	return found, err
}

// awaitLoad polls the load until pred accepts it; pred returns "" when satisfied or what is missing.
func (f *Floor) awaitLoad(ctx context.Context, loadID, what string, pred func(Load) string) (Load, error) {
	var l Load
	err := poll(ctx, f.Wait, what, func() (string, error) {
		var err error
		if l, err = f.ReadLoad(ctx, loadID); err != nil {
			return "", err
		}
		return pred(l), nil
	})
	return l, err
}

// Ship takes one manifested package out of the building: ShipAll of one.
func (f *Floor) Ship(ctx context.Context, p Package) Shipment {
	return f.ShipAll(ctx, []Package{p})[0]
}

// ShipAll takes every package out of the building, one load at a time, in the order
// the real floor needs:
//
//  1. wait until outbound-shipping has planned a load holding each package;
//  2. the stager stages each package;
//  3. the dock planner books a door for the load's cutoff (if no appointment named it
//     yet) and the floor waits until outbound-shipping gave the load that door:
//     TrailerDocked is ignored by a load without a door;
//  4. the carrier checks in, the trailer docks and door work starts (GateIn), and the
//     floor waits until TrailerDocked put the load in LOADING;
//  5. the loader scans every package onto the load through its door;
//  6. the load supervisor closes the load (LOADING, nothing outstanding);
//  7. the dock operator completes the door work as LOADED, seals the trailer and the
//     clerk checks the visit out (SealAndDepart);
//  8. the floor waits until the departure was consumed (the load is SHIP_CONFIRMED).
//
// Packages that share a load share its appointment, trailer and departure; a load that
// already has a door or a docked trailer (another floor did that part) is resumed, not
// booked again. A refusal or a timeout is recorded on the Shipment of every package of
// the load as a Problem and ends that load's run; the other loads still run. Nothing
// here panics, and a load that did not close is never departed.
func (f *Floor) ShipAll(ctx context.Context, pkgs []Package) []Shipment {
	out := make([]Shipment, len(pkgs))
	var order []string
	byLoad := map[string][]int{}
	for i, p := range pkgs {
		out[i] = Shipment{PackageID: p.ID, OrderRef: p.OrderRef}
		l, err := f.AwaitLoadOf(ctx, p)
		if err != nil {
			out[i].problem("%v", err)
			continue
		}
		out[i].LoadID = l.ID
		if _, seen := byLoad[l.ID]; !seen {
			order = append(order, l.ID)
		}
		byLoad[l.ID] = append(byLoad[l.ID], i)
	}
	for _, id := range order {
		f.shipLoad(ctx, id, byLoad[id], pkgs, out)
	}
	return out
}

func (f *Floor) shipLoad(ctx context.Context, loadID string, idx []int, pkgs []Package, out []Shipment) {
	each := func(fn func(s *Shipment)) {
		for _, i := range idx {
			fn(&out[i])
		}
	}
	fail := func(err error) { each(func(s *Shipment) { s.problem("%v", err) }) }

	// 2. stage
	var staged []int
	for _, i := range idx {
		if _, err := f.Stage(ctx, pkgs[i]); err != nil {
			out[i].problem("%v", err)
			continue
		}
		staged = append(staged, i)
	}

	// 3. the door
	l, err := f.ReadLoad(ctx, loadID)
	if err != nil {
		fail(err)
		return
	}
	var ap Appointment
	if l.DoorCode == "" {
		if ap, err = f.BookDoor(ctx, l.CptID, f.NextWindow(2*time.Minute, time.Hour)); err != nil {
			fail(err)
			return
		}
		l, err = f.awaitLoad(ctx, loadID, fmt.Sprintf("outbound-shipping takes door %s of appointment %s for load %s", ap.Door, ap.ID, loadID), func(l Load) string {
			if l.DoorCode != ap.Door || l.AppointmentID != ap.ID {
				return fmt.Sprintf("the load has door %q from appointment %q (DockAppointmentBooked not consumed, or it came before the load)", l.DoorCode, l.AppointmentID)
			}
			return ""
		})
		if err != nil {
			fail(err)
			return
		}
	} else {
		ap = Appointment{ID: l.AppointmentID, SiteID: l.SiteID, Door: l.DoorCode, CptID: l.CptID}
	}
	each(func(s *Shipment) { s.Door = ap.Door })

	// 4. the trailer
	var v Visit
	switch {
	case l.State == LoadPlanned:
		v, err = f.GateIn(ctx, ap)
		each(func(s *Shipment) { s.VisitID, s.TrailerID = v.ID, v.TrailerID })
		if err != nil {
			fail(err)
			return
		}
		_, err = f.awaitLoad(ctx, loadID, fmt.Sprintf("load %s LOADING on trailer %s", loadID, v.TrailerID), func(l Load) string {
			if l.State != LoadLoading || l.TrailerID != v.TrailerID {
				return fmt.Sprintf("the load is %s on trailer %q (TrailerDocked not consumed, or no open load uses door %s)", l.State, l.TrailerID, ap.Door)
			}
			return ""
		})
	case l.State == LoadLoading && l.TrailerID != "":
		v, err = f.visitOf(ctx, ap.Door, l.TrailerID, ap.ID)
		each(func(s *Shipment) { s.VisitID, s.TrailerID = v.ID, v.TrailerID })
	default:
		err = fmt.Errorf("load %s is %s on trailer %q: it cannot be loaded and shipped from here", loadID, l.State, l.TrailerID)
	}
	if err != nil {
		fail(err)
		return
	}

	// 5. load
	loaded := 0
	for _, i := range staged {
		if err := f.Load(ctx, pkgs[i], loadID, ap.Door); err != nil {
			out[i].problem("%v", err)
			continue
		}
		loaded++
	}
	if loaded != len(idx) {
		fail(fmt.Errorf("load %s not closed and not departed: %d of %d packages were loaded", loadID, loaded, len(idx)))
		return
	}

	// 6. close
	if err := f.CloseLoad(ctx, loadID); err != nil {
		fail(err)
		return
	}

	// 7. seal and depart
	seal := fmt.Sprintf("%s-SEAL-%03d", f.RunID, f.next())
	each(func(s *Shipment) { s.Seal = seal })
	if err := f.SealAndDepart(ctx, v, seal); err != nil {
		fail(err)
		return
	}
	each(func(s *Shipment) { s.Departed = true })

	// 8. the departure is consumed
	l, err = f.awaitLoad(ctx, loadID, fmt.Sprintf("load %s SHIP_CONFIRMED after trailer %s left", loadID, v.TrailerID), func(l Load) string {
		if l.State != LoadShipConfirmed {
			return fmt.Sprintf("the load is still %s (TrailerDeparted not consumed, or it carried no seal)", l.State)
		}
		return ""
	})
	each(func(s *Shipment) { s.LoadState = l.State })
	if err != nil {
		fail(err)
		return
	}
	f.logf("floor", "load %s shipped on trailer %s sealed %s with %d package(s)", loadID, v.TrailerID, seal, len(idx))
}
