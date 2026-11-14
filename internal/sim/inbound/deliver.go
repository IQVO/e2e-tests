package inbound

import (
	"context"
)

// Deliver runs one truck from the supplier's ASN to the stowed stock, over published REST only, in the order the
// real services need:
//
//  1. (with a tap) track the SKUs, so the staged handover can be seen later;
//  2. the supplier registers the ASN (RegisterASN);
//  3. the dock planner books an inbound door for it (BookDoor);
//  4. the carrier arrives: the clerk checks the visit in, docks the trailer and starts door work (GateIn);
//  5. the receiver opens the receipt on the appointment, counts every line Good and closes it (OpenReceipt,
//     Receive): the close is what releases the unload in yard-management and hands the lines to inventory-storage;
//  6. (with a tap) wait until inventory-storage staged the units (StockReceived);
//  7. the stower puts what the receipt counted into bins (StowReceipt);
//  8. the clerk waits, by reading, for the unload to be released and checks the visit out (Release);
//  9. the planner waits until dock-scheduling shows the appointment COMPLETED (AwaitCompleted).
//
// A step a service refuses, or that does not happen within its bound, is a finding in Problems; the run stops where
// it cannot go on and Delivery says how far it got. Nothing here panics. Two things deliberately do not stop it: a
// receipt closed with a discrepancy is reported and its units are still stowed, and a stow that finds no room is
// reported and the truck still leaves (it is empty; the goods are staged, not stowed). A receipt that could not be
// opened or fully counted leaves the trailer docked with its unload open, and the finding says so.
func (c *Crew) Deliver(ctx context.Context, lines []Line) Delivery {
	d := Delivery{Lines: normalise(lines), Received: map[string]int{}}
	if len(d.Lines) == 0 {
		d.problem("deliver: no line with a SKU and a positive quantity")
		return d
	}
	base := c.trackStaged(d.Lines)

	asn, err := c.RegisterASN(ctx, d.Lines)
	if err != nil {
		d.problem("%v", err)
		return d
	}
	d.ASN = asn.Number

	a, err := c.BookDoor(ctx, asn.Number, c.NextWindow())
	if err != nil {
		d.problem("%v", err)
		return d
	}
	d.AppointmentID, d.Door = a.ID, a.Door

	v, err := c.GateIn(ctx, a)
	d.VisitID, d.TrailerID = v.ID, v.TrailerID
	if err != nil {
		d.problem("%v", err)
		return d
	}

	rc, err := c.OpenReceipt(ctx, asn, a)
	d.ReceiptID = rc.ID
	if err != nil {
		d.problem("%v", err)
		d.problem("trailer %s stays docked at door %s with its unload open: the receipt was never opened, so the yard cannot release it", v.TrailerID, a.Door)
		return d
	}
	rc, err = c.Receive(ctx, asn, rc)
	for _, l := range rc.Lines {
		if l.ReceivedGood > 0 {
			d.Received[l.SKU] = l.ReceivedGood
		}
	}
	if err != nil {
		d.problem("%v", err)
		d.problem("receipt %s was not fully counted and closed: trailer %s stays docked at door %s with its unload open", rc.ID, v.TrailerID, a.Door)
		return d
	}
	for _, x := range rc.Discrepancies {
		d.problem("receipt %s closed with a discrepancy: %s", rc.ID, x)
	}

	if err := c.awaitStaged(ctx, base, d.Lines); err != nil {
		d.problem("%v", err)
		d.problem("stow skipped: stowing units inventory-storage never staged would hide a broken handover")
	} else {
		stowed, err := c.StowReceipt(ctx, rc)
		d.Stowed = stowed
		for _, m := range splitErr(err) {
			d.problem("%s", m)
		}
	}

	if err := c.Release(ctx, v); err != nil {
		d.problem("%v", err)
		return d
	}
	d.Released = true
	if err := c.AwaitCompleted(ctx, a); err != nil {
		d.problem("%v", err)
		return d
	}
	d.Completed = true
	c.logf("restock", "%s delivered: %s", asn.Number, describeLines(d.Lines))
	return d
}
