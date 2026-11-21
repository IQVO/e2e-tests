package floor

import (
	"context"
	"fmt"
	"net/url"
	"strings"
)

func truncate(b []byte) string {
	s := string(b)
	if len(s) > 200 {
		return s[:200] + "…"
	}
	return s
}

func (f *Floor) readReservations(ctx context.Context, orderID string) ([]reservationDTO, error) {
	res, r := listAll[reservationDTO](ctx, f.c, SvcInventory, "/reservations?demandRef", "/reservations?demandRef="+url.QueryEscape(orderID))
	if !r.OK() {
		return nil, refusal("read the reservations of order "+orderID, r)
	}
	return res, nil
}

// findReservation is the reservation of a line: the one order-management names, else the live one of
// the line's number. A REVOKED or EXPIRED one is passed over when a live one exists.
func findReservation(res []reservationDTO, ln *lineState) *reservationDTO {
	var fallback *reservationDTO
	for i := range res {
		x := &res[i]
		if ln.Reservation != "" && x.ID == ln.Reservation {
			return x
		}
		if ln.Reservation == "" && x.LineNo == ln.No {
			if x.Status == ReservationActive || x.Status == ReservationConfirmed {
				return x
			}
			fallback = x
		}
	}
	return fallback
}

// confirmStock waits (bounded, Settings.Lag) until inventory-storage shows every line's reservation
// CONFIRMED: the picks reached the ledger. In ConfirmByEvent mode that is its TaskCompleted consumer's
// work, so a reservation still ACTIVE after the bound means the consumer is not confirming. A REVOKED or
// EXPIRED reservation fails at once: its stock was released before it was picked. It returns the findings.
func (f *Floor) confirmStock(ctx context.Context, st *orderState) []string {
	what := "order " + st.id + ": inventory-storage confirms the picks"
	err := await(ctx, f.clock, f.set.Lag, what, func(ctx context.Context) (verdict, string, error) {
		res, err := f.readReservations(ctx, st.id)
		if err != nil {
			return 0, "", err
		}
		var active []string
		st.mu.Lock()
		lines := append([]*lineState(nil), st.lines...)
		st.mu.Unlock()
		for _, ln := range lines {
			x := findReservation(res, ln)
			switch {
			case x == nil:
				return 0, "", fmt.Errorf("line %d (%s) has no reservation in inventory-storage (demandRef %s): the pick consumed no stock", ln.No, ln.SKU, st.id)
			case x.Status == ReservationConfirmed:
			case x.Status == ReservationActive:
				active = append(active, fmt.Sprintf("%s (line %d)", x.ID, ln.No))
			default:
				return 0, "", fmt.Errorf("reservation %s of line %d is %s: its stock was released before the line was picked", x.ID, ln.No, x.Status)
			}
		}
		if len(active) > 0 {
			return vWait, "reservations still ACTIVE: " + strings.Join(active, ", ") + " (the TaskCompleted consumer of inventory-storage has not confirmed them)", nil
		}
		return vDone, "", nil
	})
	if err != nil {
		return []string{err.Error()}
	}
	return nil
}

// awaitManifest waits (bounded, Settings.Lag) until the tap has seen PackageManifested carrying the
// order id: SLAM raised it, which is what outbound-shipping takes over from. The tap must track the id.
func (f *Floor) awaitManifest(ctx context.Context, st *orderState) error {
	return await(ctx, f.clock, f.set.Lag, "order "+st.id+": SLAM raised PackageManifested", func(context.Context) (verdict, string, error) {
		if f.set.Events.Seen(st.id, TypePackageManifested) > 0 {
			return vDone, "", nil
		}
		return vWait, "no PackageManifested carrying the order id seen on the tap yet (is the tap tracking it?)", nil
	})
}
