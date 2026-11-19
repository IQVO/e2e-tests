package transport

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"
)

// The delivery-point chain (roadmap C5), which the driver's outcomeDetail activates:
//
//	last-mile-delivery  RecordAttempt: two counted failed visits (routes) at one stop key ->
//	                    DeliveryPointFlagged {stopKey, reason, failures, trackingNumbers, flaggedAt}
//	                    on retailnetwork.last-mile-delivery.events (its ADR 0014-delivery-point-profile)
//	shipping-and-tracking relays it per package by order id as PackageDeliveryPointFlagged
//	                    (its ADR 0014-relay-the-delivery-point-flag-by-order-id)
//	customer-ordering   shows `deliveryPointAdvisory {reason, flaggedAt}` on GET /orders/{id}/tracking
//	                    while the package has not ended (its ADR 0015)
//
// last-mile-delivery has no REST read of the profile (its ADR 0014 addendum: no GET /delivery-points),
// so the flag is observed in the two places it can be: as the event, on a read-only tap of its topic, and
// as the advisory on the order's tracking view. The actor never reads Kafka itself: the tap is injected.

// EventDeliveryPointFlagged is the short type of last-mile-delivery's event (sim.ShortType of its
// CloudEvents type com.retailnetwork.logistics.last-mile-delivery.deliverypointprofile.DeliveryPointFlagged).
const EventDeliveryPointFlagged = "DeliveryPointFlagged"

// TopicLastMile is the topic last-mile-delivery publishes DeliveryPointFlagged on; the tap must read it.
const TopicLastMile = "retailnetwork.last-mile-delivery.events"

// FlagTap is the part of *sim.EventTap the flag observation uses. Track registers a correlation id (the
// tap indexes only the messages that arrive after it), Seen counts the messages carrying it of a type.
type FlagTap interface {
	Track(ref string)
	Seen(ref, typ string) int
}

// trackFlags registers each tracking number on the tap once (EventTap.Track appends: twice would count
// every event twice). It must run before the attempt that raises the flag.
func (t *Transport) trackFlags(tns []string) {
	if t.Tap == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.tracked == nil {
		t.tracked = map[string]bool{}
	}
	for _, tn := range tns {
		if !t.tracked[tn] {
			t.tracked[tn] = true
			t.Tap.Track(tn)
		}
	}
}

// awaitFlag waits, bounded by Settings.JourneyTimeout per leg, for the flag the second counted failure must
// have raised on packages tns: the DeliveryPointFlagged event on the Tap (when there is one) naming at least
// one of them, then the advisory with the expected reason on the tracking view of orderID (when it is known).
// Which packages of a stop the event names is last-mile-delivery's rule (a strike holds ONE tracking number
// per visit, so the first package of the visit is named), so at least one package is what is required.
// Every missing leg is a finding saying which one; d.Flagged and d.FlagReason are set from what was seen.
func (t *Transport) awaitFlag(ctx context.Context, d *Delivery, orderID string, tns []string, reason string) {
	add := func(format string, a ...any) { d.Problems = append(d.Problems, fmt.Sprintf(format, a...)) }
	if t.Tap == nil && orderID == "" {
		add("delivery-point flag expected for %v but it cannot be observed: no event tap and no order id", tns)
		return
	}
	var flagged []string
	if t.Tap != nil {
		var seen []string
		err := await(ctx, t.Clock, t.Settings.JourneyTimeout, t.Settings.PollInterval, func(context.Context) (bool, string, error) {
			seen = seen[:0]
			for _, tn := range tns {
				if t.Tap.Seen(tn, EventDeliveryPointFlagged) > 0 {
					seen = append(seen, tn)
				}
			}
			return len(seen) > 0, fmt.Sprintf("%s seen for %d of %d packages", EventDeliveryPointFlagged, len(seen), len(tns)), nil
		})
		if err != nil {
			why := ""
			if te, ok := t.Tap.(interface{ Err() error }); ok && te.Err() != nil {
				why = " (the tap could not read the topic: " + te.Err().Error() + ")" // not "published nothing": it could not look
			}
			add("delivery point of %v: last-mile-delivery published no %s after two %s attempts (%s): %s%s", tns, EventDeliveryPointFlagged, reason, TopicLastMile, err, why)
		} else {
			flagged = append(flagged, seen...)
		}
	}
	if orderID != "" {
		adv, err := t.Tracker.AwaitAdvisory(ctx, orderID, tns, reason)
		if err != nil {
			add("%s", err)
		} else {
			for tn := range adv {
				if !slices.Contains(flagged, tn) {
					flagged = append(flagged, tn)
				}
			}
		}
	}
	slices.Sort(flagged)
	d.Flagged, d.FlagReason = flagged, ""
	if len(flagged) > 0 {
		d.FlagReason = reason
	}
}

// awaitAdvisoryEnded waits for the advisory to leave the tracking view of the delivered packages: it ends
// with the package (customer-ordering ADR 0015, rule 4: there is no "cleared" event).
func (t *Transport) awaitAdvisoryEnded(ctx context.Context, d *Delivery, orderID string, flagged, delivered []string) {
	var still []string
	for _, tn := range flagged {
		if slices.Contains(delivered, tn) {
			still = append(still, tn)
		}
	}
	if len(still) == 0 {
		return
	}
	err := await(ctx, t.Clock, t.Settings.JourneyTimeout, t.Settings.PollInterval, func(ctx context.Context) (bool, string, error) {
		o, err := t.Tracker.Order(ctx, orderID)
		if err != nil {
			if transient(err) {
				return false, err.Error(), nil
			}
			return false, "", err
		}
		var shown []string
		for _, p := range o.Packages {
			if p.DeliveryPointAdvisory != nil && slices.Contains(still, p.TrackingNumber) {
				shown = append(shown, p.TrackingNumber+" "+p.State)
			}
		}
		return len(shown) == 0, "advisory still shown for " + strings.Join(shown, ", "), nil
	})
	if err != nil {
		d.Problems = append(d.Problems, fmt.Sprintf("delivered packages %v still carry the delivery-point advisory on the tracking view of order %s (it ends with the package): %s", still, orderID, err))
	}
}

// Advisory is customer-ordering's deliveryPointAdvisory of one package: a reason code and the instant.
type Advisory struct {
	Reason    string    `json:"reason"`
	FlaggedAt time.Time `json:"flaggedAt"`
}

// AwaitAdvisory polls the order's tracking view until at least one of tns carries a deliveryPointAdvisory
// and returns the advisories by tracking number. The view is event-fed (the flag crosses two Kafka hops),
// so an unknown order, a 5xx and a transport error are "not yet". A reason outside the contract's two codes,
// or other than want (when want is not empty), ends the wait at once: waiting cannot change it.
func (t *Tracker) AwaitAdvisory(ctx context.Context, orderID string, tns []string, want string) (map[string]Advisory, error) {
	got := map[string]Advisory{}
	err := await(ctx, t.Clock, t.Settings.JourneyTimeout, t.Settings.PollInterval, func(ctx context.Context) (bool, string, error) {
		o, err := t.Order(ctx, orderID)
		if err != nil {
			if transient(err) {
				return false, err.Error(), nil
			}
			return false, "", err
		}
		clear(got)
		states := make([]string, 0, len(tns))
		for _, p := range o.Packages {
			if !slices.Contains(tns, p.TrackingNumber) {
				continue
			}
			states = append(states, p.TrackingNumber+" "+p.State)
			a := p.DeliveryPointAdvisory
			if a == nil {
				continue
			}
			if a.Reason != DetailNotHome && a.Reason != DetailAccessProblem {
				return false, "", fmt.Errorf("order %s package %s: deliveryPointAdvisory.reason %q is not NOT_HOME or ACCESS_PROBLEM", orderID, p.TrackingNumber, a.Reason)
			}
			if want != "" && a.Reason != want {
				return false, "", fmt.Errorf("order %s package %s: deliveryPointAdvisory.reason is %s, want %s (the reason the failed attempts reported)", orderID, p.TrackingNumber, a.Reason, want)
			}
			got[p.TrackingNumber] = *a
		}
		return len(got) > 0, "no deliveryPointAdvisory on " + strings.Join(states, ", "), nil
	})
	if err != nil {
		return nil, fmt.Errorf("delivery-point advisory of order %s: customer-ordering GET /orders/{id}/tracking: %w", orderID, err)
	}
	return maps.Clone(got), nil
}
