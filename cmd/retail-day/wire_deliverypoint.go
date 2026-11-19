package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/claudioed/warehouse-e2e/internal/retailday/dlq"
	"github.com/claudioed/warehouse-e2e/internal/sim"
	"github.com/claudioed/warehouse-e2e/internal/sim/transport"
)

// The delivery-point journey (-delivery-point, gap G67): the driver reports outcomeDetail, last-mile-delivery
// flags a stop after two counted failed visits, and the run must SEE DeliveryPointFlagged.
//
// ONE order of the day, the first one delivered, is made to fail twice (NOT_HOME on both attempts, each
// followed by a re-induction onto a later wave and a new route) before it is delivered; every other order is
// delivered as before. The flag is observed
//
//   - as the event, on last-mile-delivery's topic (transport.TopicLastMile), by a read-only tap over the
//     same -brokers address the dead letters are counted through (no group, no commit, no write), and
//   - downstream, as the deliveryPointAdvisory on customer-ordering's tracking view of the order.
//
// Why one order, and under a lock: every simulated buyer lives at the same address ("1 Simulation Way",
// prefix + "01"), so the orders of one postal prefix are one delivery point. A DELIVERED attempt ends the
// point's episode (last-mile-delivery ADR 0014, step 4), so a parallel normal delivery would clear the first
// strike of the flagged order and the flag would never come: a false finding. The flagged order therefore
// runs alone: it takes the write side of a RWMutex, every other delivery the read side.

// pointJourney is the one delivery-point journey of a day.
type pointJourney struct {
	opt   transport.Options
	log   func(format string, args ...any)
	taken atomic.Bool
	gate  sync.RWMutex
}

// newPointJourney is the journey every -delivery-point day plays: two NOT_HOME failures, then the delivery.
func newPointJourney(log func(format string, args ...any)) *pointJourney {
	return &pointJourney{
		opt: transport.Options{FailedAttempts: transport.FlagAfterStrikes, FailureDetail: transport.DetailNotHome},
		log: log,
	}
}

// begin returns the options of this delivery and what to call when it is over: the first order gets the
// journey (exclusively), the others the default (shared).
func (p *pointJourney) begin(normal transport.Options) (opt transport.Options, flagged bool, done func()) {
	if p.taken.CompareAndSwap(false, true) {
		p.gate.Lock()
		return p.opt, true, p.gate.Unlock
	}
	p.gate.RLock()
	return normal, false, p.gate.RUnlock
}

// report logs what the journey saw; a problem is the day's finding (it travels in Delivery.Problems).
func (p *pointJourney) report(orderID string, d transport.Delivery) {
	if p.log == nil {
		return
	}
	switch {
	case len(d.Flagged) > 0:
		p.log("delivery point: order %s failed %d times (%s) and DeliveryPointFlagged was observed for %v (reason %s)",
			orderID, p.opt.FailedAttempts, p.opt.FailureDetail, d.Flagged, d.FlagReason)
	default:
		p.log("delivery point: order %s failed %d times (%s) and NO flag was observed (see its findings)", orderID, p.opt.FailedAttempts, p.opt.FailureDetail)
	}
}

// ---- the tap ----

// newFlagTap builds the read-only tap of last-mile-delivery's topic for a broker address; release closes
// what it opened. A variable so a test can hand the wiring a fake. It dials nothing: the first read does.
var newFlagTap = func(addr string) (transport.FlagTap, func(), error) {
	src, err := dlq.NewKafkaSource(addr)
	if err != nil {
		return nil, nil, err
	}
	return newTopicTap(src, transport.TopicLastMile), src.Close, nil
}

// topicTap is a transport.FlagTap over a read-only dlq.Source (one broker address, the one the k3s
// port-forward gives: sim.EventTap's reader cannot follow the broker's advertised in-cluster name). It
// snapshots the end offset of every partition of the topic at the first Track, so only what the day
// produced is read, and Seen reads from the snapshot to the end now, keeping the DeliveryPointFlagged
// messages (a message matches a ref by substring of its value, like sim.EventTap).
type topicTap struct {
	src   dlq.Source
	topic string

	mu      sync.Mutex
	started bool
	cursor  map[int]int64
	hits    []string // values of the DeliveryPointFlagged messages read so far
	// why the tap could not look: the snapshot, the last read, and a snapshot that came too late
	snapErr, readErr error
	late             bool
}

const topicTapCall = 20 * time.Second

func newTopicTap(src dlq.Source, topic string) *topicTap {
	return &topicTap{src: src, topic: topic, cursor: map[int]int64{}}
}

var _ transport.FlagTap = (*topicTap)(nil)

// Track takes the snapshot the first time it is called, before the packages it names can fail.
func (t *topicTap) Track(string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.start()
}

// start takes the snapshot; the caller holds mu.
func (t *topicTap) start() {
	if t.started {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), topicTapCall)
	defer cancel()
	cur, err := t.ends(ctx)
	if err != nil {
		t.snapErr = fmt.Errorf("snapshot of %s: %w", t.topic, err)
		return
	}
	t.cursor, t.started, t.snapErr = cur, true, nil
}

// ends lists the end offset of every partition of the topic.
func (t *topicTap) ends(ctx context.Context) (map[int]int64, error) {
	topics, err := t.src.Topics(ctx)
	if err != nil {
		return nil, err
	}
	for _, tp := range topics {
		if tp.Name != t.topic {
			continue
		}
		if tp.Err != nil {
			return nil, fmt.Errorf("metadata: %w", tp.Err)
		}
		out := map[int]int64{}
		for _, id := range tp.Partitions {
			o, err := t.src.Offsets(ctx, dlq.Partition{Topic: t.topic, ID: id})
			if err != nil {
				return nil, fmt.Errorf("partition %d: %w", id, err)
			}
			out[id] = o.End
		}
		return out, nil
	}
	return nil, fmt.Errorf("the topic is not on the broker")
}

// Seen counts the DeliveryPointFlagged messages the day produced that carry ref. A read that fails is not
// "nothing published": it is kept for Err and the count so far is returned.
func (t *topicTap) Seen(ref, typ string) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.started {
		t.start()
		t.late = t.started
	}
	if t.started {
		t.poll()
	}
	n := 0
	for _, v := range t.hits {
		if typ == transport.EventDeliveryPointFlagged && strings.Contains(v, ref) {
			n++
		}
	}
	return n
}

// poll reads every partition from its cursor to the end now; the caller holds mu.
func (t *topicTap) poll() {
	ctx, cancel := context.WithTimeout(context.Background(), topicTapCall)
	defer cancel()
	ends, err := t.ends(ctx)
	if err != nil {
		t.readErr = fmt.Errorf("read of %s: %w", t.topic, err)
		return
	}
	for id, end := range ends {
		from := t.cursor[id]
		if end <= from {
			continue
		}
		msgs, err := t.src.Read(ctx, dlq.Partition{Topic: t.topic, ID: id}, from, end)
		if err != nil {
			t.readErr = fmt.Errorf("read of %s[%d]: %w", t.topic, id, err)
			return
		}
		for _, m := range msgs {
			if messageType(m) == transport.EventDeliveryPointFlagged {
				t.hits = append(t.hits, string(m.Value))
			}
		}
		t.cursor[id] = end
	}
	t.readErr = nil
}

// Err is why the tap could not look, or looked too late: nil when the snapshot was taken before the first
// attempt and the last read worked.
func (t *topicTap) Err() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	var errs []error
	if t.snapErr != nil {
		errs = append(errs, t.snapErr)
	}
	if t.readErr != nil {
		errs = append(errs, t.readErr)
	}
	if t.late {
		errs = append(errs, fmt.Errorf("the snapshot was taken after the attempts: flags published before it are not seen"))
	}
	return errors.Join(errs...)
}

// messageType is the short event type of a CloudEvents message in structured mode (`type` in the value) or
// binary mode (`ce_type` header), as sim.EventTap reads it.
func messageType(m dlq.Message) string {
	var env struct {
		Type string `json:"type"`
	}
	_ = json.Unmarshal(m.Value, &env)
	typ := env.Type
	for _, h := range m.Headers {
		if typ == "" && (h.Key == "ce_type" || h.Key == "ce-type") {
			typ = string(h.Value)
		}
	}
	return sim.ShortType(typ)
}
