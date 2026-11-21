package sim

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// EventTap reads the given topics (and every `.dlq` topic) from the offsets
// they were at when the run started (no consumer group: it never moves a
// service's offsets) and indexes what the run produced. It accepts the
// CloudEvents 1.0 structured envelope ("type"), CloudEvents binary mode
// ("ce_type" header) and the legacy flat envelope ("event_type"), so it audits
// whichever envelope the deployed images speak.
//
// Mu guards the exported maps: lock it to read them.
type EventTap struct {
	Mu     sync.Mutex
	Counts map[string]map[string]int // topic -> short type -> n
	ByRef  map[string]map[string]int // correlation id -> short type -> n
	Refs   []string
	Dlq    map[string]int // dead-lettered messages that belong to this run
	// DlqLegacy counts dead-lettered messages unrelated to this run (legacy
	// history replayed by a new consumer group on a shared broker).
	DlqLegacy map[string]int
	RunID     string
	Readers   []*kafkago.Reader
	Errs      []string
}

// StartTap opens one reader per partition of every topic in want (plus every
// `.dlq` topic) at the topic's current end. log may be nil.
func StartTap(ctx context.Context, brokers []string, runID string, topics []string, log LogFunc) (*EventTap, error) {
	t := &EventTap{Counts: map[string]map[string]int{}, ByRef: map[string]map[string]int{}, Dlq: map[string]int{}, DlqLegacy: map[string]int{}, RunID: runID}
	conn, err := IPv4Dialer().DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	all, err := conn.ReadPartitions()
	if err != nil {
		return nil, err
	}
	want := map[string]bool{}
	for _, tp := range topics {
		want[tp] = true
	}
	n := 0
	for _, p := range all {
		if !want[p.Topic] && !strings.HasSuffix(p.Topic, ".dlq") {
			continue
		}
		r := kafkago.NewReader(kafkago.ReaderConfig{
			Brokers: brokers, Topic: p.Topic, Partition: p.ID, MinBytes: 1, MaxBytes: 10 << 20, MaxWait: 500 * time.Millisecond,
			Dialer: IPv4Dialer(),
		})
		if err := r.SetOffset(kafkago.LastOffset); err != nil {
			return nil, err
		}
		t.Readers = append(t.Readers, r)
		n++
		go t.consume(ctx, r, p.Topic)
	}
	if log != nil {
		log("event-tap", "tapping %d partitions across %d integration + analytics topics and their DLQs", n, len(topics))
	}
	return t, nil
}

// Track registers a correlation id: every later message whose payload
// contains it is indexed under ByRef[ref].
func (t *EventTap) Track(ref string) {
	t.Mu.Lock()
	defer t.Mu.Unlock()
	t.Refs = append(t.Refs, ref)
}

// ShortType strips the CloudEvents type prefix: the last dotted segment.
func ShortType(s string) string {
	if i := strings.LastIndex(s, "."); i >= 0 {
		return s[i+1:]
	}
	return s
}

func (t *EventTap) consume(ctx context.Context, r *kafkago.Reader, topic string) {
	for {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() == nil && !errors.Is(err, context.Canceled) {
				t.Mu.Lock()
				t.Errs = append(t.Errs, fmt.Sprintf("%s: %v", topic, err))
				t.Mu.Unlock()
			}
			return
		}
		var env struct {
			Type      string `json:"type"`
			EventType string `json:"event_type"`
		}
		_ = json.Unmarshal(m.Value, &env)
		typ := env.Type
		if typ == "" {
			typ = env.EventType
		}
		for _, h := range m.Headers {
			if typ == "" && (h.Key == "ce_type" || h.Key == "ce-type") {
				typ = string(h.Value)
			}
		}
		typ = ShortType(typ)
		if typ == "" {
			typ = "?"
		}
		raw := string(m.Value)
		t.Mu.Lock()
		if strings.HasSuffix(topic, ".dlq") {
			// A shared, long-lived broker dead-letters legacy (pre-CloudEvents)
			// messages whenever a consumer group replays history. Those are
			// not today's failures; count them separately and fail the run
			// only on dead-lettered messages that mention this run.
			if t.RunID != "" && strings.Contains(raw, t.RunID) {
				t.Dlq[topic]++
			} else {
				t.DlqLegacy[topic]++
			}
		} else {
			if t.Counts[topic] == nil {
				t.Counts[topic] = map[string]int{}
			}
			t.Counts[topic][typ]++
			for _, ref := range t.Refs {
				if strings.Contains(raw, ref) {
					if t.ByRef[ref] == nil {
						t.ByRef[ref] = map[string]int{}
					}
					t.ByRef[ref][typ]++
				}
			}
		}
		t.Mu.Unlock()
	}
}

// Seen is how many messages carrying ref had the given short type.
func (t *EventTap) Seen(ref, typ string) int {
	t.Mu.Lock()
	defer t.Mu.Unlock()
	return t.ByRef[ref][typ]
}

// Total is how many messages of the given short type the run produced on any topic.
func (t *EventTap) Total(typ string) int {
	t.Mu.Lock()
	defer t.Mu.Unlock()
	n := 0
	for _, byType := range t.Counts {
		n += byType[typ]
	}
	return n
}

// TotalOn is how many messages of the given short type the run produced on
// the given topics only. The short type drops the producer's prefix, so two
// contexts publishing the same name (workforce-management's and
// wes-work-planning's ShiftPlanCommitted) count together in Total; the
// producer's own topics tell them apart.
func (t *EventTap) TotalOn(typ string, topics ...string) int {
	t.Mu.Lock()
	defer t.Mu.Unlock()
	n := 0
	for _, topic := range topics {
		n += t.Counts[topic][typ]
	}
	return n
}

// Close stops every reader.
func (t *EventTap) Close() {
	for _, r := range t.Readers {
		_ = r.Close()
	}
}

// IPv4Dialer forces tcp4. The broker's EXTERNAL listener is published on
// 127.0.0.1 only and advertises "localhost:9092"; Go resolves that to [::1]
// first, so every partition reader would fail with "connection refused".
func IPv4Dialer() *kafkago.Dialer {
	nd := &net.Dialer{Timeout: 10 * time.Second}
	return &kafkago.Dialer{
		Timeout: 10 * time.Second,
		DialFunc: func(ctx context.Context, _ string, addr string) (net.Conn, error) {
			return nd.DialContext(ctx, "tcp4", addr)
		},
	}
}
