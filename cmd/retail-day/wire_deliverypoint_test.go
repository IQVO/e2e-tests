package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/claudioed/warehouse-e2e/internal/retailday/dlq"
	"github.com/claudioed/warehouse-e2e/internal/sim/transport"
)

// ---- the tap over a read-only source ----

// logSource is one topic of one partition whose log the test appends to; every call is counted by kind so a
// test can prove the tap only reads.
type logSource struct {
	mu      sync.Mutex
	topic   string
	log     []dlq.Message
	missing bool  // the topic is not on the broker
	failing error // every call answers this
	calls   map[string]int
}

func (s *logSource) count(k string) { s.calls[k]++ }

func (s *logSource) Topics(context.Context) ([]dlq.Topic, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.count("Topics")
	if s.failing != nil {
		return nil, s.failing
	}
	topics := []dlq.Topic{{Name: "retailnetwork.other.events", Partitions: []int{0}}}
	if !s.missing {
		topics = append(topics, dlq.Topic{Name: s.topic, Partitions: []int{0}})
	}
	return topics, nil
}

func (s *logSource) Offsets(_ context.Context, p dlq.Partition) (dlq.Offsets, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.count("Offsets")
	if p.Topic != s.topic {
		return dlq.Offsets{}, fmt.Errorf("asked about topic %s, not %s", p.Topic, s.topic)
	}
	return dlq.Offsets{Start: 0, End: int64(len(s.log))}, nil
}

func (s *logSource) Read(_ context.Context, p dlq.Partition, from, to int64) ([]dlq.Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.count("Read")
	if s.failing != nil {
		return nil, s.failing
	}
	return append([]dlq.Message(nil), s.log[from:to]...), nil
}

func (s *logSource) add(typ, body string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v := fmt.Sprintf(`{"specversion":"1.0","type":"com.retailnetwork.logistics.last-mile-delivery.deliverypointprofile.%s","data":%s}`, typ, body)
	s.log = append(s.log, dlq.Message{Offset: int64(len(s.log)), Value: []byte(v)})
}

func newLogSource() *logSource {
	return &logSource{topic: transport.TopicLastMile, calls: map[string]int{}}
}

const flagBody = `{"stopKey":"k","reason":"NOT_HOME","failures":2,"trackingNumbers":["RN0000000000000A"],"flaggedAt":"2026-11-18T10:00:00Z"}`

func TestTheTapSeesOnlyFlagsAfterItsSnapshotThatCarryATrackedRef(t *testing.T) {
	src := newLogSource()
	src.add("DeliveryPointFlagged", flagBody) // an earlier day's flag for the same number
	tap := newTopicTap(src, transport.TopicLastMile)
	tap.Track("RN0000000000000A") // the snapshot
	if n := tap.Seen("RN0000000000000A", transport.EventDeliveryPointFlagged); n != 0 {
		t.Fatalf("saw %d message(s) published before the snapshot", n)
	}
	src.add("DeliveryWaveScheduled", `{"note":"RN0000000000000A"}`) // another type carrying the ref
	src.add("DeliveryPointFlagged", strings.ReplaceAll(flagBody, "RN0000000000000A", "RN0000000000000B"))
	if n := tap.Seen("RN0000000000000A", transport.EventDeliveryPointFlagged); n != 0 {
		t.Fatalf("saw %d for a ref no new flag names (another type, another package)", n)
	}
	src.add("DeliveryPointFlagged", flagBody)
	if n := tap.Seen("RN0000000000000A", transport.EventDeliveryPointFlagged); n != 1 {
		t.Fatalf("saw %d, want the one new flag", n)
	}
	if n := tap.Seen("RN0000000000000A", transport.EventDeliveryPointFlagged); n != 1 {
		t.Fatalf("a second read counted %d: a message must be read once", n)
	}
	if n := tap.Seen("RN0000000000000A", "PackageDeliveryPointFlagged"); n != 0 {
		t.Fatalf("another type counted %d", n)
	}
	if err := tap.Err(); err != nil {
		t.Fatalf("err = %v", err)
	}
	for _, kind := range []string{"Topics", "Offsets", "Read"} { // the only verbs a dlq.Source has: all reads
		if src.calls[kind] == 0 {
			t.Fatalf("the tap never used %s: %v", kind, src.calls)
		}
	}
}

func TestTheTapReadsBinaryModeEventsByTheirHeader(t *testing.T) {
	src := newLogSource()
	tap := newTopicTap(src, transport.TopicLastMile)
	tap.Track("x")
	src.mu.Lock()
	src.log = append(src.log, dlq.Message{Value: []byte(`{"trackingNumbers":["RN1"]}`), Headers: []dlq.Header{{Key: "ce_type", Value: []byte("com.retailnetwork.logistics.last-mile-delivery.deliverypointprofile.DeliveryPointFlagged")}}})
	src.mu.Unlock()
	if n := tap.Seen("RN1", transport.EventDeliveryPointFlagged); n != 1 {
		t.Fatalf("saw %d", n)
	}
}

// A tap that cannot look must say so (Err), never read as "nothing was published".
func TestATapThatCannotLookSaysWhy(t *testing.T) {
	for name, set := range map[string]func(*logSource){
		"the broker does not answer": func(s *logSource) { s.failing = errors.New("connection refused") },
		"the topic is not there":     func(s *logSource) { s.missing = true },
	} {
		t.Run(name, func(t *testing.T) {
			src := newLogSource()
			set(src)
			tap := newTopicTap(src, transport.TopicLastMile)
			tap.Track("RN1")
			if n := tap.Seen("RN1", transport.EventDeliveryPointFlagged); n != 0 {
				t.Fatalf("saw %d", n)
			}
			if err := tap.Err(); err == nil || !strings.Contains(err.Error(), transport.TopicLastMile) {
				t.Fatalf("err = %v, want one naming the topic", err)
			}
		})
	}
}

// A broker that was down at the first Track and is back at the first Seen: the tap reads from there and says
// that flags published before it may be missed, instead of looking clean.
func TestASnapshotTakenLateIsReported(t *testing.T) {
	src := newLogSource()
	src.failing = errors.New("connection refused")
	tap := newTopicTap(src, transport.TopicLastMile)
	tap.Track("RN1")
	src.mu.Lock()
	src.failing = nil
	src.mu.Unlock()
	_ = tap.Seen("RN1", transport.EventDeliveryPointFlagged)
	if err := tap.Err(); err == nil || !strings.Contains(err.Error(), "after the attempts") {
		t.Fatalf("err = %v", err)
	}
}

func TestAReadThatRecoversClearsItsError(t *testing.T) {
	src := newLogSource()
	tap := newTopicTap(src, transport.TopicLastMile)
	tap.Track("RN1")
	src.mu.Lock()
	src.failing = errors.New("timeout")
	src.mu.Unlock()
	_ = tap.Seen("RN1", transport.EventDeliveryPointFlagged)
	if tap.Err() == nil {
		t.Fatal("a failed read left no error")
	}
	src.mu.Lock()
	src.failing = nil
	src.mu.Unlock()
	src.add("DeliveryPointFlagged", strings.ReplaceAll(flagBody, "A", "1"))
	_ = tap.Seen("RN1", transport.EventDeliveryPointFlagged)
	if err := tap.Err(); err != nil {
		t.Fatalf("err = %v after a good read", err)
	}
}

// ---- the carrier: one order runs the journey, alone ----

type optLog struct {
	mu      sync.Mutex
	byOrder map[string]transport.Options
}

type gatedParcels struct {
	log     *optLog
	enter   chan string   // an order that started
	release chan struct{} // lets the first order finish
	out     transport.Delivery
}

func (g *gatedParcels) DeliverOrder(_ context.Context, orderID string, _ int, _, _ string, o transport.Options) transport.Delivery {
	g.log.mu.Lock()
	g.log.byOrder[orderID] = o
	g.log.mu.Unlock()
	g.enter <- orderID
	if orderID == "ord-1" {
		<-g.release
	}
	return g.out
}

func TestOnlyTheFirstOrderRunsTheDeliveryPointJourneyAndItRunsAlone(t *testing.T) {
	var logged []string
	var lmu sync.Mutex
	pj := newPointJourney(func(f string, a ...any) { lmu.Lock(); logged = append(logged, fmt.Sprintf(f, a...)); lmu.Unlock() })
	g := &gatedParcels{log: &optLog{byOrder: map[string]transport.Options{}}, enter: make(chan string, 4), release: make(chan struct{}),
		out: transport.Delivery{DeliveredAt: time.Now(), Flagged: []string{"RN1"}, FlagReason: transport.DetailNotHome}}
	a := carrierAdapter{t: g, route: func(id string) (string, string, error) { return "o", "d", nil }, point: pj}

	done := make(chan struct{})
	go func() { a.DeliverOrder(context.Background(), "ord-1", 1); close(done) }()
	if first := <-g.enter; first != "ord-1" {
		t.Fatalf("first in: %s", first)
	}
	second := make(chan struct{})
	go func() { a.DeliverOrder(context.Background(), "ord-2", 1); close(second) }()
	select {
	case id := <-g.enter:
		t.Fatalf("order %s was delivered while the delivery-point journey was running: its DELIVERED attempt would end the point's episode", id)
	case <-time.After(150 * time.Millisecond):
	}
	close(g.release)
	<-done
	if id := <-g.enter; id != "ord-2" {
		t.Fatalf("second in: %s", id)
	}
	<-second

	if o := g.log.byOrder["ord-1"]; o.FailedAttempts != transport.FlagAfterStrikes || o.FailureDetail != transport.DetailNotHome {
		t.Fatalf("first order options = %+v", o)
	}
	if o := g.log.byOrder["ord-2"]; o != (transport.Options{}) {
		t.Fatalf("second order options = %+v, want the default", o)
	}
	lmu.Lock()
	defer lmu.Unlock()
	if len(logged) != 1 || !strings.Contains(logged[0], "DeliveryPointFlagged was observed") || !strings.Contains(logged[0], "ord-1") {
		t.Fatalf("log = %q", logged)
	}
}

func TestADeliveryPointJourneyThatSawNoFlagSaysSoInTheLog(t *testing.T) {
	var logged []string
	pj := newPointJourney(func(f string, a ...any) { logged = append(logged, fmt.Sprintf(f, a...)) })
	g := &gatedParcels{log: &optLog{byOrder: map[string]transport.Options{}}, enter: make(chan string, 1), release: make(chan struct{}),
		out: transport.Delivery{DeliveredAt: time.Now(), Problems: []string{"no flag"}}}
	close(g.release)
	a := carrierAdapter{t: g, route: func(string) (string, string, error) { return "o", "d", nil }, point: pj}
	got := a.DeliverOrder(context.Background(), "ord-1", 1)
	if len(logged) != 1 || !strings.Contains(logged[0], "NO flag was observed") {
		t.Fatalf("log = %q", logged)
	}
	if len(got.Problems) != 1 {
		t.Fatalf("the finding must reach the day: %+v", got)
	}
}

func TestWithoutTheFlagEveryOrderIsDeliveredAsBefore(t *testing.T) {
	fp := &fakeParcels{out: transport.Delivery{DeliveredAt: time.Now()}}
	a := carrierAdapter{t: fp, route: func(string) (string, string, error) { return "o", "d", nil }}
	a.DeliverOrder(context.Background(), "ord-1", 1)
	if fp.opt != (transport.Options{}) {
		t.Fatalf("options = %+v", fp.opt)
	}
}

// ---- the wiring ----

type nullTap struct{ tracked []string }

func (n *nullTap) Track(ref string)     { n.tracked = append(n.tracked, ref) }
func (n *nullTap) Seen(_, _ string) int { return 0 }
func carrierOf(t *testing.T, cfg wireConfig) (carrierAdapter, func()) {
	t.Helper()
	p, cleanup, err := newPhases(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	a, ok := p.Carrier.(carrierAdapter)
	if !ok {
		t.Fatalf("carrier is %T", p.Carrier)
	}
	return a, cleanup
}

func TestDeliveryPointModeWiresTheJourneyAndTheTapFromTheBrokerAddress(t *testing.T) {
	var asked []string
	released := 0
	tap := &nullTap{}
	old := newFlagTap
	newFlagTap = func(addr string) (transport.FlagTap, func(), error) {
		asked = append(asked, addr)
		return tap, func() { released++ }, nil
	}
	t.Cleanup(func() { newFlagTap = old })
	_, cfg := phasesRig(t, func(c *wireConfig) {
		c.Brokers = "127.0.0.1:29092"
		c.DeliveryPoint = true
		c.DeadLetters = &fakeDead{known: true}
	}, nil)
	a, cleanup := carrierOf(t, cfg)
	if a.point == nil {
		t.Fatal("-delivery-point did not give the carrier a journey")
	}
	tr, ok := a.t.(*transport.Transport)
	if !ok || tr.Tap != transport.FlagTap(tap) {
		t.Fatalf("the transport's tap is %v, want the one built for the broker", tr.Tap)
	}
	if len(asked) != 1 || asked[0] != "127.0.0.1:29092" {
		t.Fatalf("the tap was built for %v, want the -brokers address once", asked)
	}
	if released != 0 {
		t.Fatal("the tap was released before the day ended")
	}
	cleanup()
	if released != 1 {
		t.Fatalf("the tap was released %d time(s), want 1", released)
	}
}

func TestDeliveryPointModeWithoutABrokerObservesTheAdvisoryOnlyAndSaysSo(t *testing.T) {
	old := newFlagTap
	newFlagTap = func(string) (transport.FlagTap, func(), error) {
		t.Fatal("a tap was built with no broker")
		return nil, nil, nil
	}
	t.Cleanup(func() { newFlagTap = old })
	var logged []string
	_, cfg := phasesRig(t, func(c *wireConfig) {
		c.DeliveryPoint = true
		c.Log = func(f string, a ...any) { logged = append(logged, fmt.Sprintf(f, a...)) }
	}, nil)
	a, cleanup := carrierOf(t, cfg)
	defer cleanup()
	if a.point == nil || a.t.(*transport.Transport).Tap != nil {
		t.Fatalf("point %v, tap %v", a.point, a.t.(*transport.Transport).Tap)
	}
	var said bool
	for _, l := range logged {
		said = said || strings.Contains(l, "tracking view only")
	}
	if !said {
		t.Fatalf("log = %q", logged)
	}
}

func TestADayWithoutTheFlagHasNoJourneyAndNoTap(t *testing.T) {
	old := newFlagTap
	newFlagTap = func(string) (transport.FlagTap, func(), error) {
		t.Fatal("a tap was built without -delivery-point")
		return nil, nil, nil
	}
	t.Cleanup(func() { newFlagTap = old })
	_, cfg := phasesRig(t, func(c *wireConfig) { c.Brokers = "127.0.0.1:29092"; c.DeadLetters = &fakeDead{known: true} }, nil)
	a, cleanup := carrierOf(t, cfg)
	defer cleanup()
	if a.point != nil || a.t.(*transport.Transport).Tap != nil {
		t.Fatal("the delivery-point journey is on without -delivery-point")
	}
}

func TestATapThatCannotBeBuiltStopsTheDayBeforeItStarts(t *testing.T) {
	old := newFlagTap
	newFlagTap = func(string) (transport.FlagTap, func(), error) { return nil, nil, errors.New("not host:port") }
	t.Cleanup(func() { newFlagTap = old })
	_, cfg := phasesRig(t, func(c *wireConfig) { c.Brokers = "x"; c.DeliveryPoint = true; c.DeadLetters = &fakeDead{known: true} }, nil)
	if _, _, err := newPhases(context.Background(), cfg); err == nil || !strings.Contains(err.Error(), "-delivery-point") {
		t.Fatalf("err = %v", err)
	}
}

func TestTheDeliveryPointFlagIsOffByDefaultAndReachesTheWiring(t *testing.T) {
	h := newHarness(t)
	h.run("-json")
	if h.gotCfg.DeliveryPoint {
		t.Error("the delivery-point journey is on by default: it changes the day (an order is delivered on its third attempt)")
	}
	h = newHarness(t)
	h.run("-json", "-delivery-point")
	if !h.gotCfg.DeliveryPoint {
		t.Error("-delivery-point did not reach the wiring")
	}
}

// A tap handed in through the config replaces the one built from -brokers (a test's fake, or a caller's own).
func TestAConfiguredTapIsTheOneTheTransportUses(t *testing.T) {
	old := newFlagTap
	newFlagTap = func(string) (transport.FlagTap, func(), error) {
		t.Fatal("a tap was built although one was configured")
		return nil, nil, nil
	}
	t.Cleanup(func() { newFlagTap = old })
	tap := &nullTap{}
	_, cfg := phasesRig(t, func(c *wireConfig) {
		c.Brokers = "127.0.0.1:29092"
		c.DeliveryPoint = true
		c.FlagTap = tap
		c.DeadLetters = &fakeDead{known: true}
	}, nil)
	a, cleanup := carrierOf(t, cfg)
	defer cleanup()
	if a.t.(*transport.Transport).Tap != transport.FlagTap(tap) {
		t.Fatal("the transport does not use the configured tap")
	}
}
