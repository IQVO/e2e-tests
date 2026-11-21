package dlq

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeKafka is the part of a broker the counter reads: topics with partitions, each a log of
// messages that starts at a log-start offset. It records every call, so a test can assert what
// was read (and that nothing else was).
type fakeKafka struct {
	mu     sync.Mutex
	topics map[string]*fakeTopic
	calls  []string

	// failure hooks
	listErr    error
	failOffs   map[string]error // by topic name
	failRead   map[string]error
	listErrNow func() error // consulted on every Topics call before listErr, when set
	hang       bool         // every call blocks until its context ends
	deaf       bool         // every call blocks forever, ignoring its context
	panicOn    string       // method name that panics
	extra      func(p Partition, from, to int64, log []Message) []Message
	onCall     func(call string) // runs on every call, before it is answered
}

type fakeTopic struct {
	err   error // metadata error
	parts [][]Message
	start []int64 // log start offset per partition
}

func newFake() *fakeKafka {
	return &fakeKafka{topics: map[string]*fakeTopic{}, failOffs: map[string]error{}, failRead: map[string]error{}}
}

// topic creates (or returns) a topic with n partitions.
func (f *fakeKafka) topic(name string, n int) *fakeTopic {
	f.mu.Lock()
	defer f.mu.Unlock()
	if t, ok := f.topics[name]; ok {
		return t
	}
	t := &fakeTopic{parts: make([][]Message, n), start: make([]int64, n)}
	f.topics[name] = t
	return t
}

// add appends one message to a partition and returns its offset.
func (f *fakeKafka) add(topic string, part int, key, value string, headers ...Header) int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	t := f.topics[topic]
	off := t.start[part] + int64(len(t.parts[part]))
	t.parts[part] = append(t.parts[part], Message{Offset: off, Key: []byte(key), Value: []byte(value), Headers: headers})
	return off
}

func (f *fakeKafka) drop(topic string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.topics, topic)
}

func (f *fakeKafka) record(call string) {
	f.mu.Lock()
	f.calls = append(f.calls, call)
	f.mu.Unlock()
}

func (f *fakeKafka) touched() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

func (f *fakeKafka) gate(ctx context.Context, method string) error {
	f.record(method)
	if f.onCall != nil {
		f.onCall(method)
	}
	if f.panicOn == method {
		panic("boom in " + method)
	}
	if f.deaf {
		select {} // never returns, whatever the context says
	}
	if f.hang {
		<-ctx.Done()
		return ctx.Err()
	}
	return nil
}

func (f *fakeKafka) Topics(ctx context.Context) ([]Topic, error) {
	if err := f.gate(ctx, "topics"); err != nil {
		return nil, err
	}
	if f.listErrNow != nil {
		if err := f.listErrNow(); err != nil {
			return nil, err
		}
	}
	if f.listErr != nil {
		return nil, f.listErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []Topic
	for name, t := range f.topics {
		tp := Topic{Name: name, Err: t.err}
		if t.err == nil {
			for i := range t.parts {
				tp.Partitions = append(tp.Partitions, i)
			}
		}
		out = append(out, tp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func (f *fakeKafka) Offsets(ctx context.Context, p Partition) (Offsets, error) {
	if err := f.gate(ctx, "offsets "+p.String()); err != nil {
		return Offsets{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.failOffs[p.Topic]; err != nil {
		return Offsets{}, err
	}
	t, ok := f.topics[p.Topic]
	if !ok || p.ID >= len(t.parts) {
		return Offsets{}, fmt.Errorf("unknown topic or partition %s", p)
	}
	return Offsets{Start: t.start[p.ID], End: t.start[p.ID] + int64(len(t.parts[p.ID]))}, nil
}

func (f *fakeKafka) Read(ctx context.Context, p Partition, from, to int64) ([]Message, error) {
	if err := f.gate(ctx, fmt.Sprintf("read %s %d-%d", p, from, to)); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.failRead[p.Topic]; err != nil {
		return nil, err
	}
	t, ok := f.topics[p.Topic]
	if !ok || p.ID >= len(t.parts) {
		return nil, fmt.Errorf("unknown topic or partition %s", p)
	}
	log := t.parts[p.ID]
	if f.extra != nil {
		return f.extra(p, from, to, log), nil
	}
	var out []Message
	for _, m := range log {
		if m.Offset >= from && m.Offset < to {
			out = append(out, m)
		}
	}
	return out, nil
}

const runID = "RDAB12"

// a run's payload, another run's, and the same run's id in other places
func ours(n int) string {
	return fmt.Sprintf(`{"type":"x.StockReceived","data":{"reference":"%s-IB-%d"}}`, runID, n)
}
func theirs(n int) string {
	return fmt.Sprintf(`{"type":"x.StockReceived","data":{"reference":"RDZZ99-IB-%d"}}`, n)
}

// world is a broker with some ordinary topics, two dead-letter topics (3 and 1 partitions) and a
// counter over it.
func world(t *testing.T) (*fakeKafka, *Counter) {
	t.Helper()
	f := newFake()
	f.topic("warehouse.yard-management.events", 2)
	f.topic("warehouse.inventory.events", 1)
	f.topic("warehouse.yard-management.events.dlq", 3)
	f.topic("retailnetwork.node-topology.events.dlq", 1)
	return f, New(f, Options{CallTimeout: 2 * time.Second, Timeout: 10 * time.Second})
}

func mustSnapshot(t *testing.T, c *Counter) {
	t.Helper()
	if err := c.Snapshot(context.Background()); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
}

func count(t *testing.T, c *Counter) (int, bool, error) {
	t.Helper()
	return c.DeadLetters(context.Background(), runID)
}

func TestNoDeadLettersOnReachableTopicsIsZeroAndKnown(t *testing.T) {
	f, c := world(t)
	// history that predates the day, carrying the run id's shape of another run, is not this run's
	f.add("warehouse.yard-management.events.dlq", 0, "k", theirs(1))
	mustSnapshot(t, c)
	n, known, err := count(t, c)
	if n != 0 || !known || err != nil {
		t.Fatalf("got (%d, %v, %v), want (0, true, nil)", n, known, err)
	}
}

func TestCountsTheRunsMessagesAppendedSinceTheSnapshotAndNothingElse(t *testing.T) {
	f, c := world(t)
	const dlq = "warehouse.yard-management.events.dlq"
	f.add(dlq, 1, "k", ours(0)) // before the snapshot: this run's id, but it was already there
	mustSnapshot(t, c)
	f.add(dlq, 1, "k", ours(1))
	f.add(dlq, 1, "k", theirs(2)) // another run's
	f.add(dlq, 1, "k", ours(3))
	n, known, err := count(t, c)
	if n != 2 || !known || err != nil {
		t.Fatalf("got (%d, %v, %v), want (2, true, nil): two dead letters of this run, one of another run, one before the snapshot", n, known, err)
	}
}

func TestTheCountIsTheTotalOverEveryTopicAndPartition(t *testing.T) {
	f, c := world(t)
	mustSnapshot(t, c)
	f.add("warehouse.yard-management.events.dlq", 0, "k", ours(1))
	f.add("warehouse.yard-management.events.dlq", 2, "k", ours(2))
	f.add("warehouse.yard-management.events.dlq", 2, "k", ours(3))
	f.add("retailnetwork.node-topology.events.dlq", 0, "k", ours(4))
	if n, known, err := count(t, c); n != 4 || !known || err != nil {
		t.Fatalf("got (%d, %v, %v), want (4, true, nil)", n, known, err)
	}
}

func TestEachPartitionIsCutAtItsOwnSnapshotOffset(t *testing.T) {
	f, c := world(t)
	const dlq = "warehouse.yard-management.events.dlq"
	f.add(dlq, 0, "k", ours(1)) // 1 old message on partition 0
	f.add(dlq, 2, "k", ours(2))
	f.add(dlq, 2, "k", ours(3)) // 2 old ones on partition 2
	mustSnapshot(t, c)
	f.add(dlq, 0, "k", ours(4))
	f.add(dlq, 2, "k", ours(5))
	if n, known, err := count(t, c); n != 2 || !known || err != nil {
		t.Fatalf("got (%d, %v, %v), want (2, true, nil)", n, known, err)
	}
}

func TestAMessageCarriesTheRunInItsValueItsHeadersOrItsKeyAndNowhereElse(t *testing.T) {
	const dlq = "retailnetwork.node-topology.events.dlq"
	cases := []struct {
		name string
		add  func(f *fakeKafka)
		want int
	}{
		{"value", func(f *fakeKafka) { f.add(dlq, 0, "k", ours(1)) }, 1},
		{"header value (the consumer's error quotes the id)", func(f *fakeKafka) {
			f.add(dlq, 0, "k", theirs(1), Header{Key: "x-dlq-error", Value: []byte("no such trailer for " + runID + "-IB-1")})
		}, 1},
		{"a later header of several", func(f *fakeKafka) {
			f.add(dlq, 0, "k", theirs(1), Header{Key: "traceparent", Value: []byte("00-abc")}, Header{Key: "x-dlq-error", Value: []byte("seal " + runID)})
		}, 1},
		{"key (the aggregate id the dead letter keeps)", func(f *fakeKafka) { f.add(dlq, 0, runID+"-LOAD-7", theirs(1)) }, 1},
		{"a header NAME is not a match", func(f *fakeKafka) { f.add(dlq, 0, "k", theirs(1), Header{Key: runID, Value: []byte("v")}) }, 0},
		{"another run's id in every field", func(f *fakeKafka) {
			f.add(dlq, 0, "RDZZ99-K", theirs(1), Header{Key: "x-dlq-error", Value: []byte("RDZZ99")})
		}, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f, c := world(t)
			mustSnapshot(t, c)
			tc.add(f)
			if n, known, err := count(t, c); n != tc.want || !known || err != nil {
				t.Fatalf("got (%d, %v, %v), want (%d, true, nil)", n, known, err, tc.want)
			}
		})
	}
}

func TestOnlyTopicsEndingInDlqAreReadAndAMatchElsewhereIsNotADeadLetter(t *testing.T) {
	f, c := world(t)
	f.topic("warehouse.dlq.events", 1) // "dlq" inside the name is not the suffix
	f.topic("warehouse.fulfillment.events.dlq2", 1)
	f.topic("dlq", 1)
	mustSnapshot(t, c)
	for _, topic := range []string{"warehouse.yard-management.events", "warehouse.dlq.events", "warehouse.fulfillment.events.dlq2", "dlq"} {
		f.add(topic, 0, "k", ours(1))
	}
	if n, known, err := count(t, c); n != 0 || !known || err != nil {
		t.Fatalf("a run id on a topic that is not a dead-letter topic counted: got (%d, %v, %v)", n, known, err)
	}
	for _, call := range f.touched() {
		for _, bad := range []string{"warehouse.yard-management.events[", "warehouse.inventory.events[", "warehouse.dlq.events[", "dlq2[", " dlq["} {
			if strings.Contains(call, bad) {
				t.Errorf("a topic without the .dlq suffix was read: %q", call)
			}
		}
	}
}

func TestADeadLetterTopicIsFoundWhateverItIsCalled(t *testing.T) {
	f, c := world(t)
	f.topic("warehouse.a-context-nobody-listed.events.dlq", 1) // a new bounded context
	mustSnapshot(t, c)
	f.add("warehouse.a-context-nobody-listed.events.dlq", 0, "k", ours(1))
	if n, known, err := count(t, c); n != 1 || !known || err != nil {
		t.Fatalf("got (%d, %v, %v), want (1, true, nil)", n, known, err)
	}
}

func TestATopicThatCannotBeReadMakesTheCountUnknownAndNamesTheTopic(t *testing.T) {
	for _, stage := range []string{"offsets", "messages"} {
		t.Run(stage, func(t *testing.T) {
			f, c := world(t)
			mustSnapshot(t, c)
			f.add("warehouse.yard-management.events.dlq", 0, "k", ours(1))
			f.add("retailnetwork.node-topology.events.dlq", 0, "k", ours(2))
			if stage == "offsets" {
				f.failOffs["retailnetwork.node-topology.events.dlq"] = errors.New("broker said LEADER_NOT_AVAILABLE")
			} else {
				f.failRead["retailnetwork.node-topology.events.dlq"] = errors.New("fetch: connection reset")
			}
			n, known, err := count(t, c)
			if known || n != 0 || err == nil {
				t.Fatalf("got (%d, %v, %v): one unreadable topic of two must make the count unknown, never 0", n, known, err)
			}
			if !strings.Contains(err.Error(), "retailnetwork.node-topology.events.dlq[0]") {
				t.Errorf("the error does not name the topic: %v", err)
			}
			if strings.Contains(err.Error(), "warehouse.yard-management.events.dlq[") {
				t.Errorf("the error blames a topic that was read: %v", err)
			}
			if !strings.Contains(err.Error(), "1 already found") {
				t.Errorf("the error hides that the readable topic held a dead letter: %v", err)
			}
		})
	}
}

func TestEveryUnreadableTopicIsNamed(t *testing.T) {
	f, c := world(t)
	mustSnapshot(t, c)
	f.failOffs["retailnetwork.node-topology.events.dlq"] = errors.New("boom1")
	f.failRead["warehouse.yard-management.events.dlq"] = errors.New("boom2")
	f.add("warehouse.yard-management.events.dlq", 1, "k", ours(1))
	_, known, err := count(t, c)
	if known || err == nil {
		t.Fatalf("known=%v err=%v", known, err)
	}
	for _, want := range []string{"retailnetwork.node-topology.events.dlq[0]", "warehouse.yard-management.events.dlq[1]", "boom1", "boom2"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the error lacks %q: %v", want, err)
		}
	}
	if strings.Contains(err.Error(), "already found") {
		t.Errorf("the error claims dead letters were found: %v", err)
	}
}

func TestATopicWhoseMetadataIsBrokenIsUnreadableNotAbsent(t *testing.T) {
	f, c := world(t)
	mustSnapshot(t, c)
	f.topics["warehouse.yard-management.events.dlq"].err = errors.New("LEADER_NOT_AVAILABLE")
	n, known, err := count(t, c)
	if known || n != 0 || err == nil || !strings.Contains(err.Error(), "warehouse.yard-management.events.dlq") {
		t.Fatalf("got (%d, %v, %v)", n, known, err)
	}
	// ... and a broken ordinary topic is nobody's business
	f.topics["warehouse.yard-management.events.dlq"].err = nil
	f.topics["warehouse.inventory.events"].err = errors.New("LEADER_NOT_AVAILABLE")
	if n, known, err := count(t, c); n != 0 || !known || err != nil {
		t.Fatalf("a broken ordinary topic blocked the count: (%d, %v, %v)", n, known, err)
	}
}

func TestATopicCreatedAfterTheSnapshotIsCountedFromItsFirstMessage(t *testing.T) {
	// A dead-letter topic is created by its first poison message (AllowAutoTopicCreation), so a day
	// that dead-letters for the first time creates it. Nothing on it predates the snapshot.
	f, c := world(t)
	mustSnapshot(t, c)
	f.topic("warehouse.outbound-shipping.events.dlq", 2)
	f.add("warehouse.outbound-shipping.events.dlq", 1, "k", ours(1)) // offset 0
	f.add("warehouse.outbound-shipping.events.dlq", 1, "k", theirs(2))
	f.add("warehouse.outbound-shipping.events.dlq", 1, "k", ours(3))
	n, known, err := count(t, c)
	if n != 2 || !known || err != nil {
		t.Fatalf("got (%d, %v, %v), want (2, true, nil): the dead letter at offset 0 of a topic created after the snapshot is the run's", n, known, err)
	}
	if !containsCall(f.touched(), "read warehouse.outbound-shipping.events.dlq[1] 0-3") {
		t.Errorf("the new topic was not read from offset 0: %v", f.touched())
	}
}

func TestATopicCreatedAfterTheSnapshotWhoseLogNoLongerStartsAtZeroIsUnknown(t *testing.T) {
	// everything on a topic created after the snapshot is the day's: offsets below the log start were
	// deleted unseen, and they may have been this run's
	f, c := world(t)
	mustSnapshot(t, c)
	f.topic("warehouse.outbound-shipping.events.dlq", 1)
	f.add("warehouse.outbound-shipping.events.dlq", 0, "k", theirs(1))
	f.add("warehouse.outbound-shipping.events.dlq", 0, "k", theirs(2))
	f.add("warehouse.outbound-shipping.events.dlq", 0, "k", theirs(3))
	f.mu.Lock()
	f.topics["warehouse.outbound-shipping.events.dlq"].start[0] = 2
	f.topics["warehouse.outbound-shipping.events.dlq"].parts[0] = f.topics["warehouse.outbound-shipping.events.dlq"].parts[0][2:]
	f.mu.Unlock()
	n, known, err := count(t, c)
	if known || n != 0 || err == nil || !strings.Contains(err.Error(), "deleted by retention") {
		t.Fatalf("got (%d, %v, %v)", n, known, err)
	}
}

func TestAPartitionAddedAfterTheSnapshotIsCountedFromItsFirstMessage(t *testing.T) {
	f, c := world(t)
	const dlq = "retailnetwork.node-topology.events.dlq"
	f.add(dlq, 0, "k", ours(0)) // already there
	mustSnapshot(t, c)
	tp := f.topics[dlq]
	f.mu.Lock()
	tp.parts, tp.start = append(tp.parts, nil), append(tp.start, 0)
	f.mu.Unlock()
	f.add(dlq, 1, "k", ours(1))
	if n, known, err := count(t, c); n != 1 || !known || err != nil {
		t.Fatalf("got (%d, %v, %v), want (1, true, nil)", n, known, err)
	}
}

func TestATopicOfTheSnapshotThatIsGoneMakesTheCountUnknown(t *testing.T) {
	f, c := world(t)
	mustSnapshot(t, c)
	f.drop("retailnetwork.node-topology.events.dlq")
	n, known, err := count(t, c)
	if known || n != 0 || err == nil || !strings.Contains(err.Error(), "retailnetwork.node-topology.events.dlq[0]") {
		t.Fatalf("got (%d, %v, %v)", n, known, err)
	}
}

func TestMessagesDeletedByRetentionBeforeTheyWereCountedMakeTheCountUnknown(t *testing.T) {
	f, c := world(t)
	const dlq = "retailnetwork.node-topology.events.dlq"
	f.add(dlq, 0, "k", theirs(0))
	mustSnapshot(t, c)
	f.add(dlq, 0, "k", ours(1))
	f.add(dlq, 0, "k", ours(2))
	f.mu.Lock()
	f.topics[dlq].start[0] = 2 // offsets 0 and 1 are gone: 1 was the day's
	f.topics[dlq].parts[0] = f.topics[dlq].parts[0][2:]
	f.mu.Unlock()
	n, known, err := count(t, c)
	if known || n != 0 || err == nil || !strings.Contains(err.Error(), "deleted by retention") {
		t.Fatalf("got (%d, %v, %v)", n, known, err)
	}
}

func TestATopicRecreatedAfterTheSnapshotMakesTheCountUnknown(t *testing.T) {
	f, c := world(t)
	const dlq = "retailnetwork.node-topology.events.dlq"
	f.add(dlq, 0, "k", theirs(0))
	f.add(dlq, 0, "k", theirs(1))
	mustSnapshot(t, c) // end offset 2
	f.drop(dlq)
	f.topic(dlq, 1)
	f.add(dlq, 0, "k", ours(1)) // end offset 1, below the snapshot's 2
	n, known, err := count(t, c)
	if known || n != 0 || err == nil || !strings.Contains(err.Error(), "recreated") {
		t.Fatalf("got (%d, %v, %v)", n, known, err)
	}
}

func TestAShortReadIsNotACount(t *testing.T) {
	f, c := world(t)
	const dlq = "retailnetwork.node-topology.events.dlq"
	mustSnapshot(t, c)
	f.add(dlq, 0, "k", ours(1))
	f.add(dlq, 0, "k", ours(2))
	f.add(dlq, 0, "k", ours(3))
	f.extra = func(_ Partition, _, _ int64, log []Message) []Message { return log[:2] } // the third never came back
	n, known, err := count(t, c)
	if known || n != 0 || err == nil || !strings.Contains(err.Error(), "2 of 3") {
		t.Fatalf("got (%d, %v, %v)", n, known, err)
	}
	// the same message twice does not stand in for the one that is missing
	f.extra = func(_ Partition, _, _ int64, log []Message) []Message { return []Message{log[0], log[0], log[1]} }
	if n, known, err := count(t, c); known || n != 0 || err == nil {
		t.Fatalf("a duplicated offset filled a gap: (%d, %v, %v)", n, known, err)
	}
}

func TestADuplicatedMessageIsCountedOnce(t *testing.T) {
	f, c := world(t)
	const dlq = "retailnetwork.node-topology.events.dlq"
	mustSnapshot(t, c)
	f.add(dlq, 0, "k", ours(1))
	f.add(dlq, 0, "k", ours(2))
	f.extra = func(_ Partition, _, _ int64, log []Message) []Message {
		return []Message{log[0], log[0], log[1], log[1]}
	}
	if n, known, err := count(t, c); n != 2 || !known || err != nil {
		t.Fatalf("got (%d, %v, %v), want (2, true, nil)", n, known, err)
	}
}

func TestATopicWithNoPartitionsOrABrokenMetadataIsUnreadable(t *testing.T) {
	// at the snapshot
	f, c := world(t)
	f.topic("warehouse.empty.events.dlq", 0) // listed with no partition
	err := c.Snapshot(context.Background())
	if err == nil || !strings.Contains(err.Error(), "warehouse.empty.events.dlq: the broker lists no partition") {
		t.Fatalf("snapshot with a partition-less dead-letter topic: %v", err)
	}
	// at the count: a topic created after the snapshot whose metadata is not ready, then one with no partition
	f, c = world(t)
	mustSnapshot(t, c)
	f.topic("warehouse.new.events.dlq", 1).err = errors.New("LEADER_NOT_AVAILABLE")
	n, known, err := count(t, c)
	if known || n != 0 || err == nil || !strings.Contains(err.Error(), "warehouse.new.events.dlq: its metadata could not be read: LEADER_NOT_AVAILABLE") {
		t.Fatalf("got (%d, %v, %v)", n, known, err)
	}
	f.topics["warehouse.new.events.dlq"].err = nil
	f.topics["warehouse.new.events.dlq"].parts = nil
	if n, known, err := count(t, c); known || n != 0 || err == nil || !strings.Contains(err.Error(), "warehouse.new.events.dlq: the broker lists no partition") {
		t.Fatalf("got (%d, %v, %v)", n, known, err)
	}
}

func TestTheCounterEnforcesItsOwnRangeWhateverTheSourceReturns(t *testing.T) {
	// a broker may return a batch that starts before the requested offset; a source that passes it on
	// must not make the counter charge the run with what was there before the snapshot
	f, c := world(t)
	const dlq = "retailnetwork.node-topology.events.dlq"
	f.add(dlq, 0, "k", ours(0))
	mustSnapshot(t, c)
	f.add(dlq, 0, "k", ours(1))
	f.extra = func(_ Partition, _, _ int64, log []Message) []Message { return log } // the whole log, from offset 0
	if n, known, err := count(t, c); n != 1 || !known || err != nil {
		t.Fatalf("got (%d, %v, %v), want (1, true, nil)", n, known, err)
	}
	// ... and nothing past the end offset the count was taken at
	f.extra = func(_ Partition, _, to int64, log []Message) []Message {
		return append(append([]Message(nil), log...), Message{Offset: to, Value: []byte(ours(9))})
	}
	if n, known, err := count(t, c); n != 1 || !known || err != nil {
		t.Fatalf("a message past the end offset counted: (%d, %v, %v)", n, known, err)
	}
}

func TestNothingIsReadFromAPartitionThatGrewByNothing(t *testing.T) {
	f, c := world(t)
	f.add("retailnetwork.node-topology.events.dlq", 0, "k", ours(0))
	mustSnapshot(t, c)
	if _, known, err := count(t, c); !known || err != nil {
		t.Fatal(known, err)
	}
	for _, call := range f.touched() {
		if strings.HasPrefix(call, "read ") {
			t.Errorf("a message range was read although nothing was appended: %q", call)
		}
	}
}

func TestOnlyTheRangeAfterTheSnapshotIsRead(t *testing.T) {
	f, c := world(t)
	const dlq = "retailnetwork.node-topology.events.dlq"
	f.add(dlq, 0, "k", ours(0))
	f.add(dlq, 0, "k", ours(1))
	mustSnapshot(t, c)
	f.add(dlq, 0, "k", ours(2))
	if _, _, err := count(t, c); err != nil {
		t.Fatal(err)
	}
	if !containsCall(f.touched(), "read "+dlq+"[0] 2-3") {
		t.Errorf("the range read was not [snapshot end, end now): %v", f.touched())
	}
}

func containsCall(calls []string, want string) bool {
	for _, c := range calls {
		if c == want {
			return true
		}
	}
	return false
}

func TestNoSnapshotIsUnknownAndNothingIsRead(t *testing.T) {
	f, c := world(t)
	f.add("retailnetwork.node-topology.events.dlq", 0, "k", ours(1))
	n, known, err := count(t, c)
	if known || n != 0 || !errors.Is(err, ErrNoSnapshot) {
		t.Fatalf("got (%d, %v, %v)", n, known, err)
	}
	if calls := f.touched(); len(calls) != 0 {
		t.Errorf("without a snapshot the counter still read: %v", calls)
	}
}

func TestAFailedSnapshotLeavesNoSnapshot(t *testing.T) {
	cases := map[string]func(f *fakeKafka){
		"the broker cannot be listed":   func(f *fakeKafka) { f.listErr = errors.New("dial tcp 127.0.0.1:9092: connect: connection refused") },
		"one end offset cannot be read": func(f *fakeKafka) { f.failOffs["retailnetwork.node-topology.events.dlq"] = errors.New("boom") },
		"a dead-letter topic has no leader yet": func(f *fakeKafka) {
			f.topics["retailnetwork.node-topology.events.dlq"].err = errors.New("LEADER_NOT_AVAILABLE")
		},
	}
	for name, breakIt := range cases {
		t.Run(name, func(t *testing.T) {
			f, c := world(t)
			breakIt(f)
			err := c.Snapshot(context.Background())
			if err == nil {
				t.Fatal("a snapshot that missed a topic was taken")
			}
			if strings.Contains(name, "offset") && !strings.Contains(err.Error(), "retailnetwork.node-topology.events.dlq[0]") {
				t.Errorf("the error does not name the topic: %v", err)
			}
			// the day went on anyway (or the caller ignored the error): the count must still be unknown
			f.listErr, f.failOffs = nil, map[string]error{}
			f.topics["retailnetwork.node-topology.events.dlq"].err = nil
			n, known, cerr := count(t, c)
			if known || n != 0 || !errors.Is(cerr, ErrNoSnapshot) {
				t.Fatalf("after a failed snapshot: (%d, %v, %v)", n, known, cerr)
			}
			if got := c.Summary(); got != "no snapshot taken" {
				t.Errorf("summary %q", got)
			}
			// and a later, good snapshot is allowed
			mustSnapshot(t, c)
		})
	}
}

func TestTheSnapshotIsTakenOnceAndASecondOneCannotMoveIt(t *testing.T) {
	f, c := world(t)
	const dlq = "retailnetwork.node-topology.events.dlq"
	mustSnapshot(t, c)
	f.add(dlq, 0, "k", ours(1))
	if err := c.Snapshot(context.Background()); err == nil {
		t.Fatal("a second snapshot was taken: it would hide the dead letter above")
	}
	if n, known, err := count(t, c); n != 1 || !known || err != nil {
		t.Fatalf("got (%d, %v, %v), want (1, true, nil)", n, known, err)
	}
}

func TestTheSnapshotSummarySaysWhatItHolds(t *testing.T) {
	f, c := world(t)
	f.add("warehouse.yard-management.events.dlq", 0, "k", ours(1))
	f.add("warehouse.yard-management.events.dlq", 0, "k", ours(2))
	f.add("retailnetwork.node-topology.events.dlq", 0, "k", ours(3))
	mustSnapshot(t, c)
	if got, want := c.Summary(), "snapshot of 2 .dlq topic(s), 4 partition(s), 3 message(s) already on them"; got != want {
		t.Errorf("summary %q, want %q", got, want)
	}
}

func TestAnEmptyClusterListingIsNotNoDeadLetters(t *testing.T) {
	f := newFake()
	c := New(f, Options{})
	err := c.Snapshot(context.Background())
	if err == nil || !strings.Contains(err.Error(), "no topic at all") {
		t.Fatalf("snapshot of a broker that lists nothing: %v", err)
	}
	f, c = world(t)
	mustSnapshot(t, c)
	for name := range f.topics {
		f.drop(name)
	}
	n, known, err := count(t, c)
	if known || n != 0 || err == nil {
		t.Fatalf("a broker that listed nothing was read as no dead letters: (%d, %v, %v)", n, known, err)
	}
}

func TestAClusterWithNoDeadLetterTopicYetHasNoDeadLetters(t *testing.T) {
	f := newFake()
	f.topic("warehouse.yard-management.events", 1)
	c := New(f, Options{})
	mustSnapshot(t, c)
	if n, known, err := count(t, c); n != 0 || !known || err != nil {
		t.Fatalf("got (%d, %v, %v), want (0, true, nil)", n, known, err)
	}
}

func TestAnEmptyRunIdIsRefusedNotMatchedAgainstEverything(t *testing.T) {
	f, c := world(t)
	mustSnapshot(t, c)
	f.add("retailnetwork.node-topology.events.dlq", 0, "k", ours(1))
	n, known, err := c.DeadLetters(context.Background(), "")
	if known || n != 0 || err == nil {
		t.Fatalf("got (%d, %v, %v)", n, known, err)
	}
	if Carries(Message{Value: []byte("anything")}, "") {
		t.Error("an empty run id carries every message")
	}
}

func TestACancelledContextIsAPromptErrorNeverAZero(t *testing.T) {
	f, c := world(t)
	mustSnapshot(t, c)
	f.add("retailnetwork.node-topology.events.dlq", 0, "k", ours(1))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	called := make(chan string, 16)
	f.onCall = func(call string) { called <- call }
	n, known, err := c.DeadLetters(ctx, runID)
	if known || n != 0 || !errors.Is(err, context.Canceled) {
		t.Fatalf("got (%d, %v, %v)", n, known, err)
	}
	if err := New(f, Options{}).Snapshot(ctx); err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("snapshot on a cancelled context: %v", err)
	}
	// a call that was started would reach the fake within moments: it must not (this wait can only
	// fail when a call was made, never because of timing)
	select {
	case call := <-called:
		t.Errorf("a cancelled context still reached Kafka: %s", call)
	case <-time.After(300 * time.Millisecond):
	}
}

func TestTheSnapshotStopsAtTheFirstPartitionAfterItsContextEndsAndSaysSoOnce(t *testing.T) {
	f, _ := world(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.onCall = func(call string) {
		if strings.HasPrefix(call, "offsets ") {
			cancel()
		}
	}
	err := New(f, Options{}).Snapshot(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("snapshot: %v", err)
	}
	if k := strings.Count(err.Error(), "stopped before"); k != 1 {
		t.Errorf("the error says it stopped %d times, want once: %v", k, err)
	}
}

func TestTheCountStopsAtTheFirstPartitionAfterItsContextEndsAndSaysSoOnce(t *testing.T) {
	f, c := world(t)
	mustSnapshot(t, c)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.mu.Lock()
	before := len(f.calls)
	f.mu.Unlock()
	f.onCall = func(call string) {
		if strings.HasPrefix(call, "offsets ") {
			cancel() // the context ends while the first partition is being measured
		}
	}
	n, known, err := c.DeadLetters(ctx, runID)
	if known || n != 0 || !errors.Is(err, context.Canceled) {
		t.Fatalf("got (%d, %v, %v)", n, known, err)
	}
	if k := strings.Count(err.Error(), "stopped before"); k != 1 {
		t.Errorf("the error says it stopped %d times, want once: %v", k, err)
	}
	offsets := 0
	for _, call := range f.touched()[before:] {
		if strings.HasPrefix(call, "offsets ") {
			offsets++
		}
	}
	if offsets != 1 {
		t.Errorf("%d partitions were measured after the context ended, want 1", offsets)
	}
}

func TestACallThatBlocksUntilItsContextEndsIsEndedByThatContext(t *testing.T) {
	f, c := world(t)
	mustSnapshot(t, c)
	f.hang = true
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var n int
	var known bool
	var err error
	go func() { n, known, err = c.DeadLetters(ctx, runID); close(done) }()
	time.AfterFunc(50*time.Millisecond, cancel)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("DeadLetters did not return after its context was cancelled")
	}
	if known || n != 0 || !errors.Is(err, context.Canceled) {
		t.Fatalf("got (%d, %v, %v)", n, known, err)
	}
}

func TestEveryCallIsBoundedEvenForASourceThatIgnoresItsContext(t *testing.T) {
	f, c := world(t)
	mustSnapshot(t, c)
	f.deaf = true
	c = New(f, Options{CallTimeout: 30 * time.Millisecond, Timeout: 5 * time.Second})
	c.snap = map[Partition]int64{{Topic: "retailnetwork.node-topology.events.dlq"}: 0}
	start := time.Now()
	n, known, err := c.DeadLetters(context.Background(), runID)
	if took := time.Since(start); took > 3*time.Second {
		t.Fatalf("a source that never answers held the count for %s", took)
	}
	if known || n != 0 || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got (%d, %v, %v)", n, known, err)
	}
	if err := New(f, Options{CallTimeout: 30 * time.Millisecond}).Snapshot(context.Background()); err == nil {
		t.Fatal("a snapshot of a broker that never answers succeeded")
	}
}

func TestTheWholeCountIsBoundedToo(t *testing.T) {
	f, c := world(t)
	mustSnapshot(t, c)
	f.hang = true
	c = New(f, Options{CallTimeout: time.Minute, Timeout: 50 * time.Millisecond})
	c.snap = map[Partition]int64{{Topic: "retailnetwork.node-topology.events.dlq"}: 0}
	start := time.Now()
	_, known, err := c.DeadLetters(context.Background(), runID)
	if known || err == nil || time.Since(start) > 3*time.Second {
		t.Fatalf("known=%v err=%v after %s", known, err, time.Since(start))
	}
}

func TestASourceThatPanicsIsAnError(t *testing.T) {
	for _, method := range []string{"topics", "offsets retailnetwork.node-topology.events.dlq[0]"} {
		f, c := world(t)
		f.panicOn = method
		err := c.Snapshot(context.Background())
		if err == nil || !strings.Contains(err.Error(), "panicked") {
			t.Fatalf("%s: snapshot %v", method, err)
		}
	}
	f, c := world(t)
	mustSnapshot(t, c)
	f.panicOn = "topics"
	n, known, err := count(t, c)
	if known || n != 0 || err == nil {
		t.Fatalf("got (%d, %v, %v)", n, known, err)
	}
}

func TestNoSourceIsAnErrorNotAPanic(t *testing.T) {
	c := New(nil, Options{})
	if err := c.Snapshot(context.Background()); !errors.Is(err, ErrNoSource) {
		t.Errorf("snapshot: %v", err)
	}
	if n, known, err := c.DeadLetters(context.Background(), runID); known || n != 0 || !errors.Is(err, ErrNoSource) {
		t.Errorf("count: (%d, %v, %v)", n, known, err)
	}
}

func TestACountCanBeAskedAgainAndGivesTheSameAnswer(t *testing.T) {
	f, c := world(t)
	mustSnapshot(t, c)
	f.add("retailnetwork.node-topology.events.dlq", 0, "k", ours(1))
	for i := 0; i < 3; i++ {
		if n, known, err := count(t, c); n != 1 || !known || err != nil {
			t.Fatalf("ask %d: (%d, %v, %v)", i, n, known, err)
		}
	}
	f.add("retailnetwork.node-topology.events.dlq", 0, "k", ours(2))
	if n, _, _ := count(t, c); n != 2 {
		t.Fatalf("a later dead letter was not counted: %d", n)
	}
}

func TestTheListingFailingAtCountTimeIsUnknown(t *testing.T) {
	f, c := world(t)
	mustSnapshot(t, c)
	f.listErr = errors.New("dial tcp 127.0.0.1:9092: connect: connection refused")
	n, known, err := count(t, c)
	if known || n != 0 || err == nil || !strings.Contains(err.Error(), "connection refused") {
		t.Fatalf("got (%d, %v, %v)", n, known, err)
	}
	if strings.Contains(err.Error(), "not listed now") {
		t.Errorf("the error blames every topic of the snapshot for a listing that failed as a whole: %v", err)
	}
}

func TestPartitionsAreReadAndNamedInAStableOrder(t *testing.T) {
	f, c := world(t)
	mustSnapshot(t, c)
	before := len(f.touched())
	if _, known, err := count(t, c); !known || err != nil {
		t.Fatal(known, err)
	}
	var got []string
	for _, call := range f.touched()[before:] {
		if strings.HasPrefix(call, "offsets ") {
			got = append(got, strings.TrimPrefix(call, "offsets "))
		}
	}
	want := []string{
		"retailnetwork.node-topology.events.dlq[0]",
		"warehouse.yard-management.events.dlq[0]", "warehouse.yard-management.events.dlq[1]", "warehouse.yard-management.events.dlq[2]",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("read order\n got %v\nwant %v", got, want)
	}
	// topics of the snapshot that vanished are named in the same order
	f.drop("warehouse.yard-management.events.dlq")
	_, _, err := count(t, c)
	if err == nil {
		t.Fatal("a dropped topic was not noticed")
	}
	i0, i1, i2 := strings.Index(err.Error(), "events.dlq[0]"), strings.Index(err.Error(), "events.dlq[1]"), strings.Index(err.Error(), "events.dlq[2]")
	if i0 < 0 || !(i0 < i1 && i1 < i2) {
		t.Errorf("the vanished partitions are not named in order: %v", err)
	}
}
