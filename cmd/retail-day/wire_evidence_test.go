package main

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/warehouse-e2e/internal/retailday"
	"github.com/claudioed/warehouse-e2e/internal/retailday/collect"
)

// fakeDead is the dead-letter counter of the wiring tests: it records when its snapshot was taken and what it
// was asked, and answers what the test says.
type fakeDead struct {
	snapErr    error
	snapshots  int
	onSnapshot func()

	n     int
	known bool
	err   error
	asked []string
}

func (f *fakeDead) Snapshot(context.Context) error {
	f.snapshots++
	if f.onSnapshot != nil {
		f.onSnapshot()
	}
	return f.snapErr
}

func (f *fakeDead) Summary() string { return "fake snapshot" }

func (f *fakeDead) DeadLetters(_ context.Context, runID string) (int, bool, error) {
	f.asked = append(f.asked, runID)
	return f.n, f.known, f.err
}

func TestPrepareTakesTheDeadLetterSnapshotBeforeAnythingIsReadOrWritten(t *testing.T) {
	r := newRig(t, nil)
	var requests []string
	var steps int
	r.dead.onSnapshot = func() {
		requests = r.cl.requests()
		steps = len(r.outCalls) + len(r.inCalls) + r.binCalls + len(r.fs.provisioned)
	}
	if err := r.b.prepare(context.Background()); err != nil {
		t.Fatal(err)
	}
	if r.dead.snapshots != 1 {
		t.Fatalf("the snapshot was taken %d time(s)", r.dead.snapshots)
	}
	if len(requests) != 0 || steps != 0 {
		t.Errorf("when the snapshot was taken the gateway had seen %v and %d readiness/provision step(s) had run: every dead letter the day causes must lie after it", requests, steps)
	}
	if len(r.cl.requests()) == 0 || len(r.outCalls) == 0 {
		t.Error("prepare did nothing after the snapshot")
	}
	if !strings.Contains(strings.Join(r.logged, "\n"), "dead-letter snapshot taken") {
		t.Errorf("the snapshot is not in the run's log: %v", r.logged)
	}
}

func TestPrepareFailsClosedWhenTheBrokerCannotBeSnapshotted(t *testing.T) {
	r := newRig(t, func(c *wireConfig) { c.Brokers = "localhost:9092" })
	r.dead.snapErr = errors.New("dlq: the dead-letter topics could not be snapshotted: the topics could not be listed: dial tcp4 127.0.0.1:9092: connect: connection refused")
	err := r.b.prepare(context.Background())
	if err == nil {
		t.Fatal("the day was prepared without a snapshot of the dead-letter topics")
	}
	for _, want := range []string{"localhost:9092", "connection refused", "snapshot", "kubectl --context k3s-multipass -n warehouse-systems port-forward", "svc/kafka 9092:9092", "k3s-edges.sh", `-brokers ""`, "-allow-unverified"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the message lacks %q:\n%v", want, err)
		}
	}
	if got := r.cl.requests(); len(got) != 0 {
		t.Errorf("the gateway was read after a failed snapshot: %v", got)
	}
	if len(r.outCalls)+len(r.inCalls)+r.binCalls+len(r.fs.provisioned) != 0 || r.b.dock() != nil || r.b.crew() != nil {
		t.Error("the building was touched after a failed snapshot")
	}
}

func TestPrepareWithoutACounterFailsClosed(t *testing.T) {
	r := newRig(t, nil)
	r.b.dead = nil
	err := r.b.prepare(context.Background())
	if err == nil || !strings.Contains(err.Error(), "no dead-letter counter") {
		t.Fatalf("prepare: %v", err)
	}
	if len(r.cl.requests()) != 0 {
		t.Errorf("the gateway was read: %v", r.cl.requests())
	}
}

func TestTheBrokerHintNamesTheForwardForTheConfiguredPort(t *testing.T) {
	if h := brokerHint("localhost:29092"); !strings.Contains(h, "--address 127.0.0.1 svc/kafka 29092:9092") {
		t.Errorf("hint for another port: %s", h)
	}
	if h := brokerHint("not an address"); !strings.Contains(h, "svc/kafka 9092:9092") {
		t.Errorf("hint for an address with no port: %s", h)
	}
}

func TestBrokersIsEmptyOrOneHostPort(t *testing.T) {
	for addr, ok := range map[string]bool{
		"": true, "localhost:9092": true, "127.0.0.1:29092": true, "kafka.warehouse-systems.svc:9092": true,
		"localhost": false, ":9092": false, "localhost:": false, "a:9092,b:9092": false, "9092": false,
	} {
		if got := checkBrokers(addr) == ""; got != ok {
			t.Errorf("checkBrokers(%q) accepted=%v, want %v (%q)", addr, got, ok, checkBrokers(addr))
		}
	}
}

// ---- the composite ----

type recEvidence struct{ calls []string }

func (r *recEvidence) Order(_ context.Context, p retailday.Placed) (retailday.OrderFacts, []string) {
	r.calls = append(r.calls, "order "+p.OrderID)
	return retailday.OrderFacts{OrderID: "rest-" + p.OrderID, Lines: 7}, []string{"a note"}
}

func (r *recEvidence) Shelf(_ context.Context, skus []string) (map[string]retailday.Shelf, error) {
	r.calls = append(r.calls, "shelf "+strings.Join(skus, ","))
	return map[string]retailday.Shelf{skus[0]: {Usable: 5, Purchasable: true}}, errors.New("shelf says")
}

func (r *recEvidence) DeadLetters(context.Context, string) (int, bool, error) {
	r.calls = append(r.calls, "rest-dead-letters")
	return 99, true, nil
}

func TestTheCompositeAnswersOrderAndShelfFromTheCollectorAndDeadLettersFromTheCounter(t *testing.T) {
	rest, dead := &recEvidence{}, &fakeDead{n: 3, known: true}
	var ev retailday.Evidence = evidence{rest: rest, dead: dead}
	ctx := context.Background()

	facts, notes := ev.Order(ctx, retailday.Placed{OrderID: "O-1"})
	if facts.OrderID != "rest-O-1" || facts.Lines != 7 || len(notes) != 1 || notes[0] != "a note" {
		t.Errorf("Order: %+v %v", facts, notes)
	}
	sh, err := ev.Shelf(ctx, []string{"SKU-1", "SKU-2"})
	if sh["SKU-1"].Usable != 5 || err == nil || err.Error() != "shelf says" {
		t.Errorf("Shelf: %v %v", sh, err)
	}
	n, known, err := ev.DeadLetters(ctx, "RDRUN1")
	if n != 3 || !known || err != nil {
		t.Errorf("DeadLetters: (%d, %v, %v), want the counter's (3, true, nil)", n, known, err)
	}
	if got := strings.Join(rest.calls, "|"); got != "order O-1|shelf SKU-1,SKU-2" {
		t.Errorf("the collector was asked %q: it must see Order and Shelf only, never DeadLetters", got)
	}
	if len(dead.asked) != 1 || dead.asked[0] != "RDRUN1" {
		t.Errorf("the counter was asked %v, want the run id once", dead.asked)
	}
	// what the counter cannot say stays unsaid
	dead.n, dead.known, dead.err = 0, false, errors.New("broker down")
	if n, known, err := ev.DeadLetters(ctx, "RDRUN1"); n != 0 || known || err == nil || err.Error() != "broker down" {
		t.Errorf("an unknown count came out as (%d, %v, %v)", n, known, err)
	}
}

func TestNewPhasesWiresTheCompositeOverTheRestCollectorAndTheCounter(t *testing.T) {
	fd := &fakeDead{n: 2, known: true}
	_, cfg := phasesRig(t, func(c *wireConfig) { c.DeadLetters = fd }, nil)
	p, cleanup, err := newPhases(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	ev, ok := p.Evidence.(evidence)
	if !ok {
		t.Fatalf("Evidence is %T, want the composite", p.Evidence)
	}
	if _, ok := ev.rest.(*collect.Collector); !ok {
		t.Errorf("Order and Shelf come from %T, want the REST collector", ev.rest)
	}
	if n, known, err := p.Evidence.DeadLetters(context.Background(), "RDT2"); n != 2 || !known || err != nil || len(fd.asked) != 1 {
		t.Errorf("DeadLetters: (%d, %v, %v), asked %v", n, known, err, fd.asked)
	}
}

func TestARunWithNoBrokerCannotCountAndSaysWhy(t *testing.T) {
	_, cfg := phasesRig(t, func(c *wireConfig) { c.Brokers = "" }, nil)
	p, cleanup, err := newPhases(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	n, known, err := p.Evidence.DeadLetters(context.Background(), "RDT2")
	if n != 0 || known || err == nil || !strings.Contains(err.Error(), "-brokers") {
		t.Fatalf("got (%d, %v, %v): without a broker the dead letters are unknown, in words", n, known, err)
	}
}

func TestNewPhasesRefusesABrokerAddressItCannotUse(t *testing.T) {
	_, cfg := phasesRig(t, func(c *wireConfig) { c.Brokers = "no-port" }, nil)
	if _, _, err := newPhases(context.Background(), cfg); err == nil || !strings.Contains(err.Error(), "-brokers") {
		t.Fatalf("err %v", err)
	}
}

func TestTheRealPrepareStopsAtTheSnapshotBeforeTouchingTheCluster(t *testing.T) {
	fd := &fakeDead{snapErr: errors.New("connection refused")}
	f, cfg := phasesRig(t, func(c *wireConfig) { c.DeadLetters = fd; c.Brokers = "localhost:9092" }, nil)
	p, cleanup, err := newPhases(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	before := len(f.requests())
	err = p.Prepare(context.Background())
	if err == nil || !strings.Contains(err.Error(), "connection refused") || !strings.Contains(err.Error(), "port-forward") {
		t.Fatalf("Prepare: %v", err)
	}
	if after := len(f.requests()); after != before {
		t.Errorf("Prepare read the gateway (%d request(s)) after a failed snapshot", after-before)
	}
	if fd.snapshots != 1 {
		t.Errorf("%d snapshots", fd.snapshots)
	}
	if err := p.Cleanup(context.Background()); err != nil {
		t.Errorf("cleanup of a day that never started: %v", err)
	}
}

func TestNewDeadLetterCounterIsUncountedWithoutABrokerAndARealCounterWithOne(t *testing.T) {
	c, release, err := newDeadLetterCounter("")
	if err != nil {
		t.Fatal(err)
	}
	release()
	if _, ok := c.(uncounted); !ok {
		t.Errorf("no broker gave %T", c)
	}
	if err := c.Snapshot(context.Background()); err != nil {
		t.Errorf("snapshot of an uncounted run: %v", err)
	}
	c, release, err = newDeadLetterCounter("127.0.0.1:1")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if _, ok := c.(uncounted); ok {
		t.Error("a broker address gave the uncounted stand-in")
	}
}

// ---- the preflight's Kafka check, the real one against a port nobody listens on ----

func TestCheckKafkaFailsClosedOnAnUnreachableBrokerAndNamesTheForward(t *testing.T) {
	if _, ok := checkKafka(context.Background(), ""); ok {
		t.Error("an empty -brokers produced a check")
	}
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Skipf("no loopback listener: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	c, ok := checkKafka(ctx, addr)
	if !ok || c.OK || !strings.Contains(c.Detail, addr) || !strings.Contains(c.Fix, "port-forward") || c.Name == "" {
		t.Fatalf("check %+v (ok=%v): an unreachable broker must be a failed check naming the address and the fix", c, ok)
	}
	if c, ok := checkKafka(ctx, "not-an-address"); !ok || c.OK {
		t.Errorf("a malformed address passed: %+v", c)
	}
}
