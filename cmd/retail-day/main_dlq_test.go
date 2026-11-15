package main

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/claudioed/warehouse-e2e/internal/retailday"
	"github.com/claudioed/warehouse-e2e/internal/retailday/day"
	"github.com/claudioed/warehouse-e2e/internal/retailday/dlq"
)

// ---- the flag ----

func TestTheBrokersFlagDefaultsToTheAddressAForwardBindsAndReachesTheWiring(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "")
	h := newHarness(t)
	h.run("-json")
	if h.gotCfg.Brokers != "localhost:9092" || defaultBrokers != "localhost:9092" {
		t.Errorf("default -brokers %q (defaultBrokers %q): it is the address cmd/warehouse-day and env.sh use and a `port-forward svc/kafka 9092:9092` binds", h.gotCfg.Brokers, defaultBrokers)
	}
	t.Setenv("KAFKA_BROKERS", "127.0.0.1:29092")
	h = newHarness(t)
	h.run("-json")
	if h.gotCfg.Brokers != "127.0.0.1:29092" {
		t.Errorf("KAFKA_BROKERS did not reach the wiring: %q", h.gotCfg.Brokers)
	}
	h = newHarness(t)
	h.run("-json", "-brokers", "kafka.example:9092")
	if h.gotCfg.Brokers != "kafka.example:9092" {
		t.Errorf("-brokers did not beat KAFKA_BROKERS: %q", h.gotCfg.Brokers)
	}
	h = newHarness(t)
	h.run("-json", "-brokers", "")
	if h.built != 1 || h.gotCfg.Brokers != "" {
		t.Errorf("-brokers \"\" reached the wiring as %q (built %d): an empty flag means do not count", h.gotCfg.Brokers, h.built)
	}
}

func TestABrokersFlagThatIsNotOneHostPortIsAUsageErrorBeforeAnythingIsRead(t *testing.T) {
	for _, args := range [][]string{
		{"-brokers", "a:9092,b:9092"},
		{"-brokers", "localhost"},
		{"-brokers", ":9092"},
		{"-preflight", "-brokers", "nope"},
	} {
		h := newHarness(t)
		code, _, errs := h.run(args...)
		if code != 2 || !strings.Contains(errs, "-brokers") {
			t.Errorf("%v: exit %d, stderr %q", args, code, errs)
		}
		if h.preflight != 0 || h.built != 0 || len(h.kafkaAsked) != 0 {
			t.Errorf("%v: the cluster was read (preflight %d, kafka %d) or the actors built (%d)", args, h.preflight, len(h.kafkaAsked), h.built)
		}
	}
}

// ---- the preflight's Kafka check ----

func TestAnUnreachableBrokerFailsThePreflightAndTheDayNeverStarts(t *testing.T) {
	h := newHarness(t)
	h.kafka = &retailday.Check{Name: "kafka dead-letter topics", Detail: "localhost:9092: connection refused", Fix: brokerHint("localhost:9092")}
	code, out, errs := h.run()
	if code != 1 || h.built != 0 || h.world.preparedN != 0 {
		t.Fatalf("exit %d, built %d, prepared %d", code, h.built, h.world.preparedN)
	}
	for _, want := range []string{"PASS  health a", "FAIL  kafka dead-letter topics: localhost:9092: connection refused", "fix: Kafka is not forwarded by k3s-edges.sh", "2 checks, 1 failed"} {
		if !strings.Contains(out, want) {
			t.Errorf("stdout lacks %q:\n%s", want, out)
		}
	}
	if !strings.Contains(errs, "preflight FAILED (1 of 2 checks)") {
		t.Errorf("stderr: %s", errs)
	}
	if got := strings.Join(h.kafkaAsked, ","); got != "localhost:9092" {
		t.Errorf("the check was asked about %q", got)
	}
}

func TestAReachableBrokerPassesThePreflightAndIsListedInIt(t *testing.T) {
	h := newHarness(t)
	h.kafka = &retailday.Check{Name: "kafka dead-letter topics", OK: true, Detail: "localhost:9092: 1 .dlq topic(s), 8 partition(s), 3 message(s) already on them"}
	code, out, errs := h.run("-preflight")
	if code != 0 || !strings.Contains(out, "PASS  kafka dead-letter topics: localhost:9092: 1 .dlq topic(s)") || !strings.Contains(out, "2 checks, 0 failed") {
		t.Fatalf("exit %d\n%s\n%s", code, out, errs)
	}
	h = newHarness(t)
	h.kafka = &retailday.Check{Name: "kafka dead-letter topics", OK: true, Detail: "ok"}
	if code, out, errs := h.run(); code != 0 || h.built != 1 {
		t.Fatalf("a day with a reachable broker: exit %d built %d\n%s\n%s", code, h.built, out, errs)
	}
}

func TestTheBrokerIsNotReadWhenTheClusterAlreadyFailedThePreflight(t *testing.T) {
	h := newHarness(t)
	h.checks = []retailday.Check{{Name: "health a", OK: false, Detail: "down"}}
	h.kafka = &retailday.Check{Name: "kafka dead-letter topics", OK: true}
	code, out, _ := h.run()
	if code != 1 || len(h.kafkaAsked) != 0 || strings.Contains(out, "kafka") {
		t.Errorf("exit %d, kafka asked %v\n%s", code, h.kafkaAsked, out)
	}
}

// ---- the verdict ----

func TestADayPassesOnlyWhenTheDeadLetterCountIsKnown(t *testing.T) {
	type answer struct {
		n     int
		known bool
		err   error
	}
	cases := []struct {
		name            string
		a               answer
		wantCode        int
		wantCodeAllowed int
		mention         string
	}{
		{"known and none", answer{0, true, nil}, 0, 0, ""},
		{"unknown, no error", answer{0, false, nil}, 1, 0, "dead letters"},
		{"unknown, broker unreachable", answer{0, false, fmt.Errorf("dlq: dead letters not counted: the topics could not be listed: connection refused")}, 1, 0, "connection refused"},
		{"known and some", answer{2, true, nil}, 1, 1, "2 dead-lettered message(s) carry this run's id"},
		{"one", answer{1, true, nil}, 1, 1, "1 dead-lettered message(s) carry this run's id"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, allow := range []bool{false, true} {
				h := newHarness(t)
				h.world.deadLetters = func() (int, bool, error) { return tc.a.n, tc.a.known, tc.a.err }
				args := []string{}
				want := tc.wantCode
				if allow {
					args, want = append(args, "-allow-unverified"), tc.wantCodeAllowed
				}
				code, out, errs := h.run(args...)
				if code != want {
					t.Fatalf("allow-unverified=%v: exit %d, want %d\n%s\n%s", allow, code, want, out, errs)
				}
				if tc.mention != "" && !strings.Contains(out, tc.mention) {
					t.Errorf("allow-unverified=%v: the report does not say %q:\n%s", allow, tc.mention, out)
				}
				if tc.a.known && tc.a.n > 0 && !strings.Contains(out, "kafka/dead-letters") {
					t.Errorf("the finding's rule is missing:\n%s", out)
				}
			}
		})
	}
}

// ---- the whole chain: real runner, real audit, real counter, a fake broker ----

// tinyBroker is a one-partition-per-topic dlq.Source over memory.
type tinyBroker struct {
	mu      sync.Mutex
	logs    map[string][]dlq.Message
	listErr error
}

func newTinyBroker() *tinyBroker {
	return &tinyBroker{logs: map[string][]dlq.Message{"warehouse.yard-management.events": nil, "warehouse.yard-management.events.dlq": nil}}
}

func (b *tinyBroker) add(topic, value string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.logs[topic] = append(b.logs[topic], dlq.Message{Offset: int64(len(b.logs[topic])), Value: []byte(value)})
}

func (b *tinyBroker) Topics(context.Context) ([]dlq.Topic, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.listErr != nil {
		return nil, b.listErr
	}
	var out []dlq.Topic
	for name := range b.logs {
		out = append(out, dlq.Topic{Name: name, Partitions: []int{0}})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func (b *tinyBroker) Offsets(_ context.Context, p dlq.Partition) (dlq.Offsets, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return dlq.Offsets{End: int64(len(b.logs[p.Topic]))}, nil
}

func (b *tinyBroker) Read(_ context.Context, p dlq.Partition, from, to int64) ([]dlq.Message, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]dlq.Message(nil), b.logs[p.Topic][from:to]...), nil
}

// withCounter swaps the harness's stub evidence and prepare for the production ones over c: the
// composite evidence, and snapshotDeadLetters as the first step of Prepare.
func (h *harness) withCounter(c deadLetterCounter) {
	inner := buildPhases
	buildPhases = func(ctx context.Context, cfg wireConfig) (day.Phases, func(), error) {
		ph, release, err := inner(ctx, cfg)
		ph.Evidence = evidence{rest: ph.Evidence, dead: c}
		prepare := ph.Prepare
		ph.Prepare = func(ctx context.Context) error {
			if err := snapshotDeadLetters(ctx, c, cfg.Brokers, cfg.Log); err != nil {
				return err
			}
			return prepare(ctx)
		}
		return ph, release, err
	}
}

const dlqRun = "RDDLQ1"

func dlqPayload(run string, n int) string {
	return fmt.Sprintf(`{"type":"x.StockReceived","data":{"reference":"%s-IB-%d"}}`, run, n)
}

func TestADeadLetterOfTheRunFailsTheDayEndToEnd(t *testing.T) {
	const dlqTopic = "warehouse.yard-management.events.dlq"
	broker := newTinyBroker()
	broker.add(dlqTopic, dlqPayload(dlqRun, 0)) // dead-lettered before the day began: not the day's
	h := newHarness(t)
	h.withCounter(dlq.New(broker, dlq.Options{}))
	h.world.duringDay = func() {
		broker.add(dlqTopic, dlqPayload(dlqRun, 1))    // the day's
		broker.add(dlqTopic, dlqPayload("RDOTHR9", 2)) // another run's
	}
	code, out, errs := h.run("-run-id", dlqRun)
	if code != 1 {
		t.Fatalf("a day that dead-lettered a message of its own run exited %d\n%s\n%s", code, out, errs)
	}
	for _, want := range []string{"VERDICT: FAILED", "kafka/dead-letters", "1 dead-lettered message(s) carry this run's id"} {
		if !strings.Contains(out, want) {
			t.Errorf("the report lacks %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "kafka/dead-letters-unverified") {
		t.Errorf("a counted dead letter was reported as unverified:\n%s", out)
	}
	// -allow-unverified excuses what could not be read, never what was found
	h = newHarness(t)
	h.withCounter(dlq.New(broker, dlq.Options{}))
	h.world.duringDay = func() { broker.add(dlqTopic, dlqPayload(dlqRun, 3)) }
	if code, _, _ := h.run("-run-id", dlqRun, "-allow-unverified"); code != 1 {
		t.Errorf("-allow-unverified excused a counted dead letter: exit %d", code)
	}
}

func TestACleanDayPassesEndToEndAndOnlyBecauseTheCountIsKnown(t *testing.T) {
	const dlqTopic = "warehouse.yard-management.events.dlq"
	broker := newTinyBroker()
	broker.add(dlqTopic, dlqPayload(dlqRun, 0)) // history that carries the run id's shape but predates the day
	h := newHarness(t)
	h.withCounter(dlq.New(broker, dlq.Options{}))
	h.world.duringDay = func() { broker.add(dlqTopic, dlqPayload("RDOTHR9", 1)) }
	code, out, errs := h.run("-run-id", dlqRun)
	if code != 0 || !strings.Contains(out, "VERDICT: PASSED") {
		t.Fatalf("a day with no dead letter of its own: exit %d\n%s\n%s", code, out, errs)
	}
	if strings.Contains(out, "dead letters:") {
		t.Errorf("a known count is listed as not verified:\n%s", out)
	}
	if !strings.Contains(errs, "dead-letter snapshot taken") {
		t.Errorf("the snapshot is not in the log:\n%s", errs)
	}
	if h.world.preparedN != 1 {
		t.Errorf("prepare ran %d time(s)", h.world.preparedN)
	}
}

func TestABrokerThatCannotBeReadAfterTheDayLeavesTheCountUnknown(t *testing.T) {
	broker := newTinyBroker()
	h := newHarness(t)
	h.withCounter(dlq.New(broker, dlq.Options{}))
	h.world.duringDay = func() {
		broker.mu.Lock()
		broker.listErr = fmt.Errorf("dial tcp4 127.0.0.1:9092: connect: connection refused")
		broker.mu.Unlock()
	}
	code, out, _ := h.run("-run-id", dlqRun)
	if code != 1 || !strings.Contains(out, "connection refused") || !strings.Contains(out, "kafka/dead-letters-unverified") {
		t.Fatalf("exit %d\n%s", code, out)
	}
	h = newHarness(t)
	h.withCounter(dlq.New(broker, dlq.Options{}))
	broker.mu.Lock()
	broker.listErr = nil
	broker.mu.Unlock()
	h.world.duringDay = func() { broker.mu.Lock(); broker.listErr = fmt.Errorf("connection refused"); broker.mu.Unlock() }
	if code, out, _ := h.run("-run-id", dlqRun, "-allow-unverified"); code != 0 || !strings.Contains(out, "NOT VERIFIED") {
		t.Errorf("-allow-unverified: exit %d\n%s", code, out)
	}
}

func TestABrokerThatCannotBeSnapshottedStopsTheDayBeforeAnyoneBuys(t *testing.T) {
	broker := newTinyBroker()
	broker.listErr = fmt.Errorf("dial tcp4 127.0.0.1:9092: connect: connection refused")
	h := newHarness(t)
	h.withCounter(dlq.New(broker, dlq.Options{}))
	code, out, errs := h.run("-run-id", dlqRun, "-allow-unverified")
	if code != 1 {
		t.Fatalf("exit %d: even -allow-unverified must not play a day whose snapshot could not be taken\n%s\n%s", code, out, errs)
	}
	if h.world.preparedN != 0 || len(h.world.buyers) != 0 {
		t.Errorf("the day went on: prepared %d, %d buyers", h.world.preparedN, len(h.world.buyers))
	}
	for _, want := range []string{"the dead letters cannot be counted", "connection refused", "port-forward"} {
		if !strings.Contains(out+errs, want) {
			t.Errorf("neither stdout nor stderr says %q:\n%s\n%s", want, out, errs)
		}
	}
}
