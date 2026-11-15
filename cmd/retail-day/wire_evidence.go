package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/claudioed/warehouse-e2e/internal/retailday"
	"github.com/claudioed/warehouse-e2e/internal/retailday/dlq"
)

// defaultBrokers is the address -brokers (KAFKA_BROKERS) has when nothing says otherwise. It is the
// address cmd/warehouse-day and env.sh already use for the shared broker (KAFKA_BROKERS, default
// localhost:9092), and the one a `kubectl port-forward svc/kafka 9092:9092` binds on this host.
//
// It is NOT an address the k3s cluster answers on by itself: scripts/k3s-edges.sh (warehouse-infra
// ADR 0011) forwards the UI, Kong, Grafana, Jaeger, Prometheus and Kiali and no Kafka, and the broker
// has no external listener (externalAccess.enabled = false). Until a forward is running the
// preflight and Prepare say so, naming the command (see brokerHint), instead of the day running
// without the one fact its verdict needs.
const defaultBrokers = "localhost:9092"

// brokerHint is what a person does when the dead letters cannot be counted. The broker advertises its
// in-cluster DNS name, so the forward works only because internal/retailday/dlq dials the address it
// was given whatever the broker advertises (see dlq.KafkaSource).
func brokerHint(addr string) string {
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		port = "9092"
	}
	return fmt.Sprintf("Kafka is not forwarded by k3s-edges.sh and the broker has no external listener: start `kubectl --context k3s-multipass -n warehouse-systems port-forward --address 127.0.0.1 svc/kafka %s:9092` and keep it open, or point -brokers (KAFKA_BROKERS) at a broker you can reach; -brokers \"\" runs the day without counting dead letters, which then passes only with -allow-unverified", port)
}

// checkBrokers is the usage rule for -brokers: empty (do not count) or ONE host:port. The fleet runs a
// single broker and dlq.KafkaSource dials the one address it is given, so a list would be a lie.
func checkBrokers(addr string) string {
	if addr == "" {
		return ""
	}
	if strings.Contains(addr, ",") {
		return fmt.Sprintf("-brokers %q: one broker only (the fleet runs exactly one, and every connection goes to this one address)", addr)
	}
	if host, port, err := net.SplitHostPort(addr); err != nil || host == "" || port == "" {
		return fmt.Sprintf("-brokers %q must be host:port (or empty to run without counting dead letters)", addr)
	}
	return ""
}

// deadLetterCounter is what the day needs of the dead-letter evidence: the snapshot Prepare takes
// before anything is written, the count the audit asks for, and a line for the log.
// *dlq.Counter is the real one.
type deadLetterCounter interface {
	Snapshot(ctx context.Context) error
	Summary() string
	DeadLetters(ctx context.Context, runID string) (n int, known bool, err error)
}

var _ deadLetterCounter = (*dlq.Counter)(nil)

// uncounted stands for a run given no broker (-brokers ""): nothing is snapshotted and the count says,
// in words, why it cannot be known. The verdict then fails the day unless -allow-unverified.
type uncounted struct{}

func (uncounted) Snapshot(context.Context) error { return nil }
func (uncounted) Summary() string                { return "not counted: no broker is configured" }
func (uncounted) DeadLetters(context.Context, string) (int, bool, error) {
	return 0, false, errors.New("dead letters are not counted: no Kafka broker is configured (-brokers is empty) and no REST route lists them")
}

// newDeadLetterCounter builds the counter for a broker address; release closes what it opened. A
// variable so a test can hand the wiring a fake broker. It dials nothing: the first call does.
var newDeadLetterCounter = func(addr string) (deadLetterCounter, func(), error) {
	if addr == "" {
		return uncounted{}, func() {}, nil
	}
	src, err := dlq.NewKafkaSource(addr)
	if err != nil {
		return nil, nil, err
	}
	return dlq.New(src, dlq.Options{}), src.Close, nil
}

// evidence is the day's retailday.Evidence: the REST collector answers Order and Shelf, the dead-letter
// counter answers DeadLetters (internal/retailday/collect cannot: no published route lists them).
type evidence struct {
	rest retailday.Evidence
	dead interface {
		DeadLetters(ctx context.Context, runID string) (int, bool, error)
	}
}

var _ retailday.Evidence = evidence{}

func (e evidence) Order(ctx context.Context, p retailday.Placed) (retailday.OrderFacts, []string) {
	return e.rest.Order(ctx, p)
}

func (e evidence) Shelf(ctx context.Context, skus []string) (map[string]retailday.Shelf, error) {
	return e.rest.Shelf(ctx, skus)
}

func (e evidence) DeadLetters(ctx context.Context, runID string) (int, bool, error) {
	return e.dead.DeadLetters(ctx, runID)
}

// snapshotDeadLetters is the first step of Prepare, before the building is touched and before any
// write: the dead-letter topics' end offsets are what "appended since the day began" is counted from.
// It fails closed: a configured broker that cannot be snapshotted stops the day, because a day that
// started without the snapshot could never be told apart from one with dead letters.
func snapshotDeadLetters(ctx context.Context, d deadLetterCounter, addr string, logf func(string, ...any)) error {
	if d == nil {
		return errors.New("no dead-letter counter was wired: the day cannot be judged on its dead letters")
	}
	if err := d.Snapshot(ctx); err != nil {
		return fmt.Errorf("the dead letters cannot be counted: the snapshot of the Kafka dead-letter topics at %s could not be taken before the day: %w. %s", addr, err, brokerHint(addr))
	}
	if logf != nil {
		logf("prepare: dead-letter snapshot taken before anything is written (%s)", d.Summary())
	}
	return nil
}

// checkKafka is the preflight's one Kafka check, run only when the rest passed and a broker is
// configured: it takes a throwaway snapshot (reads only), so an unreachable broker stops the run before
// the release window is waited for, not after. ok is false when there is nothing to check.
var checkKafka = func(ctx context.Context, addr string) (c retailday.Check, ok bool) {
	if addr == "" {
		return retailday.Check{}, false
	}
	c = retailday.Check{Name: "kafka dead-letter topics"}
	src, err := dlq.NewKafkaSource(addr)
	if err != nil {
		c.Detail, c.Fix = err.Error(), "-brokers takes host:port"
		return c, true
	}
	defer src.Close()
	probe := dlq.New(src, dlq.Options{Timeout: 30 * time.Second})
	if err := probe.Snapshot(ctx); err != nil {
		c.Detail, c.Fix = fmt.Sprintf("%s: %v", addr, err), brokerHint(addr)
		return c, true
	}
	c.OK, c.Detail = true, fmt.Sprintf("%s: %s", addr, probe.Summary())
	return c, true
}
