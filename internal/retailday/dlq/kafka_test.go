package dlq

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

func TestNewKafkaSourceRefusesWhatIsNotHostPort(t *testing.T) {
	for _, addr := range []string{"", "localhost", ":9092", "localhost:", "a:1,b:2x"} {
		if k, err := NewKafkaSource(addr); err == nil {
			k.Close()
			t.Errorf("%q was accepted", addr)
		}
	}
	k, err := NewKafkaSource("localhost:9092")
	if err != nil {
		t.Fatal(err)
	}
	k.Close()
}

// The broker advertises an in-cluster name the workstation cannot resolve: every connection must go to
// the address the source was given, whatever the metadata names.
func TestEveryConnectionGoesToTheConfiguredAddressWhateverIsAskedFor(t *testing.T) {
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Skipf("no loopback listener: %v", err)
	}
	defer ln.Close()
	accepted := make(chan struct{}, 1)
	go func() {
		if c, err := ln.Accept(); err == nil {
			accepted <- struct{}{}
			c.Close()
		}
	}()
	k, err := NewKafkaSource(ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := k.tr.Dial(ctx, "tcp", "kafka-controller-0.kafka-controller-headless.warehouse-systems.svc.cluster.local:9092")
	if err != nil {
		t.Fatalf("the advertised name was dialled instead of the configured address: %v", err)
	}
	c.Close()
	select {
	case <-accepted:
	case <-time.After(5 * time.Second):
		t.Fatal("the configured address saw no connection")
	}
}

func TestABrokerNobodyListensOnIsAnErrorWithinTheTimeoutNeverAHang(t *testing.T) {
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Skipf("no loopback listener: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close() // nobody listens on addr now
	k, err := NewKafkaSource(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	c := New(k, Options{CallTimeout: 3 * time.Second, Timeout: 6 * time.Second})
	start := time.Now()
	err = c.Snapshot(context.Background())
	if err == nil {
		t.Fatal("a snapshot of a broker nobody listens on succeeded")
	}
	if took := time.Since(start); took > 10*time.Second {
		t.Errorf("the snapshot took %s", took)
	}
	n, known, cerr := c.DeadLetters(context.Background(), runID)
	if known || n != 0 || cerr == nil {
		t.Fatalf("got (%d, %v, %v)", n, known, cerr)
	}
}

// TestAgainstARealBroker runs the counter over a real Kafka. It writes ONLY to the broker it is given
// (topics it names itself) and is skipped unless RETAIL_DAY_DLQ_IT_BROKER is set, e.g. to a throwaway
// container:
//
//	docker run -d -p 127.0.0.1:29192:9092 ... apache/kafka:3.9.0   (advertising a name the host cannot resolve)
//	RETAIL_DAY_DLQ_IT_BROKER=127.0.0.1:29192 go test -run TestAgainstARealBroker -v ./internal/retailday/dlq/
//
// Never point it at the fleet's broker.
func TestAgainstARealBroker(t *testing.T) {
	addr := os.Getenv("RETAIL_DAY_DLQ_IT_BROKER")
	if addr == "" {
		t.Skip("RETAIL_DAY_DLQ_IT_BROKER is not set")
	}
	k, err := NewKafkaSource(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	w := &kafka.Writer{
		Addr: kafka.TCP(addr), Transport: k.tr, Balancer: &kafka.Hash{}, RequiredAcks: kafka.RequireAll,
		AllowAutoTopicCreation: true, BatchTimeout: 10 * time.Millisecond,
	}
	defer w.Close()
	put := func(topic, key, value string, hs ...kafka.Header) {
		t.Helper()
		var err error
		for range 40 { // a topic that was just created has no leader for a moment, as the services' writeDLQ expects
			if err = w.WriteMessages(context.Background(), kafka.Message{Topic: topic, Key: []byte(key), Value: []byte(value), Headers: hs}); err == nil {
				return
			}
			time.Sleep(250 * time.Millisecond)
		}
		t.Fatalf("write %s: %v", topic, err)
	}
	stamp := time.Now().UnixNano() % 1_000_000_000
	id, other := fmt.Sprintf("IT%d", stamp), fmt.Sprintf("XQ%d", stamp) // neither contains the other: the match is a substring
	alpha, beta, plain := "it-"+id+".alpha.events.dlq", "it-"+id+".beta.events.dlq", "it-"+id+".gamma.events"

	// before the day: one dead letter of this very run id (must not count) and one ordinary event
	put(alpha, "k0", `{"reference":"`+id+`-OLD"}`)
	put(plain, "k0", `{"reference":"`+id+`"}`)

	// a dead letter that finds nothing new is (0, true, nil) on a broker that really answers
	quiet := New(k, Options{})
	if err := quiet.Snapshot(context.Background()); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if n, known, err := quiet.DeadLetters(context.Background(), id); n != 0 || !known || err != nil {
		t.Fatalf("nothing happened since the snapshot: got (%d, %v, %v), want (0, true, nil)", n, known, err)
	}

	c := New(k, Options{})
	if err := c.Snapshot(context.Background()); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	t.Logf("snapshot: %s", c.Summary())

	// during the day
	put(alpha, "k1", `{"reference":"`+id+`-IB-1"}`)                                                                              // value
	put(alpha, "k2", `{"reference":"`+other+`"}`, kafka.Header{Key: "x-dlq-error", Value: []byte("trailer " + id + " unknown")}) // header
	put(alpha, "k3", `{"reference":"`+other+`-1"}`)                                                                              // another run
	put(plain, "k4", `{"reference":"`+id+`"}`)                                                                                   // not a dead-letter topic
	put(beta, id+"-KEY", `{"reference":"nothing"}`)                                                                              // a topic created after the snapshot; the run id is in the key

	n, known, err := c.DeadLetters(context.Background(), id)
	if n != 3 || !known || err != nil {
		t.Fatalf("got (%d, %v, %v), want (3, true, nil): value on alpha, header on alpha, key on the topic created after the snapshot", n, known, err)
	}
	if n, _, _ := c.DeadLetters(context.Background(), other); n != 2 {
		t.Errorf("the other run has %d, want 2", n)
	}
	if n, known, err := c.DeadLetters(context.Background(), id+"-NOPE"); n != 0 || !known || err != nil {
		t.Errorf("an id nobody used: (%d, %v, %v)", n, known, err)
	}
	if !strings.Contains(c.Summary(), ".dlq topic(s)") {
		t.Errorf("summary %q", c.Summary())
	}
}
