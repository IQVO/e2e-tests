// Command ensure-topics creates, and waits for a leader on, every Kafka topic
// the harness's services publish to or consume from.
//
// Why it exists: a broker with topic auto-creation on is not enough for a
// freshly started broker. The FIRST publish to a topic can race the broker's
// own metadata propagation and fail with "Unknown Topic Or Partition", and a
// consumer group subscribed to a topic that does not exist yet is never given
// an assignment, so it silently never reads anything (this stalled the
// inter-warehouse transfer scenario at "no published capacity plan facts" on a
// clean broker). Creating every topic explicitly up front, then confirming
// each has a leader, removes both races. It is idempotent: an existing topic is
// left alone, so it is safe against the shared cluster broker too.
//
// Usage: KAFKA_BROKERS=host:port go run ./cmd/ensure-topics
package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

// topics is every topic a harness service publishes to or consumes from.
// Business topics are named warehouse.<context>.events; analytics streams
// warehouse.<context>.analytics.
var topics = []string{
	"warehouse.facility.events",
	"warehouse.facility.analytics",
	"warehouse.product-master.events",
	// inbound-receiving's integration topic: inventory-storage consumes
	// ReceiptLineReceived from it (a group on a topic that does not exist yet
	// is never assigned a partition), and inbound_receiving.feature scans it.
	"warehouse.inbound-receiving.events",
	"warehouse.inventory.events",
	"warehouse.inventory.analytics",
	"warehouse.work-planning.events",
	"warehouse.wes.analytics",
	"warehouse.fulfillment.events",
	"warehouse.fulfillment.analytics",
	"warehouse.workforce.events",
	"warehouse.workforce.analytics",
	"warehouse.order-management.events",
	"warehouse.order-management.analytics",
	"warehouse.process-path-management.events",
	"warehouse.labor-performance.events",
	"warehouse.network-inventory-planning.events",
	"warehouse.network-inventory-planning.analytics",
	"warehouse.warehouse-planning.events",
	"warehouse.warehouse-planning.analytics",
}

const leaderWait = 30 * time.Second

func main() {
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "localhost:9092"
	}
	if err := run(context.Background(), strings.Split(brokers, ",")[0]); err != nil {
		fmt.Fprintln(os.Stderr, "ensure-topics:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, broker string) error {
	conn, err := kafka.DialContext(ctx, "tcp", broker)
	if err != nil {
		return fmt.Errorf("dial %s: %w", broker, err)
	}
	defer func() { _ = conn.Close() }()

	// Topic creation must go to the controller.
	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("find controller: %w", err)
	}
	cc, err := kafka.DialContext(ctx, "tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return fmt.Errorf("dial controller %s:%d: %w", controller.Host, controller.Port, err)
	}
	defer func() { _ = cc.Close() }()

	configs := make([]kafka.TopicConfig, 0, len(topics))
	for _, t := range topics {
		configs = append(configs, kafka.TopicConfig{Topic: t, NumPartitions: 1, ReplicationFactor: 1})
	}
	// CreateTopics on an existing topic is a no-op success.
	if err := cc.CreateTopics(configs...); err != nil && !errors.Is(err, kafka.TopicAlreadyExists) {
		return fmt.Errorf("create topics: %w", err)
	}

	deadline := time.Now().Add(leaderWait)
	for _, t := range topics {
		if err := waitForLeader(conn, t, deadline); err != nil {
			return err
		}
	}
	fmt.Printf("ensure-topics: %d topics ready on %s\n", len(topics), broker)
	return nil
}

func waitForLeader(conn *kafka.Conn, topic string, deadline time.Time) error {
	for {
		parts, err := conn.ReadPartitions(topic)
		if err == nil && len(parts) > 0 && parts[0].Leader.ID >= 0 && parts[0].Leader.Host != "" {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("topic %s never got an assigned leader (last error: %v)", topic, err)
		}
		time.Sleep(250 * time.Millisecond)
	}
}
