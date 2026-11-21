package dlq

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

// KafkaSource is the Source over a real broker, through kafka-go's request/response Client: three
// requests only, Metadata, ListOffsets and Fetch. It joins no consumer group, commits nothing and
// has no way to produce or to create a topic.
//
// # Reaching the broker from the host
//
// The fleet runs exactly ONE broker, and in k3s it advertises its in-cluster DNS name
// (kafka-controller-0.kafka-controller-headless.warehouse-systems.svc.cluster.local:9092, from
// the broker's own server.properties) and has no external listener (warehouse-infra ADR 0011:
// externalAccess.enabled = false). A client that follows the advertised address, as kafka-go does
// to find a partition's leader, therefore cannot be reached through `kubectl port-forward` from
// the workstation: dlq-runbook.md says so, and runs every Kafka CLI inside the pod instead.
// KafkaSource does not follow it. Every connection it opens goes to the ONE address it was given,
// whatever the metadata says: with a single broker the leader of every partition is behind that
// address. A cluster with several brokers would answer NotLeaderForPartition for some partitions,
// which is an error (the count is then unknown), never a wrong count. The address is dialled over
// IPv4 only, like sim.IPv4Dialer, because `kubectl port-forward --address 127.0.0.1` binds IPv4
// and "localhost" resolves to ::1 first.
type KafkaSource struct {
	addr string
	tr   *kafka.Transport
	cl   *kafka.Client
}

var _ Source = (*KafkaSource)(nil)

// NewKafkaSource returns a source for the broker at addr ("host:port"). It connects lazily: an
// address nobody listens on is an error of the first call, within that call's context.
func NewKafkaSource(addr string) (*KafkaSource, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil || host == "" || port == "" {
		return nil, fmt.Errorf("dlq: broker address %q is not host:port", addr)
	}
	d := &net.Dialer{Timeout: 5 * time.Second}
	tr := &kafka.Transport{
		Dial:        func(ctx context.Context, _, _ string) (net.Conn, error) { return d.DialContext(ctx, "tcp4", addr) },
		DialTimeout: 5 * time.Second,
		IdleTimeout: 30 * time.Second,
		MetadataTTL: 10 * time.Second,
		ClientID:    "retail-day-dlq",
	}
	return &KafkaSource{addr: addr, tr: tr, cl: &kafka.Client{Addr: kafka.TCP(addr), Transport: tr}}, nil
}

// Close drops the idle connections.
func (k *KafkaSource) Close() { k.tr.CloseIdleConnections() }

// Topics lists every topic with its partitions. A topic (or a partition of it) the broker reports
// an error for comes back with Err set and no partitions: the counter decides whether it matters.
func (k *KafkaSource) Topics(ctx context.Context) ([]Topic, error) {
	res, err := k.cl.Metadata(ctx, &kafka.MetadataRequest{Addr: k.cl.Addr})
	if err != nil {
		return nil, err
	}
	out := make([]Topic, 0, len(res.Topics))
	for _, t := range res.Topics {
		tp := Topic{Name: t.Name, Err: t.Error}
		if tp.Err == nil {
			for _, p := range t.Partitions {
				if p.Error != nil {
					tp.Err, tp.Partitions = fmt.Errorf("partition %d: %w", p.ID, p.Error), nil
					break
				}
				tp.Partitions = append(tp.Partitions, p.ID)
			}
		}
		out = append(out, tp)
	}
	return out, nil
}

// Offsets reads a partition's first offset and its end (the high watermark: the offset the next
// message gets).
func (k *KafkaSource) Offsets(ctx context.Context, p Partition) (Offsets, error) {
	res, err := k.cl.ListOffsets(ctx, &kafka.ListOffsetsRequest{
		Addr:   k.cl.Addr,
		Topics: map[string][]kafka.OffsetRequest{p.Topic: {kafka.FirstOffsetOf(p.ID), kafka.LastOffsetOf(p.ID)}},
	})
	if err != nil {
		return Offsets{}, err
	}
	for _, po := range res.Topics[p.Topic] {
		if po.Partition != p.ID {
			continue
		}
		if po.Error != nil {
			return Offsets{}, po.Error
		}
		if po.FirstOffset < 0 || po.LastOffset < po.FirstOffset {
			return Offsets{}, fmt.Errorf("the broker answered offsets %d..%d", po.FirstOffset, po.LastOffset)
		}
		return Offsets{Start: po.FirstOffset, End: po.LastOffset}, nil
	}
	return Offsets{}, errors.New("the broker did not answer for the partition")
}

// maxStalls is how many fetches in a row may return nothing before Read gives up on a range the
// offsets say exists.
const maxStalls = 3

// Read returns every message with from <= offset < to. It fetches repeatedly from the next offset,
// skips records a batch carries from before the requested offset, and stops at to; a range it
// cannot finish is an error, never a short list.
func (k *KafkaSource) Read(ctx context.Context, p Partition, from, to int64) ([]Message, error) {
	var out []Message
	next, stalls := from, 0
	for next < to {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		res, err := k.cl.Fetch(ctx, &kafka.FetchRequest{
			Addr: k.cl.Addr, Topic: p.Topic, Partition: p.ID, Offset: next,
			MinBytes: 1, MaxBytes: 4 << 20, MaxWait: time.Second,
		})
		if err != nil {
			return nil, err
		}
		if res.Error != nil {
			closeRecords(res.Records)
			return nil, res.Error
		}
		got, last, err := drain(res.Records, next, to)
		closeRecords(res.Records)
		if err != nil {
			return nil, err
		}
		out = append(out, got...)
		if len(got) == 0 {
			if stalls++; stalls >= maxStalls {
				return nil, fmt.Errorf("%d fetches from offset %d returned no record although the partition ends at %d", stalls, next, to)
			}
			continue
		}
		stalls, next = 0, last+1
	}
	return out, nil
}

// drain reads the records of one fetch response with from <= offset < to.
func drain(rr kafka.RecordReader, from, to int64) (msgs []Message, last int64, err error) {
	for {
		rec, err := rr.ReadRecord()
		if errors.Is(err, io.EOF) {
			return msgs, last, nil
		}
		if err != nil {
			return nil, 0, err
		}
		if rec.Offset < from {
			continue // a batch may start before the offset asked for
		}
		if rec.Offset >= to {
			return msgs, last, nil
		}
		m := Message{Offset: rec.Offset}
		if m.Key, err = readAll(rec.Key); err != nil {
			return nil, 0, err
		}
		if m.Value, err = readAll(rec.Value); err != nil {
			return nil, 0, err
		}
		for _, h := range rec.Headers {
			m.Headers = append(m.Headers, Header{Key: h.Key, Value: append([]byte(nil), h.Value...)})
		}
		msgs, last = append(msgs, m), rec.Offset
	}
}

func readAll(b kafka.Bytes) ([]byte, error) {
	if b == nil {
		return nil, nil
	}
	defer b.Close()
	return io.ReadAll(b)
}

func closeRecords(rr kafka.RecordReader) {
	if c, ok := rr.(io.Closer); ok {
		_ = c.Close()
	}
}
