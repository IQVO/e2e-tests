package dlq

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// Suffix is what makes a topic a dead-letter topic: `<source-topic>.dlq`.
const Suffix = ".dlq"

// Partition is one partition of one topic.
type Partition struct {
	Topic string
	ID    int
}

func (p Partition) String() string { return fmt.Sprintf("%s[%d]", p.Topic, p.ID) }

// Topic is one entry of the cluster's topic listing. Err is a metadata error for that topic
// (no leader yet, unknown topic): a `.dlq` topic that has one cannot be measured or read.
type Topic struct {
	Name       string
	Partitions []int
	Err        error
}

// Offsets of one partition: Start is the first offset still on the log (older ones were deleted
// by retention), End the offset the next message will get, so [Start, End) is what can be read.
type Offsets struct{ Start, End int64 }

// Header is one record header.
type Header struct {
	Key   string
	Value []byte
}

// Message is one record of a partition.
type Message struct {
	Offset  int64
	Key     []byte
	Value   []byte
	Headers []Header
}

// Source is everything the counter needs from Kafka, and all of it is a read: there is no method
// that writes, commits an offset or creates a topic, so the counter cannot. Every method must
// honour ctx. KafkaSource is the real one; the tests use a fake.
type Source interface {
	// Topics lists every topic of the cluster (not only the dead-letter ones: an empty cluster
	// listing is how a broker that is not answering properly looks, and the counter refuses it).
	Topics(ctx context.Context) ([]Topic, error)
	// Offsets reads the first and the end offset of one partition.
	Offsets(ctx context.Context, p Partition) (Offsets, error)
	// Read returns every message of the partition with from <= offset < to, or an error. It
	// must not return fewer without saying so.
	Read(ctx context.Context, p Partition, from, to int64) ([]Message, error)
}

// Options bounds the counter. The zero value is usable.
type Options struct {
	// CallTimeout bounds one call to the Source. Default 30 s.
	CallTimeout time.Duration
	// Timeout bounds a whole Snapshot or DeadLetters. Default 2 minutes.
	Timeout time.Duration
}

func (o Options) withDefaults() Options {
	if o.CallTimeout <= 0 {
		o.CallTimeout = 30 * time.Second
	}
	if o.Timeout <= 0 {
		o.Timeout = 2 * time.Minute
	}
	return o
}

// ErrNoSource: the counter was built without a Source.
var ErrNoSource = errors.New("dlq: no Kafka source is configured")

// ErrNoSnapshot: DeadLetters was asked before Snapshot succeeded. Without the offsets the day
// started at, "what was appended since" is unknown, and counting everything would charge this
// run with other runs' dead letters.
var ErrNoSnapshot = errors.New("dlq: no snapshot of the dead-letter topics was taken before the day started")

// Counter is the dead-letter evidence. Its zero value is not usable: use New.
type Counter struct {
	src Source
	opt Options

	mu   sync.Mutex
	snap map[Partition]int64 // end offset of every `.dlq` partition when the day started; nil = none taken
}

// New returns a counter reading through src (nil is an error on every call, never a panic).
func New(src Source, opt Options) *Counter {
	return &Counter{src: src, opt: opt.withDefaults()}
}

// problems collects what could not be read; Error names every one of them and Unwrap keeps the
// causes reachable for errors.Is (a cancelled context stays a cancelled context).
type problems []error

func (p problems) Error() string {
	parts := make([]string, len(p))
	for i, e := range p {
		parts[i] = e.Error()
	}
	return strings.Join(parts, "; ")
}

func (p problems) Unwrap() []error { return p }

// bounded runs one call under d and the caller's ctx. fn runs in its own goroutine, so a Source
// that ignores its context still lets the caller go when the time is up (the goroutine ends when
// fn does; its answer goes to a buffered channel nobody has to read). A panic is an error.
func bounded[T any](ctx context.Context, d time.Duration, fn func(context.Context) (T, error)) (T, error) {
	ctx, cancel := context.WithTimeout(ctx, d)
	defer cancel()
	if err := ctx.Err(); err != nil {
		var zero T
		return zero, err // a context that already ended starts no call
	}
	type result struct {
		v   T
		err error
	}
	ch := make(chan result, 1)
	go func() {
		defer func() {
			if p := recover(); p != nil {
				ch <- result{err: fmt.Errorf("the Kafka source panicked: %v", p)}
			}
		}()
		v, err := fn(ctx)
		ch <- result{v, err}
	}()
	select {
	case r := <-ch:
		return r.v, r.err
	case <-ctx.Done():
		var zero T
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return zero, fmt.Errorf("no answer within %s: %w", d, ctx.Err())
		}
		return zero, ctx.Err()
	}
}

// deadLetterPartitions picks the `.dlq` partitions out of a listing, in a stable order, and says
// which `.dlq` topics could not be listed. Topics are recognised by the suffix alone.
func deadLetterPartitions(topics []Topic) ([]Partition, problems) {
	var parts []Partition
	var bad problems
	for _, t := range topics {
		if !strings.HasSuffix(t.Name, Suffix) {
			continue
		}
		if t.Err != nil {
			bad = append(bad, fmt.Errorf("%s: its metadata could not be read: %w", t.Name, t.Err))
			continue
		}
		if len(t.Partitions) == 0 {
			bad = append(bad, fmt.Errorf("%s: the broker lists no partition for it", t.Name))
			continue
		}
		for _, id := range t.Partitions {
			parts = append(parts, Partition{Topic: t.Name, ID: id})
		}
	}
	sort.Slice(parts, func(i, j int) bool {
		if parts[i].Topic != parts[j].Topic {
			return parts[i].Topic < parts[j].Topic
		}
		return parts[i].ID < parts[j].ID
	})
	return parts, bad
}

// list reads the topic listing and refuses an empty one. listed is false when there is no
// usable listing at all (the caller stops: whatever else it could say would be noise); otherwise
// bad names the `.dlq` topics inside it that cannot be used.
func (c *Counter) list(ctx context.Context) (parts []Partition, bad problems, listed bool) {
	topics, err := bounded(ctx, c.opt.CallTimeout, c.src.Topics)
	if err != nil {
		return nil, problems{fmt.Errorf("the topics could not be listed: %w", err)}, false
	}
	if len(topics) == 0 {
		return nil, problems{errors.New("the broker listed no topic at all (not even its internal ones): that is not an answer to \"are there dead letters\"")}, false
	}
	parts, bad = deadLetterPartitions(topics)
	return parts, bad, true
}

// Snapshot records the end offset of every `.dlq` partition. It is taken once, before the day
// does anything: a second call is an error (moving the snapshot forward would hide the dead
// letters of everything that happened in between), and so is any partition that cannot be
// listed or measured, because a snapshot that is missing one is not a starting point. A failed
// Snapshot leaves no snapshot, so DeadLetters stays unknown.
func (c *Counter) Snapshot(ctx context.Context) error {
	if c.src == nil {
		return ErrNoSource
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.snap != nil {
		return errors.New("dlq: the snapshot was already taken; it is taken once, before the day starts")
	}
	ctx, cancel := context.WithTimeout(ctx, c.opt.Timeout)
	defer cancel()

	parts, bad, listed := c.list(ctx)
	if !listed {
		return fmt.Errorf("dlq: the dead-letter topics could not be snapshotted: %w", bad)
	}
	end := make(map[Partition]int64, len(parts))
	for _, p := range parts {
		if err := ctx.Err(); err != nil {
			bad = append(bad, fmt.Errorf("stopped before %s: %w", p, err))
			break
		}
		off, err := bounded(ctx, c.opt.CallTimeout, func(ctx context.Context) (Offsets, error) { return c.src.Offsets(ctx, p) })
		if err != nil {
			bad = append(bad, fmt.Errorf("%s: its end offset could not be read: %w", p, err))
			continue
		}
		end[p] = off.End
	}
	if len(bad) > 0 {
		return fmt.Errorf("dlq: the dead-letter topics could not be snapshotted: %w", bad)
	}
	c.snap = end
	return nil
}

// Summary says what the snapshot holds, for the run's log.
func (c *Counter) Summary() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.snap == nil {
		return "no snapshot taken"
	}
	topics := map[string]bool{}
	var msgs int64
	for p, end := range c.snap {
		topics[p.Topic] = true
		msgs += end
	}
	return fmt.Sprintf("%d %s topic(s), %d partition(s), %d message(s) already on them", len(topics), Suffix, len(c.snap), msgs)
}

// DeadLetters counts the dead letters of runID appended to any `.dlq` partition since the
// snapshot. See the package comment for when known is true: it is the only case in which n may
// be believed, and n is 0 whenever known is false.
func (c *Counter) DeadLetters(ctx context.Context, runID string) (n int, known bool, err error) {
	if c.src == nil {
		return 0, false, ErrNoSource
	}
	if runID == "" {
		return 0, false, errors.New("dlq: no run id to look for: an empty id would match every dead letter")
	}
	c.mu.Lock()
	snap := c.snap
	c.mu.Unlock()
	if snap == nil {
		return 0, false, ErrNoSnapshot
	}
	ctx, cancel := context.WithTimeout(ctx, c.opt.Timeout)
	defer cancel()

	parts, bad, listed := c.list(ctx)
	if !listed {
		return 0, false, fmt.Errorf("dlq: dead letters not counted: %w", bad)
	}
	// a partition of the snapshot that is not listed now cannot be counted: its dead letters may be gone
	now := make(map[Partition]bool, len(parts))
	for _, p := range parts {
		now[p] = true
	}
	var gone []Partition
	for p := range snap {
		if !now[p] {
			gone = append(gone, p)
		}
	}
	sort.Slice(gone, func(i, j int) bool { return gone[i].String() < gone[j].String() })
	for _, p := range gone {
		bad = append(bad, fmt.Errorf("%s: it was on the snapshot and is not listed now", p))
	}

	found := 0
	for _, p := range parts {
		if err := ctx.Err(); err != nil {
			bad = append(bad, fmt.Errorf("stopped before %s: %w", p, err))
			break
		}
		// a partition that is not on the snapshot is read from offset 0 (the map's zero value),
		// where every partition starts: nothing on it predates the day
		k, err := c.count(ctx, p, snap[p], runID)
		if err != nil {
			bad = append(bad, fmt.Errorf("%s: %w", p, err))
			continue
		}
		found += k
	}
	if len(bad) > 0 {
		lead := "dlq: dead letters not counted"
		if found > 0 {
			lead = fmt.Sprintf("dlq: dead letters not counted (%d already found carrying %s on the partitions that could be read)", found, runID)
		}
		return 0, false, fmt.Errorf("%s: %w", lead, bad)
	}
	return found, true, nil
}

// count reads one partition from offset from (where the snapshot left it; 0 for a partition the
// snapshot did not have, whose every message is therefore this day's) to where it ends now. If the
// log no longer starts at from, some of those messages were deleted unseen.
func (c *Counter) count(ctx context.Context, p Partition, from int64, runID string) (int, error) {
	off, err := bounded(ctx, c.opt.CallTimeout, func(ctx context.Context) (Offsets, error) { return c.src.Offsets(ctx, p) })
	if err != nil {
		return 0, fmt.Errorf("its offsets could not be read: %w", err)
	}
	if off.End < from {
		return 0, fmt.Errorf("its end offset %d is below the snapshot's %d: the topic was recreated, nothing can be said about it", off.End, from)
	}
	if off.Start > from {
		return 0, fmt.Errorf("offsets %d-%d were deleted by retention before they were counted", from, off.Start-1)
	}
	to := off.End
	if from >= to {
		return 0, nil
	}
	msgs, err := bounded(ctx, c.opt.CallTimeout, func(ctx context.Context) ([]Message, error) { return c.src.Read(ctx, p, from, to) })
	if err != nil {
		return 0, fmt.Errorf("offsets %d-%d could not be read: %w", from, to-1, err)
	}
	seen := make(map[int64]bool, len(msgs))
	n := 0
	for _, m := range msgs {
		if m.Offset < from || m.Offset >= to || seen[m.Offset] {
			continue // not ours to count: before the snapshot, after the count, or twice
		}
		seen[m.Offset] = true
		if Carries(m, runID) {
			n++
		}
	}
	if int64(len(seen)) != to-from {
		return 0, fmt.Errorf("offsets %d-%d: %d of %d message(s) came back", from, to-1, len(seen), to-from)
	}
	return n, nil
}

// Carries says whether the message belongs to the run: its value, one of its header values or its
// key contains runID (see the package comment). An empty runID carries nothing.
func Carries(m Message, runID string) bool {
	if runID == "" {
		return false
	}
	id := []byte(runID)
	if bytes.Contains(m.Value, id) || bytes.Contains(m.Key, id) {
		return true
	}
	for _, h := range m.Headers {
		if bytes.Contains(h.Value, id) {
			return true
		}
	}
	return false
}
