// Package dlq counts the dead letters of one retail-day run: a read-only snapshot-and-count over
// the Kafka `*.dlq` topics. *Counter implements the DeadLetters half of retailday.Evidence, which
// the day's verdict (ADR 0001 D6: "no dead letter carrying the run id") fails closed on.
//
// # How a dead letter carries the original message
//
// A fleet consumer that gives up on a message republishes it to `<source-topic>.dlq`. Read on
// origin/develop (nothing here was run against the cluster to learn it):
//
//   - warehouse-infra docs/dlq-runbook.md: "the message is republished unchanged to
//     `<source-topic>.dlq` with three diagnostic headers added" (x-dlq-source-topic, x-dlq-error,
//     x-dlq-failed-at); "the message body is untouched: a CloudEvents 1.0 event in structured
//     content mode". scripts/dlq-redrive.sh reads the same shape (it republishes the value to the
//     x-dlq-source-topic header).
//   - customer-ordering internal/adapters/inbound/kafka/deadletter.go, publishDeadLetter, and
//     network-fulfillment internal/adapters/inbound/kafka/dlq.go, deadLetterMessage (the other
//     services' deadletter.go are the same pattern): the dead letter is
//     Message{Key: original key, Value: original value, Headers: original headers + x-dlq-*}.
//     The writer has AllowAutoTopicCreation: a `.dlq` topic does not exist before its first
//     poison message, so the set of dead-letter topics GROWS during a day.
//
// So the run id is wherever the actors stamped it on what they sent (references, emails, plates,
// seals, ASNs), inside the original JSON value; the key (the aggregate id) is kept; the
// consumer's error string (x-dlq-error) may quote an id as well. A message is the run's when
// the run id occurs as a substring of its value, of one of its header VALUES, or of its key.
// The value rule is the one sim.EventTap applies to `.dlq` topics (strings.Contains on the
// payload); the header and key rules add the places a copy of the id can survive that the
// payload alone would miss. Header names never match. Like the tap, this is a substring rule:
// a run id that is a prefix of another run's id would also match it.
//
// # The snapshot and the count
//
// Snapshot, taken ONCE before the day starts (cmd/retail-day takes it as the first step of
// Prepare, before any write), records the end offset of every partition of every topic whose
// name ends in ".dlq". Topics are discovered by that suffix, never listed: a new bounded context
// adds one. DeadLetters, after the day, lists the topics again and, for every `.dlq` partition,
// reads the messages from the snapshot's end offset up to the end offset at that moment and
// counts those that carry the run id. Nothing is consumed through a group, nothing is
// committed, nothing is written: the Source interface has no write method.
//
// A partition that did not exist at the snapshot (its topic was created after it, or a
// partition was added to a topic) cannot hold a message from before the snapshot, so it is read
// from offset 0, where every partition starts. That is sound only because the snapshot's topic
// listing was complete: a snapshot that could not list or measure a `.dlq` partition is not
// taken at all. If such a partition's log no longer starts at 0, messages of the day were
// deleted unseen and the count is unknown.
//
// # known
//
// DeadLetters answers (n, true, nil) only when every `.dlq` partition found at count time was
// read from its snapshot offset to its end offset with no error, every message of that range
// came back, and the snapshot was taken. A run with no dead letter on reachable topics is
// (0, true, nil). Anything else is (0, false, err) where err names every partition or step that
// could not be read: the broker unreachable, a topic whose metadata or offsets or messages could
// not be read, a topic of the snapshot that is gone, messages deleted by retention before they
// were counted, a read that returned fewer messages than the offsets say, no snapshot, an empty
// run id, an expired context. A count that failed is never 0.
//
// Every Kafka call is bounded (Options.CallTimeout per call, Options.Timeout for a whole
// Snapshot or DeadLetters) and runs behind a goroutine that is abandoned on expiry, so a Source
// that ignores its context cannot hang the day; a Source that panics is an error.
//
// # Limits (said, not hidden)
//
//   - A message still being retried by its consumer when the count runs is not a dead letter
//     yet: the count covers what had been dead-lettered by then.
//   - The substring rule can miss a dead letter whose copy of the run id was not in the value,
//     the headers or the key, and can match an id that merely contains the run id.
//   - Dead letters are read in memory: the range between two offsets of a `.dlq` topic during
//     one day is small by construction (a backlog, not an archive: dlq-runbook.md).
package dlq
