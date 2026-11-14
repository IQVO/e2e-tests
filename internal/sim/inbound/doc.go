// Package inbound simulates the people who bring stock back into the building, over the published REST contracts
// only: the supplier (announces the delivery as an ASN), the dock planner (books an inbound door), the gate and yard
// clerk (checks the carrier in, docks the trailer, starts the door work, lets the empty trailer leave), the receiver
// (opens, counts and closes the receipt) and the stower (puts the received units into bins). Crew.Deliver composes
// them into one truck; Crew.Restock orders back whatever a day consumed so the day ends at or above the stock it
// started with (ADR 0001 D4, docs/adr/0001-retail-day.md).
//
//	ASN -> dock appointment -> gate -> receipt -> stow
//
//	inbound-receiving   POST /asns                       the supplier's advance ship notice
//	dock-scheduling     POST /appointments               INBOUND, the ASN, the seal the carrier declares
//	yard-management     POST /visits                     LIVE_UNLOAD of one LOADED trailer carrying that seal
//	                    POST /moves/{id}/completions     the trailer docks (TO_DOOR), via a hostler if it was parked
//	                    POST /trailers/{id}/door-work-starts
//	inbound-receiving   POST /receipts                   on the appointment: takes its door
//	                    POST /receipts/{id}/lines        every line Good, counted once
//	                    POST /receipts/{id}/closures     releases the unload in yard-management, hands the lines over
//	inventory-storage   POST /stowages                   what the receipt counted, into bins
//	yard-management     GET /trailers/{id}, POST /visits/{id}/check-outs     once the close released the unload
//	dock-scheduling     GET /appointments/{id}           until COMPLETED
//
// # What it does NOT do, on purpose
//
// No database, no injected Kafka event, no call a real person could not make. The things that move between services
// by event are only OBSERVED: the door release (the trailer's door work turns DONE by itself once ReceiptClosed was
// consumed; the clerk never completes it), the appointment's COMPLETED, and, with a Kafka tap, inventory-storage's
// StockReceived (Crew.Staged). A refusal is a finding in Delivery.Problems or Result.Problems, never a panic, and the
// run stops where it cannot go on and says how far it got.
//
// # Lag and bounds
//
// A call that depends on a fact another service has not consumed yet is refused with a specific (status, slug) pair
// until the consumer catches up: inbound-receiving's 422 unknown-sku, dock-scheduling's 422 unknown-asn and
// unknown-door, yard-management's 422 unknown-appointment and unknown-location, inbound-receiving's 422
// unknown-appointment. Exactly those pairs are retried, in a bounded loop, as a fresh request (the services store a
// 4xx under its Idempotency-Key, so re-sending the key would replay the refusal for ever). Anything else, a transport
// error included, fails at once. Waiting for a state (the unload released, the appointment completed, the units
// staged) is done by READING it, never by retrying a refusal. Every wait is counted in injected clock time (Clock), so
// a test clock makes a bounded wait exact and instant; the defaults are the suite's own 30 s and 500 ms.
//
// # Stow
//
// inventory-storage books a Good receipt line as STAGED stock (not usable) and has no route that reads it; the units
// become usable when stowed (POST /stowages names the item, the SKU, and the location, the bin). Stow does not check
// that anything was staged, so a stow alone would make the stock look right while the receipt handover is broken:
// give the crew a tap (Crew.Staged) and it waits for the StockReceived of every SKU before it stows, and does not stow
// without it. The units are offered to Site.Bins in a SKU-hash rotation (chaotic storage, reproducible); a bin's room
// is read first and a line larger than it is split; a bin that refuses the SKU's class (hazmat, temperature) is
// skipped.
//
// # Reused from the other actor packages
//
// outbound.Slug (problem slug), outbound.Transient (a lag pair), outbound.Waits, Carrier, Window and NextWindow's slot
// arithmetic, outbound.CalendarSettings and the readiness Report/Check/Status types, outbound.Confirmations (the tap's
// Seen), transport.Clock, and sim.Client, sim.ListAll and sim.EventTap. The yard clerk's calls are written here
// because outbound's are LIVE_LOAD-only or unexported.
//
// It was written against the services' published contracts (apis/openapi.yaml of inbound-receiving, dock-scheduling,
// yard-management and inventory-storage, their ADRs) and exercised against contract fakes (fakes_test.go); it has
// never run against the cluster.
package inbound
