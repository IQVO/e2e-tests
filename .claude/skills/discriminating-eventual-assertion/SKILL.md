---
name: discriminating-eventual-assertion
description: How to design a discriminating e2e assertion for an eventually-consistent recompute or feedback loop (event in one service changes state in another) so it fails if the mechanism never ran. Use when writing a scenario that asserts a value "eventually changes" or "eventually appears" after a Kafka hop, a sweep or a consumer.
---

# A scenario that can actually fail

A polling assertion that passes whether or not the mechanism under test ran proves nothing. Before writing steps, answer: *what would this check show if the recompute never happened?*

## Technique (worked example in the repo)

`features/promise_repromise_loop.feature` proves fulfillment-execution's missed-CPT sweep makes order-management re-promise an order, across 3 services and 3 Kafka topics. Its pieces, all in `e2e_test.go`:

1. **Capture the baseline early**: `captureOrderPromiseDate` stores the promise right after allocation, before anything downstream can move it. It fails loudly if the field is missing.
2. **Find the cheapest real discriminator.** Read the computation the harness's ACTUAL config exercises, not the production path. Here `scripts/03-up-services.sh` never sets `PATH_CATALOGUE_SOURCE=kafka` for order-management, so its promise policy falls back to a lead-time policy computed as `now + longest`: a strictly later value on every recompute, with no CPT schedule, capability data or new fixture. Look for any branch whose output depends on `now` or another call-varying value.
3. **Force the trigger with the existing seam, not a new endpoint**: `theTaskForOrderLineHasCPTForcedIntoThePast` back-dates `tasks.cpt` through `dbOpen` (same style as `claimedTaskLeaseForcedExpired`), then `triggerFulfillmentCPTMissedSweep` calls the real `POST /tasks/sweep-cpt-misses`. Sweeps here are REST-triggered, not on a timer.
4. **Assert the mechanism AND the outcome.** `fulfillmentReportsAtLeastOneCPTMiss` checks the sweep's `reported >= 1` (>= only because that counter is process-wide and shared); `orderPromiseDateEventuallyChanges` then polls until the promise differs from the captured one. The strict per-order proof is the second step, because only THIS order's task can move THIS order's promise.
5. **Only wire the full domain path** (CPT schedules, capability catalogue via Kafka) when the fallback cannot discriminate the behaviour, e.g. capability-based routing where every path collapses to the same default.

## Checklist

- Poll with `eventually(...)`; tolerate the expected transient (404 while a consumer catches up), fail on anything else.
- Compare against YOUR scenario's own ids (`w.rs(...)`, `GET /tasks?orderRef=`), never aggregate counts or "any change".
- Never loosen the assertion to make it pass; a flake usually means shared state or a missing run-scope (see `adding-e2e-scenario-or-service`).
- Prove the test can fail: temporarily skip the trigger step (or point it at a wrong id) and confirm the scenario goes red, then restore it.
- Document WHY the discriminator works in the feature file's header comment, as `promise_repromise_loop.feature` does.
