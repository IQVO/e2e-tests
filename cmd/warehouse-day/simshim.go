package main

import (
	"context"

	simlib "github.com/claudioed/warehouse-e2e/internal/sim"
)

// The REST client, call statistics, suspension watch and Kafka tap live in
// internal/sim, shared with cmd/retail-day. These aliases keep the day's own
// call sites readable; the rules themselves are fixed in one place.
type (
	resp         = simlib.Resp
	api          = simlib.Client
	callStats    = simlib.CallStats
	eventTap     = simlib.EventTap
	suspendWatch = simlib.SuspendWatch
)

func newCallStats() *callStats { return simlib.NewCallStats() }

func newKey() string { return simlib.NewKey() }

func truncate(s string, n int) string { return simlib.Truncate(s, n) }

func shortType(s string) string { return simlib.ShortType(s) }

func listAll[T any](ctx context.Context, a *api, svc, route, path string) ([]T, resp) {
	return simlib.ListAll[T](ctx, a, svc, route, path)
}

func startTap(ctx context.Context, brokers []string, runID string) (*eventTap, error) {
	return simlib.StartTap(ctx, brokers, runID, tappedTopics, logf)
}
