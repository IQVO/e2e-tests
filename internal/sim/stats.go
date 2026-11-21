package sim

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// CallStats counts answers per templated route and keeps the first transport
// errors and 5xx bodies for the report.
type CallStats struct {
	mu       sync.Mutex
	byRoute  map[string]map[int]int
	latency  map[string]time.Duration
	failures []string // transport errors and 5xx, first 50
}

// NewCallStats returns empty statistics.
func NewCallStats() *CallStats {
	return &CallStats{byRoute: map[string]map[int]int{}, latency: map[string]time.Duration{}}
}

// Record notes one answer. Status 0 is a transport error.
func (s *CallStats) Record(route string, status int, d time.Duration, detail string) {
	if s == nil {
		return // a Client built without statistics must not panic on its first call
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.byRoute[route] == nil {
		s.byRoute[route] = map[int]int{}
	}
	s.byRoute[route][status]++
	s.latency[route] += d
	if (status == 0 || status >= 500) && len(s.failures) < 50 {
		s.failures = append(s.failures, fmt.Sprintf("%s -> %d %s", route, status, Truncate(detail, 300)))
	}
}

// Failures returns the recorded transport errors and 5xx answers.
func (s *CallStats) Failures() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.failures...)
}

// Count5xx returns the number of transport errors (status 0) and 5xx
// responses across every route.
func (s *CallStats) Count5xx() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, codes := range s.byRoute {
		for c, k := range codes {
			if c == 0 || c >= 500 {
				n += k
			}
		}
	}
	return n
}

// RouteLine is one route's row in the report.
type RouteLine struct {
	Route    string      `json:"route"`
	Calls    int         `json:"calls"`
	Statuses map[int]int `json:"statuses"`
	AvgMs    float64     `json:"avgMs"`
}

// Lines returns the per-route rows sorted by route.
func (s *CallStats) Lines() []RouteLine {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]RouteLine, 0, len(s.byRoute))
	for r, codes := range s.byRoute {
		n := 0
		cp := map[int]int{}
		for c, k := range codes {
			n += k
			cp[c] = k
		}
		out = append(out, RouteLine{Route: r, Calls: n, Statuses: cp, AvgMs: float64(s.latency[r].Milliseconds()) / float64(n)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Route < out[j].Route })
	return out
}
