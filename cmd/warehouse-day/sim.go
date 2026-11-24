package main

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ---------------------------------------------------------------------
// simulator state
// ---------------------------------------------------------------------

type tote struct {
	orderID string
	lineNo  int
	cpt     string
}

type parcel struct {
	packageID string
	orderID   string
}

type sim struct {
	cfg   config
	api   *api
	stats *callStats
	clock *simClock
	rnd   *rand.Rand
	rndMu sync.Mutex

	mu       sync.Mutex
	orders   []*orderRec
	byID     map[string]*orderRec
	storage  []string // inventory bin ids == facility location codes
	overflow string
	totes    chan tote
	conveyor chan parcel

	associates []*associate
	findings   []string
	// findingsDuringSuspend are findings raised inside a host-suspension
	// window (see suspendWatch); reported, but not counted as failures.
	findingsDuringSuspend []string
	snapshots             []string
	suspend               suspendWatch

	m struct {
		released, releaseRejected               atomic.Int64
		picks, picksNoLocation, foreignTasks    atomic.Int64
		confirmPickConflicts, completeConflicts atomic.Int64
		confirmPickRetries                      atomic.Int64
		rebinScans, packs, slams, labeled       atomic.Int64
		diverted, travelMetres                  atomic.Int64
		breaks, reassignments, cycleCounts      atomic.Int64
		discrepancies, leaseRenewals            atomic.Int64
	}
	intakeDone atomic.Bool
	// slotsImported is how many NEW slots facility-layout registered this
	// morning. The building is mapped once and reused on later days (as in
	// a real FC), so LocationSlotRegistered is only expected on day one.
	slotsImported int
	shiftPlanned  bool
	tap           *eventTap

	// proposalWait bounds how long the planner polls workforce-management
	// for the proposal from the labour plan (its copy of
	// LabourPlanCommitted arrives over Kafka); proposalPoll is the pause
	// between two asks.
	proposalWait, proposalPoll time.Duration
}

func newSim(cfg config) *sim {
	stats := newCallStats()
	s := &sim{
		cfg:          cfg,
		api:          newAPI(cfg, stats),
		stats:        stats,
		rnd:          rand.New(rand.NewSource(cfg.seed)),
		byID:         map[string]*orderRec{},
		totes:        make(chan tote, 1024),
		conveyor:     make(chan parcel, 1024),
		proposalWait: 30 * time.Second,
		proposalPoll: 2 * time.Second,
	}
	s.suspend.Log = logf
	return s
}

func (s *sim) rf() float64 {
	s.rndMu.Lock()
	defer s.rndMu.Unlock()
	return s.rnd.Float64()
}

func (s *sim) ri(n int) int {
	s.rndMu.Lock()
	defer s.rndMu.Unlock()
	return s.rnd.Intn(n)
}

func (s *sim) sku(key string) string { return fmt.Sprintf("SKU-%s-%s", s.cfg.runID, key) }

func (s *sim) skuKey(sku string) string {
	return strings.TrimPrefix(sku, fmt.Sprintf("SKU-%s-", s.cfg.runID))
}

// suspendGrace covers the real 5-minute claim leases that lapse while the
// host sleeps and only surface on the associates' next calls after wake.
const suspendGrace = 6 * time.Minute

func (s *sim) finding(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	duringSuspend := s.suspend.Covers(time.Now(), suspendGrace)
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, f := range append(s.findings, s.findingsDuringSuspend...) {
		if f == msg {
			return
		}
	}
	if duringSuspend {
		s.findingsDuringSuspend = append(s.findingsDuringSuspend, msg)
		logf("FINDING*", "%s (during/just after a host suspension; not counted)", msg)
		return
	}
	s.findings = append(s.findings, msg)
	logf("FINDING", "%s", msg)
}

func (s *sim) order(id string) *orderRec {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.byID[id]
}

func (s *sim) shiftID() string {
	return fmt.Sprintf("%s-DAY-%s", time.Now().Format("20060102"), s.cfg.runID)
}
