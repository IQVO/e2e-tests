package main

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"sync"
	"time"
)

// ---------------------------------------------------------------------
// the day's customer demand
// ---------------------------------------------------------------------

// hourly arrival weights, 06:00..19:00 (intake closes 19:30).
var demandCurve = []float64{0.3, 0.8, 1.4, 1.8, 1.6, 1.5, 1.9, 1.7, 1.3, 1.1, 1.0, 1.2, 0.9, 0.5}

func (s *sim) planOrders() {
	var weighted []skuDef
	for _, sd := range skuCatalogue {
		if sd.demand > 0 {
			weighted = append(weighted, sd)
		}
	}
	special := s.assignSpecialKinds()
	n := s.cfg.orders
	for i := 0; i < n; i++ {
		s.orders = append(s.orders, s.newOrder(i, special, weighted))
	}
	sort.Slice(s.orders, func(i, j int) bool { return s.orders[i].arriveAt.Before(s.orders[j].arriveAt) })
}

// pickWeightedSKU draws one SKU from the demand-weighted catalogue.
func (s *sim) pickWeightedSKU(weighted []skuDef) skuDef {
	total := 0.0
	for _, sd := range weighted {
		total += sd.demand
	}
	x := s.rf() * total
	for _, sd := range weighted {
		if x -= sd.demand; x <= 0 {
			return sd
		}
	}
	return weighted[0]
}

// simMinuteOnCurve draws one minute of the day from the demand curve
// (06:00..19:00; the first quarter hour is kept clear of orders).
func (s *sim) simMinuteOnCurve() int {
	total := 0.0
	for _, w := range demandCurve {
		total += w
	}
	x := s.rf() * total
	for h, w := range demandCurve {
		if x -= w; x <= 0 {
			m := h*60 + s.ri(60)
			if m < 15 {
				m = 15 + s.ri(30)
			}
			return m
		}
	}
	return 13*60 + 15
}

// assignSpecialKinds hands the day's special order kinds to randomly chosen
// orders (draws are stable under the run's seed).
func (s *sim) assignSpecialKinds() map[int]orderKind {
	special := map[int]orderKind{}
	n := s.cfg.orders
	assign := func(k orderKind, count int) {
		for i := 0; i < count; i++ {
			for {
				idx := s.ri(n)
				if _, taken := special[idx]; !taken {
					special[idx] = k
					break
				}
			}
		}
	}
	assign(kindBackorder, 3)
	assign(kindHeldRelease, 2)
	assign(kindHeldCancel, 2)
	assign(kindMispickInjct, max(1, n/40))
	assign(kindPartialOK, n/6)
	return special
}

// newOrder draws one customer order: its kind, its lines and its arrival
// minute on the demand curve.
func (s *sim) newOrder(i int, special map[int]orderKind, weighted []skuDef) *orderRec {
	o := &orderRec{seq: i + 1, kind: kindStandard, releaseOnAlloc: true, pickedLines: map[int]bool{}}
	if k, ok := special[i]; ok {
		o.kind = k
	}
	nLines := 1
	if x := s.rf(); x > 0.6 {
		nLines = 2
	}
	if x := s.rf(); x > 0.9 {
		nLines = 3
	}
	used := map[string]bool{}
	for len(o.lines) < nLines {
		sd := s.pickWeightedSKU(weighted)
		if used[sd.key] {
			continue
		}
		used[sd.key] = true
		o.lines = append(o.lines, orderLinePlan{sku: sd.key, qty: 1 + s.ri(2), giftWrap: s.rf() < 0.08})
	}
	switch o.kind {
	case kindBackorder:
		o.lines = append(o.lines, orderLinePlan{sku: "CAMERA", qty: 2})
	case kindHeldRelease, kindHeldCancel:
		o.releaseOnAlloc = false
	case kindPartialOK:
		o.allowPartial = true
	}
	m := s.simMinuteOnCurve()
	if o.kind == kindBackorder {
		// A backorder only exists while CAMERA is out of stock: the
		// 13:00 replenishment would let a later order allocate in full.
		for m >= (replenishHour-dayOpenHour)*60-30 {
			m = s.simMinuteOnCurve()
		}
	}
	o.arriveAt = s.clock.wallAt(dayOpenHour, 0).Add(s.clock.simDur(time.Duration(m) * time.Minute))
	o.simAt = fmt.Sprintf("%02d:%02d", dayOpenHour+m/60, m%60)
	return o
}

type omLine struct {
	LineNo        int    `json:"lineNo"`
	SKU           string `json:"sku"`
	Quantity      int    `json:"quantity"`
	PathID        string `json:"pathId"`
	GiftWrap      bool   `json:"giftWrap"`
	Status        string `json:"status"`
	ReservationID string `json:"reservationId"`
}

type omOrder struct {
	ID                string   `json:"id"`
	Status            string   `json:"status"`
	PromiseDate       string   `json:"promiseDate"`
	FulfillmentSiteID string   `json:"fulfillmentSiteId"`
	Lines             []omLine `json:"lines"`
}

func (s *sim) getOrder(ctx context.Context, id string) (omOrder, int) {
	var o omOrder
	r := s.api.Call(ctx, svcOrder, http.MethodGet, "/orders/{id}", "/orders/"+url.PathEscape(id), nil)
	if r.OK() {
		_ = r.Decode(&o)
	}
	return o, r.Status
}

// orderBody is the POST /orders request of one simulated customer order.
// fulfillmentSiteId is the simulated building (order-management ADR 0044:
// caller-assigned, immutable): order-management promises the order against
// that site's CPT schedule, the one publishCPTSchedule writes, and never
// against the storefront's site (its DEFAULT_SITE_ID).
func (s *sim) orderBody(o *orderRec) map[string]any {
	var lines []map[string]any
	for _, l := range o.lines {
		lines = append(lines, map[string]any{"sku": s.sku(l.sku), "quantity": l.qty, "giftWrap": l.giftWrap})
	}
	return map[string]any{
		"lines": lines, "allowPartialShipment": o.allowPartial, "releaseOnAllocation": o.releaseOnAlloc,
		"fulfillmentSiteId": s.cfg.siteCode,
	}
}

func (s *sim) placeOrder(ctx context.Context, o *orderRec) {
	r := s.api.Call(ctx, svcOrder, http.MethodPost, "/orders", "/orders", s.orderBody(o))
	o.intakeHTTP = r.Status
	o.placedWall = time.Now()
	if r.Status != http.StatusCreated {
		s.finding("POST /orders returned %d for a valid order: %s", r.Status, truncate(string(r.Body), 200))
		return
	}
	var created omOrder
	if err := r.Decode(&created); err != nil {
		s.finding("POST /orders body undecodable: %v", err)
		return
	}
	if created.FulfillmentSiteID != s.cfg.siteCode {
		s.finding("order-management did not keep the order's fulfillmentSiteId: sent %q, got %q (it would promise the order against its DEFAULT_SITE_ID, not the simulated trucks)", s.cfg.siteCode, created.FulfillmentSiteID)
	}
	for _, l := range created.Lines {
		if l.PathID != "" && l.PathID != s.cfg.wesPickPath {
			s.finding("order %s line %d landed on path %q, but the floor drives WES pool %q (-wes-pick-path)", created.ID, l.LineNo, l.PathID, s.cfg.wesPickPath)
		}
	}
	s.mu.Lock()
	o.id, o.intakeStatus, o.promiseDate = created.ID, created.Status, created.PromiseDate
	s.byID[o.id] = o
	s.mu.Unlock()

	want := map[orderKind]string{
		kindStandard: "Released", kindPartialOK: "Released", kindMispickInjct: "Released",
		kindHeldRelease: "Allocated", kindHeldCancel: "Allocated", kindBackorder: "Backordered",
	}[o.kind]
	if created.Status != want && !(o.kind == kindBackorder && created.Status == "PartiallyAllocated") {
		o.notes = append(o.notes, fmt.Sprintf("intake status %s, expected %s", created.Status, want))
	}
	logf("customer", "order #%03d %s %-20s %d line(s) -> %s, promise %s", o.seq, short(o.id), o.kind, len(o.lines), created.Status, shortTime(created.PromiseDate))
}

// customerChannel places every planned order at its arrival time.
func (s *sim) customerChannel(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	defer s.intakeDone.Store(true)
	for _, o := range s.orders {
		if !sleepCtx(ctx, time.Until(o.arriveAt)) {
			return
		}
		s.placeOrder(ctx, o)
	}
	logf("customer", "order intake closed: %d orders placed", len(s.orders))
}

// customerService handles held orders: releases half (a fill-or-kill
// customer confirmed) and cancels the other half, ~45 sim minutes later.
func (s *sim) customerService(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	done := map[*orderRec]bool{}
	for ctx.Err() == nil {
		s.mu.Lock()
		var due []*orderRec
		for _, o := range s.orders {
			if done[o] || o.id == "" || (o.kind != kindHeldRelease && o.kind != kindHeldCancel) {
				continue
			}
			if time.Since(o.placedWall) >= s.clock.simDur(45*time.Minute) {
				due = append(due, o)
			}
		}
		s.mu.Unlock()
		for _, o := range due {
			done[o] = true
			if o.kind == kindHeldRelease {
				r := s.api.Call(ctx, svcOrder, http.MethodPost, "/orders/{id}/releases", "/orders/"+o.id+"/releases", nil)
				logf("cust-service", "held order #%03d confirmed by customer -> release %d", o.seq, r.Status)
				if !r.OK() {
					s.finding("releasing held order %s -> %d %s", o.id, r.Status, truncate(string(r.Body), 200))
				}
			} else {
				r := s.api.Call(ctx, svcOrder, http.MethodDelete, "/orders/{id}", "/orders/"+o.id, nil)
				logf("cust-service", "held order #%03d cancelled by customer -> %d", o.seq, r.Status)
				if r.Status != http.StatusNoContent {
					s.finding("cancelling held order %s -> %d %s", o.id, r.Status, truncate(string(r.Body), 200))
				} else {
					s.mu.Lock()
					o.cancelled = true
					s.mu.Unlock()
				}
			}
		}
		if s.intakeDone.Load() && len(done) >= 4 {
			return
		}
		sleepCtx(ctx, 500*time.Millisecond)
	}
}

func short(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

func shortTime(ts string) string {
	if t, err := time.Parse(time.RFC3339, ts); err == nil {
		return t.UTC().Format("15:04Z")
	}
	if ts == "" {
		return "-"
	}
	return ts
}
