package main

import "time"

// ---------------------------------------------------------------------
// catalogue of the simulated building
// ---------------------------------------------------------------------

type skuDef struct {
	key      string
	weightKg float64
	fragile  bool
	stock    int     // opening stock received at 05:30
	demand   float64 // relative popularity in the customer order mix
}

// cartonTareKg is added to every package's expected weight at SLAM.
const cartonTareKg = 0.20

var skuCatalogue = []skuDef{
	{"TSHIRT", 0.25, false, 400, 10},
	{"JEANS", 0.80, false, 300, 7},
	{"MUG", 0.45, true, 220, 5},
	{"BOOK", 0.60, false, 260, 6},
	{"HEADPHONES", 0.35, false, 160, 4},
	{"LAMP", 1.40, true, 90, 2},
	{"SNEAKERS", 1.10, false, 160, 4},
	{"BACKPACK", 0.90, false, 130, 3},
	// Deliberately understocked: orders for it backorder until the
	// 13:00 replenishment arrives and inventory control retries them.
	{"CAMERA", 0.70, false, 1, 0},
	// Never ordered; lives in the overflow bin and is the subject of the
	// deliberate cycle-count shortage.
	{"SAMPLE", 0.10, false, 20, 0},
}

// replenishHour is when the CAMERA replenishment truck arrives (sim time).
const replenishHour = 13

const (
	storageAisles = 4
	storageBays   = 6
	binCapacity   = 2000
)

type orderKind string

const (
	kindStandard     orderKind = "standard"
	kindPartialOK    orderKind = "partial-ok"
	kindHeldRelease  orderKind = "held-then-released"
	kindHeldCancel   orderKind = "held-then-cancelled"
	kindBackorder    orderKind = "backorder"
	kindMispickInjct orderKind = "mispick"
)

type orderLinePlan struct {
	sku      string
	qty      int
	giftWrap bool
}

type orderRec struct {
	seq                          int
	kind                         orderKind
	arriveAt                     time.Time // wall
	simAt                        string
	lines                        []orderLinePlan
	allowPartial, releaseOnAlloc bool

	// filled in as the day goes
	id            string
	intakeStatus  string
	intakeHTTP    int
	promiseDate   string
	placedWall    time.Time
	packageID     string
	packageStatus string
	sortLane      string
	labeledWall   time.Time
	pickedLines   map[int]bool
	cancelled     bool
	retried       bool
	notes         []string
}

func (o *orderRec) fragile() bool {
	for _, l := range o.lines {
		if skuByKey(l.sku).fragile {
			return true
		}
	}
	return false
}

func (o *orderRec) giftWrap() bool {
	for _, l := range o.lines {
		if l.giftWrap {
			return true
		}
	}
	return false
}

// shippable reports whether the order is expected to end the day as a
// package on a truck (or diverted for an injected mispick).
func (o *orderRec) shippable() bool { return o.kind != kindHeldCancel && o.id != "" }

func skuByKey(k string) skuDef {
	for _, s := range skuCatalogue {
		if s.key == k {
			return s
		}
	}
	return skuDef{key: k, weightKg: 0.5}
}
