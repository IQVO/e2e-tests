package main

import (
	"fmt"
	"hash/fnv"
	"math"
	"sort"
	"strings"
)

// The planner is a pure function: the registered products (from
// product-master), the pilot sites' storage bins (from facility-layout) and a
// few explicit assortment rules in, a deterministic stock plan out. Nothing
// here talks to the network, so the whole policy is unit-testable.

// Product is what product-master knows about a registered SKU.
type Product struct {
	SKU         string
	Description string
	Tags        []string // handling tags
	Temp        string   // "", "Chilled", "Frozen"
	DotClass    int
	HasDims     bool
	VolumeL     float64 // litres, from the declared/effective dimensions
	WeightG     int64
}

func (p Product) has(tag string) bool {
	for _, t := range p.Tags {
		if t == tag {
			return true
		}
	}
	return false
}

// Assumed unit for products that carry no dimensions (the Open Food Facts
// groceries record net content, not packed size). It is used ONLY for the
// bin fill arithmetic below; nothing is written back to product-master.
const (
	assumedGroceryVolumeL = 1.5
	assumedGroceryWeightG = 600
)

// Bin is one facility-layout storage slot with its physical limits.
type Bin struct {
	ID         string
	ZoneCode   string
	Temp       string // zone temperature class: Ambient, Chilled, Frozen
	Hazmat     bool   // zone is hazmat-rated
	MaxVolumeL float64
	MaxWeightG int64
}

// Site is a pilot site with its role and its storage bins.
type Site struct {
	Code string
	Role string
	Bins []Bin
}

// Line is one SKU quantity stowed into one bin.
type Line struct {
	SKU string
	Qty int
}

// BinPlan is the stock planned for one bin and the capacity to register.
type BinPlan struct {
	Bin       Bin
	Site      string
	Lines     []Line
	Units     int
	Capacity  int
	usedL     float64
	usedG     float64
	dedicated bool // hazmat / DOT-classed stock gets a bin of its own (same-bin segregation, ADR 0010)
}

// Plan is the whole stock plan.
type Plan struct {
	Bins          []*BinPlan
	Units         int
	Lines         int
	SKUs          int
	SkippedSKUs   []string       // registered but not stockable (no dimensions, not a grocery, or no eligible bin)
	Unplaced      map[string]int // SKU -> units that found no bin
	UnitsBySite   map[string]int
	BinsBySite    map[string]int
	SKUsByProfile map[string]int
}

// Options tunes the planner.
type Options struct {
	Fill float64 // fraction of a bin's volume and weight limit that may be used (default 0.8)
}

// Sites by what they store. Sortable FCs hold items that fit a sortable tote;
// non-sortable and XL FCs hold the oversized and heavy; fresh grocery FCs and
// micro-FCs hold groceries and the cold chain.
var (
	sortableRoles = map[string]bool{"ROBOTICS_SORTABLE_FC": true, "TRADITIONAL_SORTABLE_FC": true, "SOFT_LINES_SORTABLE_FC": true, "SUB_SAME_DAY_FC": true}
	bulkRoles     = map[string]bool{"NON_SORTABLE_FC": true, "XL_FC": true}
)

// Product size classes. "small" fits a sortable tote (carrier-style limits: 40 L, 20 lb);
// "large" is anything Oversized or beyond a pallet-rack bay's comfortable single unit.
type sizeClass int

const (
	small sizeClass = iota
	medium
	large
)

func (c sizeClass) String() string { return [...]string{"small", "medium", "large"}[c] }

const (
	smallMaxL     = 40.0
	smallMaxG     = 9072 // 20 lb
	mediumMaxL    = 400.0
	mediumMaxG    = 22680 // 50 lb
	maxLineWeight = 600_000
)

func classify(p Product) sizeClass {
	switch {
	case p.has("Oversized") || p.VolumeL > mediumMaxL || p.WeightG > mediumMaxG:
		return large
	case p.VolumeL <= smallMaxL && p.WeightG <= smallMaxG:
		return small
	default:
		return medium
	}
}

// Zone codes (facility-layout archetypes): POD pod bins, TOT tote wall, SHF each-pick shelving,
// FWD carton-flow forward pick, RSV pallet reserve, BLK bulk floor, HAZ hazmat shelving,
// CHL chilled and FRZ frozen pallet racks.
func zonePreference(p Product, c sizeClass) []string {
	switch {
	case p.Temp == "Frozen":
		return []string{"FRZ"}
	case p.Temp == "Chilled":
		return []string{"CHL"}
	case p.has("Hazmat"):
		return []string{"HAZ"}
	case c == small:
		return []string{"POD", "TOT", "SHF", "FWD"}
	case c == medium:
		return []string{"FWD", "SHF", "RSV"}
	default:
		return []string{"BLK", "RSV"}
	}
}

// roleAccepts says whether a site role stocks this product at all.
func roleAccepts(role string, p Product, c sizeClass) bool {
	grocery := !p.HasDims
	switch {
	case p.Temp != "":
		return role == "FRESH_GROCERY_FC" || role == "MICRO_FC"
	case p.has("Hazmat"):
		return sortableRoles[role] || bulkRoles[role] || role == "FRESH_GROCERY_FC"
	case grocery:
		return sortableRoles[role] || role == "FRESH_GROCERY_FC" || role == "MICRO_FC"
	case c == large:
		return bulkRoles[role]
	case c == medium:
		return sortableRoles[role] || bulkRoles[role]
	default: // small
		return sortableRoles[role] || role == "MICRO_FC" && p.VolumeL <= 5
	}
}

func h64(parts ...string) uint64 {
	h := fnv.New64a()
	for _, s := range parts {
		h.Write([]byte(s))
		h.Write([]byte{0})
	}
	return h.Sum64()
}

// quantityFor is the stocked quantity of one SKU at one site: case-pack scale by unit size,
// spread by a stable hash so the same inputs always give the same plan.
func quantityFor(p Product, c sizeClass, site string) int {
	lo, hi := 1, 3
	switch {
	case p.Temp != "":
		lo, hi = 8, 36
	case p.has("Hazmat"):
		lo, hi = 4, 16
	case !p.HasDims:
		lo, hi = 12, 48
	case p.VolumeL < 0.25:
		lo, hi = 12, 36
	case p.VolumeL < 2:
		lo, hi = 6, 24
	case p.VolumeL < 10:
		lo, hi = 3, 10
	case p.VolumeL < 60:
		lo, hi = 2, 6
	}
	q := lo + int(h64("qty", p.SKU, site)%uint64(hi-lo+1))
	if w := p.WeightG; w > 0 && int64(q)*w > maxLineWeight {
		q = int(maxLineWeight / w)
	}
	if q < 1 {
		q = 1
	}
	return q
}

// siteCount is how many sites carry a SKU: big items live in one place, small fast ones in several.
func siteCount(p Product, c sizeClass, eligible int) int {
	n := 1
	if c != large && p.Temp == "" && !p.has("Hazmat") {
		n = 1 + int(h64("sites", p.SKU)%3)
	} else if p.Temp != "" {
		n = 1 + int(h64("sites", p.SKU)%2)
	}
	if n > eligible {
		n = eligible
	}
	return n
}

type zoneBins struct {
	bins   []*BinPlan
	cursor int
}

// BuildPlan places every stockable registered product.
func BuildPlan(products []Product, sites []Site, opt Options) *Plan {
	if opt.Fill <= 0 || opt.Fill > 1 {
		opt.Fill = 0.8
	}
	plan := &Plan{Unplaced: map[string]int{}, UnitsBySite: map[string]int{}, BinsBySite: map[string]int{}, SKUsByProfile: map[string]int{}}

	// bins per site and zone code, in a stable order
	type siteState struct {
		Site
		zones map[string][]*BinPlan
	}
	states := make([]*siteState, 0, len(sites))
	all := map[string]*BinPlan{}
	for _, s := range sites {
		st := &siteState{Site: s, zones: map[string][]*BinPlan{}}
		bins := append([]Bin(nil), s.Bins...)
		sort.Slice(bins, func(i, j int) bool { return bins[i].ID < bins[j].ID })
		for _, b := range bins {
			bp := &BinPlan{Bin: b, Site: s.Code}
			st.zones[b.ZoneCode] = append(st.zones[b.ZoneCode], bp)
			all[b.ID] = bp
		}
		states = append(states, st)
	}
	sort.Slice(states, func(i, j int) bool { return states[i].Code < states[j].Code })

	ordered := append([]Product(nil), products...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].SKU < ordered[j].SKU })

	stocked := map[string]bool{}
	for _, p := range ordered {
		c := classify(p)
		vol, wt := p.VolumeL, float64(p.WeightG)
		if !p.HasDims {
			vol, wt = assumedGroceryVolumeL, assumedGroceryWeightG
		}
		prefs := zonePreference(p, c)
		var eligible []*siteState
		for _, st := range states {
			if !roleAccepts(st.Role, p, c) {
				continue
			}
			for _, z := range prefs {
				if len(st.zones[z]) > 0 {
					eligible = append(eligible, st)
					break
				}
			}
		}
		if len(eligible) == 0 {
			plan.SkippedSKUs = append(plan.SkippedSKUs, p.SKU)
			continue
		}
		n := siteCount(p, c, len(eligible))
		start := int(h64("start", p.SKU) % uint64(len(eligible)))
		for k := 0; k < n; k++ {
			st := eligible[(start+k)%len(eligible)]
			qty := quantityFor(p, c, st.Code)
			// first zone of the preference list this site has, rotated by hash among those it has
			var have []string
			for _, z := range prefs {
				if len(st.zones[z]) > 0 {
					have = append(have, z)
				}
			}
			zone := have[int(h64("zone", p.SKU, st.Code)%uint64(len(have)))]
			left := placeInZone(st.zones[zone], p, vol, wt, qty, opt.Fill)
			if left > 0 { // try the site's other eligible zones
				for _, z := range have {
					if z != zone && left > 0 {
						left = placeInZone(st.zones[z], p, vol, wt, left, opt.Fill)
					}
				}
			}
			if left > 0 {
				plan.Unplaced[p.SKU] += left
			}
			if left < qty {
				stocked[p.SKU] = true
			}
		}
		plan.SKUsByProfile[profileOf(p, c)]++
	}

	for _, st := range states {
		for _, bins := range st.zones {
			for _, bp := range bins {
				if bp.Units == 0 {
					continue
				}
				bp.Capacity = int(math.Ceil(float64(bp.Units) / opt.Fill))
				plan.Bins = append(plan.Bins, bp)
				plan.Units += bp.Units
				plan.Lines += len(bp.Lines)
				plan.UnitsBySite[bp.Site] += bp.Units
				plan.BinsBySite[bp.Site]++
			}
		}
	}
	sort.Slice(plan.Bins, func(i, j int) bool { return plan.Bins[i].Bin.ID < plan.Bins[j].Bin.ID })
	plan.SKUs = len(stocked)
	sort.Strings(plan.SkippedSKUs)
	return plan
}

func profileOf(p Product, c sizeClass) string {
	switch {
	case p.Temp != "":
		return strings.ToLower(p.Temp)
	case p.has("Hazmat"):
		return "hazmat"
	case !p.HasDims:
		return "grocery (no dimensions)"
	default:
		return c.String()
	}
}

// placeInZone puts up to qty units into the zone's bins starting at a hash-chosen bin (chaotic
// storage: no fixed product-to-bin mapping) and returns the units left unplaced. A bin takes as many
// units as fit within fill x its volume and weight limits; hazmat and DOT-classed stock gets a bin of
// its own.
func placeInZone(zone []*BinPlan, p Product, volL, wtG float64, qty int, fill float64) int {
	if len(zone) == 0 {
		return qty
	}
	dedicated := p.has("Hazmat") || p.DotClass != 0
	start := int(h64("bin", p.SKU, zone[0].Bin.ID) % uint64(len(zone)))
	left := qty
	for i := 0; i < len(zone) && left > 0; i++ {
		bp := zone[(start+i)%len(zone)]
		if bp.dedicated || (dedicated && bp.Units > 0) {
			continue
		}
		remL := bp.Bin.MaxVolumeL*fill - bp.usedL
		remG := float64(bp.Bin.MaxWeightG)*fill - bp.usedG
		fit := left
		if volL > 0 {
			fit = minInt(fit, int(remL/volL))
		}
		if wtG > 0 {
			fit = minInt(fit, int(remG/wtG))
		}
		if fit < 1 {
			continue
		}
		bp.Lines = append(bp.Lines, Line{SKU: p.SKU, Qty: fit})
		bp.Units += fit
		bp.usedL += float64(fit) * volL
		bp.usedG += float64(fit) * wtG
		bp.dedicated = dedicated
		left -= fit
	}
	return left
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Summary renders the plan for the log.
func (p *Plan) Summary() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%d units of %d SKUs in %d bins (%d stow lines)", p.Units, p.SKUs, len(p.Bins), p.Lines)
	return b.String()
}
