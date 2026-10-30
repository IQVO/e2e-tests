package main

import (
	"encoding/json"
	"net/http"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"time"
)

// The WMS (process-path-management) and network-fulfillment slice of the contract fake. It is
// written from the published contract, independently of the seeder's own validation.

var fakeHHMM = regexp.MustCompile(`^([01][0-9]|2[0-3]):[0-5][0-9]$`)

// readWMS answers the process-path-management and network-fulfillment reads; false: not one.
func (f *fakeCluster) readWMS(w http.ResponseWriter, p string) bool {
	switch {
	case p == "/process-path-management/process-paths":
		type path struct {
			PathID string `json:"pathId"`
			Status string `json:"status"`
		}
		out := []path{}
		for id, st := range f.paths {
			if st == "ACTIVE" { // the default list is Active paths only
				out = append(out, path{id, st})
			}
		}
		sort.Slice(out, func(i, j int) bool { return out[i].PathID < out[j].PathID })
		writeJSON(w, http.StatusOK, out)
	case strings.HasPrefix(p, "/process-path-management/sites/") && strings.HasSuffix(p, "/cpt-schedule"):
		site := strings.TrimSuffix(strings.TrimPrefix(p, "/process-path-management/sites/"), "/cpt-schedule")
		s := f.cpt[site]
		if s == nil {
			problem(w, http.StatusNotFound, "not-found")
			return true
		}
		writeJSON(w, http.StatusOK, s)
	case p == "/network-fulfillment/capability-offers":
		writeJSON(w, http.StatusOK, map[string]any{"capabilityOffers": f.offers()})
	default:
		return false
	}
	return true
}

// offers is what the ACL would advertise now: stock (PHYSICAL) while the site's schedule agrees
// with the network node profile, THROUGHPUT_CONSTRAINED 0 otherwise (a leftover simulated
// schedule puts the next cutoff minutes away) or while the pre-cutoff gap is simulated.
func (f *fakeCluster) offers() []CapabilityOffer {
	ok := false
	if s := f.cpt[f.node.VendorNodeCode]; s != nil && s.Timezone == f.node.Timezone && !f.offersStuck {
		var times []string
		for _, c := range s.Cutoffs {
			times = append(times, c.LocalTime)
		}
		sort.Strings(times)
		ok = reflect.DeepEqual(times, f.node.CutoffTimes)
	}
	now := time.Now().UTC()
	out := []CapabilityOffer{}
	for _, sku := range f.offerSKUs {
		o := CapabilityOffer{SKU: sku, SiteID: f.node.VendorNodeCode, Basis: "THROUGHPUT_CONSTRAINED", ComputedAt: now}
		if ok {
			o.Basis, o.AdvertisedQuantity = "PHYSICAL", 10
		}
		out = append(out, o)
	}
	return out
}

// putCPT is PUT /process-path-management/sites/{siteId}/cpt-schedule.
func (f *fakeCluster) putCPT(w http.ResponseWriter, r *http.Request, p string) {
	f.puts++
	if !strings.HasPrefix(p, "/process-path-management/sites/") || !strings.HasSuffix(p, "/cpt-schedule") {
		problem(w, http.StatusMethodNotAllowed, "method-not-allowed")
		return
	}
	site := strings.TrimSuffix(strings.TrimPrefix(p, "/process-path-management/sites/"), "/cpt-schedule")
	if site == "" || strings.Contains(site, "/") {
		problem(w, http.StatusNotFound, "not-found")
		return
	}
	var in DefineCPTScheduleRequest
	if r.Body == nil || json.NewDecoder(r.Body).Decode(&in) != nil {
		problem(w, http.StatusBadRequest, "malformed-request")
		return
	}
	if slug := f.invalidSchedule(in); slug != "" {
		problem(w, http.StatusUnprocessableEntity, slug)
		return
	}
	now := time.Now().UTC().Format(time.RFC3339)
	cur := f.cpt[site]
	if cur != nil && cur.Timezone == in.Timezone && reflect.DeepEqual(cur.Cutoffs, in.Cutoffs) {
		writeJSON(w, http.StatusOK, cur) // a no-op revision: nothing changes, nothing is published
		return
	}
	next := &CPTSchedule{SiteID: site, Timezone: in.Timezone, Cutoffs: in.Cutoffs, UpdatedAt: now}
	f.cpt[site] = next
	f.cptChanges++
	f.mutations++
	f.byKind["cpt-schedule"]++
	writeJSON(w, http.StatusOK, next)
}

// invalidSchedule returns the 422 slug of a DefineCPTScheduleRequest, "" when it is valid.
func (f *fakeCluster) invalidSchedule(in DefineCPTScheduleRequest) string {
	if _, err := time.LoadLocation(in.Timezone); in.Timezone == "" || in.Timezone == "Local" || err != nil {
		return "invalid-timezone"
	}
	if len(in.Cutoffs) == 0 {
		return "no-cutoffs"
	}
	days := map[string]bool{"Mon": true, "Tue": true, "Wed": true, "Thu": true, "Fri": true, "Sat": true, "Sun": true}
	ids := map[string]bool{}
	for _, c := range in.Cutoffs {
		if c.CptID == "" || ids[c.CptID] {
			return "invalid-cpt-id"
		}
		ids[c.CptID] = true
		if !fakeHHMM.MatchString(c.LocalTime) {
			return "invalid-local-time"
		}
		if len(c.DaysOfWeek) == 0 {
			return "invalid-days-of-week"
		}
		for _, d := range c.DaysOfWeek {
			if !days[d] {
				return "invalid-days-of-week"
			}
		}
		if c.ShipMethod == "" {
			return "invalid-ship-method"
		}
		if len(c.EligiblePathIDs) == 0 {
			return "invalid-eligible-paths"
		}
		for _, id := range c.EligiblePathIDs {
			if f.paths[id] != "ACTIVE" {
				return "unknown-process-path"
			}
		}
	}
	return ""
}

// simLeftoverSchedule is what make warehouse-day leaves behind at site-1: UTC, the simulated
// 12:00/16:00/20:00 trucks compressed onto consecutive wall-clock minutes.
func simLeftoverSchedule() *CPTSchedule {
	all := []string{"Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"}
	c := func(id, at string) Cutoff {
		return Cutoff{CptID: id, LocalTime: at, DaysOfWeek: all, ShipMethod: "ground", EligiblePathIDs: []string{"PICK"}}
	}
	return &CPTSchedule{SiteID: "site-1", Timezone: "UTC", Cutoffs: []Cutoff{c("sim-1200", "09:39"), c("sim-1600", "09:40"), c("sim-2000", "09:41")}}
}
