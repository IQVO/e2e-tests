package inbound

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func registered(t *testing.T, c *Crew, lines ...Line) ASN {
	t.Helper()
	asn, err := c.RegisterASN(context.Background(), lines)
	if err != nil {
		t.Fatalf("RegisterASN: %v", err)
	}
	return asn
}

func TestNextWindowStartsOnTheGridAtLeastTheLeadFromNow(t *testing.T) {
	w := newWorld(t)
	c := w.crew(w.seedSite())
	got := c.NextWindow() // now is 15:41:07, lead 2 minutes: 15:43:07, the next 5 minute boundary is 15:45
	wantStart := time.Date(2026, 11, 14, 15, 45, 0, 0, time.UTC)
	if !got.Start.Equal(wantStart) || got.End.Sub(got.Start) != time.Hour {
		t.Errorf("window = %s..%s, want 15:45..16:45", got.Start.Format("15:04:05"), got.End.Format("15:04:05"))
	}
	c.GridMinutes, c.Lead, c.WindowLength = 15, 10*time.Minute, 30*time.Minute
	got = c.NextWindow() // 15:51:07 -> 16:00
	if !got.Start.Equal(time.Date(2026, 11, 14, 16, 0, 0, 0, time.UTC)) || got.End.Sub(got.Start) != 30*time.Minute {
		t.Errorf("window on a 15 minute grid = %s..%s, want 16:00..16:30", got.Start.Format("15:04"), got.End.Format("15:04"))
	}
}

func TestBookDoorBooksAnInboundAppointmentForTheASNWithItsSeal(t *testing.T) {
	w := newWorld(t)
	c := w.crew(w.seedSite())
	asn := registered(t, c, Line{skuA, 10})
	win := c.NextWindow()
	a, err := c.BookDoor(context.Background(), asn.Number, win)
	if err != nil {
		t.Fatalf("BookDoor: %v", err)
	}
	if a.ID == "" || a.Door != inDoorA || a.ASN != asn.Number || a.State != "BOOKED" || a.SiteID != siteCode || a.Seal != "SEAL-"+asn.Number {
		t.Fatalf("appointment = %+v", a)
	}
	var body map[string]any
	bodies := w.bodies("dock-scheduling POST /appointments")
	if err := json.Unmarshal(bodies[len(bodies)-1], &body); err != nil {
		t.Fatal(err)
	}
	asns, _ := body["asnNumbers"].([]any)
	if body["direction"] != "INBOUND" || body["doorCode"] != inDoorA || body["carrier"] != "ACME Freight" || body["carrierScac"] != "ACMF" ||
		len(asns) != 1 || asns[0] != asn.Number || body["expectedSealNumber"] != "SEAL-"+asn.Number {
		t.Errorf("booking body = %v", body)
	}
	if body["windowStart"] != win.Start.Format(time.RFC3339) || body["windowEnd"] != win.End.Format(time.RFC3339) {
		t.Errorf("window sent = %v..%v, want %s..%s", body["windowStart"], body["windowEnd"], win.Start.Format(time.RFC3339), win.End.Format(time.RFC3339))
	}
}

func TestBookDoorRetriesTheLagOfTheDoorAndOfTheASN(t *testing.T) {
	w := newWorld(t)
	c := w.crew(w.seedSite())
	asn := registered(t, c, Line{skuA, 10})
	w.with(func() {
		w.dock.doorLag[inDoorA] = &lag{n: 2}
		w.dock.asnLag[asn.Number] = &lag{n: 2}
	})
	if _, err := c.BookDoor(context.Background(), asn.Number, c.NextWindow()); err != nil {
		t.Fatalf("BookDoor: %v", err)
	}
	if n := w.served("dock-scheduling POST /appointments"); n != 5 {
		t.Errorf("%d bookings sent, want 5 (2 unknown-door, 2 unknown-asn, 1 booked)", n)
	}
}

func TestBookDoorTriesTheNextDoorWhenOneIsDoubleBooked(t *testing.T) {
	w := newWorld(t)
	c := w.crew(w.seedSite())
	a1, a2 := registered(t, c, Line{skuA, 1}), registered(t, c, Line{skuB, 1})
	win := c.NextWindow()
	first, err := c.BookDoor(context.Background(), a1.Number, win)
	if err != nil {
		t.Fatal(err)
	}
	second, err := c.BookDoor(context.Background(), a2.Number, win)
	if err != nil {
		t.Fatalf("BookDoor on a busy door: %v", err)
	}
	if first.Door != inDoorA || second.Door != inDoorB {
		t.Errorf("doors = %s then %s, want %s then %s", first.Door, second.Door, inDoorA, inDoorB)
	}
}

func TestBookDoorWithEveryDoorBusyIsAFindingNamingThem(t *testing.T) {
	w := newWorld(t)
	c := w.crew(w.seedSite())
	a1, a2, a3 := registered(t, c, Line{skuA, 1}), registered(t, c, Line{skuB, 1}), registered(t, c, Line{skuC, 1})
	win := c.NextWindow()
	for _, n := range []string{a1.Number, a2.Number} {
		if _, err := c.BookDoor(context.Background(), n, win); err != nil {
			t.Fatal(err)
		}
	}
	_, err := c.BookDoor(context.Background(), a3.Number, win)
	if err == nil || !strings.Contains(err.Error(), "double-booked") || !strings.Contains(err.Error(), inDoorA) || !strings.Contains(err.Error(), inDoorB) {
		t.Fatalf("err = %v, want every door named as double-booked", err)
	}
}

func TestBookDoorDoesNotRetryAWindowInThePast(t *testing.T) {
	w := newWorld(t)
	c := w.crew(w.seedSite())
	asn := registered(t, c, Line{skuA, 1})
	past := Window{Start: w.clock.Now().Add(-time.Hour).Truncate(5 * time.Minute), End: w.clock.Now().Truncate(5 * time.Minute)}
	before := w.served("dock-scheduling POST /appointments")
	_, err := c.BookDoor(context.Background(), asn.Number, past)
	if err == nil || !strings.Contains(err.Error(), "422 window-in-past") {
		t.Fatalf("err = %v, want 422 window-in-past", err)
	}
	if n := w.served("dock-scheduling POST /appointments") - before; n != 1 {
		t.Errorf("%d bookings, want exactly 1: only unknown-door and unknown-asn are lag", n)
	}
}

func TestBookDoorOnAnOutboundOnlyDoorIsRefusedNotRetried(t *testing.T) {
	w := newWorld(t)
	site := w.seedSite()
	site.Doors = []string{outDoor}
	c := w.crew(site)
	asn := registered(t, c, Line{skuA, 1})
	_, err := c.BookDoor(context.Background(), asn.Number, c.NextWindow())
	if err == nil || !strings.Contains(err.Error(), "422 door-flow-mismatch") {
		t.Fatalf("err = %v, want 422 door-flow-mismatch", err)
	}
	if n := w.served("dock-scheduling POST /appointments"); n != 1 {
		t.Errorf("%d bookings, want 1", n)
	}
}

func TestBookDoorWithNoInboundDoorSendsNothing(t *testing.T) {
	w := newWorld(t)
	site := w.seedSite()
	site.Doors = nil
	c := w.crew(site)
	asn := registered(t, c, Line{skuA, 1})
	writes := w.writes()
	_, err := c.BookDoor(context.Background(), asn.Number, c.NextWindow())
	if err == nil || !strings.Contains(err.Error(), "no INBOUND door") {
		t.Fatalf("err = %v, want it to say the site has no INBOUND door configured", err)
	}
	if w.writes() != writes {
		t.Error("BookDoor wrote something with no door to book")
	}
}
