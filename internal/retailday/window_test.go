package retailday

import (
	"fmt"
	"testing"
	"time"
)

func chicago(t *testing.T) *time.Location {
	t.Helper()
	loc, err := time.LoadLocation("America/Chicago")
	if err != nil {
		t.Skipf("tzdata unavailable: %v", err)
	}
	return loc
}

func TestReleaseWindowForTheSeventeenHundredPickup(t *testing.T) {
	loc := chicago(t)
	at := func(day int, hm string) time.Time {
		var h, m int
		if _, err := fmt.Sscanf(hm, "%d:%d", &h, &m); err != nil {
			t.Fatal(err)
		}
		return time.Date(2026, 11, day, h, m, 0, 0, loc)
	}
	cases := []struct {
		name       string
		now        time.Time
		wantStart  time.Time
		wantIn     bool
		wantWait   time.Duration
		wantRemain time.Duration
	}{
		{"morning waits for 15:30 (ADR: 09:00 is released 6h30 later)", at(14, "09:00"), at(14, "15:30"), false, 6*time.Hour + 30*time.Minute, 0},
		{"noon waits 3h30", at(14, "12:00"), at(14, "15:30"), false, 3*time.Hour + 30*time.Minute, 0},
		{"exactly at the start is in the window", at(14, "15:30"), at(14, "15:30"), true, 0, 30 * time.Minute},
		{"inside the window", at(14, "15:45"), at(14, "15:30"), true, 0, 15 * time.Minute},
		{"the end is exclusive: 16:00 belongs to tomorrow", at(14, "16:00"), at(15, "15:30"), false, 23*time.Hour + 30*time.Minute, 0},
		{"evening waits for tomorrow", at(14, "21:10"), at(15, "15:30"), false, 18*time.Hour + 20*time.Minute, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			w, err := ReleaseWindow(c.now, loc, "17:00", nil, time.Hour, 30*time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			if !w.Start.Equal(c.wantStart) || w.InWindow != c.wantIn || w.Wait != c.wantWait || w.Remaining != c.wantRemain {
				t.Fatalf("got start %s in=%v wait=%s remaining=%s, want start %s in=%v wait=%s remaining=%s",
					w.Start, w.InWindow, w.Wait, w.Remaining, c.wantStart, c.wantIn, c.wantWait, c.wantRemain)
			}
			if !w.Cutoff.Equal(w.End.Add(time.Hour)) {
				t.Fatalf("cutoff %s is not end %s + handling", w.Cutoff, w.End)
			}
		})
	}
}

func TestReleaseWindowUsesTheSitesWallClockAcrossDST(t *testing.T) {
	loc := chicago(t)
	// 2027-03-14 is the spring-forward day in the US: 15:30 local is CDT (UTC-5), the day before it is CST (UTC-6).
	before, err := ReleaseWindow(time.Date(2027, 3, 13, 8, 0, 0, 0, loc), loc, "17:00", nil, time.Hour, 30*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	after, err := ReleaseWindow(time.Date(2027, 3, 14, 8, 0, 0, 0, loc), loc, "17:00", nil, time.Hour, 30*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if got := before.Start.UTC().Format("15:04"); got != "21:30" {
		t.Errorf("CST window starts %s UTC, want 21:30", got)
	}
	if got := after.Start.UTC().Format("15:04"); got != "20:30" {
		t.Errorf("CDT window starts %s UTC, want 20:30", got)
	}
}

func TestReleaseWindowSkipsDaysTheCutoffDoesNotRun(t *testing.T) {
	loc := chicago(t)
	// Saturday 14 Nov 2026 after the window; the cutoff runs Mon-Fri only: the next window is Monday 16th.
	w, err := ReleaseWindow(time.Date(2026, 11, 14, 17, 0, 0, 0, loc), loc, "17:00", []string{"Mon", "Tue", "Wed", "Thu", "Fri"}, time.Hour, 30*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if w.Start.Weekday() != time.Monday || w.Start.Day() != 16 {
		t.Fatalf("window starts %s, want Monday 16th", w.Start)
	}
}

func TestReleaseWindowRejectsWhatItCannotRead(t *testing.T) {
	loc := chicago(t)
	now := time.Date(2026, 11, 14, 9, 0, 0, 0, loc)
	cases := map[string]func() error{
		"cutoff not HH:MM":          func() error { _, err := ReleaseWindow(now, loc, "5pm", nil, time.Hour, 0); return err },
		"cutoff 24:00":              func() error { _, err := ReleaseWindow(now, loc, "24:00", nil, time.Hour, 0); return err },
		"unknown weekday":           func() error { _, err := ReleaseWindow(now, loc, "17:00", []string{"Funday"}, time.Hour, 0); return err },
		"lower-case weekday":        func() error { _, err := ReleaseWindow(now, loc, "17:00", []string{"mon"}, time.Hour, 0); return err },
		"negative handling":         func() error { _, err := ReleaseWindow(now, loc, "17:00", nil, -time.Hour, 0); return err },
		"lead longer than a window": func() error { _, err := ReleaseWindow(now, loc, "17:00", nil, 400*time.Hour, 0); return err },
	}
	for name, run := range cases {
		if err := run(); err == nil {
			t.Errorf("%s: want an error", name)
		}
	}
	if _, err := ReleaseWindow(now, loc, "17:00", []string{}, time.Hour, 30*time.Minute); err != nil {
		t.Errorf("an empty day list must mean every day, got %v", err)
	}
}

func TestLatestCutoffIsTheLatestLocalTime(t *testing.T) {
	got, ok := LatestCutoff([]Cutoff{{LocalTime: "11:00"}, {LocalTime: "17:00", DaysOfWeek: []string{"Mon"}}, {LocalTime: "09:30"}})
	if !ok || got.LocalTime != "17:00" || len(got.DaysOfWeek) != 1 {
		t.Fatalf("got %+v ok=%v", got, ok)
	}
	if _, ok := LatestCutoff(nil); ok {
		t.Fatal("no cutoffs must report not found")
	}
}

func TestWindowStringNamesTheZoneAndTheWait(t *testing.T) {
	loc := chicago(t)
	now := time.Date(2026, 11, 14, 12, 0, 0, 0, loc)
	w, _ := ReleaseWindow(now, loc, "17:00", nil, time.Hour, 30*time.Minute)
	if got := w.String(); got != "next release window 15:30-16:00 CST on Sat 14 Nov, in 3h30m0s" {
		t.Errorf("got %q", got)
	}
	in, _ := ReleaseWindow(time.Date(2026, 11, 14, 15, 40, 0, 0, loc), loc, "17:00", nil, time.Hour, 30*time.Minute)
	if got := in.String(); got != "in the release window 15:30-16:00 CST, 20m0s left" {
		t.Errorf("got %q", got)
	}
}
