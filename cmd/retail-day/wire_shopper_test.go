package main

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/warehouse-e2e/internal/retailday"
	"github.com/claudioed/warehouse-e2e/internal/retailday/day"
	"github.com/claudioed/warehouse-e2e/internal/sim/shopper"
)

func fullBuyer() day.Buyer {
	return day.Buyer{
		Index:  7,
		Expect: retailday.Delivered,
		Lines: []day.Line{
			{Item: day.Item{CatalogItemID: "cat-1", SKU: "SKU-1"}, Qty: 2},
			{Item: day.Item{CatalogItemID: "cat-2", SKU: "SKU-2"}, Qty: 1},
		},
		Prefix:               "787",
		Tier:                 day.TierTwoDay,
		Token:                day.TokenDecline,
		FallbackToken:        day.TokenApprove,
		CancelAfterPlacement: true,
		ExpectOutOfArea:      true,
	}
}

func TestToPlanCarriesEveryFieldOfTheBuyer(t *testing.T) {
	got := toPlan(fullBuyer())
	want := shopper.Plan{
		Lines:                []shopper.Line{{CatalogItemID: "cat-1", Qty: 2}, {CatalogItemID: "cat-2", Qty: 1}},
		Prefix:               "787",
		Tier:                 shopper.TierTwoDay,
		Token:                "tok_decline",
		FallbackToken:        "tok_approve",
		CancelAfterPlacement: true,
		ExpectOutOfArea:      true,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("plan\n got %+v\nwant %+v", got, want)
	}
}

func TestToPlanKeepsAnEmptyTierAndNoFallback(t *testing.T) {
	b := day.Buyer{Index: 1, Lines: []day.Line{{Item: day.Item{CatalogItemID: "c"}, Qty: 1}}, Prefix: "786", Token: day.TokenApprove}
	got := toPlan(b)
	if got.Tier != "" || got.FallbackToken != "" || got.CancelAfterPlacement || got.ExpectOutOfArea {
		t.Fatalf("a plain buyer gained something: %+v", got)
	}
}

func TestBuyerEmailIsUniquePerRunAndBuyer(t *testing.T) {
	seen := map[string]string{}
	for _, run := range []string{"RDAAAA", "RDBBBB"} {
		for i := 1; i <= 12; i++ {
			e := buyerEmail(run, i)
			if strings.ToLower(e) != e || !strings.HasSuffix(e, "@example.com") {
				t.Errorf("e-mail %q is not a lower-case example.com address", e)
			}
			if prev, dup := seen[e]; dup {
				t.Errorf("e-mail %s is run %s buyer and %s buyer %d", e, prev, run, i)
			}
			seen[e] = run
		}
	}
	if len(seen) != 24 {
		t.Fatalf("%d distinct addresses for 2 runs of 12 buyers", len(seen))
	}
	if buyerEmail("RDAAAA", 3) != buyerEmail("RDAAAA", 3) {
		t.Error("an address must be a function of run and index: a rerun of the same run id signs in to the same account")
	}
}

func TestEveryShopperStatusMapsToTheDaysOwnWord(t *testing.T) {
	for _, tc := range []struct {
		in   shopper.Status
		want day.Status
	}{
		{shopper.StatusPlaced, day.StatusPlaced},
		{shopper.StatusDeclined, day.StatusDeclined},
		{shopper.StatusInDoubt, day.StatusInDoubt},
		{shopper.StatusRequoteExhausted, day.StatusRequoteExhausted},
		{shopper.StatusRefused, day.StatusRefused},
		{shopper.StatusCancelled, day.StatusCancelled},
		{shopper.StatusNotServed, day.StatusNotServed},
	} {
		got := toBought(shopper.Outcome{Status: tc.in})
		if got.Status != tc.want {
			t.Errorf("shopper status %q -> %q, want %q", tc.in, got.Status, tc.want)
		}
		if len(got.Problems) != 0 {
			t.Errorf("status %q: invented problems %v", tc.in, got.Problems)
		}
	}
	if len(statuses) != 7 {
		t.Errorf("%d statuses mapped, the shopper has 7", len(statuses))
	}
}

func TestAnUnknownShopperStatusIsARefusalNotAPass(t *testing.T) {
	got := toBought(shopper.Outcome{Status: "teleported", Problems: []string{"first"}})
	if got.Status != day.StatusRefused {
		t.Errorf("status %q", got.Status)
	}
	if len(got.Problems) != 2 || got.Problems[0] != "first" || !strings.Contains(got.Problems[1], "teleported") {
		t.Errorf("problems %v: the unknown status must be named after the shopper's own", got.Problems)
	}
	if got := toBought(shopper.Outcome{}); got.Status != day.StatusRefused {
		t.Errorf("an empty status reads %q", got.Status)
	}
}

func TestToBoughtCarriesEveryFactAndTheProblemList(t *testing.T) {
	problems := []string{"POST /checkout 409 offer-not-quotable", "outcome in doubt"}
	o := shopper.Outcome{
		Status: shopper.StatusCancelled, OrderID: "ord-1", TotalMinor: 4321, AuthorizationID: "pay-1",
		OrderStatus: "CANCELLED", CancelSettled: true, CancelRefused: "cancel-after-lock", Problems: problems,
		PromiseWindows: []shopper.PromiseWindow{{GroupNo: 1, DeliverByLatest: "2026-11-17T23:00:00Z"}},
	}
	got := toBought(o)
	want := day.Bought{
		Status: day.StatusCancelled, OrderID: "ord-1", TotalMinor: 4321, AuthorizationID: "pay-1",
		PromiseEnd:  time.Date(2026, 11, 17, 23, 0, 0, 0, time.UTC),
		OrderStatus: "CANCELLED", CancelSettled: true, CancelRefused: "cancel-after-lock",
		Problems: problems,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("bought\n got %+v\nwant %+v", got, want)
	}
	problems[0] = "changed after the fact"
	if got.Problems[0] == "changed after the fact" {
		t.Error("Problems alias the shopper's slice")
	}
}

func TestPromiseEndIsTheLatestOfEveryGroup(t *testing.T) {
	ws := []shopper.PromiseWindow{
		{GroupNo: 1, DeliverByLatest: "2026-11-17T23:00:00Z"},
		{GroupNo: 2, DeliverByLatest: "2026-11-19T01:00:00Z"},
		{GroupNo: 3, DeliverByLatest: "2026-11-18T12:00:00Z"},
	}
	want := time.Date(2026, 11, 19, 1, 0, 0, 0, time.UTC)
	if got := promiseEnd(ws); !got.Equal(want) {
		t.Fatalf("promise end %s, want %s", got, want)
	}
}

func TestPromiseEndIsUnknownRatherThanGuessed(t *testing.T) {
	for name, ws := range map[string][]shopper.PromiseWindow{
		"no group":             nil,
		"empty instant":        {{GroupNo: 1}},
		"not a date":           {{GroupNo: 1, DeliverByLatest: "tomorrow"}},
		"one group unreadable": {{GroupNo: 1, DeliverByLatest: "2026-11-17T23:00:00Z"}, {GroupNo: 2, DeliverByLatest: "soon"}},
		"unknown zone":         {{GroupNo: 1, DeliverByLatest: "2026-11-17T23:00:00", TimeZone: "Mars/Olympus"}},
	} {
		if got := promiseEnd(ws); !got.IsZero() {
			t.Errorf("%s: promise end %s, want the zero time (unknown)", name, got)
		}
	}
}

func TestAnInstantWithoutAnOffsetIsReadInTheGroupsZone(t *testing.T) {
	// 17:00 on 17 Nov in Chicago (CST, UTC-6) is 23:00 UTC; the same text with no zone is UTC.
	if got, ok := parseInstant("2026-11-17T17:00:00", "America/Chicago"); !ok || !got.Equal(time.Date(2026, 11, 17, 23, 0, 0, 0, time.UTC)) {
		t.Errorf("chicago reading %s %v", got, ok)
	}
	if got, ok := parseInstant("2026-11-17T17:00:00", ""); !ok || !got.Equal(time.Date(2026, 11, 17, 17, 0, 0, 0, time.UTC)) {
		t.Errorf("utc reading %s %v", got, ok)
	}
	// an explicit offset is an absolute instant: the group's zone does not move it
	if got, ok := parseInstant("2026-11-17T17:00:00-06:00", "Asia/Tokyo"); !ok || !got.Equal(time.Date(2026, 11, 17, 23, 0, 0, 0, time.UTC)) {
		t.Errorf("offset reading %s %v", got, ok)
	}
}

type fakeBuyer struct {
	plans []shopper.Plan
	out   shopper.Outcome
	err   error
}

func (f *fakeBuyer) Buy(_ context.Context, p shopper.Plan) (shopper.Outcome, error) {
	f.plans = append(f.plans, p)
	return f.out, f.err
}

func TestShopperAdapterGivesEachBuyerItsOwnBrowserAndRecordsWhereTheOrderShips(t *testing.T) {
	var emails []string
	fb := &fakeBuyer{out: shopper.Outcome{Status: shopper.StatusPlaced, OrderID: "ord-9", TotalMinor: 100}}
	ad := &shopperAdapter{runID: "RDX1", orders: &orderPrefixes{}, newBuyer: func(email string) (buyer, error) {
		emails = append(emails, email)
		return fb, nil
	}}
	b := fullBuyer()
	b.ExpectOutOfArea, b.CancelAfterPlacement = false, false
	got, err := ad.Buy(context.Background(), b)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != day.StatusPlaced || got.OrderID != "ord-9" {
		t.Errorf("bought %+v", got)
	}
	if len(emails) != 1 || emails[0] != buyerEmail("RDX1", 7) {
		t.Errorf("e-mails %v", emails)
	}
	if len(fb.plans) != 1 || !reflect.DeepEqual(fb.plans[0], toPlan(b)) {
		t.Errorf("plan %+v", fb.plans)
	}
	if p, ok := ad.orders.get("ord-9"); !ok || p != "787" {
		t.Errorf("the order's prefix: %q %v", p, ok)
	}
}

func TestShopperAdapterRecordsNoPrefixForNothingPlaced(t *testing.T) {
	fb := &fakeBuyer{out: shopper.Outcome{Status: shopper.StatusDeclined}}
	ad := &shopperAdapter{runID: "RDX1", orders: &orderPrefixes{}, newBuyer: func(string) (buyer, error) { return fb, nil }}
	if _, err := ad.Buy(context.Background(), fullBuyer()); err != nil {
		t.Fatal(err)
	}
	if _, ok := ad.orders.get(""); ok {
		t.Error("a prefix was recorded for an empty order id")
	}
}

func TestShopperAdapterPassesSetupErrorsAndTheEndOfTheContextOn(t *testing.T) {
	boom := errors.New("shopper: the client has no \"storefront\" base URL")
	ad := &shopperAdapter{runID: "R", newBuyer: func(string) (buyer, error) { return nil, boom }}
	if _, err := ad.Buy(context.Background(), fullBuyer()); !errors.Is(err, boom) {
		t.Errorf("err %v", err)
	}
	ended := errors.New("context canceled")
	fb := &fakeBuyer{out: shopper.Outcome{Status: shopper.StatusPlaced, OrderID: "x"}, err: ended}
	ad = &shopperAdapter{runID: "R", orders: &orderPrefixes{}, newBuyer: func(string) (buyer, error) { return fb, nil }}
	got, err := ad.Buy(context.Background(), fullBuyer())
	if !errors.Is(err, ended) {
		t.Errorf("err %v", err)
	}
	if got.OrderID != "" {
		t.Errorf("an interrupted buy reported an order %+v", got)
	}
	if _, ok := ad.orders.get("x"); ok {
		t.Error("a prefix was recorded for an order whose buy did not finish")
	}
}
