package paging

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"testing"
)

// fakeProvider serves n items in pages of size, keyed by an opaque cursor.
func fakeProvider(n, size int, seen *[]string) Getter {
	return func(_ context.Context, raw string, out any) error {
		*seen = append(*seen, raw)
		u, err := url.Parse(raw)
		if err != nil {
			return err
		}
		start := 0
		if c := u.Query().Get("cursor"); c != "" {
			start, err = strconv.Atoi(c[len("c"):])
			if err != nil {
				return err
			}
		}
		end := min(start+size, n)
		p := map[string]any{"items": rangeInts(start, end)}
		if end < n {
			p["nextCursor"] = fmt.Sprintf("c%d", end)
		}
		b, _ := json.Marshal(p)
		return json.Unmarshal(b, out)
	}
}

func rangeInts(a, b int) []int {
	out := make([]int, 0, b-a)
	for i := a; i < b; i++ {
		out = append(out, i)
	}
	return out
}

func TestAllWalksEveryPageWithoutDuplicatesOrGaps(t *testing.T) {
	var seen []string
	got, err := All[int](context.Background(), fakeProvider(23, 5, &seen), "/things?kind=a")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 23 {
		t.Fatalf("got %d items, want 23", len(got))
	}
	for i, v := range got {
		if v != i {
			t.Fatalf("item %d = %d: duplicate or skipped row", i, v)
		}
	}
	if len(seen) != 5 {
		t.Fatalf("%d requests, want 5 pages: %v", len(seen), seen)
	}
	if seen[0] != "/things?kind=a&limit=500" || seen[1] != "/things?kind=a&limit=500&cursor=c5" {
		t.Fatalf("filters and limit must be repeated on every page: %v", seen[:2])
	}
}

func TestAllStopsOnAnEmptyCollection(t *testing.T) {
	var seen []string
	got, err := All[int](context.Background(), fakeProvider(0, 5, &seen), "/things")
	if err != nil || len(got) != 0 || len(seen) != 1 {
		t.Fatalf("got %v err %v requests %v", got, err, seen)
	}
}

func TestAllRejectsARepeatedCursor(t *testing.T) {
	stuck := func(_ context.Context, _ string, out any) error {
		return json.Unmarshal([]byte(`{"items":[1],"nextCursor":"same"}`), out)
	}
	if _, err := All[int](context.Background(), stuck, "/things"); err == nil {
		t.Fatal("a provider that never ends must fail, not loop")
	}
}

func TestWithKeepsAnExplicitLimit(t *testing.T) {
	if got := With("/p?limit=1", ""); got != "/p?limit=1" {
		t.Fatalf("got %q", got)
	}
	if got := With("/p", "a b"); got != "/p?limit=500&cursor=a+b" {
		t.Fatalf("got %q", got)
	}
}

func TestDecode(t *testing.T) {
	items, next, err := Decode[string]([]byte(`{"items":["a"],"nextCursor":"n"}`))
	if err != nil || len(items) != 1 || next != "n" {
		t.Fatalf("%v %q %v", items, next, err)
	}
	if _, _, err := Decode[string]([]byte(`["a"]`)); err == nil {
		t.Fatal("a bare array is not the guideline envelope")
	}
}
