package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

// fakeOFF serves the search API: category "good" returns two pages of
// valid products (EAN-13s built with a correct check digit), category
// "flaky" always answers 503, and every request is counted.
func fakeOFF(t *testing.T) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if r.Header.Get("User-Agent") != offUserAgent {
			t.Errorf("request without the identifying User-Agent: %q", r.Header.Get("User-Agent"))
		}
		q := r.URL.Query()
		if q.Get("categories_tags_en") == "flaky" {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		page, _ := strconv.Atoi(q.Get("page"))
		var ps []offProduct
		if page <= 2 {
			for i := 0; i < 3; i++ {
				ps = append(ps, offProduct{Code: ean13(page*100 + i), ProductName: "Product " + strconv.Itoa(page*100+i), Brands: "Brand", Quantity: "100 g"})
			}
		}
		_ = json.NewEncoder(w).Encode(offPage{Count: 6, Products: ps})
	}))
	t.Cleanup(srv.Close)
	return srv, &hits
}

// ean13 builds a valid EAN-13 from n (12-digit body + check digit).
func ean13(n int) string {
	body := "400000" + leftPad(strconv.Itoa(n), 6)
	for d := 0; d <= 9; d++ {
		if c := body + strconv.Itoa(d); validGTIN(c) {
			return c
		}
	}
	panic("unreachable")
}

func leftPad(s string, n int) string {
	for len(s) < n {
		s = "0" + s
	}
	return s
}

func testClient(url, cache string) *offClient {
	c := newOFFClient(0, cache)
	c.searchURL = url
	c.backoff = time.Millisecond
	c.attempts = 2
	return c
}

// A slice that keeps answering 503 is cut short; what other slices
// harvested survives, and the fetch succeeds when the total meets target.
func TestFetchCatalogSurvivesAPermanentlyFailingSlice(t *testing.T) {
	srv, _ := fakeOFF(t)
	got, err := fetchCatalog(context.Background(), testClient(srv.URL, ""),
		[]offSlice{{Category: "good", Target: 6}, {Category: "flaky", Target: 50}}, 6)
	if err != nil {
		t.Fatalf("fetch failed although the good slice met the target: %v", err)
	}
	if len(got) != 6 {
		t.Fatalf("kept %d products, want the good slice's 6", len(got))
	}
	for i := 1; i < len(got); i++ {
		if got[i-1].SKU >= got[i].SKU {
			t.Fatalf("fixture not sorted by sku: %s before %s", got[i-1].SKU, got[i].SKU)
		}
	}
}

func TestFetchCatalogFailsBelowTarget(t *testing.T) {
	srv, _ := fakeOFF(t)
	if _, err := fetchCatalog(context.Background(), testClient(srv.URL, ""),
		[]offSlice{{Category: "good", Target: 6}, {Category: "flaky", Target: 50}}, 7); err == nil {
		t.Fatal("fetch succeeded with fewer products than the target")
	}
}

// A second fetch with the same cache never touches the network.
func TestFetchCatalogResumesFromCache(t *testing.T) {
	srv, hits := fakeOFF(t)
	cache := t.TempDir()
	slices := []offSlice{{Category: "good", Target: 6}}
	first, err := fetchCatalog(context.Background(), testClient(srv.URL, cache), slices, 6)
	if err != nil {
		t.Fatal(err)
	}
	before := hits.Load()
	second, err := fetchCatalog(context.Background(), testClient(srv.URL, cache), slices, 6)
	if err != nil {
		t.Fatal(err)
	}
	if hits.Load() != before {
		t.Fatalf("cached re-fetch made %d network requests, want 0", hits.Load()-before)
	}
	if len(first) != len(second) {
		t.Fatalf("cached fetch returned %d products, first returned %d", len(second), len(first))
	}
}
