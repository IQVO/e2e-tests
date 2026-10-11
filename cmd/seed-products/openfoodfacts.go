package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

// Open Food Facts (https://world.openfoodfacts.org) is an open database of
// real food products keyed by their GS1 barcode, published under the Open
// Database License (ODbL 1.0). Its search API caps page_size at 100 and
// answers 503 when overloaded, so the fetch is paced and retried.

const (
	offSearchURL = "https://world.openfoodfacts.org/api/v2/search"
	offUserAgent = "warehouse-systems-e2e/1.0 (study project; seed-products fetch)"
	offPageSize  = 100
	offFields    = "code,product_name,product_name_en,brands,quantity,categories_tags"
)

// offSlice is one query against the search API and how many usable
// products to keep from it. Slicing by category gives the catalogue real
// variety (the unfiltered most-scanned list is dominated by a handful of
// brands) and guarantees cold-chain items are present.
type offSlice struct {
	Category string // categories_tags_en filter; empty = no filter
	Target   int
}

var defaultSlices = []offSlice{
	{Category: "frozen-foods", Target: 300},
	{Category: "ice-creams", Target: 100},
	{Category: "yogurts", Target: 150},
	{Category: "cheeses", Target: 150},
	{Category: "beverages", Target: 300},
	{Category: "snacks", Target: 250},
	{Category: "breakfast-cereals", Target: 150},
	{Category: "pastas", Target: 120},
	{Category: "coffees", Target: 100},
	{Category: "chocolates", Target: 150},
	{Category: "canned-foods", Target: 150},
	{Category: "condiments", Target: 150},
	{Category: "", Target: 300},
}

type offProduct struct {
	Code          string   `json:"code"`
	ProductName   string   `json:"product_name"`
	ProductNameEn string   `json:"product_name_en"`
	Brands        string   `json:"brands"`
	Quantity      string   `json:"quantity"`
	Categories    []string `json:"categories_tags"`
}

type offPage struct {
	Count    int          `json:"count"`
	Products []offProduct `json:"products"`
}

type offClient struct {
	http      *http.Client
	searchURL string        // offSearchURL; a test server in tests
	pause     time.Duration // between requests (search API: ~10 req/min)
	backoff   time.Duration // first retry wait after a 503/429/5xx; doubles to 60s
	attempts  int           // per page, including the first
	last      time.Time
	cacheDir  string // page responses already fetched; "" = no cache
}

func newOFFClient(pause time.Duration, cacheDir string) *offClient {
	return &offClient{
		http:      &http.Client{Timeout: 90 * time.Second},
		searchURL: offSearchURL,
		pause:     pause,
		backoff:   10 * time.Second,
		attempts:  6,
		cacheDir:  cacheDir,
	}
}

// cachePath maps a request URL to its cache file. Pages are cached raw, so
// a fetch interrupted by Open Food Facts' 503s resumes where it stopped
// instead of re-asking a rate-limited API for pages it already returned.
func (c *offClient) cachePath(u string) string {
	if c.cacheDir == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(u))
	return filepath.Join(c.cacheDir, hex.EncodeToString(sum[:12])+".json")
}

func (c *offClient) page(ctx context.Context, category string, page int) (offPage, error) {
	q := url.Values{}
	q.Set("page_size", strconv.Itoa(offPageSize))
	q.Set("page", strconv.Itoa(page))
	q.Set("fields", offFields)
	q.Set("sort_by", "unique_scans_n")
	if category != "" {
		q.Set("categories_tags_en", category)
	}
	u := c.searchURL + "?" + q.Encode()

	var out offPage
	cache := c.cachePath(u)
	if cache != "" {
		if raw, err := os.ReadFile(cache); err == nil && json.Unmarshal(raw, &out) == nil {
			return out, nil
		}
	}
	backoff := c.backoff
	for attempt := 1; ; attempt++ {
		if wait := c.pause - time.Since(c.last); wait > 0 {
			time.Sleep(wait)
		}
		c.last = time.Now()

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			return out, err
		}
		req.Header.Set("User-Agent", offUserAgent)
		resp, err := c.http.Do(req)
		if err == nil && resp.StatusCode == http.StatusOK {
			raw, rerr := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if rerr != nil {
				return out, rerr
			}
			if err := json.Unmarshal(raw, &out); err != nil {
				return out, err
			}
			if cache != "" {
				_ = os.WriteFile(cache, raw, 0o644)
			}
			return out, nil
		}
		status := "transport error"
		if err == nil {
			status = resp.Status
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusServiceUnavailable && resp.StatusCode != http.StatusTooManyRequests && resp.StatusCode < 500 {
				return out, fmt.Errorf("open food facts %s: %s", u, resp.Status)
			}
		}
		if attempt >= c.attempts {
			return out, fmt.Errorf("open food facts %s: giving up after %d attempts (%s, %v)", u, attempt, status, err)
		}
		log.Printf("  open food facts %s (attempt %d), retrying in %s", status, attempt, backoff)
		time.Sleep(backoff)
		if backoff < 60*time.Second {
			backoff *= 2
		}
		if backoff > 60*time.Second {
			backoff = 60 * time.Second
		}
	}
}

// fetchCatalog harvests at least target distinct, valid products. A product
// is kept only when its barcode is a GS1-valid GTIN and it has a name;
// duplicates across slices keep their first occurrence. Classification is
// derived from the product's OWN category tags, never from the slice that
// found it.
//
// A slice whose page keeps failing after the retries is cut short, not
// fatal: what it already returned is kept and the next slice runs. The
// fetch only fails when the total still falls short of target (losing a
// 2,000-product harvest to the last slice's 503s is the failure this
// avoids).
func fetchCatalog(ctx context.Context, c *offClient, slices []offSlice, target int) ([]FixtureProduct, error) {
	if c.cacheDir != "" {
		if err := os.MkdirAll(c.cacheDir, 0o755); err != nil {
			return nil, fmt.Errorf("cache dir: %w", err)
		}
	}
	seen := map[string]bool{}
	var out []FixtureProduct

	for _, s := range slices {
		label := s.Category
		if label == "" {
			label = "(most scanned)"
		}
		kept := 0
		for page := 1; kept < s.Target; page++ {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			p, err := c.page(ctx, s.Category, page)
			if err != nil {
				log.Printf("slice %-18s cut short at page %d: %v", label, page, err)
				break
			}
			if len(p.Products) == 0 {
				break
			}
			for _, op := range p.Products {
				if kept >= s.Target {
					break
				}
				fp, ok := toFixtureProduct(op)
				if !ok || seen[fp.SKU] {
					continue
				}
				seen[fp.SKU] = true
				out = append(out, fp)
				kept++
			}
		}
		log.Printf("slice %-18s kept %4d  total %4d", label, kept, len(out))
	}
	if len(out) < target {
		return nil, fmt.Errorf("harvested only %d valid products, want at least %d (raise slice targets)", len(out), target)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SKU < out[j].SKU })
	return out, nil
}

func toFixtureProduct(op offProduct) (FixtureProduct, bool) {
	if !validGTIN(op.Code) {
		return FixtureProduct{}, false
	}
	name := op.ProductNameEn
	if cleanText(name) == "" {
		name = op.ProductName
	}
	if cleanText(name) == "" {
		return FixtureProduct{}, false
	}
	temp, evidence := deriveTemperature(op.Categories)
	return FixtureProduct{
		SKU:              op.Code,
		Description:      buildDescription(op.Brands, name, op.Quantity),
		TemperatureClass: temp,
		Categories:       evidence,
	}, true
}
