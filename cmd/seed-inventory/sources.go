package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

// reader fetches JSON over a service's published REST contract. GETs are retried
// on transport errors and 5xx/429 with a short backoff.
type reader struct {
	base string
	http *http.Client
}

func (r *reader) getJSON(ctx context.Context, path string, out any) error {
	var last error
	backoff := 200 * time.Millisecond
	for attempt := 1; attempt <= 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.base+path, nil)
		if err != nil {
			return err
		}
		resp, err := r.http.Do(req)
		if err == nil {
			b, rerr := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			switch {
			case rerr != nil:
				err = rerr
			case resp.StatusCode == http.StatusOK:
				return json.Unmarshal(b, out)
			case resp.StatusCode >= 500 || resp.StatusCode == http.StatusTooManyRequests:
				err = &apiError{Status: resp.StatusCode, Body: strings.TrimSpace(string(b))}
			default:
				return &apiError{Status: resp.StatusCode, Body: strings.TrimSpace(string(b))}
			}
		}
		last = err
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff *= 2
	}
	return fmt.Errorf("GET %s after 5 attempts: %w", path, last)
}

type apiError struct {
	Status int
	Body   string
}

func (e *apiError) Error() string { return fmt.Sprintf("HTTP %d: %s", e.Status, e.Body) }

// loadProducts lists every product registered in product-master (GET /products, 500 per page)
// and keeps the ones that can be stocked: a product with declared or effective dimensions, or a
// grocery (a GTIN SKU without dimensions, as Open Food Facts records none). Registered products
// that are neither (legacy test SKUs with an empty description) are returned as skipped.
func loadProducts(ctx context.Context, r *reader) (stockable []Product, skipped []string, err error) {
	cursor := ""
	for {
		path := "/products?limit=500"
		if cursor != "" {
			path += "&cursor=" + url.QueryEscape(cursor)
		}
		var page struct {
			Items []struct {
				SKU            string `json:"sku"`
				Description    string `json:"description"`
				Classification *struct {
					HandlingTags     []string `json:"handlingTags"`
					TemperatureClass string   `json:"temperatureClass"`
					DotHazardClass   *int     `json:"dotHazardClass"`
				} `json:"classification"`
				PhysicalProfile struct {
					Effective *struct {
						LengthMm int64 `json:"lengthMm"`
						WidthMm  int64 `json:"widthMm"`
						HeightMm int64 `json:"heightMm"`
						WeightG  int64 `json:"weightG"`
					} `json:"effective"`
				} `json:"physicalProfile"`
			} `json:"items"`
			NextCursor string `json:"nextCursor"`
		}
		if err := r.getJSON(ctx, path, &page); err != nil {
			return nil, nil, fmt.Errorf("product-master GET /products: %w", err)
		}
		for _, it := range page.Items {
			p := Product{SKU: it.SKU, Description: it.Description}
			if c := it.Classification; c != nil {
				p.Tags, p.Temp = c.HandlingTags, c.TemperatureClass
				if c.DotHazardClass != nil {
					p.DotClass = *c.DotHazardClass
				}
			}
			if e := it.PhysicalProfile.Effective; e != nil && e.LengthMm > 0 {
				p.HasDims = true
				p.VolumeL = float64(e.LengthMm) * float64(e.WidthMm) * float64(e.HeightMm) / 1e6
				p.WeightG = e.WeightG
			}
			switch {
			case p.HasDims:
				stockable = append(stockable, p)
			case it.Description != "" && allDigits(it.SKU):
				stockable = append(stockable, p)
			default:
				skipped = append(skipped, it.SKU)
			}
		}
		if page.NextCursor == "" {
			return stockable, skipped, nil
		}
		cursor = page.NextCursor
	}
}

func allDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// loadSites reads the pilot sites from facility-layout: role, and every Active storage slot with
// its zone's temperature class and hazmat rating and its volume and weight limits. Amnesty slots
// (exceptions) are not stock locations and are left out.
func loadSites(ctx context.Context, r *reader, codes []string) ([]Site, error) {
	var all []struct {
		SiteCode string `json:"siteCode"`
		Role     string `json:"siteRole"`
		Status   string `json:"status"`
	}
	if err := r.getJSON(ctx, "/sites", &all); err != nil {
		return nil, fmt.Errorf("facility-layout GET /sites: %w", err)
	}
	roles := map[string]string{}
	for _, s := range all {
		if s.Status == "Active" {
			roles[s.SiteCode] = s.Role
		}
	}
	var out []Site
	for _, code := range codes {
		role, ok := roles[code]
		if !ok {
			return nil, fmt.Errorf("site %s is not an Active site in facility-layout", code)
		}
		var zones []struct {
			ZoneID   string `json:"zoneId"`
			ZoneCode string `json:"zoneCode"`
			Temp     string `json:"temperatureClass"`
			Hazmat   bool   `json:"hazmat"`
			Status   string `json:"status"`
		}
		if err := r.getJSON(ctx, "/sites/"+url.PathEscape(code)+"/zones", &zones); err != nil {
			return nil, fmt.Errorf("facility-layout zones of %s: %w", code, err)
		}
		zone := map[string]int{}
		for i, z := range zones {
			zone[z.ZoneID] = i
		}
		var locs []struct {
			Code     string `json:"locationCode"`
			ZoneID   string `json:"zoneId"`
			Status   string `json:"status"`
			Capacity struct {
				MaxWeightKg float64 `json:"maxWeightKg"`
				MaxVolumeM3 float64 `json:"maxVolumeM3"`
			} `json:"capacity"`
		}
		if err := r.getJSON(ctx, "/sites/"+url.PathEscape(code)+"/locations?role=Storage", &locs); err != nil {
			return nil, fmt.Errorf("facility-layout storage locations of %s: %w", code, err)
		}
		s := Site{Code: code, Role: role}
		for _, l := range locs {
			zi, ok := zone[l.ZoneID]
			if !ok || l.Status != "Active" || zones[zi].ZoneCode == "AMN" || l.Capacity.MaxVolumeM3 <= 0 {
				continue
			}
			z := zones[zi]
			s.Bins = append(s.Bins, Bin{ID: l.Code, ZoneCode: z.ZoneCode, Temp: z.Temp, Hazmat: z.Hazmat,
				MaxVolumeL: l.Capacity.MaxVolumeM3 * 1000, MaxWeightG: int64(l.Capacity.MaxWeightKg * 1000)})
		}
		sort.Slice(s.Bins, func(i, j int) bool { return s.Bins[i].ID < s.Bins[j].ID })
		out = append(out, s)
	}
	return out, nil
}
