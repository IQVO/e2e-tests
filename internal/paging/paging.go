// Package paging is the one place the harness reads a paginated collection.
//
// Every collection GET in the fleet answers {"items":[...],"nextCursor":"<opaque>"}
// (warehouse-docs REST guideline, section 3). A consumer must never assume one page: it
// follows nextCursor until it is absent. The query parameters are limit and cursor; the
// retired after/offset/from/next parameters now answer 400 unknown-query-parameter.
package paging

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
)

// PageSize is the explicit limit sent on every page request (the guideline's maximum).
const PageSize = 500

// maxPages bounds a walk so a provider that keeps returning a cursor cannot loop forever.
const maxPages = 100000

// Page is the wire envelope of one page.
type Page[T any] struct {
	Items      []T    `json:"items"`
	NextCursor string `json:"nextCursor,omitempty"`
}

// Getter fetches one URL and decodes the JSON answer into out. Callers adapt their own HTTP
// client (context-aware or not) to this shape.
type Getter func(ctx context.Context, rawURL string, out any) error

// With returns rawURL with limit=PageSize and, when cursor is not empty, cursor=<cursor>
// added. Any filter already in rawURL is kept: filters are repeated on every page request.
func With(rawURL, cursor string) string {
	sep := "?"
	if strings.Contains(rawURL, "?") {
		sep = "&"
	}
	u := rawURL
	if !strings.Contains(rawURL, "limit=") {
		u += sep + fmt.Sprintf("limit=%d", PageSize)
		sep = "&"
	}
	if cursor != "" {
		u += sep + "cursor=" + url.QueryEscape(cursor)
	}
	return u
}

// All walks the collection at rawURL page by page and returns every item.
func All[T any](ctx context.Context, get Getter, rawURL string) ([]T, error) {
	var all []T
	cursor := ""
	for i := 0; i < maxPages; i++ {
		var p Page[T]
		if err := get(ctx, With(rawURL, cursor), &p); err != nil {
			return nil, err
		}
		all = append(all, p.Items...)
		if p.NextCursor == "" {
			return all, nil
		}
		if p.NextCursor == cursor {
			return nil, fmt.Errorf("paging %s: provider repeated cursor %q", rawURL, cursor)
		}
		cursor = p.NextCursor
	}
	return nil, fmt.Errorf("paging %s: no end after %d pages", rawURL, maxPages)
}

// Decode reads the items and the next cursor out of one raw response body. It is for callers
// that already hold the bytes of a page (the Godog world, the warehouse-day API client).
func Decode[T any](body []byte) ([]T, string, error) {
	var p Page[T]
	if err := json.Unmarshal(body, &p); err != nil {
		return nil, "", fmt.Errorf("decode {items,nextCursor}: %w (body: %.200s)", err, body)
	}
	return p.Items, p.NextCursor, nil
}
