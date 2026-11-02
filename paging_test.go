package main

import (
	"context"
	"fmt"
	"net/http"

	"github.com/claudioed/warehouse-e2e/internal/paging"
)

// getItems GETs a paginated collection and returns every item, following nextCursor until it
// is absent (the fleet REST guideline: every collection answers {"items":[...],"nextCursor":".."}).
// Filters already in url are repeated on every page. w.last is the LAST page fetched, so a
// caller that prints the body on a miss shows what the provider actually answered. A non-200
// page is an error carrying its status and body.
func (w *world) getItems(url string) ([]map[string]any, error) {
	return paging.All[map[string]any](context.Background(), func(_ context.Context, u string, out any) error {
		if err := w.doJSON(http.MethodGet, u, nil); err != nil {
			return err
		}
		if w.last.status != http.StatusOK {
			return fmt.Errorf("GET %s: status %d (body: %s)", u, w.last.status, w.last.body)
		}
		items, next, err := paging.Decode[map[string]any](w.last.body)
		if err != nil {
			return err
		}
		*(out.(*paging.Page[map[string]any])) = paging.Page[map[string]any]{Items: items, NextCursor: next}
		return nil
	}, url)
}
