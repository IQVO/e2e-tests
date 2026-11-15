package collect

import (
	"context"

	"github.com/claudioed/warehouse-e2e/internal/retailday"
)

// Order follows one placed order across every context. This first step only carries the
// identity of the order; the readers arrive in the following commits.
func (c *Collector) Order(_ context.Context, p retailday.Placed) (retailday.OrderFacts, []string) {
	return retailday.OrderFacts{OrderID: p.OrderID, Expect: p.Expect, Lines: p.Lines(), PromiseEnd: p.PromiseEnd, TotalMinor: p.TotalMinor},
		[]string{"unread order: the collector does not read the contexts yet"}
}

// Shelf reads each SKU's usable stock and whether the storefront sells it. Not read yet.
func (c *Collector) Shelf(_ context.Context, skus []string) (map[string]retailday.Shelf, error) {
	return nil, nil
}
