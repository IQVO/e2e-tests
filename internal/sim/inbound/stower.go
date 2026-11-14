package inbound

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"net/http"
	"net/url"
	"strings"

	"github.com/claudioed/warehouse-e2e/internal/sim"
	"github.com/claudioed/warehouse-e2e/internal/sim/outbound"
)

// maxBinTries bounds how many bins one line is offered to.
const maxBinTries = 16

// ---------------------------------------------------------------------
// stower (inventory-storage stowages)
// ---------------------------------------------------------------------

// binsFor is the order in which the bins of the site are offered a SKU: the configured list, rotated by a hash
// of the SKU. Chaotic storage has no fixed product-to-bin mapping, and a seed-inventory style hash keeps the choice
// deterministic: the same SKU always starts at the same bin, so a rerun is reproducible.
func (c *Crew) binsFor(sku string) []string {
	var bins []string
	seen := map[string]bool{}
	for _, b := range c.Site.Bins {
		if b != "" && !seen[b] {
			seen[b] = true
			bins = append(bins, b)
		}
	}
	if len(bins) < 2 {
		return bins
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(sku))
	start := int(h.Sum32() % uint32(len(bins)))
	return append(append([]string(nil), bins[start:]...), bins[:start]...)
}

// binRefusals are the 409s of POST /stowages that say "not this bin" (it is full, or the SKU's hazmat or
// temperature class does not fit the bin's zone): the stower offers the units to the next bin. Any other
// refusal is not about the bin and fails the line.
var binRefusals = map[string]bool{
	"bin-full":                            true,
	"hazmat-zone-required":                true,
	"temperature-class-mismatch":          true,
	"hazmat-class-incompatible":           true,
	"location-classification-unavailable": true,
}

// Stow is the RF stow of one line: the units of the SKU go into the site's bins, each stow naming the item (SKU)
// and the location (bin), as inventory-storage requires. Bins are offered in binsFor order; the room of each is
// read first, a line larger than the room of a bin is split across bins, and a bin that refuses the SKU's class
// (409 hazmat-zone-required, temperature-class-mismatch, ...) or does not exist (404 bin-not-found) is skipped
// with the reason kept. Any other refusal, and a transport error, fails the line at once.
//
// It returns every stow that went through. When the whole quantity could not be placed the error names each bin
// that was tried and why, and how many units did go in.
func (c *Crew) Stow(ctx context.Context, l Line) ([]Stowed, error) {
	bins := c.binsFor(l.SKU)
	if len(bins) == 0 {
		return nil, fmt.Errorf("stow %d units of %s: site %s has no bin configured", l.Qty, l.SKU, c.Site.Code)
	}
	if len(bins) > maxBinTries {
		bins = bins[:maxBinTries]
	}
	var done []Stowed
	var skipped []string
	remaining := l.Qty
	for _, b := range bins {
		if remaining <= 0 {
			break
		}
		room, err := c.roomOf(ctx, b)
		switch {
		case errors.Is(err, errNoBin):
			skipped = append(skipped, b+" (bin-not-found)")
			continue
		case err != nil:
			return done, err
		case room <= 0:
			skipped = append(skipped, b+" (full)")
			continue
		}
		chunk := min(remaining, room)
		what := fmt.Sprintf("stow %d units of %s into bin %s", chunk, l.SKU, b)
		r := c.call(ctx, SvcInventory, http.MethodPost, "/stowages", "/stowages", map[string]any{"sku": l.SKU, "quantity": chunk, "binId": b})
		if !r.OK() {
			if slug := outbound.Slug(r); r.Status == http.StatusConflict && binRefusals[slug] {
				skipped = append(skipped, fmt.Sprintf("%s (%s)", b, slug))
				continue
			}
			return done, refusal(what, r)
		}
		var unit struct {
			ID    string `json:"id"`
			SKU   string `json:"sku"`
			BinID string `json:"binId"`
			State string `json:"state"`
		}
		if err := r.Decode(&unit); err != nil {
			return done, fmt.Errorf("%s: %w", what, err)
		}
		if r.Status != http.StatusCreated || unit.ID == "" || unit.SKU != l.SKU || unit.BinID != b {
			return done, fmt.Errorf("%s: status %d unit %q sku %q bin %q, want 201 with a unit of %s in %s (body: %s)",
				what, r.Status, unit.ID, unit.SKU, unit.BinID, l.SKU, b, sim.Truncate(string(r.Body), 200))
		}
		done = append(done, Stowed{SKU: l.SKU, Qty: chunk, Bin: b})
		remaining -= chunk
		if remaining > 0 {
			skipped = append(skipped, fmt.Sprintf("%s (took %d, then full)", b, chunk))
		}
		c.logf("stower", "stowed %d of %s into %s (%s)", chunk, l.SKU, b, unit.ID)
	}
	if remaining > 0 {
		return done, fmt.Errorf("stow %s: only %d of %d units found a bin; bins tried: %s", l.SKU, l.Qty-remaining, l.Qty, strings.Join(skipped, ", "))
	}
	return done, nil
}

var errNoBin = errors.New("bin not found")

// roomOf reads how many units still fit into the bin (GET /bins/{binId}). 404 is errNoBin.
func (c *Crew) roomOf(ctx context.Context, bin string) (int, error) {
	what := "read bin " + bin
	r := c.call(ctx, SvcInventory, http.MethodGet, "/bins/{binId}", "/bins/"+url.PathEscape(bin), nil)
	if r.Status == http.StatusNotFound && outbound.Slug(r) == "bin-not-found" {
		return 0, errNoBin
	}
	if !r.OK() {
		return 0, refusal(what, r)
	}
	var got struct {
		Available int `json:"available"`
	}
	if err := r.Decode(&got); err != nil {
		return 0, fmt.Errorf("%s: %w", what, err)
	}
	return got.Available, nil
}

// StowReceipt stows what the receipt counted Good, per line (not what the ASN announced). A line that cannot be
// stowed is reported and the other lines are still stowed; the error joins every line's finding.
func (c *Crew) StowReceipt(ctx context.Context, rc Receipt) ([]Stowed, error) {
	var all []Stowed
	var errs []error
	for _, l := range rc.Lines {
		if l.ReceivedGood <= 0 {
			continue
		}
		got, err := c.Stow(ctx, Line{SKU: l.SKU, Qty: l.ReceivedGood})
		all = append(all, got...)
		if err != nil {
			errs = append(errs, err)
		}
	}
	return all, errors.Join(errs...)
}

// splitErr returns the messages of an errors.Join (or the one message of a plain error).
func splitErr(err error) []string {
	if err == nil {
		return nil
	}
	if j, ok := err.(interface{ Unwrap() []error }); ok {
		var out []string
		for _, e := range j.Unwrap() {
			out = append(out, splitErr(e)...)
		}
		return out
	}
	return []string{err.Error()}
}
