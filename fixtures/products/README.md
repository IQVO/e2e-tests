# Product catalogue fixture

`openfoodfacts-products.json` is the catalogue `scripts/08-seed-products.sh`
registers in product-master: at least 2,000 **real** retail products.

- **Source:** [Open Food Facts](https://world.openfoodfacts.org), search API v2,
  harvested by `go run ./cmd/seed-products -fetch` across category slices
  (frozen foods, ice cream, yoghurt, cheese, beverages, snacks, cereals, pasta,
  coffee, chocolate, canned food, condiments, plus the most-scanned list).
- **Licence:** the data is © Open Food Facts contributors and available under the
  [Open Database License (ODbL) 1.0](https://opendatacommons.org/licenses/odbl/1-0/);
  individual contents under the
  [Database Contents License](https://opendatacommons.org/licenses/dbcl/1-0/).
  This file is a derived database and stays under the ODbL.

## What every entry guarantees

| Field | Rule |
|---|---|
| `sku` | The product's GS1 GTIN (EAN-8, UPC-A, EAN-13, GTIN-14) with a **verified mod-10 check digit**; look it up at `https://world.openfoodfacts.org/product/<sku>` |
| `description` | `<brand> <name> (<net quantity>)`, English name preferred, ≤ 200 runes, no control characters (product-master's own limits) |
| `temperatureClass` | `Frozen` or `Chilled` only when the product's **own** category tags say so (`categories` lists the evidence); empty otherwise |

Deliberately **not** in the file: handling tags the source cannot support
(Hazmat, Fragile, Oversized, HighValue) and unit dimensions or weight (Open Food
Facts records net content, not packed unit dimensions). Nothing is invented.

The file is validated on load (GTIN, uniqueness, description rules), so a hand
edit that breaks a rule fails before the first request.
