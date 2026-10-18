# Product catalogue fixtures

`scripts/08-seed-products.sh` registers both files in product-master:
**at least 10,000 real products**, ~13,700 in all.

| File | Products | What is real | What is derived |
|---|---|---|---|
| `openfoodfacts-products.json` | ~2,370 grocery | GTIN, brand, name, net quantity | `temperatureClass` (Frozen/Chilled) from the product's own category tags |
| `fulfilment-products.json` | ~11,300 | SKU (source id), name, category path, **length / width / height (mm), weight (g)** | `handlingTags`, `temperatureClass`, `dotHazardClass` - each with its `evidence` |

## `openfoodfacts-products.json`

- **Source:** [Open Food Facts](https://world.openfoodfacts.org), search API v2,
  harvested by `go run ./cmd/seed-products -fetch`.
- **Licence:** © Open Food Facts contributors,
  [ODbL 1.0](https://opendatacommons.org/licenses/odbl/1-0/); contents under the
  [Database Contents License](https://opendatacommons.org/licenses/dbcl/1-0/).
  This file is a derived database and stays under the ODbL.
- `sku` is the GTIN with a verified mod-10 check digit
  (`https://world.openfoodfacts.org/product/<sku>`). It carries no dimensions: the
  source records net content, not packed unit size.

## `fulfilment-products.json`

Harvested by `python3 scripts/harvest-fulfilment-products.py` from two open datasets:

- **ABO product listings** (`abo-listings.tar`, ~6,600 kept of 147,702 listings),
  [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/). Credit: Amazon.com
  (data); Guillaumin, Dideriksen, Deng, Arora, Collins, Malik (dataset). Records
  were filtered and converted to millimetres and grams, and handling classes were
  derived (changes indicated, as the licence requires). SKU = `ABO-<item id>`.
- **Wayfair WANDS** (`dataset/product.csv`, ~4,700 kept of 42,994),
  MIT, Copyright (c) 2021 ecir2022. Overall width / depth / height and product
  weight come from each product's feature list. SKU = `WANDS-<product id>`.

The retailer's own name is removed from titles ("AmazonBasics Stapler" becomes
"Stapler"); the product, its sub-brand and its measurements are untouched.

### What a listing must pass

Dropped, never repaired: no English name; no dimensions or weight; any side outside
3..20,000 mm or weight outside 1 g..150 kg (product-master allows 2,000 kg; one retail
unit above 150 kg is a pallet or bulk-pack error); bounding-box density outside
0.005..8 g/cm³ (food: ≤ 2), which removes placeholder or swapped measurements such as a
454 g crab cake in a 5 cm cube. The remaining pool is spread over the sources' own
product types / classes with a stable hash order (no RNG), so the file is varied,
reproducible and not dominated by the biggest categories. Hazmat and cold-chain products
are always kept, because the sources hold few of them.

### How the handling classes are derived

The sources carry no handling flags, so these rules (see the script) produce them and
each product lists the matching rule in `evidence`:

| Tag | Rule |
|---|---|
| `Oversized` | longest side > 1,219 mm, second-longest > 762 mm, or weight > 22.68 kg (carrier *additional handling* thresholds) |
| `Fragile` | the **tail** of the category path (leaf + parent) or product type names glass, ceramic, mirrors, lamps, bulbs, vases, frames, wall art, dinnerware, televisions, wine/beer; disposables, stands and cabinets are excluded by name |
| `HighValue` | the category leaf is laptops, tablets, phones, cameras, televisions, watches or fine jewelry (accessories do not count) |
| `Hazmat` + `dotHazardClass` | the product **name** names an aerosol (2), a flammable liquid such as hand sanitizer, nail polish or perfume (3), a lithium battery (9) or bleach/drain cleaner (8); never read from marketing bullets |
| `TemperatureSensitive` + `temperatureClass` | category path under Frozen (`Frozen`) or dairy/produce/meat/seafood/deli (`Chilled`), minus dried, canned, shelf-stable, potatoes, onions |

Dimensions are the **product's** size, as the source lists it, in the source's own
length/width/height order; they are not a shipping-carton size.

### Limits to know

- Handling classes are heuristics. Real hazmat determination needs the safety data sheet;
  here ~50 products are tagged, only where the name says so.
- Many ABO titles carry retailer house labels (for example Whole Foods, Happy Belly, Rivet);
  they are real brands and kept.
- Some measurements in the sources are noisy; the filters above remove the implausible
  ones, not the merely unusual.

Every file is validated on load (SKU rules, GTIN check digit for Open Food Facts entries,
dimension and weight limits, tag enum, temperature class and DOT class invariants, no
SKU in two files), so a hand edit that breaks a rule fails before the first request.
