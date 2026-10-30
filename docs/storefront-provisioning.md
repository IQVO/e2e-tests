# Storefront provisioning: the register

Everything a shopper needs to buy a product in the storefront (<http://storefront.localhost>) and see
its delivery promise, on the kind cluster, and who provisions it. One command does all of it,
repeatably:

```sh
make provision-storefront-dry-run   # checks + plans, writes nothing
make provision-storefront           # provisions, then verifies (a second run writes nothing)
make provision-storefront-verify    # read-only: is the storefront still sellable?
```

`scripts/10-provision-storefront.sh` runs, in this order: **preflight** (Kong answers, the sibling
`../warehouse-infra` checkout exists, one real read per layer, WMS usable stock of every storefront
SKU) -> **WMS** (`go run ./cmd/seed-storefront -wms-only`: site-1's CPT schedule in
process-path-management, first because everything that reads stock through network-fulfillment
depends on it) -> **network tier** (`warehouse-infra/scripts/seed-retail-network.py`, then its
12-check verify) -> **product dictionary check** (`seed-retail-network.py --check-translation
helm-values/network-fulfillment.yaml`) -> **storefront tier** (`go run ./cmd/seed-storefront`:
CHECK, PLAN, APPLY, VERIFY; it re-checks the CPT schedule, equal by then) -> **summary** (storefront
URL, deliverable postal codes).
Environment: `WAREHOUSE_INFRA_DIR` (default `<repos root>/warehouse-infra`), `KONG_URL`
(`http://localhost:8000`), `STOREFRONT_URL` (`http://storefront.localhost`),
`STOREFRONT_VERIFY_TIMEOUT` (`3m`), `STOREFRONT_SKIP_NETWORK=1` (skip the network steps when you just
ran them). Other arguments go to `cmd/seed-storefront` (`-media-base`, `-quote-prefix`, ...).

Inputs of the storefront tier: `fixtures/storefront/catalogue.json` (**curated**: currency, tax
bands, delivery charges, 13 departments, and for each of the 81 products its department,
subcategory and list price; no open dataset carries retail prices), the **real** product data of
`fixtures/products/fulfilment-products.json` (dimensions, weight, handling tags; see
`fixtures/products/README.md` for sources and licences) and the **real** network catalogue of
`warehouse-infra/scripts/data/retail-network-seed.json` (SKU -> the catalogue item's title).

## The register

One row per thing a storefront order needs. "Real" means taken from a real dataset or from the
running fleet; "curated" means chosen by hand for this study (and committed).

### WMS (the warehouse fleet) - prerequisites, checked but NOT re-run

| Layer | Owning context | What | API | Provisioned by | Natural key / idempotency | Real or curated |
|---|---|---|---|---|---|---|
| WMS | facility-layout | the pilot sites, zones, aisles and storage locations (bins) | `POST /sites`, `/zones`, `/aisles`, `/locations/import` | `~/warehouse-systems/provisioning` (`run_all.sh`, see its README) | site code / location id | curated layouts |
| WMS | product-master | the products (dimensions, weight, handling tags, classification) | `PUT /products/{sku}`, `PUT /products/{sku}/classification` | `scripts/08-seed-products.sh` (`make seed-products`) | SKU; idempotent PUTs | real (open product datasets, `fixtures/products/README.md`) |
| WMS | inventory-storage | usable stock of the 81 storefront SKUs | `PUT /bins/{binId}`, `POST /stock/stow` | `scripts/09-seed-inventory.sh` (`make seed-inventory`) | bin; deterministic stow `Idempotency-Key` (bin, SKU, qty) | real products, planned quantities |

The preflight reads `GET /api/inventory-storage/inventory/<sku>/usable` for every storefront SKU and
fails with the commands above when one has no usable stock. It never stocks anything: stocking is
heavy and one-way.

### WMS side of the promise - `cmd/seed-storefront -wms-only`, step 2 (re-checked in step 5)

| Layer | Owning context | What | API | Provisioned by | Natural key / idempotency | Real or curated |
|---|---|---|---|---|---|---|
| WMS | process-path-management | CPT schedule of the fulfilment site `site-1` (network-fulfillment's `SITE_ID`): timezone **America/Chicago**, cutoffs `aus-1100` **11:00** and `aus-1700` **17:00**, every day, ship method `ground`, eligible path `PICK` (the customer-order path, intake `CUSTOMER_ORDER`) | `GET /sites/{siteId}/cpt-schedule`, `PUT /sites/{siteId}/cpt-schedule` (wholesale replacement; no `Idempotency-Key`, no version) | seed-storefront, from `fixtures/storefront/fulfilment-site.json` | siteId; PUT only when the timezone or the cutoffs differ (compared order-insensitively), so a rerun sends nothing | curated ids; times and timezone = the network node profile (`warehouse-infra/scripts/data/retail-network-seed.json`, `fulfillmentNodes[site-1].profile`) |

Why it matters: network-fulfillment advertises each listing's quantity as `min(usable stock,
process-path capacity before site-1's NEXT CPT cutoff)` (`GET
/api/network-fulfillment/capability-offers`, recomputed every minute) and the network declares the
same node with the cutoffs of its profile. The two must agree: CHECK fails, before any write, when
the fixture and the network data file disagree (and `fixture_test.go` pins it in the unit tests), or
when an eligible path is not Active in process-path-management. VERIFY then reads the capability
offers of the 81 SKUs: after a PUT it waits (within `-verify-timeout`) for a recompute and prints a
WARNING, not a failure, for SKUs that stay `THROUGHPUT_CONSTRAINED 0`.

> **`make warehouse-day` overwrites site-1's CPT schedule** (`cmd/warehouse-day/sim.go`
> `publishCPTSchedule`: timezone UTC, cutoffs `sim-1200/1600/2000` a few minutes apart, the
> simulated trucks compressed onto the wall clock). After it, every capability offer is
> `THROUGHPUT_CONSTRAINED 0` and every product shows unavailable in the storefront. Re-running
> `make provision-storefront` restores the schedule (one PUT).

### Network tier (the retail network) - `warehouse-infra/scripts/seed-retail-network.py`, step 3

| Layer | Owning context | What | API | Provisioned by | Natural key / idempotency | Real or curated |
|---|---|---|---|---|---|---|
| network | vendor-connectivity | vendor `V-WS` (ack window 240 min) | `POST /vendors` | network seed | vendorId | curated |
| network | node-topology | region `SIM-SC` | `POST /regions` | network seed | region code (409 region-code-taken = exists) | curated |
| network | node-topology | delivery station `DS-SIM-1`, postal 78744 (Austin, TX), activated | `POST /nodes`, `POST /nodes/{id}:activate` | network seed | type + region + postal code | curated, real geography |
| network | node-topology | service areas `786`, `787` -> DS-SIM-1 | `PUT /service-areas/{prefix}` | network seed | postal prefix (another station: reported, `--reassign`) | real Austin-area ZIP prefixes |
| network | vendor-connectivity + node-topology | fulfillment node `site-1` (vendorNodeCode = network-fulfillment's `SITE_ID`), postal 78701, activated, + operational profile rev 1 | `POST /vendor/nodes`, `PUT /vendor/nodes/{id}/profile` | network seed | vendorRef (V-WS, site-1), adopted if it exists; profile only if the revision is lower | curated |
| network | node-topology | lane `site-1 -> DS-SIM-1` (GROUND, 120 min) | `POST /lanes` | network seed | origin + destination (409 duplicate-lane = exists) | curated |
| network | linehaul-transportation | pickup departures of site-1 | none (generated) | linehaul-transportation itself, once the node is ACTIVE with a profile and a lane | n/a | generated |
| network | last-mile-delivery | 14 days of delivery waves at DS-SIM-1 | `POST /waves` | network seed | station + date + induct cutoff; **dated**: every run extends the horizon | curated schedule |
| network | offer-availability | 86 catalogue items (81 storefront + 5 legacy whose titles start with their SKU; the storefront never touches the legacy ones) | `POST /catalog-items` | network seed | **title** (the network generates the id) | real products, shopper-facing titles |
| network | offer-availability | one LIVE listing per item at site-1 | `POST /listings` | network seed | item + node (409 duplicate-listing = exists) | n/a |
| network | vendor-connectivity | inventory feed of site-1: seeded once, then fed LIVE by network-fulfillment's ACL from WMS usable stock (every 5 min, ADR 0027) | `POST /vendor/inventory` | network seed (first feed) + network-fulfillment (continuous) | a listing that already carries a feed is not overwritten (unless `--restock`) | real WMS stock |
| network | network-fulfillment (helm values) | product dictionary: listing id -> SKU, `productTranslation` of `helm-values/network-fulfillment.yaml` | none (GitOps values) | a person: `make seed-network-translation` in warehouse-infra, values PR, ArgoCD | listing id; checked by step 4 | generated from the network |
| network | delivery-promise | promise policy | read only | nothing (built-in default v0) | n/a | code default |

### Storefront tier - `cmd/seed-storefront`, step 5

| Layer | Owning context | What | API | Provisioned by | Natural key / idempotency | Real or curated |
|---|---|---|---|---|---|---|
| storefront | pricing (helm values) | configured currency **USD** (`PRICING_CURRENCY`) | none | `warehouse-infra/helm-values/pricing.yaml` (`config.currency`) | n/a | curated (US network) |
| storefront | pricing | tax table v1: postal prefixes 786-787 at **8.25 %** (825 basis points) | `GET /tax-tables/current`, `POST /tax-tables` | seed-storefront | compared as a set of bands; published only when different, with version = current + 1 | real combined Texas rate (6.25 % state + 2 % local) |
| storefront | pricing | delivery charge schedule v1: SAME_DAY 14.99, NEXT_DAY 12.99, TWO_DAY 7.99, STANDARD 5.99 (free over 35.00), USD | `GET /delivery-charge-schedules/current`, `POST /delivery-charge-schedules` | seed-storefront | compared as a set keyed by tier; version = current + 1 | curated |
| storefront | product-content | category tree: 13 departments (top level) > 51 subcategories, published on every create (`Jewelry` already existed and is adopted) | `GET /categories`, `POST /categories` | seed-storefront | **path** (`Department/Subcategory`, compared case-insensitively like the service) | curated |
| storefront | product-content | content of the 81 items: description, bullets, attributes, one PRIMARY department image, primary category; PUBLISHED | `POST /product-contents`, `:revise`, `:publish` | seed-storefront | catalogItemId; revised only when the body differs, published only when the current revision is not; RETIRED is reported and left alone | derived from real data only (title, real size and weight, handling tags, department) |
| storefront | storefront-web | department images `<media-base>/<department-slug>.svg` (the CSP allows same-host images only) | static | storefront-web (`public/media/departments`) | slug | curated |
| storefront | pricing | list price of the 81 items, USD | `GET /item-prices/{id}`, `POST /item-prices`, `:set-list-price` | seed-storefront | catalogItemId; created when absent, changed when different (with the version guard); WITHDRAWN is reported and left alone | curated prices |
| storefront | product-discovery | the ProductView projection (`purchasable` = item ACTIVE + content PUBLISHED + price ACTIVE + in stock) | `GET /products/{id}`, `GET /products?q=` | nothing: event-driven from the four contexts above; VERIFY polls it | n/a | derived |

Every storefront write carries a deterministic `Idempotency-Key`: `seed-sf:` + the first 40 hex
digits of sha256(operation, path, canonical body); `:publish` and `:revise` also hash the revision
they apply to, so a later change never replays an older stored answer. A retry after a lost
response replays instead of writing twice.

### Shopper side - NOT seeded

| Layer | Owning context | What | API | Provisioned by | Natural key / idempotency | Real or curated |
|---|---|---|---|---|---|---|
| shopper | customer-ordering | the shopper (e-mail) and a saved address in 786xx/787xx | BFF `POST /shoppers`, `/sessions`, `/me/addresses` | the shopper, in the UI | e-mail | entered by hand |
| shopper | payment-provider-simulator (via customer-payments) | payment token scenario: `tok_approve` (paid), `tok_decline` (declined), `tok_timeout` (provider timeout); built into the simulator | BFF `POST /checkout/{id}:confirm` (`paymentMethodToken`) | the shopper, in the UI | n/a | simulator scenarios |

## Run order and reruns

1. Cluster up (warehouse-infra), WMS prerequisites done once (provisioning, `make seed-products`,
   `make seed-inventory`).
2. `make provision-storefront-dry-run`, read the plan.
3. `make provision-storefront`.

**What a rerun does.** The network seed answers `0 created, 0 updated` except for the delivery
waves: they are dated, so every run on a later day adds the days that entered the 14-day horizon
(that is why the orchestrator runs it every time). The storefront tier reads everything first and
plans only differences; against an unchanged cluster it prints `writes planned 0` and
`apply: 0 writes`, then verifies. An interrupted run (Ctrl-C, a lost connection) is resumed by
running it again: it plans what is left. A changed fixture price becomes one `:set-list-price`; a
changed derivation rule becomes `:revise` + `:publish` of the affected contents; a changed tax table
or charge schedule is published as the next version; site-1's CPT schedule rewritten by `make
warehouse-day` becomes one `PUT /sites/site-1/cpt-schedule`.

**Live evidence** (2026-10-29): first run `apply: 306 writes` (63 categories, 81 prices, 81 contents
created and 81 published; tax table and charge schedule v1 already equal, skipped), 81/81
purchasable; second run `writes planned 0, writes sent 0`, verify PASS.

**Live evidence, WMS step** (2026-10-30, after `make warehouse-day` had left UTC `sim-*` cutoffs):
first run, step 2 `writes planned 1, writes sent 1` (`PUT /sites/site-1/cpt-schedule`, America/Chicago
11:00/17:00); second run, step 2 `matches, skip`, `writes sent 0`; the storefront tier
(`STOREFRONT_SKIP_NETWORK=1`) `writes planned 0` with the schedule `matches, skip`. Both runs then
stopped at step 3 (`listings of site-1 LIVE with stock` FAIL): after two recomputes with the correct
schedule all 81 capability offers stayed `THROUGHPUT_CONSTRAINED 0`, the pre-cutoff gap of
network-fulfillment's next-feasible-cutoff defect (fixed in network-fulfillment, separate PR), so the
storefront showed 0/81 purchasable.

## Deliverable postal codes

**786xx and 787xx** (Austin, Texas area), e.g. **78701**. The network's destination key is the
FIRST 3 characters of the postal code (customer-ordering `shipto.PrefixLength = 3`): the product
page asks for a prefix (`787`), a full 5-digit ZIP sent as a prefix answers `DESTINATION_NOT_SERVED`
by design. Tiers: STANDARD and TWO_DAY normally; NEXT_DAY only before the 11:00 America/Chicago
cutoff; SAME_DAY is not offered by this network (`DELIVERY_SLA_UNAVAILABLE`).

## Stock depletion

Every completed purchase consumes WMS stock (pick), network-fulfillment feeds the lower figure to
the network, and at zero the product turns `inStock: false` / `purchasable: false`; VERIFY then
fails for it and the preflight stops on its SKU. Check with
`curl localhost:8000/api/inventory-storage/inventory/<sku>/usable`. Re-running
`scripts/09-seed-inventory.sh` is not a restock: it is built to add nothing on a rerun (bins holding
their plan are skipped, stow keys are deterministic). Restock deliberately: stow fresh units into a
bin through inventory-storage (`POST /stock/stow` with a new `Idempotency-Key`) or receive them
through inbound-receiving; the ACL feeds the new figure within its interval (5 min).

## Cluster rebuild

A new kind cluster has new ids everywhere. In order:

1. warehouse-infra up; facility layouts (`provisioning`), `make seed-products`, `make seed-inventory`.
2. `make provision-storefront`: the WMS step defines site-1's CPT schedule, the network seed creates
   the network again (new listing ids), and step 4 then FAILS on purpose: the product dictionary in
   `helm-values/network-fulfillment.yaml` still names the old listing ids.
3. In warehouse-infra: `make seed-network-translation > block.yaml`, replace the `productTranslation`
   block of `helm-values/network-fulfillment.yaml`, `make seed-network-translation-check`, open the
   values PR into develop; once merged, ArgoCD rolls network-fulfillment.
4. `make provision-storefront` again: tax table and charge schedule are published as v1, the
   category tree, prices and contents are created, VERIFY passes.

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| delivery dates answer `DESTINATION_NOT_SERVED` | a full ZIP (78701) sent where a prefix is expected, or a prefix outside 786/787 | send the first 3 characters (`787`); only 786xx/787xx are served |
| `422 currency-mismatch` from pricing | pricing runs with another `PRICING_CURRENCY` than the fixture's USD | `config.currency: "USD"` in `warehouse-infra/helm-values/pricing.yaml` (merged), then rerun |
| `purchasable: false` after a run | VERIFY names the missing part: out of stock (WMS stock -> ACL feed), no ACTIVE price, no PUBLISHED content; or the projection is still catching up | stock: see Stock depletion; a WITHDRAWN price or RETIRED content is reported as BLOCKED and needs a deliberate decision (neither can be undone); otherwise rerun `make provision-storefront-verify` |
| every product shows unavailable (out of stock) although the WMS has usable stock | `GET /api/network-fulfillment/capability-offers` shows `THROUGHPUT_CONSTRAINED` with `advertisedQuantity: 0`: the ACL caps the offer at the process-path capacity before site-1's next CPT cutoff. Either site-1's CPT schedule is wrong (`GET /api/process-path-management/sites/site-1/cpt-schedule` shows UTC `sim-*` cutoffs: `make warehouse-day` ran) or the PICK path's cycle time (`cycleTimeP95`, `GET /api/process-path-management/process-paths/PICK`) does not fit before the next cutoff | `make provision-storefront` (restores the schedule; VERIFY prints the offers). If they stay `THROUGHPUT_CONSTRAINED 0` with the correct schedule, the remaining pre-cutoff gap is network-fulfillment's next-feasible-cutoff defect (separate PR, `fix/next-feasible-cutoff`); the listing feed refreshes within 5 min of a good offer |
| step 3 fails on `listings of site-1 LIVE with stock` (`availableQty ...=0`) | the same cause seen from the network: the ACL feeds the advertised quantity, 0 while the offers are `THROUGHPUT_CONSTRAINED 0` | if step 2 just redefined the schedule (`apply: wms: CPT schedule of site-1 defined`), the ACL feeds the new figure within its interval (5 min): rerun. If step 2 reported the schedule as matching and the offers stay constrained, see the row above |
| step 4 fails: `wrong` / `missing` mappings | stale product dictionary after a reseed: orders never reach the warehouse | Cluster rebuild, step 3 |
| CHECK fails: `no catalogue item titled ...` | the network seed did not run, or warehouse-infra's data file is older than the fixture | update warehouse-infra (`git pull`), run `make provision-storefront` (it runs the network seed first) |
| product page without a picture | `storefront-web` does not serve `/media/departments/<slug>.svg` yet (VERIFY prints a WARNING) | ship the department images in storefront-web; the content needs no change |
