#!/usr/bin/env python3
"""Harvest a catalogue of REAL products with fulfilment data (dimensions, weight,
handling class) from two open datasets, and write it as a fixture the
seed-products command registers in product-master.

Sources (both downloaded from the internet on demand and cached):

  abo    Berkeley "ABO" product-listing metadata, CC BY 4.0
         https://amazon-berkeley-objects.s3.amazonaws.com/archives/abo-listings.tar
         Real marketplace listings: title, brand, category path, item
         dimensions and item weight. Only third-party-brand listings are kept
         (house-brand listings are dropped, so no house brand appears here).
  wands  Wayfair WANDS product catalogue, MIT
         https://github.com/wayfair/WANDS (dataset/product.csv)
         Real home-goods and furniture products with overall width / depth /
         height and weight in their feature lists.

WHAT IS REAL AND WHAT IS DERIVED
  real     SKU (source id), name, category path, length / width / height, weight.
  derived  handling tags, temperature class and DOT hazard class. The sources
           carry no handling flags, so they are derived from the category path
           and text by the explicit rules below, and every product records the
           evidence ("evidence") that triggered each flag. Nothing is guessed
           without a recorded reason.

SANITY FILTERS (a listing is dropped, not repaired, when it fails)
  * all three dimensions >= 3 mm and <= 20 000 mm; weight 1 g .. 2 000 kg
    (product-master's published limits)
  * bounding-box density between 0.005 and 8 g/cm3 (8 = solid steel; 0.005 =
    air-filled cushions); food categories are capped at 2 g/cm3, which removes
    swapped or placeholder measurements such as a 454 g crab cake in a 5 cm cube
  * only listings with an English name; unit conversions use the sources'
    own normalized values (inches / pounds)

Usage:
    python3 scripts/harvest-fulfilment-products.py            # downloads + writes the fixture
    python3 scripts/harvest-fulfilment-products.py --abo-tar PATH --wands-csv PATH   # offline
Stdlib only.
"""
from __future__ import annotations

import argparse
import collections
import csv
import gzip
import hashlib
import html
import io
import json
import re
import sys
import tarfile
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

ABO_URL = "https://amazon-berkeley-objects.s3.amazonaws.com/archives/abo-listings.tar"
WANDS_URL = "https://raw.githubusercontent.com/wayfair/WANDS/main/dataset/product.csv"
CACHE = Path.home() / ".cache" / "warehouse-e2e" / "fulfilment"
OUT = Path(__file__).resolve().parent.parent / "fixtures" / "products" / "fulfilment-products.json"

IN_MM, LB_G = 25.4, 453.59237
MAX_DESC = 200
ABO_TARGET = 6500             # products kept from ABO, spread over its product types
WANDS_TARGET = 4600           # products kept from WANDS, spread over its product classes

# ---------------------------------------------------------------- derivation rules
# Carrier "additional handling" thresholds (UPS/FedEx): longest side > 48 in,
# second-longest > 30 in, or weight > 50 lb.
OVERSIZE_LONGEST_MM, OVERSIZE_SECOND_MM, OVERSIZE_WEIGHT_G = 1219, 762, 22_680

FROZEN_PATH = re.compile(r"/frozen\b", re.I)
CHILLED_PATH = re.compile(r"/(dairy, cheese & eggs|produce|meat & seafood|meat|seafood|deli|refrigerated)\b", re.I)
FOOD_PATH = re.compile(r"/(grocery|pantry|snack|beverages|breakfast|breads|dairy|produce|meat|seafood|deli|frozen|"
                       r"candy|baby food|cooking|canned)\b", re.I)
FOOD_TYPES = {"GROCERY", "COFFEE", "TEA", "BREAD", "SAUCE", "LEGUME", "NOODLE", "VEGETABLE", "BREAKFAST_CEREAL",
              "DAIRY_BASED_DRINK", "DAIRY_BASED_CHEESE", "DAIRY_BASED_CREAM", "JUICE_AND_JUICE_DRINK", "FRUIT",
              "SNACK_CHIP_AND_CRISP", "SNACK_MIX", "SALAD_DRESSING", "WATER", "CRACKER", "EDIBLE_OIL_VEGETABLE",
              "COOKIE", "CAKE", "MILK_SUBSTITUTE", "WINE", "PACKAGED_SOUP_AND_STEW", "FRUIT_SNACK", "SNACK_FOOD_BAR",
              "NUT_BUTTER", "POPCORN", "FISH", "VINEGAR", "POULTRY", "MEAT", "PRETZEL", "DRINK_FLAVORED", "HERB",
              "PANTRY", "SPICE", "SWEETENER", "CANDY", "CHOCOLATE", "BEVERAGE", "JAM_JELLY"}

# Fragile / high-value judgements look at the TAIL of the category path (leaf + parent) and the
# source product type only - never the whole path or the marketing bullets, which mention
# "perfume", "bleach" or "glass" in care instructions and accessory lists.
FRAGILE_TAIL = re.compile(
    r"\b(glassware|stemware|dinnerware|dishware|tableware|drinkware|vases?|figurines?|mirrors?|picture frames?|"
    r"photo frames?|wall art|candle holders?|candleholders?|lamps?|light bulbs?|chandeliers?|light fixtures?|"
    r"sconces?|pendants?|ceramics?|porcelain|televisions?|wine|beer|champagne)\b", re.I)
NOT_FRAGILE_NAME = re.compile(
    r"\b(paper|plastic|disposable|melamine|stainless|insulated|aluminum|stand|cabinet|shelf|shelves|mount|rack|"
    r"bracket|solar|string lights?|led strip|landscape|vinegar|cooking wine)\b", re.I)
HIGH_VALUE_LEAF = re.compile(
    r"^(laptops?|notebooks?|tablets?|smartphones?|cell phones?|digital cameras?|dslr cameras?|mirrorless cameras?|"
    r"televisions?|watches|wrist watches|fine jewelry.*)$", re.I)
CHILLED_NOT = re.compile(r"dried|canned|jarred|shelf[- ]stable|potatoes|onions|garlic|bananas|nuts|seeds|pickles", re.I)

# (regex over the product NAME only, DOT class, label, negative guard). First match wins.
HAZMAT_RULES = [
    (re.compile(r"\baerosol\b", re.I), 2, "aerosol (DOT class 2)", re.compile(r"non[- ]aerosol|aerosol[- ]free", re.I)),
    (re.compile(r"\b(nail polish remover|nail polish|nail lacquer|perfume|eau de (?:parfum|toilette)|cologne|"
                r"hand sanitizer|rubbing alcohol|isopropyl alcohol|lighter fluid|paint thinner|acetone)\b", re.I),
     3, "flammable liquid (DOT class 3)", re.compile(r"\b(holder|case|bag|dispenser|bottle holder|keychain|can)\b|alcohol[- ]free", re.I)),
    (re.compile(r"\blithium\b.{0,60}\b(batter(?:y|ies)|power bank|cell)\b|\b(batter(?:y|ies)|power bank)\b.{0,60}\blithium\b", re.I),
     9, "lithium battery (DOT class 9)", re.compile(r"lithium[- ]free|no lithium|charger|case", re.I)),
    (re.compile(r"\b(bleach|drain cleaner|oven cleaner)\b", re.I), 8, "corrosive (DOT class 8)",
     re.compile(r"\b(safe|free|friendly|sheet|towel|cloth|fabric)\b", re.I)),
]


def classify(name: str, segments: list, ptype: str, dims: tuple, weight_g: int, temp_path: str = ""):
    """Return (tags, temperatureClass, dotHazardClass, evidence). Every flag records its evidence."""
    tags, evidence, temp, dot = [], [], "", None
    tail = " / ".join(segments[-2:] + [ptype.replace("_", " ")])
    leaf = segments[-1].strip() if segments else ""
    longest, second = sorted(dims, reverse=True)[:2]
    if longest > OVERSIZE_LONGEST_MM or second > OVERSIZE_SECOND_MM or weight_g > OVERSIZE_WEIGHT_G:
        tags.append("Oversized")
        evidence.append(f"oversized: {dims[0]}x{dims[1]}x{dims[2]} mm / {weight_g} g exceeds carrier additional-handling thresholds "
                        f"(longest > {OVERSIZE_LONGEST_MM} mm, second > {OVERSIZE_SECOND_MM} mm or > {OVERSIZE_WEIGHT_G} g)")
    m = FRAGILE_TAIL.search(tail)
    if m and not NOT_FRAGILE_NAME.search(name):
        tags.append("Fragile")
        evidence.append(f"fragile: category tail '{m.group(0).lower()}'")
    if HIGH_VALUE_LEAF.match(leaf) or ptype.startswith("FINE"):
        tags.append("HighValue")
        evidence.append(f"high value: {'category leaf ' + repr(leaf.lower()) if HIGH_VALUE_LEAF.match(leaf) else 'product type ' + ptype.lower()}")
    for rx, cls, label, guard in HAZMAT_RULES:
        hit = rx.search(name)
        if hit and not (guard and guard.search(name)):
            tags.append("Hazmat")
            dot = cls
            evidence.append(f"hazmat: {label} - name contains '{hit.group(0).lower()[:50]}'")
            break
    if temp_path:
        if FROZEN_PATH.search(temp_path):
            temp = "Frozen"
        elif CHILLED_PATH.search(temp_path) and not CHILLED_NOT.search(temp_path):
            temp = "Chilled"
        if temp:
            tags.append("TemperatureSensitive")
            evidence.append(f"temperature: {temp} - category path {temp_path[:90]}")
    order = ["Hazmat", "Fragile", "TemperatureSensitive", "Oversized", "HighValue"]
    return sorted(set(tags), key=order.index), temp, dot, evidence


# ---------------------------------------------------------------- helpers
def clean(s: str) -> str:
    s = html.unescape(s or "")
    s = "".join(ch for ch in s if ch.isprintable() or ch.isspace())
    return re.sub(r"\s+", " ", s).strip()


def truncate(s: str, n: int = MAX_DESC) -> str:
    return s if len(s) <= n else s[:n].rstrip()


def density_ok(dims, weight_g, food: bool) -> bool:
    vol_cm3 = (dims[0] / 10) * (dims[1] / 10) * (dims[2] / 10)
    d = weight_g / vol_cm3
    return 0.005 <= d <= (2.0 if food else 8.0)


MAX_PLAUSIBLE_WEIGHT_G = 150_000     # one retail unit above 150 kg is a pallet / bulk-pack error, not a product


def in_limits(dims, weight_g) -> bool:
    """product-master's limits (1..20000 mm, 1..2000000 g) plus a plausibility cap on one unit's weight."""
    return min(dims) >= 3 and max(dims) <= 20000 and 1 <= weight_g <= MAX_PLAUSIBLE_WEIGHT_G


def fetch(url: str, dest: Path) -> Path:
    if dest.exists() and dest.stat().st_size > 0:
        return dest
    dest.parent.mkdir(parents=True, exist_ok=True)
    print(f"downloading {url}", file=sys.stderr)
    req = urllib.request.Request(url, headers={"User-Agent": "warehouse-systems-e2e/1.0 (study project)"})
    with urllib.request.urlopen(req, timeout=300) as resp, open(dest.with_suffix(dest.suffix + ".part"), "wb") as out:  # type: ignore[union-attr]
        while chunk := resp.read(1 << 20):
            out.write(chunk)
    dest.with_suffix(dest.suffix + ".part").rename(dest)
    return dest


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(1 << 20):
            h.update(chunk)
    return h.hexdigest()


def stratified(products, key, target, stats):
    """Spread `target` products over the strata (product type / class): the smallest per-stratum
    cap whose total reaches the target, taking each stratum in a stable hash order (no RNG), so the
    catalogue is varied instead of dominated by the biggest categories and re-runs are identical."""
    # Products that matter most to fulfilment (hazmat, cold chain) are always kept: the sources hold
    # few of them, so capping them would drop most of the interesting handling cases.
    always = [p for p in products if "Hazmat" in (p.get("handlingTags") or []) or p.get("temperatureClass")]
    stats["always kept (hazmat / cold chain)"] = len(always)
    keep_ids = {id(p) for p in always}
    products = [p for p in products if id(p) not in keep_ids]
    target = max(0, target - len(always))
    groups = collections.defaultdict(list)
    for p in products:
        groups[p[key]].append(p)
    ordered = {k: sorted(v, key=lambda p: hashlib.sha1(p["sku"].encode()).hexdigest()) for k, v in groups.items()}
    cap = 1
    while sum(min(len(v), cap) for v in ordered.values()) < target and cap < 100_000:
        cap += 1
    stats["strata"], stats["per-stratum cap"] = len(ordered), cap
    return always + [p for v in ordered.values() for p in v[:cap]]


# ---------------------------------------------------------------- ABO
# The retailer's own name is removed from titles and brands ("AmazonBasics Stapler" -> "Stapler",
# "Amazon Brand - Rivet Sofa" -> "Rivet Sofa"); the product itself, its sub-brand and its data are kept.
AMAZON_TOKEN = re.compile(
    r"\bamazon\s?basics\b|\bamazon\s(?:brand|essentials|elements|commercial|collection|aware|gear)\b|\bby\s+amazon\b|\bamazon\b",
    re.I)


def strip_retailer(s: str) -> str:
    s = AMAZON_TOKEN.sub(" ", s)
    s = re.sub(r"^[\s\-\u2013\u2014:,.]+|[\s\-\u2013\u2014:,]+$", "", s)
    s = re.sub(r"\(\s*\)", "", s)
    return re.sub(r"\s+", " ", s).strip()


def en(values):
    for v in values or []:
        if str(v.get("language_tag", "")).lower().startswith("en"):
            return v.get("value")
    return None


def abo_products(tar_path: Path, target: int):
    stats = collections.Counter()
    out, seen = [], {}
    with tarfile.open(tar_path) as tar:
        for member in tar.getmembers():
            if not member.name.endswith(".json.gz"):
                continue
            for line in gzip.open(io.BytesIO(tar.extractfile(member).read()), "rt", encoding="utf-8"):
                r = json.loads(line)
                stats["listings"] += 1
                name = clean(en(r.get("item_name")) or "")
                if not name:
                    stats["no english name"] += 1
                    continue
                brand = clean(en(r.get("brand")) or ((r.get("brand") or [{}])[0].get("value") or ""))
                if re.search("amazon", f"{name} {brand}", re.I):
                    stats["retailer name stripped"] += 1
                    name, brand = strip_retailer(name), strip_retailer(brand)
                    if len(name) < 8 or re.search("amazon", f"{name} {brand}", re.I):
                        stats["nothing left after stripping the retailer name"] += 1
                        continue
                try:
                    d = r["item_dimensions"]
                    for k in ("length", "width", "height"):
                        assert d[k]["normalized_value"]["unit"] == "inches"
                    dims = tuple(round(d[k]["normalized_value"]["value"] * IN_MM) for k in ("length", "width", "height"))
                    w = r["item_weight"][0]["normalized_value"]
                    assert w["unit"] == "pounds"
                    weight_g = round(w["value"] * LB_G)
                except Exception:
                    stats["no dimensions/weight"] += 1
                    continue
                if not in_limits(dims, weight_g):
                    stats["outside product-master limits"] += 1
                    continue
                ptype = ((r.get("product_type") or [{}])[0].get("value")) or ""
                path = " | ".join((n.get("node_name") or "") for n in (r.get("node") or [])[:3])
                first_path = ((r.get("node") or [{}])[0].get("node_name")) or ""
                food = ptype in FOOD_TYPES or bool(FOOD_PATH.search(first_path))
                if not density_ok(dims, weight_g, food):
                    stats["implausible density"] += 1
                    continue
                segs = [x for x in first_path.split("/") if x]
                tags, temp, dot, ev = classify(name, segs, ptype, dims, weight_g, first_path)
                desc = truncate(name if not brand or brand.lower() in name.lower() else f"{brand} {name}")
                sku = "ABO-" + r["item_id"]
                ptype_key = ptype or "UNKNOWN"
                prev = seen.get(sku)
                if prev is not None:
                    # the same item_id is listed once per marketplace: keep a single record,
                    # preferring the US listing, then the first one seen
                    stats["same item in another marketplace"] += 1
                    if r.get("country") != "US" or prev[1] == "US":
                        continue
                    out.remove(prev[0])
                out.append({
                    "sku": sku,
                    "description": desc,
                    "source": "abo",
                    "category": first_path or ptype,
                    "dimensions": {"lengthMm": dims[0], "widthMm": dims[1], "heightMm": dims[2], "weightG": weight_g},
                    "handlingTags": tags or None,
                    "temperatureClass": temp or None,
                    "dotHazardClass": dot,
                    "evidence": ev or None,
                })
                out[-1]["_stratum"] = ptype_key
                seen[sku] = (out[-1], r.get("country"))
    stats["eligible"] = len(out)
    picked = stratified(out, "_stratum", target, stats)
    for p in picked:
        del p["_stratum"]
    stats["kept"] = len(picked)
    return picked, stats


# ---------------------------------------------------------------- WANDS
def detokenize(s: str) -> str:
    s = clean(s).replace(" '' ", '" ').replace("''", '"')
    s = re.sub(r"\s+([.,;:!?)])", r"\1", s)
    s = re.sub(r"\(\s+", "(", s)
    s = re.sub(r"\s+'s\b", "'s", s)
    s = re.sub(r"\s+n't\b", "n't", s)
    s = re.sub(r"(\d)\s+\"", r'\1"', s)
    return s.strip()


def title(s: str) -> str:
    return re.sub(r"(?<![0-9])\b([a-z])([a-z']*)", lambda m: m.group(1).upper() + m.group(2), s)


def feats(raw: str):
    out = collections.defaultdict(list)
    for part in (raw or "").split("|"):
        if ":" in part:
            k, v = part.split(":", 1)
            out[k.strip().lower().replace(" ", "")].append(v.strip())
    return out


def single_number(values):
    nums = set()
    for v in values:
        m = re.fullmatch(r"\s*([0-9]+(?:\.[0-9]+)?)\s*", v)
        if not m:
            return None
        nums.add(float(m.group(1)))
    return nums.pop() if len(nums) == 1 else None


def wands_products(csv_path: Path, target: int):
    csv.field_size_limit(10 ** 9)
    stats = collections.Counter()
    pool = collections.defaultdict(list)
    with open(csv_path, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f, delimiter="\t"):
            stats["listings"] += 1
            name = title(detokenize(r["product_name"]))
            if not name:
                stats["no name"] += 1
                continue
            f_ = feats(r["product_features"])
            width = single_number(f_.get("overallwidth-sidetoside", []))
            height = single_number(f_.get("overallheight-toptobottom", []))
            weight = single_number(f_.get("overallproductweight", []))
            depth = (single_number(f_.get("overalldepth-fronttoback", []))
                     or single_number(f_.get("overalllength-headtotoe", []))
                     or single_number(f_.get("overalllength-endtoend", [])))
            if None in (width, height, weight, depth):
                stats["no dimensions/weight"] += 1
                continue
            dims = (round(depth * IN_MM), round(width * IN_MM), round(height * IN_MM))
            weight_g = round(weight * LB_G)
            if not in_limits(dims, weight_g):
                stats["outside product-master limits"] += 1
                continue
            if not density_ok(dims, weight_g, False):
                stats["implausible density"] += 1
                continue
            pclass = (r["product_class"] or "").split("|")[0].strip() or "Unclassified"
            path = r["category hierarchy"] or ""
            segs = [x.strip() for x in path.split("/") if x.strip()] + [pclass]
            tags, temp, dot, ev = classify(name, segs, "", dims, weight_g)
            pool[pclass].append({
                "sku": "WANDS-" + r["product_id"],
                "description": truncate(name),
                "source": "wands",
                "category": path or pclass,
                "dimensions": {"lengthMm": dims[0], "widthMm": dims[1], "heightMm": dims[2], "weightG": weight_g},
                "handlingTags": tags or None,
                "temperatureClass": temp or None,
                "dotHazardClass": dot,
                "evidence": ev or None,
            })
    flat = [dict(p, _stratum=c) for c, ps in pool.items() for p in ps]
    stats["eligible"] = len(flat)
    picked = stratified(flat, "_stratum", target, stats)
    for p in picked:
        del p["_stratum"]
    stats["kept"] = len(picked)
    return picked, stats


# ---------------------------------------------------------------- main
def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--abo-tar", type=Path, help="local abo-listings.tar (default: download)")
    ap.add_argument("--wands-csv", type=Path, help="local WANDS product.csv (default: download)")
    ap.add_argument("--abo-target", type=int, default=ABO_TARGET)
    ap.add_argument("--wands-target", type=int, default=WANDS_TARGET)
    ap.add_argument("--out", type=Path, default=OUT)
    args = ap.parse_args()

    abo_tar = args.abo_tar or fetch(ABO_URL, CACHE / "abo-listings.tar")
    wands_csv = args.wands_csv or fetch(WANDS_URL, CACHE / "wands-product.csv")

    abo, abo_stats = abo_products(abo_tar, args.abo_target)
    wands, wands_stats = wands_products(wands_csv, args.wands_target)
    products = sorted(abo + wands, key=lambda p: p["sku"])
    assert len({p["sku"] for p in products}) == len(products), "duplicate SKUs"
    for p in products:
        for k in [k for k, v in p.items() if v is None]:
            del p[k]

    fixture = {
        "source": "Berkeley ABO product listings (abo-listings.tar) - " + ABO_URL + "; Wayfair WANDS product catalogue "
                  "(dataset/product.csv) - " + WANDS_URL,
        "license": "ABO: Creative Commons Attribution 4.0 (CC BY 4.0) - credit: Amazon.com (data); Guillaumin, Dideriksen, "
                   "Deng, Arora, Collins, Malik (dataset). Records were filtered and converted to millimetres and grams, "
                   "and handling classes were derived (changes indicated). WANDS: MIT License, Copyright (c) 2021 ecir2022.",
        "fetchedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "sourceChecksums": {"abo-listings.tar": sha256(abo_tar), "wands-product.csv": sha256(wands_csv)},
        "derivation": "handlingTags, temperatureClass and dotHazardClass are derived from category + text by "
                      "scripts/harvest-fulfilment-products.py; each product lists its evidence.",
        "products": products,
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(fixture, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")

    tag_counts = collections.Counter(t for p in products for t in p.get("handlingTags", []))
    print(f"\nABO   : {dict(abo_stats)}")
    print(f"WANDS : {dict(wands_stats)}")
    print(f"wrote {args.out}: {len(products)} products ({len(abo)} abo, {len(wands)} wands)")
    print("handling tags:", dict(tag_counts))
    print("temperature  :", dict(collections.Counter(p.get("temperatureClass", "-") for p in products)))
    print("DOT classes  :", dict(collections.Counter(p.get("dotHazardClass") for p in products if p.get("dotHazardClass"))))


if __name__ == "__main__":
    main()
