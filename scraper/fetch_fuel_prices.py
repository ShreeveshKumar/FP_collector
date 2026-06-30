"""
fetch_fuel_prices.py
--------------------
Triggered on every push via GitHub Actions.

Logic:
  1. Load the last-fetch timestamp from the database (if it exists).
  2. If today's date is present AND the last fetch was < 6 hours ago → skip.
  3. Otherwise pull from two upstream sources (all endpoints/credentials are
     configured via environment variables — nothing is hard-coded):
       - Source A — national + all-region averages (HTML)
       - Source B — official weekly retail price series (JSON API)
  4. Guarantee full coverage of all 56 US regions (50 states + DC + the five
     inhabited territories/islands): any region the source omits is synthesised
     and any missing grade is backfilled from the national average — every
     substitution is flagged (backfilled / region_synthesized) so it stays
     auditable.
  5. Persist results to MongoDB (one document per day, plus a rolling
     "latest" pointer and the last-fetch marker).
"""

import os
import sys
from datetime import datetime, timezone
from urllib.parse import quote_plus

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from pymongo import MongoClient

# Load environment variables from a local .env file (if present).
load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────

INTERVAL_HOURS = 6


def _require_env(name: str) -> str:
    """Fetch a required environment variable or fail loudly."""
    val = os.getenv(name)
    if not val:
        sys.exit(
            f"[config] Missing required environment variable '{name}'. "
            "Copy .env.example to .env and fill it in."
        )
    return val


# All external endpoints/secrets are loaded from the environment so that the
# relevant terms are never exposed in source. See .env / .env.example.
SRC_A_PRIMARY_URL = _require_env("SRC_A_PRIMARY_URL")
SRC_A_DETAIL_URL  = _require_env("SRC_A_DETAIL_URL")

SRC_B_API_KEY      = os.getenv("SRC_B_API_KEY", "DEMO_KEY")
SRC_B_BASE_URL     = _require_env("SRC_B_BASE_URL")
SRC_B_SERIES_1_URL = _require_env("SRC_B_SERIES_1_URL")
SRC_B_SERIES_2_URL = _require_env("SRC_B_SERIES_2_URL")

def _opt_env(name: str) -> str | None:
    """Optional environment variable (returns None if unset/empty)."""
    return os.getenv(name) or None


# Request headers — User-Agent is required; the rest are optional (randomly
# generated values live in .env) and only added when present.
HEADERS = {
    "User-Agent": _require_env("HTTP_USER_AGENT"),
    **{
        header: value
        for header, env in {
            "Accept":          "HTTP_ACCEPT",
            "Accept-Language": "HTTP_ACCEPT_LANGUAGE",
            "X-Request-Id":    "HTTP_REQUEST_ID",
            "X-Trace-Id":      "HTTP_TRACE_ID",
            "X-Session-Token": "HTTP_SESSION_TOKEN",
            "X-Client-Id":     "HTTP_CLIENT_ID",
            "X-Nonce":         "HTTP_NONCE",
        }.items()
        if (value := _opt_env(env))
    },
}

# ── Database ────────────────────────────────────────────────────────────────────

MONGO_DB           = _require_env("MONGO_DB")
MONGO_PRICES_COLL  = _require_env("MONGO_PRICES_COLLECTION")
MONGO_META_COLL    = _require_env("MONGO_META_COLLECTION")
_META_ID           = "last_fetch"


def get_db():
    """
    Build a Mongo connection from credentials held only in the environment.

    Prefer a full MONGO_URI when provided; otherwise assemble one from the
    individual MONGO_USERNAME / MONGO_PASSWORD / MONGO_HOST components. The
    components are only required when MONGO_URI is absent.
    """
    uri = _opt_env("MONGO_URI")
    if not uri:
        user = quote_plus(_require_env("MONGO_USERNAME"))
        pwd  = quote_plus(_require_env("MONGO_PASSWORD"))
        host = _require_env("MONGO_HOST")
        uri = f"mongodb+srv://{user}:{pwd}@{host}/?retryWrites=true&w=majority"
    return MongoClient(uri, serverSelectionTimeoutMS=15000)[MONGO_DB]


def ensure_indexes(db):
    """
    Indexes that make the observations collection efficient to slice for
    analytical / research queries (by day, source, region, grade, period).
    """
    coll = db[MONGO_PRICES_COLL]
    # One observation is uniquely identified by this tuple — keeps re-runs
    # idempotent and prevents duplicate rows for the same day.
    coll.create_index(
        [("date", 1), ("source", 1), ("scope", 1), ("region", 1),
         ("grade", 1), ("series", 1)],
        unique=True,
        name="uniq_observation",
    )
    coll.create_index([("date", 1)], name="by_date")
    coll.create_index([("source", 1), ("grade", 1)], name="by_source_grade")
    coll.create_index([("region", 1), ("grade", 1)], name="by_region_grade")


# ── Helpers ───────────────────────────────────────────────────────────────────


def now_utc() -> datetime:
    return datetime.now(tz=timezone.utc)


def load_last_fetch(db) -> dict:
    return db[MONGO_META_COLL].find_one({"_id": _META_ID}) or {}


def should_skip(last: dict) -> bool:
    """Return True if today's data was already fetched within INTERVAL_HOURS."""
    today = now_utc().strftime("%Y-%m-%d")
    if last.get("date") != today:
        return False                           # different day → always fetch
    last_ts = datetime.fromisoformat(last["timestamp"])
    elapsed_hours = (now_utc() - last_ts).total_seconds() / 3600
    return elapsed_hours < INTERVAL_HOURS


def save_last_fetch(db, date: str):
    db[MONGO_META_COLL].update_one(
        {"_id": _META_ID},
        {"$set": {"date": date, "timestamp": now_utc().isoformat()}},
        upsert=True,
    )


# ── Source A scraper ────────────────────────────────────────────────────────────


def scrape_src_a_national() -> dict:
    """Scrape the national average banner from source A."""
    resp = requests.get(SRC_A_PRIMARY_URL, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    # Prices render in elements with class "price-index__price" or inside a
    # <span class="numb"> — both are tried for resilience.
    prices = {}
    labels_map = {
        "regular":   ["regular", "reg"],
        "mid_grade": ["mid-grade", "midgrade", "mid grade"],
        "premium":   ["premium", "prem"],
        "diesel":    ["diesel"],
    }

    # Strategy A — look for labelled price cards
    cards = soup.select(".price-index__card, .card-gas")
    for card in cards:
        label_el = card.select_one(".card-title, .type, h3, h4, .label")
        price_el  = card.select_one(".price, .numb, [class*='price']")
        if not (label_el and price_el):
            continue
        label = label_el.get_text(strip=True).lower()
        price = price_el.get_text(strip=True).replace("$", "").strip()
        for key, aliases in labels_map.items():
            if any(a in label for a in aliases):
                try:
                    prices[key] = float(price)
                except ValueError:
                    pass

    # Strategy B — fallback: first .numb span is the national regular average
    if not prices:
        spans = soup.select(".numb")
        if spans:
            try:
                prices["regular"] = float(spans[0].get_text(strip=True))
            except ValueError:
                pass

    return prices


def scrape_src_a_regions() -> list[dict]:
    """Scrape per-region averages table from source A."""
    resp = requests.get(SRC_A_DETAIL_URL, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    rows = []
    table = soup.select_one("table")
    if not table:
        return rows

    headers_row = [th.get_text(strip=True).lower() for th in table.select("thead th")]
    for tr in table.select("tbody tr"):
        cells = [td.get_text(strip=True) for td in tr.select("td")]
        if not cells:
            continue
        row = dict(zip(headers_row, cells))
        # Normalise common column names
        entry = {
            "region":    row.get("state", cells[0] if cells else ""),
            "regular":   _safe_float(row.get("regular", row.get("current", ""))),
            "mid_grade": _safe_float(row.get("mid-grade", row.get("mid grade", ""))),
            "premium":   _safe_float(row.get("premium", "")),
            "diesel":    _safe_float(row.get("diesel", "")),
        }
        rows.append(entry)

    return rows


def _safe_float(val: str) -> float | None:
    try:
        return float(str(val).replace("$", "").strip())
    except (ValueError, TypeError):
        return None


PRICE_FIELDS = ("regular", "mid_grade", "premium", "diesel")

# Canonical roster — every US region we must cover: 50 states, the federal
# district, and the five inhabited territories/islands. Any region the source
# omits is synthesised and backfilled from the national average so coverage is
# always complete and auditable.
US_REGIONS = (
    "Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado",
    "Connecticut", "Delaware", "Florida", "Georgia", "Hawaii", "Idaho",
    "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana", "Maine",
    "Maryland", "Massachusetts", "Michigan", "Minnesota", "Mississippi",
    "Missouri", "Montana", "Nebraska", "Nevada", "New Hampshire", "New Jersey",
    "New Mexico", "New York", "North Carolina", "North Dakota", "Ohio",
    "Oklahoma", "Oregon", "Pennsylvania", "Rhode Island", "South Carolina",
    "South Dakota", "Tennessee", "Texas", "Utah", "Vermont", "Virginia",
    "Washington", "West Virginia", "Wisconsin", "Wyoming",
    # Federal district
    "District of Columbia",
    # Inhabited US territories / islands
    "Puerto Rico", "Guam", "U.S. Virgin Islands", "American Samoa",
    "Northern Mariana Islands",
)


def _norm_region(name: str) -> str:
    """Loose key for matching region names across spelling/punctuation."""
    return " ".join(str(name).strip().lower().replace(".", "").replace("'", "").split())


# Map any recognised spelling/alias back to its canonical display name.
CANON_BY_KEY = {_norm_region(name): name for name in US_REGIONS}
CANON_BY_KEY.update({
    _norm_region(alias): canonical
    for alias, canonical in {
        "washington dc":      "District of Columbia",
        "washington d c":     "District of Columbia",
        "dc":                 "District of Columbia",
        "virgin islands":     "U.S. Virgin Islands",
        "us virgin islands":  "U.S. Virgin Islands",
        "northern marianas":  "Northern Mariana Islands",
        "cnmi":               "Northern Mariana Islands",
    }.items()
})


def ensure_full_coverage(
    regions: list[dict],
    national: dict,
) -> tuple[list[dict], list[str]]:
    """
    Guarantee every entry in US_REGIONS is present.

    Existing rows get their display name canonicalised; any roster region the
    source omitted is appended with empty prices (later filled from the national
    average) and flagged ``_synthesized`` so it stays auditable. Returns the
    region list plus the names of every region that had to be synthesised.
    """
    by_key: dict[str, dict] = {}
    for r in regions:
        key = _norm_region(r.get("region", ""))
        canonical = CANON_BY_KEY.get(key)
        if canonical:
            r["region"] = canonical          # normalise display name
            key = _norm_region(canonical)    # …and dedup under the canonical key
        by_key[key] = r

    synthesized: list[str] = []
    for canonical in US_REGIONS:
        key = _norm_region(canonical)
        if key not in by_key:
            entry = {
                "region": canonical,
                "regular": None, "mid_grade": None,
                "premium": None, "diesel": None,
                "_synthesized": True,
            }
            regions.append(entry)
            by_key[key] = entry
            synthesized.append(canonical)

    return regions, synthesized


def fill_missing_with_national(
    regions: list[dict],
    national: dict,
) -> tuple[list[dict], list[dict]]:
    """
    For every region entry, replace any None price field with the national
    average for that grade.  Returns:
      - patched region list
      - list of patch-log dicts describing every substitution made
    """
    patches = []

    for region in regions:
        region_name = region.get("region", "?")
        for field in PRICE_FIELDS:
            if region.get(field) is None:
                national_val = national.get(field)
                if national_val is not None:
                    region[field] = national_val
                    region.setdefault("_patched_fields", []).append(field)
                    patches.append(
                        {
                            "region": region_name,
                            "field": field,
                            "filled_with": national_val,
                            "source": "national_average",
                        }
                    )
                    print(
                        f"     [patch] {region_name}: missing '{field}' "
                        f"→ filled with national avg ${national_val:.3f}"
                    )

    return regions, patches


# ── Source B fetcher ────────────────────────────────────────────────────────────


def fetch_src_b_series(url_template: str, label: str) -> dict:
    url = url_template.format(key=SRC_B_API_KEY)
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    payload = resp.json()
    data = payload.get("response", {}).get("data", [])
    if not data:
        return {}
    latest = data[0]
    return {
        "source": "src_b",
        "series": label,
        "period": latest.get("period"),
        "price":  latest.get("value"),
        "unit":   "USD/gallon",
    }


# ── Record assembly ─────────────────────────────────────────────────────────────


UNIT = "USD/gallon"


def build_observations(
    *,
    date: str,
    fetched_at: str,
    a_national: dict,
    a_regions: list[dict],
    b_series_1: dict,
    b_series_2: dict,
) -> list[dict]:
    """
    Flatten everything into tidy, one-observation-per-document records.

    Each record is a single (date, source, scope, region/series, grade) price
    point — the canonical "long" shape for analytics and research: easy to
    filter, group, aggregate, and join without unpacking nested objects.
    """
    records: list[dict] = []

    def base(**extra) -> dict:
        return {
            "date": date,
            "fetched_at": fetched_at,
            "unit": UNIT,
            "series": None,      # present so the unique index is stable
            "region": None,
            **extra,
        }

    # Source A — national averages (one row per grade)
    for grade, price in a_national.items():
        records.append(base(
            source="src_a", scope="national", region="US",
            grade=grade, price=price, backfilled=False,
        ))

    # Source A — regional averages (one row per region per grade)
    for region in a_regions:
        region_name = region.get("region", "?")
        patched = set(region.get("_patched_fields", []))
        synthesized = bool(region.get("_synthesized"))
        for grade in PRICE_FIELDS:
            price = region.get(grade)
            if price is None:
                continue
            records.append(base(
                source="src_a", scope="regional", region=region_name,
                grade=grade, price=price,
                backfilled=grade in patched,
                backfill_source="national_average" if grade in patched else None,
                # True when the whole region was absent from the source and
                # synthesised from the national average for full coverage.
                region_synthesized=synthesized,
            ))

    # Source B — weekly national series (one row per series)
    for series in (b_series_1, b_series_2):
        if not series:
            continue
        records.append(base(
            source="src_b", scope="national", region="US",
            series=series.get("series"), grade=series.get("series"),
            price=series.get("price"), period=series.get("period"),
            backfilled=False,
        ))

    return records


def persist_observations(db, records: list[dict]):
    """Upsert each observation keyed on its identifying tuple (idempotent)."""
    coll = db[MONGO_PRICES_COLL]
    written = 0
    for rec in records:
        if rec.get("price") is None:
            continue
        key = {
            "date":   rec["date"],
            "source": rec["source"],
            "scope":  rec["scope"],
            "region": rec.get("region"),
            "grade":  rec.get("grade"),
            "series": rec.get("series"),
        }
        coll.update_one(key, {"$set": rec}, upsert=True)
        written += 1
    return written


# ── Main ──────────────────────────────────────────────────────────────────────


def main():
    today = now_utc().strftime("%Y-%m-%d")

    db = get_db()
    ensure_indexes(db)

    last = load_last_fetch(db)
    if should_skip(last):
        elapsed = (
            now_utc() - datetime.fromisoformat(last["timestamp"])
        ).total_seconds() / 3600
        print(
            f"[skip] Already fetched today ({today}). "
            f"Last fetch was {elapsed:.1f}h ago (< {INTERVAL_HOURS}h). "
            "Nothing to do."
        )
        sys.exit(0)

    print(f"[run] Fetching fuel prices for {today} …")

    # ── Source A ───────────────────────────────────────────────────────────────
    print("  → source A national average …")
    try:
        a_national = scrape_src_a_national()
        print(f"     {a_national}")
    except Exception as exc:
        print(f"     [warn] source A national failed: {exc}")
        a_national = {}

    print("  → source A regional averages …")
    try:
        a_regions = scrape_src_a_regions()
        print(f"     {len(a_regions)} regions scraped")
    except Exception as exc:
        print(f"     [warn] source A regions failed: {exc}")
        a_regions = []

    # ── Source B ───────────────────────────────────────────────────────────────
    # Fetched before backfilling so its national figures can serve as a fallback
    # when source A's national banner is unavailable.
    print("  → source B weekly series 1 …")
    try:
        b_series_1 = fetch_src_b_series(SRC_B_SERIES_1_URL, "series_1_national")
        print(f"     {b_series_1}")
    except Exception as exc:
        print(f"     [warn] source B series 1 failed: {exc}")
        b_series_1 = {}

    print("  → source B weekly series 2 …")
    try:
        b_series_2 = fetch_src_b_series(SRC_B_SERIES_2_URL, "series_2_national")
        print(f"     {b_series_2}")
    except Exception as exc:
        print(f"     [warn] source B series 2 failed: {exc}")
        b_series_2 = {}

    # ── National average used for backfill (source A, with source B fallback) ──
    effective_national = dict(a_national)
    if effective_national.get("regular") is None and b_series_1:
        effective_national["regular"] = b_series_1.get("price")
    if effective_national.get("diesel") is None and b_series_2:
        effective_national["diesel"] = b_series_2.get("price")

    # ── Guarantee every state / territory / island is present ──────────────────
    print(f"  → Ensuring full coverage of all {len(US_REGIONS)} US regions …")
    a_regions, synthesized = ensure_full_coverage(a_regions, effective_national)
    if synthesized:
        print(f"     {len(synthesized)} region(s) missing from source, "
              f"synthesised from national avg: {', '.join(synthesized)}")
    else:
        print("     (source already covered every region)")

    # ── Fill missing regional prices with national average ─────────────────────
    print("  → Patching missing regional prices with national average …")
    a_regions, patch_log = fill_missing_with_national(a_regions, effective_national)
    if not patch_log:
        print("     (no gaps — all regional prices present)")

    # ── Flatten into tidy records & persist ────────────────────────────────────
    records = build_observations(
        date=today,
        fetched_at=now_utc().isoformat(),
        a_national=a_national,
        a_regions=a_regions,
        b_series_1=b_series_1,
        b_series_2=b_series_2,
    )
    written = persist_observations(db, records)

    save_last_fetch(db, today)

    print(
        f"\n[done] Upserted {written} observation record(s) → "
        f"{MONGO_DB}.{MONGO_PRICES_COLL} (date={today})"
    )


if __name__ == "__main__":
    main()
