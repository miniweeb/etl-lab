"""ETL Lab fake-data generator.

Produces the raw zone described in SPEC.md: one CSV per day of e-commerce
orders (with realistic dirt, late corrections, schema eras) plus dimension
tables and per-file JSON manifests.

Deterministic: the file for a given date is identical every time you generate
it (seeded per date), so regeneration is always safe — the generator itself is
an example of idempotent design.

Usage:
  python datagen/generate.py --scale small --month 1
  python datagen/generate.py --scale full  --month 1 --workers 4
  python datagen/generate.py --scale full  --month 2          # assignment A10
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import date, timedelta
from multiprocessing import Pool
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.csv as pacsv

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from datagen import profiles as P  # noqa: E402
from lib.labpaths import data_root  # noqa: E402


# --------------------------------------------------------------------------- #
# shared deterministic tables
# --------------------------------------------------------------------------- #
def product_price_cents(scale: str) -> np.ndarray:
    """Price per product index, in cents. Same in every process (fixed seed)."""
    rng = np.random.default_rng(P.DIM_SEED)
    prices = np.clip(np.exp(rng.normal(3.0, 0.8, P.N_PRODUCTS[scale])), 1.0, 3000.0)
    return np.round(prices * 100).astype(np.int64)


def _fmt_cents(cents: np.ndarray) -> np.ndarray:
    """int64 cents -> '12.34' as a numpy U array (exact, no float wobble)."""
    sign = np.where(cents < 0, "-", "")
    c = np.abs(cents)
    return np.char.add(
        np.char.add(np.char.add(sign, np.char.mod("%d", c // 100)), "."),
        np.char.mod("%02d", c % 100),
    ).astype("U20")


def _ts_strings(dt: np.ndarray) -> np.ndarray:
    """datetime64[s] -> 'YYYY-MM-DD HH:MM:SS' (U19)."""
    return np.char.replace(np.datetime_as_string(dt, unit="s"), "T", " ")


def _euro_fmt(cents: int) -> str:
    """European locale: decimal comma, '.' thousands sep ('93,32', '1.234,56')."""
    whole, frac = divmod(cents, 100)
    return f"{whole:,}".replace(",", ".") + f",{frac:02d}"


# --------------------------------------------------------------------------- #
# orders: one day
# --------------------------------------------------------------------------- #
def gen_day(d: date, scale: str, root: Path, force: bool = False) -> dict:
    era = P.era_for(d)
    out = root / "raw" / "orders" / f"orders_{d.isoformat()}.csv"
    man_path = root / "raw" / "manifest" / f"orders_{d.isoformat()}.json"
    if out.exists() and man_path.exists() and not force:
        return {"date": d.isoformat(), "skipped": True}

    t0 = time.time()
    rng = np.random.default_rng(P.seed_for(d))
    n_total = P.rows_for(d, scale)
    n_late = P.late_for(d, scale)
    n_dup = P.dup_for(d, scale)
    n_base = n_total - n_late - n_dup
    n_bl = n_base + n_late  # base + late (dup rows are copies of base rows)

    ncust = P.N_CUSTOMERS[scale]
    nprod = P.N_PRODUCTS[scale]
    prices = product_price_cents(scale)

    # ---- base + late: order-level numerics -------------------------------- #
    order_num = np.empty(n_bl, dtype=np.int64)
    order_num[:n_base] = P.id_base(d) + np.arange(n_base)

    day0 = np.datetime64(d.isoformat()).astype("datetime64[s]")
    order_dt = np.empty(n_bl, dtype="datetime64[s]")
    order_dt[:n_base] = day0 + rng.integers(0, 86_400, n_base).astype("timedelta64[s]")
    updated_dt = np.empty(n_bl, dtype="datetime64[s]")
    # Known quirk (documented in SPEC §3.4): the flat 0-48h draw lets updated_at
    # overshoot the file's own D+1 02:00 delivery deadline by up to ~46h.
    # Do not clamp: that would regenerate every file and invalidate all
    # doc-quoted, data-dependent numbers.
    updated_dt[:n_base] = order_dt[:n_base] + rng.integers(
        0, 2 * 86_400, n_base
    ).astype("timedelta64[s]")

    status = rng.choice(P.STATUSES, n_bl, p=P.STATUS_P).astype("U13")
    if n_late:
        # corrections reference real order_ids from prior days, weighted recent
        lo = max(P.MONTH1_START, d - timedelta(days=P.LATE_WINDOW_DAYS))
        cands = []
        cur = lo
        while cur < d:
            cands.append(cur)
            cur += timedelta(days=1)
        w = np.arange(1, len(cands) + 1, dtype=np.float64)
        t_idx = rng.choice(len(cands), n_late, p=w / w.sum())
        c_base = np.array([P.id_base(c) for c in cands], dtype=np.int64)
        c_rows = np.array([P.base_rows_for(c, scale) for c in cands], dtype=np.int64)
        c_day0 = np.array(
            [np.datetime64(c.isoformat()) for c in cands], dtype="datetime64[s]"
        )
        order_num[n_base:] = c_base[t_idx] + (
            rng.random(n_late) * c_rows[t_idx]
        ).astype(np.int64)
        order_dt[n_base:] = c_day0[t_idx] + rng.integers(
            0, 86_400, n_late
        ).astype("timedelta64[s]")
        updated_dt[n_base:] = day0 + rng.integers(0, 86_400, n_late).astype(
            "timedelta64[s]"
        )
        status[n_base:] = rng.choice(
            ["delivered", "cancelled", "refunded"], n_late, p=[0.6, 0.2, 0.2]
        )

    cust = 1 + (ncust * rng.power(3, n_bl)).astype(np.int64)
    store = 1 + (P.N_STORES * rng.power(2, n_bl)).astype(np.int64)
    payment = rng.choice(P.PAYMENTS, n_bl, p=P.PAYMENT_P).astype("U16")

    # ---- items (item-level, then folded per order) ------------------------ #
    n_items = rng.choice(np.arange(1, 7), n_bl, p=P.N_ITEMS_P)
    tot_items = int(n_items.sum())
    offsets = np.concatenate([[0], np.cumsum(n_items)])
    sku_idx = (nprod * rng.power(5, tot_items)).astype(np.int64)
    orphan = rng.random(tot_items) < P.SKU_ORPHAN_FRAC
    sku_idx[orphan] = nprod + rng.integers(0, max(nprod // 20, 10), int(orphan.sum()))
    qty = rng.choice(np.arange(1, 6), tot_items, p=P.QTY_P)
    price_c = np.where(
        sku_idx < nprod,
        prices[np.minimum(sku_idx, nprod - 1)],
        rng.integers(500, 20_000, tot_items),
    ).astype(np.int64)

    # currency scaling (era v2+): VND rows have all amounts x25000
    if era in ("v2", "v3"):
        currency = rng.choice(P.CURRENCIES, n_bl, p=P.CURRENCY_P).astype("U3")
        row_factor = np.where(currency == "VND", P.VND_FACTOR, 1).astype(np.int64)
        price_c *= np.repeat(row_factor, n_items)
    else:
        currency = None

    line_c = qty * price_c
    items_total_c = np.add.reduceat(line_c, offsets[:-1])

    if era in ("v2", "v3"):
        disc_mask = rng.random(n_bl) < P.DISCOUNT_NONZERO
        discount_c = np.where(
            disc_mask, (rng.random(n_bl) * 0.30 * items_total_c), 0
        ).astype(np.int64)
        total_c = items_total_c - discount_c
    else:
        discount_c = None
        total_c = items_total_c.copy()

    # ---- items JSON strings ----------------------------------------------- #
    strtype_row = rng.random(n_bl) < P.ITEMS_STRTYPE  # qty/price as JSON strings
    v3_items = era == "v3"
    if v3_items:
        disc_item = np.where(
            rng.random(tot_items) < 0.30, np.round(rng.random(tot_items) * 0.3, 2), 0.0
        )
        disc_l = disc_item.tolist()
    sku_l = np.char.mod("SKU-%06d", sku_idx).tolist()
    qty_l = qty.tolist()
    wh_l = (price_c // 100).tolist()
    fr_l = (price_c % 100).tolist()
    strtype_item = np.repeat(strtype_row, n_items).tolist()

    item_strs: list = [None] * tot_items
    for j in range(tot_items):
        p_s = f"{wh_l[j]}.{fr_l[j]:02d}"
        if strtype_item[j]:
            core = f'{{"sku":"{sku_l[j]}","qty":"{qty_l[j]}","unit_price":"{p_s}"'
        else:
            core = f'{{"sku":"{sku_l[j]}","qty":{qty_l[j]},"unit_price":{p_s}'
        if v3_items:
            core += f',"disc":{disc_l[j]:.2f}'
        item_strs[j] = core + "}"

    off_l = offsets.tolist()
    items_json = np.empty(n_bl, dtype=object)
    for i in range(n_bl):
        items_json[i] = "[" + ",".join(item_strs[off_l[i]: off_l[i + 1]]) + "]"

    # ---- meta JSON --------------------------------------------------------- #
    dev_l = rng.choice(P.DEVICES, n_bl, p=P.DEVICE_P).tolist()
    # three flavors of "no utm": string 'none', JSON null, key absent
    utm_l = rng.choice(
        ["google", "facebook", "tiktok", "email", "none", "\0null", "\0absent"],
        n_bl,
        p=[0.30, 0.20, 0.12, 0.08, 0.10, 0.10, 0.10],
    ).tolist()
    sess_l = np.char.mod("%08x", rng.integers(0, 2**32, n_bl)).tolist()
    meta = np.empty(n_bl, dtype=object)
    for i in range(n_bl):
        u = utm_l[i]
        if u == "\0null":
            up = '"utm":null,'
        elif u == "\0absent":
            up = ""
        else:
            up = f'"utm":"{u}",'
        meta[i] = f'{{"device":"{dev_l[i]}",{up}"sess":"{sess_l[i]}"}}'

    if era in ("v2", "v3"):
        channel = rng.choice(P.CHANNELS, n_bl, p=P.CHANNEL_P).astype("U11")
    if era == "v3":
        loyalty = rng.choice(P.LOYALTY, n_bl, p=P.LOYALTY_P).astype("U8")

    # ---- duplicates: copies of base rows, later update, advanced status ---- #
    dup_src = rng.choice(n_base, n_dup, replace=False)

    def _dup(a: np.ndarray) -> np.ndarray:
        return np.concatenate([a, a[dup_src]])

    order_num = _dup(order_num)
    cust = _dup(cust)
    store = _dup(store)
    order_dt = _dup(order_dt)
    upd_extra = updated_dt[dup_src] + rng.integers(60, 36_000, n_dup).astype(
        "timedelta64[s]"
    )
    updated_dt = np.concatenate([updated_dt, upd_extra])
    status = np.concatenate(
        [status, rng.choice(["shipped", "delivered", "refunded"], n_dup)]
    )
    payment = _dup(payment)
    total_c = _dup(total_c)
    items_json = _dup(items_json)
    meta = _dup(meta)
    if era in ("v2", "v3"):
        currency = _dup(currency)
        discount_c = _dup(discount_c)
        channel = _dup(channel)
    if era == "v3":
        loyalty = _dup(loyalty)
    n = n_total
    assert len(order_num) == n

    # ---- shuffle so late/dup rows are scattered --------------------------- #
    perm = rng.permutation(n)
    order_num, cust, store = order_num[perm], cust[perm], store[perm]
    order_dt, updated_dt = order_dt[perm], updated_dt[perm]
    status, payment, total_c = status[perm], payment[perm], total_c[perm]
    items_json, meta = items_json[perm], meta[perm]
    if era in ("v2", "v3"):
        currency, discount_c, channel = currency[perm], discount_c[perm], channel[perm]
    if era == "v3":
        loyalty = loyalty[perm]

    min_ts, max_ts = str(_ts_strings(order_dt.min())), str(_ts_strings(order_dt.max()))

    # ---- value dirt -------------------------------------------------------- #
    # timestamps: one draw decides the format category per row
    r = rng.random(n)
    ts = _ts_strings(order_dt).astype("U19")
    m_ddmm = r < P.TS_DDMM
    m_isot = (r >= P.TS_DDMM) & (r < P.TS_DDMM + P.TS_ISOT)
    m_bad = (r >= P.TS_DDMM + P.TS_ISOT) & (r < P.TS_DDMM + P.TS_ISOT + P.TS_IMPOSSIBLE)
    if m_ddmm.any():
        sub = ts[m_ddmm]
        ts[m_ddmm] = np.array(
            [f"{s[8:10]}/{s[5:7]}/{s[0:4]} {s[11:]}" for s in sub.tolist()], dtype="U19"
        )
    if m_isot.any():
        ts[m_isot] = np.char.replace(ts[m_isot], " ", "T")
    if m_bad.any():
        ts[m_bad] = np.array(
            [s[:8] + "32" + s[10:] for s in ts[m_bad].tolist()], dtype="U19"
        )

    # status junk / payment casing
    r = rng.random(n)
    m = r < P.STATUS_JUNK
    if m.any():
        status[m] = rng.choice(["PAID ", "Paid", "unknown", ""], int(m.sum()))
    r = rng.random(n)
    m = r < P.PAY_CASING
    if m.any():
        pm = payment[m]
        variants = np.char.capitalize(pm).astype("U16")
        up = rng.random(int(m.sum()))
        variants[up < 0.33] = np.char.upper(pm[up < 0.33])
        variants[up > 0.80] = np.char.add(np.char.add(" ", pm[up > 0.80]), " ")
        payment[m] = variants

    # customer_id: one draw per row -> empty | orphan | excel-float
    cust_s = np.char.mod("%d", cust).astype("U16")
    r = rng.random(n)
    m = r < P.CUST_EMPTY
    cust_s[m] = ""
    m2 = (r >= P.CUST_EMPTY) & (r < P.CUST_EMPTY + P.CUST_ORPHAN)
    if m2.any():
        cust_s[m2] = np.char.mod(
            "%d", ncust + rng.integers(1, max(ncust // 100, 10), int(m2.sum()))
        )
    m3 = (r >= P.CUST_EMPTY + P.CUST_ORPHAN) & (
        r < P.CUST_EMPTY + P.CUST_ORPHAN + P.CUST_FLOAT
    )
    if m3.any():
        cust_s[m3] = np.char.mod("%d.0", cust[m3])

    # order_total: mismatch first (value), then formatting dirt (disjoint draw)
    r = rng.random(n)
    m = r < P.TOTAL_MISMATCH
    if m.any():
        total_c[m] += rng.integers(100, 5_000, int(m.sum()))
    total_s = _fmt_cents(total_c)
    r = rng.random(n)
    b = 0.0
    m = r < P.TOTAL_NA
    total_s[m] = rng.choice(["N/A", ""], int(m.sum())) if m.any() else total_s[m]
    b += P.TOTAL_NA
    m = (r >= b) & (r < b + P.TOTAL_NEG)
    if m.any():
        total_s[m] = np.char.add("-", total_s[m])
    b += P.TOTAL_NEG
    m = (r >= b) & (r < b + P.TOTAL_COMMA)
    if m.any():
        total_s[m] = np.array(
            [_euro_fmt(c) for c in total_c[m].tolist()], dtype="U20"
        )
    b += P.TOTAL_COMMA
    m = (r >= b) & (r < b + P.TOTAL_DOLLAR)
    if m.any():
        total_s[m] = np.char.add("$", total_s[m])

    # ---- final column dict in era order ------------------------------------ #
    upd_s = _ts_strings(updated_dt)
    if era == "v3":
        order_id_col = pa.array(np.char.mod("ORD-%010d", order_num))
    else:
        order_id_col = pa.array(order_num)
    cols = {
        "order_id": order_id_col,
        "customer_id": pa.array(cust_s),
        "store_id": pa.array(store),
        "order_ts": pa.array(ts),
        "updated_at": pa.array(upd_s),
        "status": pa.array(status),
        ("payment_type" if era == "v3" else "payment_method"): pa.array(payment),
        "order_total": pa.array(total_s),
        "items": pa.array(items_json.tolist(), type=pa.string()),
        "meta": pa.array(meta.tolist(), type=pa.string()),
    }
    if era in ("v2", "v3"):
        cols["currency"] = pa.array(currency)
        cols["discount_amount"] = pa.array(_fmt_cents(discount_c))
        cols["channel"] = pa.array(channel)
    if era == "v3":
        cols["loyalty_tier"] = pa.array(loyalty)
    table = pa.table({name: cols[name] for name in P.COLUMNS[era]})

    out.parent.mkdir(parents=True, exist_ok=True)
    man_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_suffix(".csv.tmp")
    try:
        pacsv.write_csv(
            table, str(tmp),
            pacsv.WriteOptions(include_header=True, quoting_style="needed"),
        )

        n_corrupt = 0
        if d == P.CORRUPT_DATE:
            n_corrupt = _inject_corruption(tmp, P.CORRUPT_LINES[scale], n, rng)
        os.replace(tmp, out)
    finally:
        tmp.unlink(missing_ok=True)  # never orphan a partial day (~1 GB on spikes)

    manifest = {
        "file": out.name,
        "date": d.isoformat(),
        "era": era,
        "scale": scale,
        "rows": n,
        "late_rows": n_late,
        "dup_rows": n_dup,
        "corrupt_rows": n_corrupt,
        "bytes": out.stat().st_size,
        "min_order_ts": min_ts,
        "max_order_ts": max_ts,
        "generator_version": P.GENERATOR_VERSION,
    }
    man_path.write_text(json.dumps(manifest, indent=1), encoding="utf-8")
    manifest["secs"] = round(time.time() - t0, 1)
    return manifest


def _inject_corruption(path: Path, n_bad: int, n_rows: int, rng) -> int:
    """Intersperse truncated lines; append exactly one unclosed-quote line."""
    every = max(n_rows // max(n_bad, 1), 2)
    tmp2 = path.with_suffix(".corrupting")
    injected = 0
    with open(path, "r", encoding="utf-8", newline="") as src, open(
        tmp2, "w", encoding="utf-8", newline=""
    ) as dst:
        dst.write(src.readline())  # header
        for i, line in enumerate(src):
            dst.write(line)
            if injected < n_bad and i % every == every - 1:
                bad_id = 9_000_000_000 + injected
                dst.write(f"{bad_id},{rng.integers(1, 99999)},2026-07-22 12:00:00\n")
                injected += 1
        dst.write('9999999999,42,"this quote never closes\n')
    os.replace(tmp2, path)
    return injected + 1


# --------------------------------------------------------------------------- #
# dimensions
# --------------------------------------------------------------------------- #
FIRST = ["James", "Mary", "John", "Linh", "Anna", "Minh", "Wei", "Sofia", "Liam",
         "Emma", "Noah", "Olivia", "Huong", "Carlos", "Yuki", "Fatima", "Ivan",
         "Chloe", "Raj", "Elena", "Tom", "Nina", "Omar", "Julia", "David", "Sara",
         "Ken", "Lucia", "Marco", "Aisha", "Peter", "Hana", "Jack", "Mai", "Leo"]
LAST = ["Smith", "Nguyen", "Johnson", "Tran", "Williams", "Le", "Brown", "Pham",
        "Jones", "Garcia", "Miller", "Chen", "Davis", "Kim", "Lopez", "Singh",
        "Wilson", "Sato", "Anderson", "Mueller", "Taylor", "Ivanov", "Thomas",
        "Silva", "Moore", "Tanaka", "Martin", "Rossi", "Lee", "Novak", "White",
        "Kumar", "Harris", "Costa", "Clark"]
DOMAINS = ["gmail.com", "yahoo.com", "outlook.com", "proton.me", "example.org"]
COUNTRY_FORMS = {
    "US": ["US", "USA", "United States", "us"],
    "VN": ["VN", "Vietnam", "Viet Nam"],
    "GB": ["GB", "UK", "United Kingdom"],
    "DE": ["DE", "Germany"], "FR": ["FR", "France"], "JP": ["JP", "Japan"],
    "AU": ["AU", "Australia"], "CA": ["CA", "Canada"], "BR": ["BR", "Brazil"],
    "IN": ["IN", "India"],
}
COUNTRY_P = [0.30, 0.15, 0.10, 0.08, 0.07, 0.06, 0.05, 0.05, 0.04, 0.10]
CITIES = ["Hanoi", "Saigon", "New York", "London", "Berlin", "Paris", "Tokyo",
          "Sydney", "Toronto", "Sao Paulo", "Mumbai", "Chicago", "Osaka",
          "Munich", "Lyon", "Da Nang", "Austin", "Manchester", "Delhi", "Rio"]
CATEGORIES = ["electronics", "home", "fashion", "beauty", "sports", "toys",
              "grocery", "books", "garden", "automotive", "pets", "office"]
TAGS = ["sale", "new", "eco", "premium", "wireless", "organic", "limited",
        "bestseller", "clearance", "gift", "bundle", "refurb", "import",
        "handmade", "vegan", "outdoor", "indoor", "kids", "pro", "mini",
        "max", "classic", "smart", "portable"]
BRANDS = ["Acme", "Globex", "Initech", "Umbrella", "Stark", "Wayne", "Hooli",
          "Vandelay", "Wonka", "Tyrell", "Cyberdyne", "Aperture"]
COLORS = ["black", "white", "red", "blue", "green", "silver", "gold", "pink"]


def gen_customers(scale: str, root: Path, force: bool = False) -> int:
    n = P.N_CUSTOMERS[scale]
    out = root / "raw" / "dims" / "customers.csv"
    if out.exists() and not force:
        return n  # deterministic, so an existing file is already correct
    rng = np.random.default_rng(P.DIM_SEED + 1)
    cid = np.arange(1, n + 1)
    fi = rng.integers(0, len(FIRST), n)
    la = rng.integers(0, len(LAST), n)
    first = np.array(FIRST)[fi]
    last = np.array(LAST)[la]
    name = np.char.add(np.char.add(first, " "), last).astype("U40")

    r = rng.random(n)
    m = r < P.CUST_NAME_SHOUT
    name[m] = np.char.upper(name[m])
    b = P.CUST_NAME_SHOUT
    m = (r >= b) & (r < b + P.CUST_NAME_LASTFIRST)
    name[m] = np.char.add(np.char.add(np.char.upper(last[m]), ", "), first[m])
    b += P.CUST_NAME_LASTFIRST
    m = (r >= b) & (r < b + P.CUST_NAME_PAD)
    name[m] = np.char.add(np.char.add("  ", name[m]), " ")

    email = np.char.add(
        np.char.add(
            np.char.add(np.char.lower(first), "."),
            np.char.add(np.char.lower(last), np.char.mod("%d", cid)),
        ),
        np.char.add("@", rng.choice(DOMAINS, n)),
    ).astype("U60")
    m = rng.random(n) < P.CUST_EMAIL_INVALID
    email[m] = np.char.replace(email[m], "@", "_at_")
    n_dup = int(n * P.CUST_EMAIL_DUP)
    dup_to = rng.choice(n, n_dup, replace=False)
    email[dup_to] = email[(dup_to + n // 2) % n]  # same email, different customer

    # signup_ts: 80% iso / 10% DD-MM date / 8% epoch / 2% empty
    lo = int(np.datetime64("2019-01-01T00:00:00").astype("datetime64[s]").astype(int))
    hi = int(np.datetime64("2026-05-31T23:59:59").astype("datetime64[s]").astype(int))
    epoch_s = rng.integers(lo, hi, n)
    dt = epoch_s.astype("datetime64[s]")
    signup = _ts_strings(dt).astype("U19")
    r = rng.random(n)
    m = r < P.CUST_SIGNUP_DDMM
    if m.any():
        signup[m] = np.array(
            [f"{s[8:10]}/{s[5:7]}/{s[0:4]}" for s in signup[m].tolist()], dtype="U19"
        )
    b = P.CUST_SIGNUP_DDMM
    m = (r >= b) & (r < b + P.CUST_SIGNUP_EPOCH)
    signup[m] = np.char.mod("%d", epoch_s[m])
    b += P.CUST_SIGNUP_EPOCH
    m = (r >= b) & (r < b + P.CUST_SIGNUP_EMPTY)
    signup[m] = ""

    active = rng.choice(
        ["true", "True", "TRUE", "1", "yes", "Y", "false", "False", "0", "no", "N", ""],
        n,
        p=[0.30, 0.15, 0.08, 0.15, 0.08, 0.04, 0.06, 0.04, 0.05, 0.02, 0.01, 0.02],
    )

    keys = list(COUNTRY_FORMS)
    ck = rng.choice(len(keys), n, p=COUNTRY_P)
    country = np.empty(n, dtype="U16")
    for k, key in enumerate(keys):
        idx = np.where(ck == k)[0]
        forms = COUNTRY_FORMS[key]
        country[idx] = np.array(forms)[rng.integers(0, len(forms), idx.size)]
    country[rng.random(n) < P.CUST_COUNTRY_EMPTY] = ""
    city = rng.choice(CITIES, n)

    table = pa.table(
        {
            "customer_id": pa.array(cid),
            "name": pa.array(name),
            "email": pa.array(email),
            "country": pa.array(country),
            "city": pa.array(city),
            "signup_ts": pa.array(signup),
            "is_active": pa.array(active),
        }
    )
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_suffix(".csv.tmp")
    pacsv.write_csv(table, str(tmp), pacsv.WriteOptions(include_header=True))
    os.replace(tmp, out)
    return n


def gen_products(scale: str, root: Path, force: bool = False) -> int:
    n = P.N_PRODUCTS[scale]
    out = root / "raw" / "dims" / "products.csv"
    if out.exists() and not force:
        return n  # deterministic, so an existing file is already correct
    rng = np.random.default_rng(P.DIM_SEED + 2)
    prices = product_price_cents(scale)
    sku = np.char.mod("SKU-%06d", np.arange(n))
    cat = rng.choice(CATEGORIES, n)
    sub = np.char.add(cat, np.char.mod("-%d", rng.integers(1, 6, n)))
    adj = rng.choice(["Ultra", "Basic", "Pro", "Eco", "Smart", "Classic"], n)
    noun = rng.choice(["Widget", "Gadget", "Kit", "Pack", "Device", "Set"], n)
    pname = np.char.add(np.char.add(np.char.add(adj, " "), noun), np.char.mod(" %d", np.arange(n)))
    price_s = _fmt_cents(prices)
    m = rng.random(n) < P.PROD_PRICE_COMMA
    if m.any():
        price_s[m] = np.array(
            [_euro_fmt(c) for c in prices[m].tolist()], dtype="U20"
        )
    tags, attrs = [], []
    for i in range(n):
        k = int(rng.integers(1, 5))
        tags.append("|".join(rng.choice(TAGS, k, replace=False)))
        attrs.append(
            f'{{"brand":"{BRANDS[int(rng.integers(0, len(BRANDS)))]}",'
            f'"weight_g":{int(rng.integers(20, 20000))},'
            f'"color":"{COLORS[int(rng.integers(0, len(COLORS)))]}"}}'
        )
    table = pa.table(
        {
            "sku": pa.array(sku),
            "product_name": pa.array(pname),
            "category": pa.array(cat),
            "subcategory": pa.array(sub),
            "unit_price": pa.array(price_s),
            "tags": pa.array(tags, type=pa.string()),
            "attrs": pa.array(attrs, type=pa.string()),
        }
    )
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_suffix(".csv.tmp")
    pacsv.write_csv(table, str(tmp), pacsv.WriteOptions(include_header=True))
    os.replace(tmp, out)
    return n


def gen_stores(root: Path, force: bool = False) -> int:
    n = P.N_STORES
    out = root / "raw" / "dims" / "stores.csv"
    if out.exists() and not force:
        return n  # deterministic, so an existing file is already correct
    rng = np.random.default_rng(P.DIM_SEED + 3)
    sid = np.arange(1, n + 1)
    region = rng.choice(["NA", "EU", "APAC", "LATAM"], n, p=[0.35, 0.30, 0.25, 0.10])
    country = rng.choice(list(COUNTRY_FORMS), n)
    opened = (
        np.datetime64("2015-01-01")
        + rng.integers(0, 3650, n).astype("timedelta64[D]")
    )
    table = pa.table(
        {
            "store_id": pa.array(sid),
            "store_name": pa.array(np.char.mod("Store #%03d", sid)),
            "region": pa.array(region),
            "country": pa.array(country),
            "opened_date": pa.array(np.datetime_as_string(opened, unit="D")),
        }
    )
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_suffix(".csv.tmp")
    pacsv.write_csv(table, str(tmp), pacsv.WriteOptions(include_header=True))
    os.replace(tmp, out)
    return n


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def _worker(t):
    d_iso, scale, root_s, force = t
    try:
        return gen_day(date.fromisoformat(d_iso), scale, Path(root_s), force)
    except Exception as e:  # one bad day must not kill the whole run
        return {"date": d_iso, "error": f"{type(e).__name__}: {e}"}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--scale", required=True, choices=["small", "full"])
    ap.add_argument("--month", default=None, choices=["1", "2", "all"],
                    help="default: 1, or all when --start/--end are given")
    ap.add_argument("--start", type=date.fromisoformat, default=None)
    ap.add_argument("--end", type=date.fromisoformat, default=None)
    ap.add_argument("--workers", type=int, default=min(4, os.cpu_count() or 1),
                    help="parallel processes; each peaks at ~1.5-2 GB at full scale")
    ap.add_argument("--force", action="store_true", help="regenerate existing files")
    ap.add_argument("--no-dims", action="store_true")
    args = ap.parse_args()

    root = data_root(args.scale)
    # --start/--end filter within the month window; an explicit date range with
    # no --month means "the whole calendar", not silently month 1.
    month = args.month or ("all" if args.start or args.end else "1")
    dates = [d for d in P.all_dates(month)]
    if args.start:
        dates = [d for d in dates if d >= args.start]
    if args.end:
        dates = [d for d in dates if d <= args.end]
    if not dates:
        lo = P.MONTH2_START if month == "2" else P.MONTH1_START
        hi = P.MONTH1_END if month == "1" else P.MONTH2_END
        print(f"Nothing to do: empty date range (--month {month} covers "
              f"{lo}..{hi}; --start/--end only filter within it).")
        return 1

    print(f"ETL Lab generator v{P.GENERATOR_VERSION}")
    print(f"  scale={args.scale}  dates={dates[0]}..{dates[-1]} ({len(dates)} files)")
    print(f"  root={root}  workers={args.workers}")
    est = sum(P.rows_for(d, args.scale) for d in dates)
    print(f"  ~{est:,} order rows to generate\n")

    if not args.no_dims:
        t0 = time.time()
        c = gen_customers(args.scale, root, args.force)
        p = gen_products(args.scale, root, args.force)
        s = gen_stores(root, args.force)
        print(f"dims: customers={c:,} products={p:,} stores={s} "
              f"({time.time() - t0:.1f}s)")

    tasks = [(d.isoformat(), args.scale, str(root), args.force) for d in dates]
    t0 = time.time()
    done = skipped = failed = total_rows = 0
    if args.workers <= 1:
        pool = None
        results = map(_worker, tasks)
    else:
        pool = Pool(args.workers)
        results = pool.imap_unordered(_worker, tasks)
    try:
        for r in results:
            if r.get("skipped"):
                skipped += 1
                print(f"  {r['date']}  SKIP (exists; use --force to regenerate)")
                continue
            if "error" in r:
                failed += 1
                print(f"  {r['date']}  FAIL  {r['error']}")
                continue
            done += 1
            total_rows += r["rows"]
            print(f"  {r['date']}  era={r['era']}  rows={r['rows']:>9,}  "
                  f"{r['bytes'] / 1e6:7.1f} MB  {r['secs']:5.1f}s")
    finally:
        if pool is not None:
            pool.close()
            pool.join()
    dt = time.time() - t0
    print(f"\ndone: {done} files ({skipped} skipped, {failed} failed), "
          f"{total_rows:,} rows in {dt / 60:.1f} min "
          f"({total_rows / max(dt, 1e-9):,.0f} rows/s)")
    if failed:
        print("some days failed -- fix the cause and re-run the same command "
              "(finished files are skipped).")
        return 1
    print("next: python datagen/selftest.py --scale", args.scale)
    return 0


if __name__ == "__main__":
    sys.exit(main())
