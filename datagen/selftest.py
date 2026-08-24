"""Validate generated data against SPEC.md / profiles.py.

Run after generating:  python datagen/selftest.py --scale small
Checks manifests vs actual files, era schemas, dirt rates, late-row id
validity, spike volumes, corrupt-file behavior, JSON parseability and
order_total reconciliation. Exit code 0 = all PASS.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from datagen import profiles as P  # noqa: E402
from lib.labpaths import data_root  # noqa: E402

RESULTS: list[tuple[str, bool, str]] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    RESULTS.append((name, bool(ok), detail))
    mark = "PASS" if ok else "FAIL"
    print(f"  [{mark}] {name}" + (f"  ({detail})" if detail else ""))


def approx(x: float, target: float, rel: float = 0.5) -> bool:
    """Within +-rel of target (dirt rates are random; wide tolerance)."""
    return target * (1 - rel) <= x <= target * (1 + rel)


def varchar_cols(era: str) -> str:
    cols = ", ".join(f"'{c}': 'VARCHAR'" for c in P.COLUMNS[era])
    return "{" + cols + "}"


def sql_path(p: Path) -> str:
    """Posix path safe inside a single-quoted SQL literal (quotes doubled —
    a data root like C:/Users/O'Brien would otherwise break every read_csv)."""
    return p.as_posix().replace("'", "''")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--scale", required=True, choices=["small", "full"])
    args = ap.parse_args()
    scale = args.scale
    root = data_root(scale)
    orders = root / "raw" / "orders"
    mans = root / "raw" / "manifest"
    con = duckdb.connect()
    con.execute("SET memory_limit='6GB'; SET threads=4;")

    man_files = sorted(mans.glob("orders_*.json"))
    if not man_files:
        print(f"No manifests under {mans} — generate data first.")
        return 2
    print(f"selftest: scale={scale} root={root} files={len(man_files)}\n")

    # ---- 1. every manifest: file exists, bytes match, row count matches ---- #
    print("- manifests vs files")
    all_rows_ok = all_bytes_ok = True
    manifests = {}
    for mf in man_files:
        man = json.loads(mf.read_text(encoding="utf-8"))
        d = date.fromisoformat(man["date"])
        manifests[d] = man
        f = orders / man["file"]
        if not f.exists():
            all_rows_ok = False
            check(f"file exists {man['file']}", False)
            continue
        if f.stat().st_size != man["bytes"]:
            all_bytes_ok = False
        era = man["era"]
        extra = ", store_rejects=true" if d == P.CORRUPT_DATE else ""
        n = con.execute(
            f"SELECT count(*) FROM read_csv('{sql_path(f)}', header=true, "
            f"columns={varchar_cols(era)}{extra})"
        ).fetchone()[0]
        if n != man["rows"]:
            all_rows_ok = False
            check(f"rows {man['file']}", False, f"csv={n} manifest={man['rows']}")
    check("all files exist with matching row counts", all_rows_ok)
    check("all file sizes match manifest bytes", all_bytes_ok)

    # ---- 2. headers per era ------------------------------------------------ #
    print("- era headers")
    for d, man in sorted(manifests.items()):
        f = orders / man["file"]
        with open(f, "r", encoding="utf-8") as fh:
            header = fh.readline().strip().replace('"', "")
        want = ",".join(P.COLUMNS[man["era"]])
        if header != want:
            check(f"header {man['file']}", False, header)
            break
    else:
        check("headers match era column order for every file", True)

    # ---- 3. expected volumes ---------------------------------------------- #
    print("- volumes")
    vol_ok = all(
        man["rows"] == P.rows_for(d, scale) for d, man in manifests.items()
    )
    check("rows == profiles.rows_for(date) for every file", vol_ok)
    spike = [d for d in P.SPIKE_DATES if d in manifests]
    if spike:
        s = spike[0]
        base_day = date(2026, 6, 3)  # a plain Wednesday
        if base_day in manifests:
            ratio = manifests[s]["rows"] / manifests[base_day]["rows"]
            check("spike day ~3x weekday volume", 2.5 <= ratio <= 3.6,
                  f"ratio={ratio:.2f}")

    # ---- 4. one v1 day in depth ------------------------------------------- #
    probe = date(2026, 6, 10)
    if probe in manifests:
        print(f"- dirt rates on {probe} (tolerance +-50%)")
        f = sql_path(orders / f"orders_{probe}.csv")
        rel = f"read_csv('{f}', header=true, columns={varchar_cols('v1')})"
        n = manifests[probe]["rows"]
        q = lambda sql: con.execute(sql).fetchone()[0]  # noqa: E731

        r = q(f"SELECT count(*) FROM {rel} WHERE order_ts LIKE '__/__/____%'") / n
        check("order_ts DD/MM rate", approx(r, P.TS_DDMM), f"{r:.4%}")
        r = q(f"SELECT count(*) FROM {rel} WHERE order_ts LIKE '%T%'") / n
        check("order_ts ISO-T rate", approx(r, P.TS_ISOT), f"{r:.4%}")
        r = q(f"SELECT count(*) FROM {rel} WHERE order_ts LIKE '%-32 %'") / n
        check("impossible ts rate", approx(r, P.TS_IMPOSSIBLE, 0.8), f"{r:.4%}")
        r = q(f"SELECT count(*) FROM {rel} WHERE status NOT IN "
              f"('created','paid','shipped','delivered','cancelled','refunded')") / n
        check("junk status rate", approx(r, P.STATUS_JUNK), f"{r:.4%}")
        r = q(f"SELECT count(*) FROM {rel} WHERE order_total IN ('N/A','') "
              f"OR order_total IS NULL") / n
        check("total N/A rate", approx(r, P.TOTAL_NA), f"{r:.4%}")
        r = q(f"SELECT count(*) FROM {rel} WHERE order_total LIKE '-%'") / n
        check("negative total rate", approx(r, P.TOTAL_NEG), f"{r:.4%}")
        r = q(f"SELECT count(*) FROM {rel} WHERE order_total LIKE '%,%'") / n
        check("comma total rate", approx(r, P.TOTAL_COMMA), f"{r:.4%}")
        r = q(f"SELECT count(*) FROM {rel} WHERE order_total LIKE '$%'") / n
        check("dollar total rate", approx(r, P.TOTAL_DOLLAR), f"{r:.4%}")
        r = q(f"SELECT count(*) FROM {rel} WHERE customer_id LIKE '%.0'") / n
        check("excel-float customer rate", approx(r, P.CUST_FLOAT), f"{r:.4%}")
        r = q(f"SELECT count(*) FROM {rel} WHERE items LIKE '%\"qty\":\"%'") / n
        check("string-typed items rate", approx(r, P.ITEMS_STRTYPE), f"{r:.4%}")
        dup = q(f"SELECT count(*) - count(DISTINCT order_id) FROM {rel}")
        check("in-file dup order_ids", approx(dup, manifests[probe]["dup_rows"], 0.6),
              f"{dup} vs manifest {manifests[probe]['dup_rows']}")

        # JSON validity + reconciliation on a sample
        bad_json = q(
            f"SELECT count(*) FROM (SELECT items FROM {rel} USING SAMPLE 20000 ROWS) "
            f"WHERE NOT json_valid(items)"
        )
        check("items JSON always valid", bad_json == 0, f"bad={bad_json}")
        recon = con.execute(f"""
            WITH s AS (
              SELECT TRY_CAST(order_total AS DOUBLE) AS total,
                     list_sum(list_transform(
                        CAST(items AS JSON)::STRUCT(sku VARCHAR, qty VARCHAR,
                                                    unit_price VARCHAR)[],
                        x -> CAST(x.qty AS DOUBLE) * CAST(x.unit_price AS DOUBLE)
                     )) AS items_sum
              FROM {rel}
              WHERE order_total SIMILAR TO '[0-9]+\\.[0-9]{{2}}'
              USING SAMPLE 20000 ROWS
            )
            SELECT avg(CASE WHEN abs(total - items_sum) <= 0.011
                            THEN 1 ELSE 0 END) FROM s
        """).fetchone()[0]
        check("order_total == items sum for ~99.4% of clean rows",
              recon is not None and 0.985 <= recon <= 0.999, f"match={recon:.4%}")

        # late rows reference real prior-day id ranges
        late = con.execute(f"""
            SELECT CAST(order_id AS BIGINT) AS oid,
                   CAST(substr(order_ts, 1, 10) AS DATE) AS od
            FROM {rel}
            WHERE order_ts NOT LIKE '__/__/____%' AND order_ts NOT LIKE '%-32 %'
              AND substr(order_ts, 1, 10) < '{probe}'
        """).fetchall()
        ok = len(late) > 0
        for oid, od in late:
            lo = P.id_base(od)
            if not (lo <= oid < lo + P.base_rows_for(od, scale)):
                ok = False
                break
        exp_late = manifests[probe]["late_rows"]
        check("late rows have valid prior-day order_ids",
              ok and approx(len(late), exp_late, 0.2),
              f"n={len(late)} manifest={exp_late}")

        # reviewer additions (2026-08-16): updated_at, payment, customer_id,
        # meta and manifest ts bounds on the same probe day
        ts_clean = "'[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}'"
        bad = q(f"SELECT count(*) FROM {rel} "
                f"WHERE NOT updated_at SIMILAR TO {ts_clean}")
        check("updated_at always clean format", bad == 0, f"bad={bad}")
        bad = q(f"SELECT count(*) FROM {rel} WHERE updated_at < order_ts "
                f"AND order_ts SIMILAR TO {ts_clean} "
                f"AND order_ts NOT LIKE '%-32 %'")
        check("updated_at >= order_ts on clean-format order_ts rows", bad == 0,
              f"bad={bad}")

        pay_list = ", ".join(f"'{p}'" for p in P.PAYMENTS)
        r = q(f"SELECT count(*) FROM {rel} "
              f"WHERE payment_method NOT IN ({pay_list})") / n
        check("payment casing-variant rate", approx(r, P.PAY_CASING), f"{r:.4%}")

        r = q(f"SELECT count(*) FROM {rel} WHERE customer_id = '' "
              f"OR customer_id IS NULL") / n
        check("empty customer rate", approx(r, P.CUST_EMPTY), f"{r:.4%}")
        r = q(f"SELECT count(*) FROM {rel} WHERE "
              f"TRY_CAST(customer_id AS BIGINT) > {P.N_CUSTOMERS[scale]}") / n
        check("orphan customer rate", approx(r, P.CUST_ORPHAN, 0.6), f"{r:.4%}")

        bad_json = q(
            f"SELECT count(*) FROM (SELECT meta FROM {rel} USING SAMPLE 20000 ROWS) "
            f"WHERE NOT json_valid(meta)"
        )
        check("meta JSON always valid", bad_json == 0, f"bad={bad_json}")
        n_none = q(f"SELECT count(*) FROM {rel} WHERE meta LIKE '%\"utm\":\"none\"%'")
        n_null = q(f"SELECT count(*) FROM {rel} WHERE meta LIKE '%\"utm\":null%'")
        n_abs = q(f"SELECT count(*) FROM {rel} WHERE meta NOT LIKE '%\"utm\"%'")
        check("all three no-utm flavors present ('none'/null/absent)",
              n_none > 0 and n_null > 0 and n_abs > 0,
              f"none={n_none} null={n_null} absent={n_abs}")

        mn, mx = con.execute(
            f"SELECT min(order_ts), max(order_ts) FROM {rel} "
            f"WHERE order_ts SIMILAR TO {ts_clean} AND order_ts NOT LIKE '%-32 %'"
        ).fetchone()
        check("manifest min/max_order_ts match clean-format file min/max",
              mn == manifests[probe]["min_order_ts"]
              and mx == manifests[probe]["max_order_ts"],
              f"file=[{mn} .. {mx}]")

    # ---- 4b. one v2 day: money semantics ----------------------------------- #
    probe2 = date(2026, 7, 20)  # v2 weekday, clear of the corrupt 07-22 file
    if probe2 in manifests:
        print(f"- v2 money semantics on {probe2}")
        f = sql_path(orders / f"orders_{probe2}.csv")
        rel = f"read_csv('{f}', header=true, columns={varchar_cols('v2')})"
        cur_list = ", ".join(f"'{c}'" for c in P.CURRENCIES)
        bad_cur = con.execute(
            f"SELECT count(*) FROM {rel} WHERE currency NOT IN ({cur_list})"
        ).fetchone()[0]
        check("currency always one of USD/EUR/GBP/VND", bad_cur == 0,
              f"bad={bad_cur}")

        # v2+ reconciliation rule: order_total = items_total - discount_amount
        def v2_match(where: str, sample: str) -> float | None:
            return con.execute(f"""
                WITH s AS (
                  SELECT TRY_CAST(order_total AS DOUBLE) AS total,
                         TRY_CAST(discount_amount AS DOUBLE) AS disc,
                         list_sum(list_transform(
                            CAST(items AS JSON)::STRUCT(sku VARCHAR, qty VARCHAR,
                                                        unit_price VARCHAR)[],
                            x -> CAST(x.qty AS DOUBLE) * CAST(x.unit_price AS DOUBLE)
                         )) AS items_sum
                  FROM {rel}
                  WHERE order_total SIMILAR TO '[0-9]+\\.[0-9]{{2}}' {where}
                  {sample}
                )
                SELECT avg(CASE WHEN abs(total - (items_sum - disc)) <= 0.011
                                THEN 1 ELSE 0 END) FROM s
            """).fetchone()[0]

        recon = v2_match("", "USING SAMPLE 20000 ROWS")
        check("v2 order_total == items sum - discount for ~99.4% of clean rows",
              recon is not None and 0.985 <= recon <= 0.999, f"match={recon:.4%}")
        recon = v2_match("AND currency = 'VND'", "")
        check("VND rows reconcile too (amounts x25000 consistently)",
              recon is not None and recon >= 0.98,
              f"match={recon:.4%}" if recon is not None else "no VND rows")

    # ---- 5. corrupt file --------------------------------------------------- #
    if P.CORRUPT_DATE in manifests:
        print("- corrupt file")
        f = sql_path(orders / f"orders_{P.CORRUPT_DATE}.csv")
        strict_failed = False
        try:
            con.execute(
                f"SELECT count(*) FROM read_csv('{f}', header=true, "
                f"columns={varchar_cols('v2')})"
            ).fetchone()
        except Exception:
            strict_failed = True
        check("strict read of corrupt file fails", strict_failed)
        # note: reject tables only fill on a materializing scan (not bare count(*))
        con.execute(
            f"CREATE OR REPLACE TEMP TABLE _corrupt_probe AS "
            f"SELECT * FROM read_csv('{f}', header=true, "
            f"columns={varchar_cols('v2')}, store_rejects=true)"
        )
        n = con.execute("SELECT count(*) FROM _corrupt_probe").fetchone()[0]
        rej = con.execute(
            "SELECT count(DISTINCT line) FROM reject_errors"
        ).fetchone()[0]
        man = manifests[P.CORRUPT_DATE]
        check("store_rejects recovers all clean rows", n == man["rows"],
              f"rows={n} manifest={man['rows']}")
        check("rejected line count == injected corrupt lines",
              rej == man["corrupt_rows"],
              f"reject lines={rej} injected={man['corrupt_rows']}")

    # ---- 6. era v3 --------------------------------------------------------- #
    v3 = [d for d, m in manifests.items() if m["era"] == "v3"]
    if v3:
        print("- era v3")
        f = sql_path(orders / f"orders_{min(v3)}.csv")
        rel = f"read_csv('{f}', header=true, columns={varchar_cols('v3')})"
        bad = con.execute(
            f"SELECT count(*) FROM {rel} WHERE order_id NOT LIKE 'ORD-%'"
        ).fetchone()[0]
        check("v3 order_ids all ORD-prefixed", bad == 0, f"bad={bad}")
        disc = con.execute(
            f"SELECT count(*) FROM (SELECT items FROM {rel} LIMIT 5000) "
            f"WHERE items LIKE '%\"disc\":%'"
        ).fetchone()[0]
        check("v3 items carry disc field", disc == 5000, f"{disc}/5000")

    # ---- 7. dims ----------------------------------------------------------- #
    print("- dims")
    dims = root / "raw" / "dims"
    nc = con.execute(
        f"SELECT count(*) FROM read_csv('{sql_path(dims / 'customers.csv')}', "
        f"header=true, all_varchar=true)"
    ).fetchone()[0]
    check("customers count", nc == P.N_CUSTOMERS[scale], f"{nc:,}")
    rel = (f"read_csv('{sql_path(dims / 'customers.csv')}', header=true, "
           f"all_varchar=true)")
    r = con.execute(
        f"SELECT count(*) FROM {rel} WHERE signup_ts SIMILAR TO '[0-9]+'"
    ).fetchone()[0] / nc
    check("epoch signup_ts rate ~8%", approx(r, P.CUST_SIGNUP_EPOCH, 0.4), f"{r:.2%}")
    nb = con.execute(
        f"SELECT count(DISTINCT is_active) FROM {rel}"
    ).fetchone()[0]
    check("is_active has many boolean spellings", nb >= 8, f"{nb} variants")
    ncv = con.execute(
        f"SELECT count(DISTINCT country) FROM {rel} WHERE country IN "
        f"('US','USA','United States','us','VN','Vietnam','Viet Nam')"
    ).fetchone()[0]
    check("country variant forms present", ncv >= 6, f"{ncv}")
    np_ = con.execute(
        f"SELECT count(*) FROM read_csv('{sql_path(dims / 'products.csv')}', "
        f"header=true, all_varchar=true)"
    ).fetchone()[0]
    check("products count", np_ == P.N_PRODUCTS[scale], f"{np_:,}")
    ns = con.execute(
        f"SELECT count(*) FROM read_csv('{sql_path(dims / 'stores.csv')}', "
        f"header=true, all_varchar=true)"
    ).fetchone()[0]
    check("stores count", ns == P.N_STORES, f"{ns:,}")
    with open(dims / "stores.csv", "r", encoding="utf-8") as fh:
        header = fh.readline().strip().replace('"', "")
    check("stores header matches SPEC 3.6",
          header == "store_id,store_name,region,country,opened_date", header)

    # ---- summary ----------------------------------------------------------- #
    fails = [r for r in RESULTS if not r[1]]
    print(f"\n{'=' * 60}\n{len(RESULTS) - len(fails)}/{len(RESULTS)} checks passed.")
    if fails:
        print("FAILED:")
        for name, _, detail in fails:
            print(f"  - {name} {detail}")
        return 1
    print("All good — the dataset matches SPEC.md.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
