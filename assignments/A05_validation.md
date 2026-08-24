# A05 — Validation & quarantine
**Topic:** validation · **Est. time:** 4h · **Difficulty:** ⭐⭐⭐
**Requires:** A04 done; month-1 raw data at both scales; your `warehouse.duckdb` from A03/A04;
the signed contract at `contracts/shopcore_orders_daily.contract.yaml` (in the repo).

## Why this matters

In A03 you fixed the dirt that *can* be fixed. Today you deal with what can't: an order with no
total, a customer id that exists in no dimension, a total that contradicts its own line items.
The worst thing a pipeline can do with such rows is pass them through silently — wrong numbers,
nobody notices for months. The second worst is to crash the whole load over 1% of bad rows.
Validation plus quarantine is the middle path: load the good 98%, park the bad rows *with
evidence*, and stop the line only when the dirt level itself is abnormal.

## Background

**Five kinds of checks.** Every data quality suite is some mix of these:

1. **Schema checks** — right columns and types at all? Reading with an explicit `columns={...}`
   schema already fails loudly on a wrong-shaped file; A06 turns this into a proper pre-flight
   gate, so today the strict read is enough.
2. **Row checks** — is *this row* acceptable? Not-null keys, values from an allowed list (an
   **enum whitelist** — a fixed set like the six valid statuses), numeric ranges, cross-field
   rules ("the total must equal the sum of the items").
3. **Aggregate checks** — is the *set* of rows plausible? Uniqueness of a key, duplicate rates,
   counts in an expected band. No single row is guilty; the group is.
4. **Reconciliation** — does what we loaded match what the producer — **shopcore**, the
   e-commerce platform behind this feed — says it sent? Our manifest JSON carries exact
   per-file row counts; loaded must equal manifest, every day, no excuses. Cheapest,
   highest-value check in the whole suite.
5. **Freshness** — is the table *current* at all? Core can pass every check above and still be
   three days stale — perfectly clean, silently lying to every dashboard that reads it. A03's
   `_data_date` lineage column makes this a one-liner: `max(_data_date)` vs the latest day you
   expect.

**Detect, don't fix.** A03 normalized *representation* (case, whitespace, number and date
formats); today you judge *values* after that cleaning. `" CARD "` is not dirt — it is `card` in
a costume. `"unknown"` is dirt: no formatting makes it a valid status. Validation always runs on
the *cleaned* staging table, never on raw text, or you reject rows your own pipeline could save.

**Quarantine, don't delete.** A rejected row is evidence: for "why 300 more rejects than
usual?", for showing the producer exactly which rows are broken, for *replaying* rows if a rule
proves too strict. Bad rows go to `<DATA_ROOT>/quarantine/orders/` as Parquet, tagged with
**why** (`reject_reason`), **when** (`_rejected_at`) and **which delivery** (`_data_date` — this
feed is one file per business date, so the contract's `path_pattern` turns that date back into
`orders_<_data_date>.csv`; from A07 on, `_run_id` joins `ops.etl_runs` for the file name, the
load timestamp and the run's status). A row can fail several checks at once, so `reject_reason`
is a `'; '`-separated list.

**Dirt has a budget — and you don't set it.** This feed is ~97% clean, and that is *normal*:
rejecting ~1.7% of rows every day is not an incident, it is Tuesday. The real question is not
"is there dirt?" but "is today's dirt *unusual*?" — and "unusual" is not your opinion. shopcore
and the warehouse team signed a data contract, `contracts/shopcore_orders_daily.contract.yaml`,
whose `quality.row_level_tolerances` block states, rule by rule, exactly how much dirt is
tolerated — "above these, the load hard-fails" is written in the file. Your job is to *load*
those numbers, never to invent or hard-code them: when the contract changes, the gate follows
without a code edit. One consequence holds for any budget: the gate must run **before** good
rows are loaded. Quarantine the bad, count, compare to the tolerances, *then* insert into core.
A gate that fires after loading is a smoke alarm installed after the fire.

## Setup

- Git ritual (§7.4): `git switch -c a05-validation`, commit after each numbered task, merge to
  `main` when the DoD is green (`guides/git_basics.md` if any step feels fuzzy).
- Work at **small scale** for Tasks 1–10 and 12; Task 11 re-runs at full scale. Task Manager
  open for Task 11; `journal.md` open throughout.
- **Optional install, Task 12 only:** `pip install pandera`. `requirements.txt` carries the line
  commented out for exactly this reason — install it when you reach the tool-bridge task, not
  before. It is one pip install with pure-python dependencies (typeguard, typing-inspect, on top
  of the pydantic you already have) and no service to run; nothing else in the lab imports it.
- Start `work/a05_validation.py` (it becomes `work/validate_day.py` in Task 8):

```python
import duckdb
import yaml
from lib.labpaths import (raw_orders_dir, raw_dims_dir, manifest_dir,
                          warehouse_path, quarantine_dir, tmp_dir)

SCALE = "small"                 # develop here; Task 11 switches to "full"
DAY   = "2026-06-02"            # any v1 day; the verify numbers assume this one
CONTRACT = "contracts/shopcore_orders_daily.contract.yaml"   # in the repo, not <DATA_ROOT>

ORDERS_CSV = (raw_orders_dir(SCALE) / f"orders_{DAY}.csv").as_posix()
MANIFEST   = (manifest_dir(SCALE)   / f"orders_{DAY}.json").as_posix()
CUSTOMERS  = (raw_dims_dir(SCALE)   / "customers.csv").as_posix()
PRODUCTS   = (raw_dims_dir(SCALE)   / "products.csv").as_posix()
STORES     = (raw_dims_dir(SCALE)   / "stores.csv").as_posix()
QUARANTINE = quarantine_dir(SCALE)  / "orders"

con = duckdb.connect(str(warehouse_path(SCALE)))
con.execute(f"SET memory_limit='8GB'; SET threads=8; "
            f"SET temp_directory='{tmp_dir(SCALE).as_posix()}';")
con.execute("CREATE SCHEMA IF NOT EXISTS staging; CREATE SCHEMA IF NOT EXISTS core; "
            "CREATE SCHEMA IF NOT EXISTS ops;")
```

The code below joins referential-integrity checks against the **raw** dim CSVs so this doc
stands alone; joining your A03 `core.customers` / `core.stores` instead is the better
architecture — same queries, different `FROM`.

## Tasks

### Task 1 — Read the rules — they live in the contract (small)

The checks you are about to build enforce *promises*, and the promises are written down:
`contracts/shopcore_orders_daily.contract.yaml`, v1.3.0, signed by shopcore's Checkout Platform
team and yours. Skim its `schema` and `quality` blocks now (A06 dissects the rest); every check
in Task 5 traces back to a line in it. The tolerances are not yours to pick — load them:

```python
contract = yaml.safe_load(open(CONTRACT, encoding="utf-8"))
TOL = {rule: float(str(tol).rstrip('%'))     # YAML has no percent type —
       for rule, tol in                      # '0.5%' arrives as a *string*
       contract["quality"]["row_level_tolerances"].items()}

CHECK_FOR = {                                # contract rule -> Task 5 check name
    "null_or_invalid_status":  "bad_status",
    "unparseable_order_ts":    "bad_order_ts",
    "unparseable_order_total": "null_total",
    "orphan_customer_id":      "orphan_customer",
    "orphan_sku_lines":        "orphan_sku",
    "total_vs_items_mismatch": "total_mismatch",
}
THRESHOLDS = {CHECK_FOR[rule]: tol for rule, tol in TOL.items()}
print(contract["version"], THRESHOLDS)
```

Why not six literals? Because a tolerance is a *shared decision*, not your opinion — one number
in one reviewed, versioned place. When v1.4.0 tightens `orphan_customer_id`, every consumer's
gate follows on the next run, zero code edits. Two pieces of fine print worth a journal line:
the contract budgets orphan SKU *lines* while your check will flag whole *orders* (close enough
at these rates; noticing such mismatches is an A06 skill), and rules pinned without any
tolerance (`order_id` is `nullable: false`, full stop) stay informational today — A06's
pre-flight gate enforces them structurally.

**Accept when:** the script prints `1.3.0` and six thresholds — `bad_order_ts` and
`orphan_customer` at 0.5, the rest at 1.0 — and you can point at the YAML line each one came
from.

### Task 2 — Read the producer's claim (small)

Every orders file ships with a manifest — the producer stating what it wrote (the contract's
`delivery.completeness_signal` clause is exactly this promise). DuckDB reads JSON files like
tables:
`con.sql(f"SELECT rows, late_rows, dup_rows FROM read_json_auto('{MANIFEST}')").show()`

**Accept when:** you see `rows = 59024` for small `2026-06-02`, and you can say what `late_rows`
and `dup_rows` mean (revisit A02's partition surprise if not).

### Task 3 — Stage one day, cleaned (small)

Validation runs on cleaned data, so rebuild the A03-style staging table for your day — the
`CREATE OR REPLACE` overwrites A04's scratch table of the same name, and that is intended:
today's version needs the full column set plus the §7.2 lineage columns, because its rejected
rows become quarantine evidence. Revision — code given; reread each cleaner and recall which
dirt it handles:

```python
con.execute(f"""
CREATE OR REPLACE TABLE staging.shopcore_orders_day AS
SELECT
    order_id,
    -- Excel-float '123456.0' -> BIGINT (why we read it as VARCHAR below)
    CAST(TRY_CAST(customer_id AS DOUBLE) AS BIGINT)            AS customer_id,
    store_id,
    -- three timestamp formats; impossible dates become NULL
    try_strptime(order_ts, ['%Y-%m-%d %H:%M:%S',
                            '%d/%m/%Y %H:%M:%S',
                            '%Y-%m-%dT%H:%M:%S'])              AS order_ts,
    updated_at,
    NULLIF(LOWER(TRIM(status)), '')                            AS status,
    NULLIF(LOWER(TRIM(payment_method)), '')                    AS payment_method,
    -- '$79.98', 'N/A', European '93,32' / '1.234,56'
    TRY_CAST(NULLIF(
      CASE WHEN order_total LIKE '%,%'
           THEN replace(replace(order_total, '.', ''), ',', '.')
           ELSE replace(TRIM(order_total), '$', '') END,
      'N/A') AS DECIMAL(14,2))                                 AS order_total,
    items,
    meta,
    -- A03's lineage columns — exactly two (§7.2), on every warehouse-written
    -- row from here on. No file name column: one file per business date, so
    -- _data_date already names the delivery. No load timestamp either — when a
    -- row was loaded is a fact about the *run*, and A07's ops.etl_runs ledger
    -- holds it (with the file name and the status), keyed by _run_id, which
    -- stays NULL until A07 invents run identity
    DATE '{DAY}'                                               AS _data_date,
    CAST(NULL AS BIGINT)                                       AS _run_id
FROM read_csv('{ORDERS_CSV}',
    header=true,
    columns={{'order_id':'BIGINT','customer_id':'VARCHAR','store_id':'INTEGER',
             'order_ts':'VARCHAR','updated_at':'TIMESTAMP','status':'VARCHAR',
             'payment_method':'VARCHAR','order_total':'VARCHAR',
             'items':'VARCHAR','meta':'VARCHAR'}});
""")
```

A §7.4 note: the timestamp, money and id cleaners are not new code — they are `clean_order_ts`,
`clean_money` and `clean_id`, the expressions that graduated into `work/cleaners.py` at the end
of A03, spelled out inline only so you can reread each rule next to the dirt it handles; in your
script, build those three columns by importing them (`from work import cleaners`) rather than
re-pasting — the module, pinned by `tests/test_cleaners.py`, stays their single home.

Note what staging does **not** do: it does not map `'unknown'` status to NULL or drop anything.
Representation is normalized; judging values is the check suite's job — which is why the two
enum columns deliberately *skip* `clean_enum` (its whitelist already judges: junk → NULL) and
keep only the lighter `NULLIF(LOWER(TRIM(...)), '')`. A junk status must reach quarantine with
its value intact as evidence.

**Accept when:** `SELECT count(*) FROM staging.shopcore_orders_day` equals the manifest `rows`
exactly. That equality *is* reconciliation — you formalize it in Task 7.

### Task 4 — What do the items say each order costs? (small)

The cross-field check needs, per order, `sum(qty * unit_price)` over its items — and while
unnesting anyway, count SKUs missing from the products dim (you met them in A04). Remember:
`unnest` cannot share a `SELECT` with `GROUP BY` — explode in a subquery:

```python
con.execute(f"""
CREATE OR REPLACE TEMP TABLE items_agg AS
SELECT order_id, updated_at,
       round(sum(it.qty * it.unit_price), 2) AS items_total,
       count(*) FILTER (p.sku IS NULL)       AS n_orphan_sku
FROM (
    SELECT order_id, updated_at,
           unnest(CAST(items AS JSON)::STRUCT(sku VARCHAR, qty INTEGER,
                                              unit_price DOUBLE)[]) AS it
    FROM staging.shopcore_orders_day
) LEFT JOIN read_csv('{PRODUCTS}', header=true) p ON it.sku = p.sku
GROUP BY 1, 2;
""")
```

Why group by `(order_id, updated_at)`, not just `order_id`? Because ~0.05% of order_ids appear
twice in one file (in-file corrections — A08's problem). The pair is unique; the id alone is
not. You prove that in Task 7.

**Accept when:** `items_agg` has exactly as many rows as `staging.shopcore_orders_day`, and
`SELECT count(*) FROM items_agg WHERE n_orphan_sku > 0` is roughly 310 (≈0.5% of orders).

### Task 5 — The check suite → one `reject_reason` per row (small)

Flag every row, collecting **all** reasons it fails: one `CASE` per check yielding the check's
name (or NULL if it passes), glued with `concat_ws` — which skips NULLs — and `NULLIF(..., '')`
so a fully clean row gets `reject_reason = NULL`. Eight checks are written for you. **You write
the four `TODO`s** — they mirror ones already there, and the dim joins are already in the `FROM`:

```python
con.execute(f"""
CREATE OR REPLACE TEMP TABLE flagged AS
SELECT s.*, i.items_total,
       NULLIF(concat_ws('; ',
         CASE WHEN s.order_id IS NULL THEN 'null_order_id' END,
         CASE WHEN s.order_ts IS NULL THEN 'bad_order_ts' END,
         -- the contract's range clause on order_ts: within
         -- [business_date - 7 days, business_date + 1 day); the GREATEST
         -- guard just respects the era start
         CASE WHEN s.order_ts IS NOT NULL AND NOT (
                s.order_ts >= GREATEST(DATE '2026-06-01',
                                       DATE '{DAY}' - INTERVAL 7 DAY)
            AND s.order_ts <  DATE '{DAY}' + INTERVAL 1 DAY)
              THEN 'ts_out_of_range' END,
         -- enum whitelist — the contract's allowed_values, verbatim
         CASE WHEN s.status IS NULL OR s.status NOT IN
              ('created','paid','shipped','delivered','cancelled','refunded')
              THEN 'bad_status' END,
         -- TODO 'bad_payment': whitelist card/wallet/bank_transfer/cod/paypal
         CASE WHEN s.order_total IS NULL THEN 'null_total' END,
         CASE WHEN s.order_total < 0 THEN 'negative_total' END,
         -- cross-field: v1 rule is total == items sum, one cent of slack;
         -- guarded so a negative total is flagged once, not twice
         CASE WHEN s.order_total >= 0 AND i.items_total IS NOT NULL
               AND abs(s.order_total - i.items_total) > 0.01
              THEN 'total_mismatch' END,
         -- referential integrity
         -- TODO 'null_customer':   customer_id IS NULL
         -- TODO 'orphan_customer': customer_id set, but c.customer_id IS NULL
         -- TODO 'orphan_store':    no such store in the dim
         CASE WHEN i.n_orphan_sku > 0 THEN 'orphan_sku' END
       ), '') AS reject_reason
FROM staging.shopcore_orders_day s
LEFT JOIN items_agg i USING (order_id, updated_at)
LEFT JOIN read_csv('{CUSTOMERS}', header=true) c ON s.customer_id = c.customer_id
LEFT JOIN read_csv('{STORES}',    header=true) st ON s.store_id   = st.store_id;
""")
print(con.execute("SELECT count(*), count(reject_reason) FROM flagged").fetchone())
```

`count(reject_reason)` counts non-NULLs — your quarantine count. Look at ten victims
(`... FROM flagged WHERE reject_reason IS NOT NULL LIMIT 10`); every reason should make you nod.

**Accept when:** with the reference rules, small `2026-06-02` flags **992 rows (1.68%)**. Within
a few dozen means your rules differ slightly — find and justify each difference. Zero or
thousands means a bug (wrong join key, or you validated raw instead of staged).

### Task 6 — Split load: quarantine, gate, then core (small)

Order matters. Say it out loud before running: *park the evidence → check the tolerances →
load.*

```python
bad, staged = con.execute(
    "SELECT count(reject_reason), count(*) FROM flagged").fetchone()
pct = 100.0 * bad / staged

# 1) evidence is ALWAYS written, even (especially) on a terrible day
QUARANTINE.mkdir(parents=True, exist_ok=True)   # COPY creates partition dirs,
                                                # but not missing parents
con.execute(f"""
COPY (
  SELECT *, now() AT TIME ZONE 'UTC' AS _rejected_at,   -- payload, not lineage (§7.2)
         COALESCE(CAST(order_ts AS DATE), DATE '{DAY}') AS order_date
  FROM flagged WHERE reject_reason IS NOT NULL
) TO '{QUARANTINE.as_posix()}'
  (FORMAT parquet, PARTITION_BY (order_date), OVERWRITE_OR_IGNORE,
   FILENAME_PATTERN 'from_{DAY}_{{i}}');
""")

# 2) the gate — the contract's tolerances, checked BEFORE core sees a row.
#    ops.alerts (the lab standard) is the pager: a table row, a console
#    line, and a non-zero exit
con.execute("""
CREATE TABLE IF NOT EXISTS ops.alerts (
  alert_ts TIMESTAMP, run_id BIGINT, severity VARCHAR,
  source VARCHAR, message VARCHAR);
""")
rate = dict(con.execute(f"""
    SELECT reason, round(100.0 * count(*) / {staged}, 3) FROM (
      SELECT unnest(string_split(reject_reason, '; ')) AS reason
      FROM flagged WHERE reject_reason IS NOT NULL) GROUP BY 1""").fetchall())
breaches = [(c, rate.get(c, 0.0), t)
            for c, t in THRESHOLDS.items() if rate.get(c, 0.0) > t]
if breaches:
    for check, got, limit in breaches:
        msg = (f"DQ HARD FAIL {DAY}: {check} at {got}% "
               f"exceeds contract tolerance {limit}%")
        con.execute("INSERT INTO ops.alerts VALUES "
                    "(current_timestamp, NULL, 'error', 'dq_gate', ?)", [msg])
        print("ALERT:", msg)
    raise RuntimeError(f"DQ HARD FAIL {DAY}: {len(breaches)} contract "
                       f"tolerance(s) breached. Nothing loaded to core.")

# 3) the good rows
con.execute("""
-- grain: one row = one delivered version of an order (unique on order_id,
-- _data_date). Not one row per order: ~1.5% of every file re-delivers a prior
-- day's order as a correction, and A08 is where you fold those down.
CREATE TABLE IF NOT EXISTS core.orders AS
SELECT * EXCLUDE (reject_reason), CAST(order_ts AS DATE) AS order_date
FROM flagged WHERE 1 = 0;              -- WHERE 1=0: clone the shape, zero rows
""")
con.execute("""
INSERT INTO core.orders
SELECT * EXCLUDE (reject_reason), CAST(order_ts AS DATE) AS order_date
FROM flagged WHERE reject_reason IS NULL;
""")
```

Five deliberate details, each worth a journal line:

- **What a quarantined row carries.** Its whole payload, plus `reject_reason` and `_rejected_at`
  (why and when it was rejected — payload facts about *this row*), plus the two §7.2 lineage
  columns and nothing more. That is enough for forensics: `_data_date` names the delivery — one
  file per business date, so 2026-06-02 *is* `orders_2026-06-02.csv` — and from A07 on `_run_id`
  joins `ops.etl_runs` for the file name, the load timestamp and the run's status. Stamping the
  file name onto all 992 rows would store one fact 992 times.
- `COALESCE(..., DATE '{DAY}')` — a row with an unparseable timestamp has no `order_date`; it
  borrows the file's date rather than becoming a NULL partition key.
- `FILENAME_PATTERN` — late bad rows land in *earlier* `order_date=` partitions, so consecutive
  days write into shared folders; default naming would let day D+1 overwrite day D's
  `data_0.parquet` there. Per-day names coexist, and a same-day re-run overwrites only itself.
- `ops.alerts` is created here and reused for the rest of the lab: A07 writes `error` alerts
  when a run dies for good, A11 when a file misses its SLA. Its `run_id` stays NULL for the
  same reason `_run_id` does — until A07, whose `BIGINT` run-ledger ids the column is typed
  to hold. The `RuntimeError` doubles as the non-zero exit
  code; in production that line is where the pager fires.
- Plain `INSERT` into core is **not** rerun-safe: run it twice, the day is in core twice. Live
  with it today (re-run = `DROP TABLE core.orders` and reload); A07 fixes this properly.

Now look inside `Get-ChildItem "$env:ETL_LAB_DATA\small\quarantine\orders"`.

**Accept when:** `core.orders` has 58,032 rows (= 59,024 − 992), the quarantine Parquet holds
992 rows, `SELECT count(*) FROM ops.alerts` returns 0 (a clean day pages nobody), and you can
explain why a folder `order_date=2026-06-01` exists even though you loaded 2026-06-02.

### Task 7 — `ops.dq_report` — the suite's paper trail (small)

Checks nobody can see later might as well not have run. Every run appends one row **per (date,
check)** — including zero-failure checks. Zeros matter: `bad_payment = 0` *proves* A03's
normalization works.

```python
con.execute("""
CREATE TABLE IF NOT EXISTS ops.dq_report (
  run_at TIMESTAMP, order_date DATE, check_type VARCHAR, check_name VARCHAR,
  rows_checked BIGINT, rows_failed BIGINT, pct_failed DOUBLE,
  threshold_pct DOUBLE, passed BOOLEAN);
""")
# row checks: observed failure counts, left-joined onto the FULL check list.
# Thresholds flow YAML -> THRESHOLDS -> SQL; checks the contract does not
# budget stay informational (threshold NULL, passed true)
check_rows = ", ".join(
    f"('{c}', {THRESHOLDS.get(c, 'NULL')})"
    for c in ['null_order_id','bad_order_ts','ts_out_of_range','bad_status',
              'bad_payment','null_total','negative_total','total_mismatch',
              'null_customer','orphan_customer','orphan_store','orphan_sku'])
con.execute(f"""
INSERT INTO ops.dq_report
WITH observed AS (
  SELECT reason, count(*) AS n FROM (
    SELECT unnest(string_split(reject_reason, '; ')) AS reason
    FROM flagged WHERE reject_reason IS NOT NULL)
  GROUP BY 1
)
SELECT current_timestamp, DATE '{DAY}', 'row', c.check_name,
       {staged}, coalesce(o.n, 0),
       round(100.0 * coalesce(o.n, 0) / {staged}, 3) AS pct,
       c.threshold_pct,
       coalesce(pct <= c.threshold_pct, true)
FROM (VALUES {check_rows}) c(check_name, threshold_pct)
LEFT JOIN observed o ON o.reason = c.check_name;
""")
# aggregate: (order_id, updated_at) must be unique — Task 4's claim, proven
con.execute(f"""
INSERT INTO ops.dq_report
SELECT current_timestamp, DATE '{DAY}', 'aggregate', 'unique_order_id_updated_at',
       {staged}, count(*), round(100.0 * count(*) / {staged}, 3), 0.0, count(*) = 0
FROM (SELECT order_id, updated_at FROM staging.shopcore_orders_day
      GROUP BY 1, 2 HAVING count(*) > 1);
""")
# aggregate TODO — YOU write 'dup_order_id': same shape as the uniqueness
# check but grouped by order_id alone; informational (threshold NULL,
# passed true). Expected in-file corrections; resolving them is A08's job.

# reconciliation: our count vs the producer's manifest
con.execute(f"""
INSERT INTO ops.dq_report
SELECT current_timestamp, DATE '{DAY}', 'reconciliation', 'rowcount_vs_manifest',
       {staged}, abs({staged} - rows), NULL, 0.0, {staged} = rows
FROM read_json_auto('{MANIFEST}');
""")
# aggregate TODO — YOU write 'quarantine_rate', the day-level overall rate:
# rows_failed = bad, pct_failed = pct, threshold_pct = NULL (the contract
# budgets per rule, not per day), passed = true. Informational — and the
# row Stretch goal 1's trend chart is built on.
```

**Accept when:** `SELECT count(*) FROM ops.dq_report WHERE order_date = DATE '2026-06-02'`
returns **16** (12 row + 3 aggregate + 1 reconciliation), every `passed` is true, and
`threshold_pct` carries the contract's number on exactly the six mapped row checks.

> **Violation or gap?** Almost every reason in today's quarantine breaks a rule *written in
> the contract* — a **violation** to count and report to shopcore. Not `null_customer`: the
> contract pins `customer_id` as `nullable: true` ("Null = guest checkout"), so no clause is
> broken there. Classifying the whole pile — violation, gap, or tolerated — is A06's opening
> exercise on exactly this output.

### Task 8 — Wrap it into `work/validate_day.py`; run three days (small)

Refactor Tasks 3–7 into functions parameterized by `day`, in gate order. This module is your
next rung on the §7.4 composable-code ladder, so it plays by the ladder's rules: a docstring
stating its job and assumptions, comments that state constraints (not narration), cleaner
expressions imported from `work/cleaners.py` — and later assignments (A07's pipeline, A11's
runner) will import *this* module the same way, never re-paste it.

```python
def validate_day(con, day: str) -> None:
    staged = stage_day(con, day)          # Task 3
    flag_rows(con, day)                   # Tasks 4 + 5
    bad = con.execute("SELECT count(reject_reason) FROM flagged").fetchone()[0]
    pct = 100.0 * bad / staged
    report(con, day, staged, bad, pct)    # Task 7
    quarantine_bad(con, day)              # Task 6 step 1
    check_gate(con, day, staged)          # Task 6 step 2 — alerts, then raises
    load_good(con)                        # Task 6 step 3
    print(f"{day}: staged={staged} core+={staged - bad} "
          f"quarantined={bad} ({pct:.2f}%)")
```

Start clean — everything below `raw/` is derived and disposable, so rebuilding is always allowed:
`con.execute("DROP TABLE IF EXISTS core.orders; DELETE FROM ops.dq_report; DELETE FROM ops.alerts;")` and
`Remove-Item -Recurse -Force "$env:ETL_LAB_DATA\small\quarantine\orders"`. Then run a normal day
and *both* flash-sale spike days (all inside month 1), timing each:
`for day in ["2026-06-02", "2026-06-19", "2026-07-11"]: validate_day(con, day)`.

**Accept when:** all three days pass in a few seconds total, and — the key observation — the
spike days have ~3× the *rows* but the same ~1.7% quarantine *rate*. Volume changed, quality
didn't; that is exactly why the threshold is a percentage, not a row count. Journal it.

### Task 9 — Freshness — is core current at all? (small)

Every check so far judged one day's *content*; none of them notices when the pipeline simply
stops running. That is freshness's job, and A03's `_data_date` makes it a one-liner. What
"expected" means comes from the contract's `delivery.schedule` (file complete by 02:00 UTC on
D+1): in production the expected latest day is simply *yesterday*, and A11 wires that clock to
the real SLA. Today, hand-set it to the latest day you loaded. New file
`work/freshness_check.py`:

```python
import sys
import duckdb
from lib.labpaths import warehouse_path

SCALE, FRESH_EXPECT = "small", "2026-07-11"   # latest day core should contain

con = duckdb.connect(str(warehouse_path(SCALE)))
latest, stale = con.execute(f"""
    SELECT max(_data_date), max(_data_date) < DATE '{FRESH_EXPECT}'
    FROM core.orders""").fetchone()
if stale:
    msg = (f"FRESHNESS BREACH: core.orders max(_data_date) = {latest}, "
           f"expected {FRESH_EXPECT}")
    con.execute("INSERT INTO ops.alerts VALUES "
                "(current_timestamp, NULL, 'error', 'freshness', ?)", [msg])
    print("ALERT:", msg)
    sys.exit(1)                               # non-zero exit: the scheduler pages
print(f"fresh: core.orders is at {latest}")
```

Run it from the repo root with `python -m work.freshness_check` (A03's `-m` habit — by path,
`lib` doesn't import): `fresh: core.orders is at 2026-07-11`. Now the breach demo — set
`FRESH_EXPECT` to `"2026-07-12"` (a day you never loaded) and re-run:

```powershell
python -m work.freshness_check; $LASTEXITCODE     # the ALERT line, then 1
```

**Accept when:** the green run exits 0; the breach run prints the ALERT, `$LASTEXITCODE` is 1,
and `ops.alerts` holds exactly one new row (`severity = 'error'`, `source = 'freshness'`). Set
`FRESH_EXPECT` back to `2026-07-11`.

### Task 10 — Break it on purpose (small)

A gate you have never seen fire is a rumor. Do **not** edit the contract YAML to stage the
failure — that file is a signed agreement, not your config. Override the loaded value in code
instead: after building `THRESHOLDS`, add `THRESHOLDS["total_mismatch"] = 0.1` (pretend an
amendment tightened it tenfold), re-run `2026-06-19`.

**Accept when:** you get the `RuntimeError` (`1 contract tolerance(s) breached`);
`SELECT count(*) FROM core.orders` is exactly what it was after Task 8 (the failed day added
nothing); the newest `total_mismatch` row for 06-19 in `ops.dq_report` shows the overridden
`threshold_pct = 0.1` and `passed = false` — the report row exists *even though the run died*,
which is why reporting happens before the gate; and `ops.alerts` gained one `dq_gate` error
naming the check. Delete the override line.

### Task 11 — Full scale (full)

Flip `SCALE = "full"` and re-run Task 8's three days. Every path — warehouse, quarantine, tmp —
re-roots automatically via `labpaths`, so small and full never mix. While the spike day runs,
watch Task Manager: python should climb to roughly 2–3 GB (unnest plus three dim joins on 3.6M
rows — your heaviest run yet), all cores busy, well inside the 8 GB limit. On the baseline
machine expect roughly 5–10 s for a normal day, 15–25 s for a spike day.

**Accept when:** all three days pass; staged counts equal the full-scale manifests exactly
(1,180,490 / 3,643,024 / 3,602,310); the quarantine rate is still ~1.7% at 20× the volume;
timings are in your journal next to the small-scale ones.

### Task 12 — Tool bridge: the same checks, declared — pandera (small)

You have hand-rolled a validation suite, which is the only honest way to meet a validation
*framework*: you now know exactly what it would be doing for you. Back to `SCALE = "small"`, and
install the one optional library (§7.5's rule is that the tool never replaces the hand-built
version — it stands next to it):

```powershell
pip install pandera     # 0.32 at the time of writing; it pulls typeguard,
                        # typing-inspect and mypy-extensions, all pure python
```

pandera validates a **DataFrame** against a **schema object**: columns, dtypes, nullability and
value rules declared in one place instead of spread down a `CASE` chain. Express three of *your
own* Task 5 checks — non-null key, status enum, non-negative total — over one day's staging
pulled into pandas. New file `work/a05_pandera_demo.py`:

```python
import duckdb
import pandera.pandas as pa
from lib.labpaths import warehouse_path
from work.validate_day import stage_day     # Task 8's module — imported, never re-pasted

SCALE, DAY = "small", "2026-06-02"
STATUSES = ['created', 'paid', 'shipped', 'delivered', 'cancelled', 'refunded']

con = duckdb.connect(str(warehouse_path(SCALE)))
stage_day(con, DAY)                           # staging holds whichever day ran last
df = con.execute("""
    SELECT order_id, status, order_total FROM staging.shopcore_orders_day
""").df()                                     # 3 columns only — see the size note below
print(f"{DAY}: {len(df):,} rows -> pandas; order_total dtype = {df.order_total.dtype}")

schema = pa.DataFrameSchema(
    {
        "order_id":    pa.Column("int64", nullable=False),
        "status":      pa.Column(nullable=False, checks=pa.Check.isin(STATUSES)),
        "order_total": pa.Column("float64", nullable=False, checks=pa.Check.ge(0)),
    },
    name="staging.shopcore_orders_day",
    strict=False,                             # tolerate columns you did not declare
)

try:
    schema.validate(df, lazy=True)            # lazy: collect ALL failures, not just the first
    print("pandera: all checks passed")
except pa.errors.SchemaErrors as err:
    fc = err.failure_cases.assign(            # drop each check's argument repr
        check=lambda d: d["check"].str.split("(").str[0])
    print(fc.groupby(["column", "check"]).size().to_string())
    print(fc.dropna(subset=["failure_case"])[
        ["column", "check", "failure_case", "index"]].head(5).to_string(index=False))
```

`import pandera.pandas as pa` is the current namespace for the pandas API (older builds import
the same objects as plain `import pandera as pa`). Three arguments carry the lesson: `lazy=True`
collects *every* failure instead of raising on the first — the same reason your `concat_ws`
collects every `reject_reason`; `strict=False` tolerates undeclared columns, which is the
contract's own `change_management` rule ("consumer must tolerate unknown columns"); and
`nullable=False` is a *separate* statement from the value check, which turns out to matter.
Run it from the repo root — `python -m work.a05_pandera_demo`:

```
2026-06-02: 59,024 rows -> pandas; order_total dtype = float64
column       check
order_total  greater_than_or_equal_to     52
             not_nullable                117
status       isin                         35
             not_nullable                 36
     column                    check failure_case  index
order_total greater_than_or_equal_to      -105.73  38814
order_total greater_than_or_equal_to       -54.08  25107
order_total greater_than_or_equal_to      -383.83  25163
order_total greater_than_or_equal_to       -99.35  27449
order_total greater_than_or_equal_to      -153.19  28697
```

The four counts are exact and reconcile below; the five sample rows are only whichever
offenders `failure_cases` happens to list first — its row order is not part of pandera's API,
so a different version may hand you five `status` rows instead.

**Now read that against Task 7's report.** Same rows, same rules, and the numbers reconcile:
35 + 36 = **71**, exactly your `bad_status`; 117 + 52 = **169**, exactly `null_total` +
`negative_total`. Three differences are the actual lesson:

- **pandera splits what your SQL fused.** Your `bad_status` `CASE` folded "NULL" and "not in the
  whitelist" into one reason; pandera reports them as two checks, because nullability is a
  property of the column, not a value rule. Which shape you want is a real design decision — the
  split triages faster, the fused one thresholds more simply against the contract's single
  `null_or_invalid_status: 1%`.
- **`order_id` says nothing at all.** It passed, so it emits no rows. `ops.dq_report`
  deliberately records the zeros as well (Task 7) — a schema library reports failures, and
  *proving a check ran* stays your job.
- **`err.failure_cases` is a typed DataFrame**, not a string: `column`, `check`, `failure_case`,
  `index`, one row per offending value. You can insert it into a table, join it back to the data
  by `index`, or diff two runs. That is precisely what your
  `unnest(string_split(reject_reason, '; '))` was hand-building — and it is pandera's strongest
  argument.

**When NOT to use it.** pandera validates a DataFrame, so the rows have to be *in memory* first.
Measure it (`df.memory_usage(deep=True).sum()` — `deep=True` earns its keystrokes here: `status`
is a pandas `object` column, and the shallow count charges eight bytes for a pointer and nothing
for the string it points at, about a third of the real figure) and extrapolate: on the baseline
machine these three columns for this one small day are about 4.3 MB, so the same three columns
across the full 82.3M-row dataset would be roughly 5.9 GB — and all twelve staging columns
roughly 36 GB, past your 8 GB limit, in order to check rules DuckDB evaluates without
materializing a single row. (Watch the print, too: DuckDB's `DECIMAL(14,2)` lands in pandas as
`float64` — money turned into binary floating point on the way out the door. A second reason to
judge money where it is stored.) That is the whole reason the lab's pipeline keeps the SQL
suite: **the checks run where the 82M rows already are.** Reach for pandera where the data is
already a DataFrame and already small — a feature frame, an API payload, a test fixture — or as
the *readable* statement of the rules a reviewer approves, standing beside the SQL that enforces
them at scale. Do not swap it in for the gate.

**One size up: Great Expectations / Soda.** Where pandera hands you a schema object, **Great
Expectations** and **Soda** hand you the apparatus around it. An *expectation suite* is a named,
versioned, reusable rule set — your `THRESHOLDS` dict and check list grown up and shared between
pipelines. A *results store* persists every validation run with its history — that is
`ops.dq_report`, but managed, queryable and retained by the tool rather than by you. And **data
docs** are the piece you have no equivalent for: generated HTML showing which expectations
passed and failed, per column, per run, over time — something you can point a finance analyst or
a producer at, which `SELECT * FROM ops.dq_report` is not. Both also plug into orchestrators, so
a failing suite stops a DAG the way your `RuntimeError` stops your script. The lab names them
and stops there on purpose: each brings its own config layer, its own datasource/backend wiring
and a real learning curve, and installing one here would swap today's lesson — what a check *is*
and where the gate must sit — for a lesson in someone else's YAML. Know they exist, know what
they add over what you built, and reach for one the day your team has more suites than a person
can hold in their head.

**Accept when:** the schema raises `SchemaErrors` on 2026-06-02 (not a clean pass — that would
mean you pointed it at `core.orders`, which is validated data by construction); the failure
counts reconcile to `ops.dq_report` as above (status 71, order_total 169); and your journal
names one check you would keep in SQL forever and one you would rather read as a schema, with
the reason for each.

## Definition of Done

- [ ] `work/validate_day.py` runs a day end to end: stage → flag → report → quarantine → gate → load.
- [ ] Every threshold in the gate and in `ops.dq_report.threshold_pct` is loaded from
      `contracts/shopcore_orders_daily.contract.yaml` via pyyaml — no hard-coded tolerance
      anywhere in `work/`.
- [ ] `ops.dq_report`: 16 rows per day for ≥3 days incl. a spike day, both scales, all
      `passed = true` under the contract's tolerances.
- [ ] Quarantine Parquet under `<DATA_ROOT>/quarantine/orders/order_date=*/` with
      `reject_reason`, `_rejected_at` and the two §7.2 lineage columns (`_data_date`,
      `_run_id`) on every row — and no other warehouse-added column; per-day file names.
- [ ] Quarantine totals ≈ 1.7% per day; per-check breakdown consistent with the documented dirt
      rates (see Verify yourself).
- [ ] One deliberately breached tolerance demonstrated: `RuntimeError` raised, `core.orders`
      unchanged, a `passed = false` row in `ops.dq_report`, an `error` row in `ops.alerts`.
- [ ] Freshness: `work/freshness_check.py` green on current core, plus one staged breach —
      `source = 'freshness'` alert row and exit code 1.
- [ ] Tool bridge: `work/a05_pandera_demo.py` states three of your checks as a pandera
      `DataFrameSchema` and its failure counts reconcile with `ops.dq_report` for 2026-06-02
      (status 35 + 36 = 71; order_total 117 + 52 = 169). The pipeline still gates on the SQL.
- [ ] Journal: small vs full timings, spike vs normal day, the rate-vs-volume observation, and
      the pandera-vs-SQL call (which rule belongs in which form, and why).

## Verify yourself

Numbers assume the reference rules — explain every gap. Small scale, after Task 8's run:

```sql
SELECT count(*) FROM core.orders;
-- 414095   (58032 + 179044 + 177019)

SELECT check_name, rows_failed, pct_failed FROM ops.dq_report
WHERE order_date = DATE '2026-06-02' AND check_type = 'row'
ORDER BY rows_failed DESC;
-- orphan_sku 310 · total_mismatch 308 · null_total 117 · null_customer 94
-- bad_status 71 · negative_total 52 · orphan_customer 27 · bad_order_ts 18
-- ts_out_of_range, bad_payment, orphan_store, null_order_id: all 0

SELECT count(*) FROM ops.dq_report;
-- 48 (16 × 3 days; Task 10's failed run will append 16 more)
```

The per-check sum for 06-02 (997) exceeds the quarantined row count (992): five rows failed two
checks at once. Confirm: count quarantine rows where `_data_date = DATE '2026-06-02'
AND reject_reason LIKE '%;%'` → 5. (`_data_date` is how you pick one delivery out of the
quarantine pile — the folders are partitioned by `order_date`, which late bad rows do *not*
share with the file they arrived in.)

Full scale: reconciliation pins staged counts to the manifests exactly (2026-06-02 → 1,180,490);
quarantined should be roughly 19,700 (~1.67%) that day and roughly 60,600 (~1.66%) on
2026-06-19. Task 10's breach (small scale) looks like:

```
ALERT: DQ HARD FAIL 2026-06-19: total_mismatch at 0.522% exceeds contract tolerance 0.1%
RuntimeError: DQ HARD FAIL 2026-06-19: 1 contract tolerance(s) breached. Nothing loaded to core.
```

And after the Task 9 + Task 10 demos, the pager trail reads:

```sql
SELECT severity, source, message FROM ops.alerts ORDER BY alert_ts;
-- error · freshness · FRESHNESS BREACH: core.orders max(_data_date) = 2026-07-11, expected 2026-07-12
-- error · dq_gate   · DQ HARD FAIL 2026-06-19: total_mismatch at 0.522% exceeds contract tolerance 0.1%
```

## Common beginner mistakes

- **Validating raw text instead of cleaned staging.** `" CARD "` gets flagged as bad payment and
  your quarantine rate triples with rows your own cleaners would have saved.
- **`unnest` next to `GROUP BY` in one SELECT.** Binder error, every time. Explode in a
  subquery, aggregate outside (Task 4's shape).
- **`COPY` dying with "Failed to create directory".** Partitioned `COPY` creates the
  `order_date=…` folders but not missing *parents* — `mkdir(parents=True)` the target first.
- **Gating after loading.** A threshold checked after the `INSERT` means the rotten day is
  already in core and "fail the day" means manual cleanup. Count → quarantine → gate → load.
- **Quarantining duplicate order_ids.** In-file duplicates are expected corrections (latest
  `updated_at` wins — A08's job), not dirt. Report the rate; reject nothing. A quarantine count
  ~30 rows above the reference usually means this.
- **Re-running a green day and doubling core.** Plain `INSERT` is not idempotent. Until A07,
  re-run = drop and reload; counts at exactly 2× the manifest are this, nothing else.

## Stretch goals

1. **Trend line.** Run all 45 v1 days at small scale (~2 minutes) and eyeball `quarantine_rate`
   by day from `ops.dq_report`. It should sit flat at ~1.7% — spikes included. A rate chart like
   this is the most useful DQ dashboard in real teams.
2. **Volume band.** The contract's `volume_expectation` clause says loads outside 0.5×–4× of
   the trailing-7-day median should alert. Implement it as an aggregate check over the
   `rows_checked` history in `ops.dq_report` (run stretch 1 first for history), writing a
   `warn`-severity alert on breach. Confirm the spike days stay inside the band — that width
   is deliberate.
3. **Quarantine replay.** Write `work/replay_quarantine.py`: read quarantined rows for a date
   range, re-run the current checks over them, print how many would pass today — how you recover
   rows after loosening a too-strict rule, and a first taste of A11's backfill thinking.
4. **Tests for two checks.** Follow `tests/test_cleaners.py`'s pattern (A03: each rule run over
   a VALUES table of hand-picked rows in an in-memory DuckDB): write `tests/test_checks.py`
   covering two check expressions — good candidates are `total_mismatch` (equal totals, off by
   exactly 0.01 — the boundary passes — and off by 0.02) and `ts_out_of_range` (inside the
   window, the day before it, the day after). `python -m pytest tests/ -q` green before the
   branch merges.
