# A03 — Transformation: staging → core

**Topic:** transformation, cleansing & type normalization · **Est. time:** 4–6 h · **Difficulty:** ⭐⭐⭐
**Requires:** A02 done; month-1 raw data at both scales (from A00). Nothing from month 2.

## Why this matters

Our daily feed from **shopcore**, the e-commerce platform, is about 97% clean — the dangerous
kind of clean, because the 3% hides until it corrupts a report.
At full scale, 0.2% of one day is ~2,400 rows: far too many to fix by hand,
so you fix them in code. Every real data team solves this the same way: a layered warehouse —
raw stays untouched, *staging* makes it clean and typed, *core* makes it joined and
meaningful. Today you build that machine, and learn the habit that marks a professional:
**every cleaning rule counts the rows it touched**. A fix nobody can audit is data loss with
better PR.

## Background

**The layered model.** Three layers, three jobs:

- **raw** — files exactly as delivered. Never edited: the one thing you can always rebuild from.
- **staging** — one row in, one row out (minus provable duplicates). Same meaning as raw, but
  every column has the *right type* and dirty values are normalized. Nothing joined yet.
- **core** — business tables: staging joined to dimensions, derived columns, aggregates.

Why the middle layer? It isolates blame. When a number looks wrong: wrong in raw = upstream's
fault; wrong in staging = your cleaning; wrong in core = your joins. Without layers, every bug
hunt starts from zero.

> **Naming: the warehouse is a shared namespace** *(lab standard, SPEC §7.1).* A warehouse
> hosts data from many applications, so a table name must say *whose* data it holds. Staging
> tables are therefore **source-prefixed**: our orders land as `staging.shopcore_orders`; a
> second app's users would land as `staging.adsight_users`. Bare `users` in a shared schema is
> a collision waiting to happen. `core` earns its short names (`core.customers`) because that
> layer *conforms* sources into one agreed definition — but generic names (`data`, `final`,
> `tmp2`) stay banned. Helper tables that conform one source's values take the prefix too
> (`staging.shopcore_country_map`, Task 6). Lake directories stay short (`lake/orders/`) only
> because this lab has a single source; a multi-source lake would use `lake/shopcore/orders/`.

> **Lineage columns — exactly two** *(lab standard, SPEC §7.2).* Every staging and core row
> carries two warehouse-added columns the source never ships — ingest stamps them on, and the
> underscore prefix marks them as metadata per the naming rules:
>
> - **`_data_date` (DATE)** — the business date of the delivery this row came from.
>   Load-bearing, not decoration: it is the DELETE key for idempotent reloads (A07/A08), the
>   partition key, and the staleness probe — `max(_data_date)` answers "how fresh is this
>   table?" in one query, which is exactly what A05's freshness alert runs.
> - **`_run_id` (BIGINT)** — which pipeline run wrote this row. NULL for now; it comes alive
>   in A07, when runs get a ledger for it to point at.
>
> **And no more — the restraint is the lesson.** A junior's instinct is to also stamp
> `_source_file` and `_loaded_at` on every row, but those are facts about the *run*, not about
> the *order*: they belong on the single `ops.etl_runs` row that `_run_id` points at, where A07
> will join back to them to recover the file, the load time, the status and the row count for
> any row you pick. Copy them onto the rows instead and you store 1.2M identical strings, then
> rewrite all 1.2M of them whenever the truth about one load changes. In this feed the file name
> is redundant anyway: the contract's `path_pattern` is one file per business date, so
> `_data_date` *is* `orders_<_data_date>.csv` — storing both is storing one fact twice. (A feed
> with many files per date genuinely needs the name; it still belongs on the ledger row.)

> **Modeling: grain, facts, keys** *(lab standard, SPEC §7.7).* Before you build a table, say
> what one of its rows *is*. That sentence is its **grain**, and it belongs on the DDL as a
> comment: a table whose grain you cannot state in one line is a table you have not designed.
> Every DDL below carries its grain as a comment — read them as you build. One pair is worth
> meeting up front, because the two read like the same table and are not:
>
> - `staging.shopcore_orders_raw` — one row = one **order version as delivered** in the
>   2026-06-02 file. A corrected order ships twice.
> - `staging.shopcore_orders` — one row = one **order** in that delivery, latest version only.
>
> The gap between those two sentences is all of Task 4.
>
> **Fact or dimension?** `core.orders_enriched` records *events* at order grain — an order
> happened at `order_ts` — and carries **measures** you add up: `order_total`, and the row count
> itself. That makes it a **fact**. The dims describe the *entities* a fact points at and carry
> the attributes you filter and group by (`country`, `region`, `category`). Which table is which
> is a decision you make, not a label the source ships; A15 recaps it and names the fact *types*.
>
> **Keys.** `order_id` is a **natural key**: shopcore mints it, so it is free and meaningful —
> and outside your control. A10 is where that bill arrives.
>
> Fuller treatment, with every lab table's grain in one place: `guides/data_modeling.md`
> §1–§2. (Stop there for now — §3 onward answers questions you haven't been asked yet.)

**Cleansing vs. validation.** Today you *fix what has an obvious fix*: `"$79.98"` is clearly
79.98, `"Paid"` is clearly `paid`. Values with no defensible fix (an impossible date, status
`"unknown"`) become **NULL** — the honest way to say "we don't know". Deciding whether a whole
*row* is too broken to load is validation — A05's job. Nothing is thrown away today except
exact-key duplicates.

**Why types matter.** A `VARCHAR` column full of numbers is a bug magnet: `'9' > '10'` is true
for strings, and a date kept as text can't be compared, bucketed, or partitioned. The staging
rule: when data leaves staging, *numbers are numeric, timestamps are timestamps, booleans are
boolean*. Two tools do most of the work: `TRY_CAST` (NULL instead of an explosion on a bad
value — but you must count those NULLs or you lose data silently) and `try_strptime` (the same
idea for parsing timestamps in given formats).

**Counted, never silent.** You will build a *cleaning catalog* — one rule per known pathology —
and a **cleaning report**: `rule → rows_affected` for the day, compared against the documented
dirt rates (SPEC §3.5). If a rule suddenly touches 40% of rows instead of 0.4%, something
upstream changed, and your report catches it on day one. The catalog is not folklore, either:
most of its rules mirror clauses of the signed data contract with shopcore
(`contracts/shopcore_orders_daily.contract.yaml`) — A05 loads its tolerances straight from
that YAML, and A06 teaches you to read it clause by clause.

**CASE vs. mapping table.** Small normalizations fit in a `CASE` expression. Long lists of
value-variants (country spellings) belong in a *mapping table* you extend with an `INSERT`,
not a code edit. The pattern is called **conformance**: many spellings in, one canonical code
out. You build one today.

**Code that graduates, and tests that pin it.** Today's cleaning rules are not throwaway:
A05 will check values with the very same rules, A07's pipeline will rebuild staging with them
on every run, A15 ports them into dbt. So a lab-wide engineering rule (SPEC §7.4) starts
here: SQL that has proven itself moves into a small Python module — `work/cleaners.py`,
Task 7 — and from then on later code **imports** it, never re-pastes it. Why so strict?
Two copies of a rule always drift apart, and the copy you forgot about is the one that lies.
A module also buys you something a SQL snippet can never have: a test file. Task 8 pins
every pathology you fix today into `tests/test_cleaners.py`, so it *stays* fixed while the
module evolves under the rest of the lab.

## Setup

- Terminal in the repo root, venv active. **Scale: small for Tasks 1–9**; Task 10 goes full.
- Git ritual (SPEC §7.4): `git switch -c a03-transformation` before anything else; commit
  after each numbered task (`git commit -m "A03 task 4: staging cleaners assembled"`); merge
  to `main` only when the Definition of Done — pytest included — is green. Rusty?
  `guides/git_basics.md`.
- Start Python with `python -i -m work.a03_bootstrap` (`-i` = stay interactive afterwards —
  perfect for running SQL bit by bit; `-m` = run as a *module* from the repo root, which puts
  the repo root on Python's import path — the reason `lib.labpaths` below, and `work.cleaners`
  from Task 7 on, are importable at all; `python -i work/a03_bootstrap.py` would die with
  `ModuleNotFoundError`). Create that file first:
```python
# work/a03_bootstrap.py
import duckdb
from lib.labpaths import data_root, warehouse_path, ensure_dirs

SCALE = "small"; ensure_dirs(SCALE)   # Task 10 switches this to "full"
ROOT = data_root(SCALE)               # <DATA_ROOT>/small   (full scale: <DATA_ROOT>)
R = ROOT.as_posix()                   # forward slashes for SQL string literals

con = duckdb.connect(str(warehouse_path(SCALE)))
con.execute("SET memory_limit='8GB'; SET threads=8; SET TimeZone='UTC';")   # TZ: see Task 6
con.execute(f"SET temp_directory='{R}/tmp';")
```

Peek with `con.sql("""...""").show()`; run DDL with `con.execute`. Snippets containing `{R}`
run as f-strings.

## Tasks

### Task 1 — Create the warehouse schemas *(small)*

One database file, four schemas — this layout is the lab convention from here to A15:
```sql
CREATE SCHEMA IF NOT EXISTS staging;   -- clean, typed, 1:1 with raw
CREATE SCHEMA IF NOT EXISTS core;      -- joined, enriched
CREATE SCHEMA IF NOT EXISTS marts;     -- aggregates for consumers (A04+)
CREATE SCHEMA IF NOT EXISTS ops;       -- pipeline bookkeeping: reports, run logs (A05, A07)
```

**Accept when:** `SELECT schema_name FROM information_schema.schemata WHERE schema_name IN
('staging','core','marts','ops')` returns all four rows.

### Task 2 — Land one day, exactly as delivered *(small)*

We work with **2026-06-02** all day (any v1 date works; the expected numbers assume this one).
Load it 1:1 into a raw staging table — source-prefixed per the naming box, stamped with the
two lineage columns per the lineage box. Note which columns we *refuse* to type yet:
```python
con.execute(f"""
-- grain: one row = one order version as delivered in the 2026-06-02 file
CREATE OR REPLACE TABLE staging.shopcore_orders_raw AS
SELECT *,
    DATE '2026-06-02'    AS _data_date,   -- lineage: the delivery's business date
    CAST(NULL AS BIGINT) AS _run_id       -- lineage: comes alive in A07
FROM read_csv('{R}/raw/orders/orders_2026-06-02.csv',
    header=true,
    columns={{'order_id':'BIGINT','customer_id':'VARCHAR','store_id':'INTEGER',
             'order_ts':'VARCHAR','updated_at':'TIMESTAMP','status':'VARCHAR',
             'payment_method':'VARCHAR','order_total':'VARCHAR',
             'items':'VARCHAR','meta':'VARCHAR'}});
""")
```

The file never contained those `_` columns — ingest adds them, and from here they ride along
into every table we build. Twelve columns out, then: the file's ten, plus the two. `order_ts`
and `order_total` stay VARCHAR because they contain dirt we must clean ourselves.
But why `customer_id`, when A01's explicit schema read it as BIGINT and *worked*? "Worked" hid
something:
```sql
SELECT count(*) FILTER (WHERE customer_id IS NULL)     AS missing,
       count(*) FILTER (WHERE customer_id LIKE '%.0')  AS excel_float
FROM staging.shopcore_orders_raw;
-- -> 94 missing, 120 excel_float
```

(`FILTER (WHERE …)` restricts just that one aggregate — a per-aggregate `WHERE`, letting one
`SELECT` count several conditions at once.)

Re-run that count with a BIGINT read (swap the column type): 94 NULLs and *no way to see the
rest* — the CSV reader silently converted 120 Excel-mangled values like `"123456.0"`.
Convenient, and unaccountable. A cleaning layer reads dirty columns as text first so it can
**see and count** what it fixes. That's this whole assignment in one query.

**Accept when:** `SELECT count(*) FROM staging.shopcore_orders_raw` returns **59,024** — exactly the
`rows` field of the manifest: `json.loads((ROOT/'raw'/'manifest'/'orders_2026-06-02.json').read_text())['rows']`.

### Task 3 — Build the cleaning rules, one at a time *(small)*

Never write a 60-line cleaning query in one go: build each rule as a small `SELECT`, eyeball,
count, then assemble. **Always look at the dirt first.**

**a) Timestamps — three formats plus impossible dates.** Meet the enemy first:
`SELECT order_ts FROM staging.shopcore_orders_raw WHERE order_ts LIKE '%/%' OR order_ts LIKE '%T%'
LIMIT 5;` The cleaner is `try_strptime` with a *list* of formats — it tries each in order and
returns NULL if none fits (which catches the ~0.02% impossible dates like `2026-06-31 12:00:00`):
```sql
try_strptime(order_ts, ['%Y-%m-%d %H:%M:%S', '%d/%m/%Y %H:%M:%S', '%Y-%m-%dT%H:%M:%S'])
```

Count the leftovers: rows where this returns NULL → **18** (0.03%; the docs promised ~0.02% —
on 59k rows that noise is normal).

**b) Money — four pathologies, one expression.** `order_total` contains `"$79.98"`, `"N/A"`,
empty, European decimal-comma forms `"93,32"` / `"1.234,56"`, and negatives. Walk the
canonical cleaner inside-out: comma form → kill thousands-dots, comma becomes decimal point;
otherwise trim and strip `$`; `NULLIF` turns `'N/A'` into NULL; `TRY_CAST` does the rest:
```sql
TRY_CAST(NULLIF(
  CASE WHEN order_total LIKE '%,%'
       THEN replace(replace(order_total, '.', ''), ',', '.')
       ELSE replace(TRIM(order_total), '$', '') END,
  'N/A') AS DECIMAL(14,2))
```

Test it on a values table before trusting it with real data — cheap and habit-forming:
`SELECT v, <the cleaner> FROM (VALUES ('93,32'),('1.234,56'),('$79.98'),('N/A'),(''),
('-42.10')) t(v)` must give `93.32, 1234.56, 79.98, NULL, NULL, -42.10`. Negatives survive as
negatives — on purpose. A negative total is not a *format* problem, it is a *validity*
problem; A05 quarantines those. Cleansing and validation stay separate.

**c) customer_id — Excel-float to BIGINT.** The portable trick is a two-step cast through
DOUBLE — `CAST(TRY_CAST(customer_id AS DOUBLE) AS BIGINT)` — turning `'123456.0'` → 123456.0
→ 123456. (DuckDB's direct `TRY_CAST(... AS BIGINT)` happens to tolerate a trailing `.0`, but
many engines don't; learn the detour, it travels. IDs here are ≤ 1.2M, far below where DOUBLE
loses integer precision.)

**d) Enums — status and payment_method.** `TRIM` first, then `lower`, then whitelist; anything
not in the enum becomes NULL. `status` is given — `payment_method` is yours (same pattern,
easier: casing/padding variants like `"CARD"`, `" cod "` are all fixable, so after
trim+lower there is nothing left to NULL):
```sql
CASE WHEN lower(TRIM(status)) IN
          ('created','paid','shipped','delivered','cancelled','refunded')
     THEN lower(TRIM(status)) END          AS status
```

**Accept when:** each cleaner runs standalone against `staging.shopcore_orders_raw`, touch-counts noted.

### Task 4 — Assemble `staging.shopcore_orders` *(small)*

Now the full staging table: all cleaners, plus **in-file dedupe**. About 0.05% of order_ids
appear twice *within one file*; the second copy is a correction (later `updated_at`, advanced
status) — keep the latest. A *window function* numbers rows within each group without
collapsing them: `row_number() OVER (PARTITION BY order_id ORDER BY updated_at DESC)` gives
the newest row of each order_id rank 1.
```sql
-- grain: one row = one order (its latest version in this file)
CREATE OR REPLACE TABLE staging.shopcore_orders AS
WITH cleaned AS (
    SELECT
        order_id,
        CAST(TRY_CAST(customer_id AS DOUBLE) AS BIGINT)          AS customer_id,
        store_id,
        try_strptime(order_ts, ['%Y-%m-%d %H:%M:%S', '%d/%m/%Y %H:%M:%S',
                                '%Y-%m-%dT%H:%M:%S'])            AS order_ts,
        updated_at,
        CASE WHEN lower(TRIM(status)) IN
                  ('created','paid','shipped','delivered','cancelled','refunded')
             THEN lower(TRIM(status)) END                        AS status,
        -- TODO: your payment_method cleaner from Task 3d
        -- TODO: the order_total cleaner from Task 3b
        items,          -- still a JSON string: parsing it is A04's whole show
        meta,           -- same
        _data_date, _run_id            -- lineage rides along untouched
    FROM staging.shopcore_orders_raw
),
numbered AS (
    SELECT *, row_number() OVER (PARTITION BY order_id
                                 ORDER BY updated_at DESC) AS rn FROM cleaned
)
SELECT * EXCLUDE (rn), CAST(order_ts AS DATE) AS order_date
FROM numbered WHERE rn = 1;
```

This is **in-file dedupe only**. The same order_id also reappears across *later files* (the
late corrections that grew extra partitions in A02) — handling that properly is A08's topic.
Four things to check, and one to explain:

1. `SELECT count(*) FROM staging.shopcore_orders` → **58,993** = 59,024 − 31. But the manifest says
   `dup_rows: 29` — who are the other 2? Two *late-correction* rows happen to reference the
   same corrected order — a collision the dedupe rightly folds. Lesson: never expect data to
   match theory to the row; expect to *explain* the difference.
2. `DESCRIBE staging.shopcore_orders` — 13 columns, every one typed: BIGINT ids, TIMESTAMPs,
   DECIMAL(14,2) total, DATE order_date, and the lineage pair as DATE + BIGINT (no lineage
   *strings* anywhere — that is the two-column rule showing up in the type list). Exactly four
   VARCHARs survive: `items`/`meta` (JSON-for-A04) and the two enums.
3. `SELECT order_date, count(*) FROM staging.shopcore_orders GROUP BY 1 ORDER BY 1` →
   `2026-06-01: 883` (late corrections for yesterday!), `2026-06-02: 58,092`, `NULL: 18` (the
   unparseable timestamps — A05 will quarantine them; staging keeps them, honestly NULL).
4. `SELECT max(_data_date) FROM staging.shopcore_orders` → `2026-06-02` — the staleness probe
   from the lineage box: one query says how fresh the table is, no file spelunking. On any
   real warehouse this instantly answers "did last night's load run?"; A05 automates it.

**Accept when:** all four checks pass.

### Task 5 — The cleaning report *(small)*

Make the cleaning auditable: one row per rule. Six rules are given; extend the pattern until
all **13** rules from the Verify table exist. A CSV subtlety bites three of them: **an empty
CSV field arrives as NULL, not ''** — `WHERE col = ''` matches nothing; test `col IS NULL`.
```sql
-- grain: one row = one cleaning rule for one file_date
CREATE OR REPLACE TABLE ops.cleaning_report AS
WITH raw AS (SELECT * FROM staging.shopcore_orders_raw)
SELECT DATE '2026-06-02' AS file_date, rule, rows_affected FROM (
    SELECT 'ts_ddmmyyyy_fixed' AS rule, count(*) AS rows_affected
    FROM raw WHERE order_ts LIKE '%/%'
    UNION ALL SELECT 'ts_isot_fixed', count(*) FROM raw WHERE order_ts LIKE '%T%'
    UNION ALL SELECT 'ts_unparseable_to_null', count(*) FROM raw
        WHERE try_strptime(order_ts, ['%Y-%m-%d %H:%M:%S','%d/%m/%Y %H:%M:%S',
                                      '%Y-%m-%dT%H:%M:%S']) IS NULL
    UNION ALL SELECT 'total_na_or_empty_to_null', count(*) FROM raw
        WHERE order_total IS NULL OR TRIM(order_total) = 'N/A'
    UNION ALL SELECT 'customer_id_float_fixed', count(*) FROM raw WHERE customer_id LIKE '%.0'
    UNION ALL SELECT 'dup_order_id_removed', count(*) - count(DISTINCT order_id) FROM raw
    -- TODO: total_dollar_stripped, total_decimal_comma_fixed, total_negative_kept,
    --       status_recased, status_junk_to_null, payment_recased, customer_id_missing
);
```

**Accept when:** the report has 13 rules matching the Verify table exactly, and each rate
(rows_affected / 59,024) agrees with SPEC §3.5 within noise — proof the cleaners fix the
*documented* dirt and nothing else.

### Task 6 — Clean the dimensions into core *(small)*

`customers.csv` is deliberately the dirtiest table per-capita in the lab. **Discover first**
— run these and *read* the output before coding:
```sql
SELECT country, count(*)   FROM read_csv('{R}/raw/dims/customers.csv', header=true)
GROUP BY 1 ORDER BY 2 DESC;          -- 24 spellings of 10 countries, plus NULL
SELECT is_active, count(*) FROM read_csv('{R}/raw/dims/customers.csv', header=true)
GROUP BY 1 ORDER BY 2 DESC;          -- 11 flavors of true/false, plus NULL
```

**a) Country conformance table.** Start it; finish it from your discovery query (join on
lowercase so `US`/`us` need one row):
```sql
-- grain: one row = one spelling variant of one country
CREATE OR REPLACE TABLE staging.shopcore_country_map(variant VARCHAR, iso2 VARCHAR);
INSERT INTO staging.shopcore_country_map VALUES
 ('us','US'),('usa','US'),('united states','US'),
 ('vn','VN'),('vietnam','VN'),('viet nam','VN'),
 ('gb','GB'),('uk','GB'),('united kingdom','GB');
 -- TODO: DE, FR, JP, AU, CA, BR, IN variants — extend until Verify's country check passes
```

**b) Build `core.customers`.** The star exhibit: `signup_ts` is *one column with three
formats* — ISO timestamps, `DD/MM/YYYY` dates, and **epoch seconds** (integers like
`1712345678` = seconds since 1970). Digits-only rows take the epoch route via `to_timestamp`,
which returns a timezone-aware value — that is why Setup pinned the session to UTC, or every
laptop would decode epochs differently.
```python
con.execute(f"""
-- grain: one row = one customer
CREATE OR REPLACE TABLE core.customers AS
WITH raw AS (
    SELECT * FROM read_csv('{R}/raw/dims/customers.csv', header=true,
        columns={{'customer_id':'BIGINT','name':'VARCHAR','email':'VARCHAR',
                 'country':'VARCHAR','city':'VARCHAR','signup_ts':'VARCHAR',
                 'is_active':'VARCHAR'}})
),
fixed AS (
    SELECT
        customer_id,
        CASE WHEN name LIKE '%,%'    -- "NGUYEN, Linh" -> "Linh Nguyen"
             THEN TRIM(split_part(name, ',', 2)) || ' ' ||
                  upper(substr(TRIM(split_part(name, ',', 1)), 1, 1)) ||
                  lower(substr(TRIM(split_part(name, ',', 1)), 2))
             ELSE TRIM(name) END                                  AS name,
        lower(TRIM(email))                                        AS email,
        TRIM(country)                                             AS country_raw,
        city,
        CASE WHEN signup_ts IS NULL THEN NULL
             WHEN regexp_full_match(signup_ts, '[0-9]+')
                 THEN CAST(to_timestamp(CAST(signup_ts AS BIGINT)) AS TIMESTAMP)
             ELSE try_strptime(signup_ts, ['%Y-%m-%d %H:%M:%S', '%d/%m/%Y'])
        END                                                       AS signup_ts,
        CASE WHEN lower(TRIM(is_active)) IN ('true','1','yes','y','t') THEN true
             WHEN lower(TRIM(is_active)) IN ('false','0','no','n','f') THEN false
        END                                                       AS is_active
    FROM raw
)
SELECT f.customer_id, f.name, f.email, m.iso2 AS country, f.city,
       f.signup_ts, f.is_active,
       current_date         AS _data_date,   -- dims are undated snapshots: sync date = load date
       CAST(NULL AS BIGINT) AS _run_id
FROM fixed f
LEFT JOIN staging.shopcore_country_map m ON lower(f.country_raw) = m.variant;
""")
```

(~2% of names are FULL SHOUTING — counted today, fixed in the stretch goal. And beware
`TRY_CAST(is_active AS BOOLEAN)`: it accepts `'yes'` but not `'N'` — a half-right cleaner is
worse than none. Whitelists are explicit.)

**c) Products & stores.** `products.unit_price` has the European-comma disease too — reuse
the Task 3b pattern on an explicit read. The file's seven columns, all VARCHAR in the
`columns` dict: `sku`, `product_name`, `category`, `subcategory`, `unit_price`, `tags`,
`attrs`. Clean `unit_price` to `DECIMAL(10,2)`; keep `tags`/`attrs` as VARCHAR, they're
A04 material. You write that `CREATE TABLE` yourself, so write its grain on it first, like
every block above: **one row = one sku**. `stores.csv` is clean and tiny:
```sql
-- grain: one row = one store
CREATE TABLE core.stores AS SELECT * FROM read_csv('{R}/raw/dims/stores.csv', header=true);
```
Auto-detect is fine here, but `DESCRIBE` the result before you trust it. Give both tables
the two lineage columns too (same `SELECT *, …` pattern as Task 2; dims use `current_date`
as `_data_date`, since an undated snapshot's sync date is the day you pulled it) — from today,
every staging/core table carries them, and nothing carries more.

**Accept when:** the Verify-section dim checks all pass (NULL counts, no unmapped countries,
no comma-form names, no VARCHAR prices).

### Task 7 — Graduate the cleaners into `work/cleaners.py` *(small)*

Your cleaning rules now live in two places (Task 4's staging SQL, Task 6's dim SQL), about
to be four — time for the first rung of SPEC §7.4's *composable code ladder* (rationale in
Background and `guides/composable_code.md`): `work/cleaners.py`. A05's `work/validate_day.py`
and A07's `work/a07_load_day.py` (stitched into `work/pipeline.py` in A11) will import it.

The extraction is mechanical: each cleaner becomes a function returning its SQL fragment as a
string, with the column name as a parameter — so `clean_money` serves `order_total` now and
products' `unit_price` five minutes later. Note the module docstring (its *job* and its
*assumptions*, per §7.4) and that comments state constraints, not narration:
```python
"""Cleaning rules for the shopcore feed, one per known pathology (SPEC 3.5).

Job: return canonical SQL expressions (as strings) that later code embeds in
its queries with f-strings. This module is the single home of these rules --
A05's checks and A07's pipeline import it; nobody re-pastes the SQL.

Assumptions:
  - dirty columns arrive as VARCHAR (staging reads dirt as text so it can count it)
  - session TimeZone is 'UTC' (clean_signup_ts decodes epoch seconds)
  - ids fit in a DOUBLE without precision loss (ours < 1.2M; limit ~2^53)
"""

TS_FORMATS = ['%Y-%m-%d %H:%M:%S', '%d/%m/%Y %H:%M:%S', '%Y-%m-%dT%H:%M:%S']

STATUS_VALUES = ('created', 'paid', 'shipped', 'delivered', 'cancelled', 'refunded')
PAYMENT_VALUES = ('card', 'wallet', 'bank_transfer', 'cod', 'paypal')


def _sql_list(values) -> str:
    """('a','b') -> "'a', 'b'" -- values must not contain single quotes."""
    return ", ".join(f"'{v}'" for v in values)


def clean_order_ts(col: str = "order_ts") -> str:
    """Three known formats -> TIMESTAMP; anything else (incl. impossible dates) -> NULL."""
    return f"try_strptime({col}, [{_sql_list(TS_FORMATS)}])"


def clean_money(col: str = "order_total") -> str:
    """'93,32'/'1.234,56' (euro), '$79.98', 'N/A'/empty -> DECIMAL(14,2) or NULL.

    Negatives survive on purpose: sign is a validity problem (A05), not a format one.
    """
    return (f"TRY_CAST(NULLIF("
            f"CASE WHEN {col} LIKE '%,%' "
            f"THEN replace(replace({col}, '.', ''), ',', '.') "  # thousands-dots first
            f"ELSE replace(TRIM({col}), '$', '') END, "
            f"'N/A') AS DECIMAL(14,2))")


def clean_id(col: str = "customer_id") -> str:
    """Excel-float '123456.0' -> BIGINT; empty/garbage -> NULL. DOUBLE detour: portable."""
    return f"CAST(TRY_CAST({col} AS DOUBLE) AS BIGINT)"


def clean_enum(col: str, allowed: tuple) -> str:
    """TRIM + lower, then whitelist; junk -> NULL. A cleaner never guesses a category."""
    return (f"CASE WHEN lower(TRIM({col})) IN ({_sql_list(allowed)}) "
            f"THEN lower(TRIM({col})) END")


def clean_signup_ts(col: str = "signup_ts") -> str:
    """ISO / DD-MM-YYYY / epoch-seconds strings -> TIMESTAMP. Digits-only = epoch."""
    return (f"CASE WHEN {col} IS NULL THEN NULL "
            f"WHEN regexp_full_match({col}, '[0-9]+') "
            f"THEN CAST(to_timestamp(CAST({col} AS BIGINT)) AS TIMESTAMP) "
            f"ELSE try_strptime({col}, ['%Y-%m-%d %H:%M:%S', '%d/%m/%Y']) END")


def clean_bool(col: str = "is_active") -> str:
    """Mixed booleans -> BOOLEAN; unknown -> NULL. Explicit whitelists on both sides."""
    return (f"CASE WHEN lower(TRIM({col})) IN ('true','1','yes','y','t') THEN true "
            f"WHEN lower(TRIM({col})) IN ('false','0','no','n','f') THEN false END")
```

Every expression is lifted verbatim from Tasks 3–6 — extraction, not invention. Now prove
that. Keep the hand-built table, rebuild staging *through* the module, and show the refactor
changed the plumbing and not one value. In your `python -i -m work.a03_bootstrap` session:
```python
con.execute("""CREATE OR REPLACE TABLE staging.shopcore_orders_hand AS
               SELECT * FROM staging.shopcore_orders""")

from work.cleaners import (PAYMENT_VALUES, STATUS_VALUES, clean_enum, clean_id,
                           clean_money, clean_order_ts)

con.execute(f"""
-- grain: one row = one order (its latest version in this file)
CREATE OR REPLACE TABLE staging.shopcore_orders AS
WITH cleaned AS (
    SELECT
        order_id,
        {clean_id('customer_id')}                       AS customer_id,
        store_id,
        {clean_order_ts('order_ts')}                    AS order_ts,
        updated_at,
        {clean_enum('status', STATUS_VALUES)}           AS status,
        {clean_enum('payment_method', PAYMENT_VALUES)}  AS payment_method,
        {clean_money('order_total')}                    AS order_total,
        items, meta,
        _data_date, _run_id
    FROM staging.shopcore_orders_raw
),
numbered AS (
    SELECT *, row_number() OVER (PARTITION BY order_id
                                 ORDER BY updated_at DESC) AS rn FROM cleaned
)
SELECT * EXCLUDE (rn), CAST(order_ts AS DATE) AS order_date
FROM numbered WHERE rn = 1;
""")
```

The proof uses `EXCEPT` (rows in the first set but not the second): two empty EXCEPTs plus
equal counts = identical tables. It works because both builds read the *same* unchanged
`_raw` table, lineage columns and all:
```sql
SELECT
  (SELECT count(*) FROM (SELECT * FROM staging.shopcore_orders_hand
                         EXCEPT SELECT * FROM staging.shopcore_orders)) AS hand_not_module,
  (SELECT count(*) FROM (SELECT * FROM staging.shopcore_orders
                         EXCEPT SELECT * FROM staging.shopcore_orders_hand)) AS module_not_hand;
-- -> 0 | 0   (and count(*) is still 58,993)
DROP TABLE staging.shopcore_orders_hand;
```

Do the dims too: rebuild `core.customers` with `{clean_signup_ts('signup_ts')}` and
`{clean_bool('is_active')}` in place of the inline expressions, and products with
`{clean_money('unit_price')}` (add `::DECIMAL(10,2)` to keep Task 6's type). Every Task 5/6
Verify number must stay put.

**Accept when:** staging count is still **58,993**, both EXCEPT counts are 0, and the
Task 5/6 numbers are unchanged. Commit:
`git commit -m "A03 task 7: cleaners graduate to work/cleaners.py"`.

### Task 8 — Pin the rules with tests: `tests/test_cleaners.py` *(small)*

A module you import is a module you can *test* — and these rules are about to be depended on
by every remaining assignment, then edited underneath them (stretch goal 2 rewires the enums;
A10 adds new schema eras). A test file is how a fix stays fixed: each pathology you cleaned
today gets a hand-picked dirty input with its expected output pinned beside it.

The pattern (SPEC §7.4 — every future `work/` module gets a test file shaped like this): run
each cleaner over a **VALUES table** in an **in-memory DuckDB** — `duckdb.connect()` with no
path is a database born empty that dies with the test. No data files, no warehouse: the suite
runs anywhere in about a second. You already did this by hand in Task 3b when you tried the
money cleaner on a VALUES table; pytest makes the habit permanent. Create
`tests/test_cleaners.py`:
```python
"""Every pathology the lab cleans (SPEC 3.5), pinned as a test forever."""
from datetime import datetime
from decimal import Decimal

import duckdb
import pytest

from work.cleaners import (PAYMENT_VALUES, STATUS_VALUES, clean_bool,
                           clean_enum, clean_id, clean_money, clean_order_ts,
                           clean_signup_ts)


@pytest.fixture
def con():
    c = duckdb.connect()                # in-memory: born empty, gone after the test
    c.execute("SET TimeZone='UTC';")    # module assumption: epochs decode as UTC
    yield c
    c.close()


def check(con, expr, cases):
    """Run one cleaner over a VALUES table of (dirty, expected); compare row by row."""
    placeholders = ", ".join("(?)" for _ in cases)
    got = [row[0] for row in con.execute(
        f"SELECT {expr} FROM (VALUES {placeholders}) t(v)",
        [dirty for dirty, _ in cases]).fetchall()]
    assert got == [expected for _, expected in cases]


def test_money(con):
    check(con, clean_money("v"), [
        ("79.98",    Decimal("79.98")),     # clean passes through
        ("93,32",    Decimal("93.32")),     # European decimal comma
        ("1.234,56", Decimal("1234.56")),   # comma + thousands dot
        ("$79.98",   Decimal("79.98")),     # currency prefix
        ("N/A",      None),
        (None,       None),                 # empty CSV field
        ("-42.10",   Decimal("-42.10")),    # negatives survive: A05's call, not ours
    ])


def test_status(con):
    check(con, clean_enum("v", STATUS_VALUES), [
        ("delivered", "delivered"),
        ("PAID ",     "paid"),              # casing + padding
        ("Paid",      "paid"),
        ("unknown",   None),                # junk -> NULL, never a guess
        (None,        None),
    ])
```

Those two are the pattern; the other five tests you write yourself. Each follows the same
one-VALUES-row-per-pathology shape — full shape in `guides/composable_code.md` §4 — and the
import line above already names every cleaner you need: `test_order_ts` (all three formats,
the impossible `2026-06-31`, NULL), `test_customer_id` (the Excel-float `'123456.0'`),
`test_payment` (`clean_enum` again with `PAYMENT_VALUES`: casing, padding, `'bitcoin'` →
NULL), `test_signup_ts` (ISO, `DD/MM/YYYY`, and a digits-only epoch — pin the exact UTC
datetime it must decode to), and `test_is_active` (hits from both whitelists, `'maybe'` →
NULL). Your suite must land at **7 passed**.

Run it from the repo root — `python -m pytest`, and now you know why the `-m`: it puts the
repo root on the import path so `from work.cleaners import …` resolves (a bare `pytest` may
not even be on your PATH). Each dot is one passing test:
```text
> python -m pytest tests/ -q
.......                                                                  [100%]
7 passed in 1.16s
```

Now prove the suite *can* fail — a test you have never seen fail tells you nothing. Change
one expected value (say `("PAID ", "paid")` to `("PAID ", "Paid")`), rerun, read the report:
```text
FAILED tests/test_cleaners.py::test_status - AssertionError: assert ['deliver...
1 failed, 6 passed in 1.57s
```

Revert it; green again. **From this assignment on, the lab rule is: `python -m pytest
tests/ -q` is green before your branch merges to `main`.** The DoD says so, and so will
every later assignment's.

**Accept when:** 7 passed in about a second; you have watched one deliberate failure and
reverted it. Commit: `git commit -m "A03 task 8: cleaner tests"`.

### Task 9 — Enrich and aggregate *(small)*

The payoff layer. Join staging to the cleaned dims — `LEFT JOIN`, because ~0.2% of orders have
a missing or orphan customer_id and an INNER JOIN would silently drop them (A05 decides their
fate; today we keep and count):
```sql
-- grain: one row = one order — unchanged by the joins (the count below proves it)
CREATE OR REPLACE TABLE core.orders_enriched AS
SELECT
    o.*,           -- incl. both lineage columns: they ride into core for free
    c.country AS customer_country, c.is_active  AS customer_is_active,
    s.store_name, s.region                      AS store_region,
    extract(hour FROM o.order_ts)               AS order_hour,
    extract(isodow FROM o.order_date) IN (6, 7) AS is_weekend,
    CASE WHEN o.order_total IS NULL THEN 'unknown'
         WHEN o.order_total < 0    THEN 'invalid'
         WHEN o.order_total < 20   THEN 'small'
         WHEN o.order_total < 100  THEN 'medium'
         WHEN o.order_total < 500  THEN 'large'
         ELSE 'huge' END                        AS total_bucket
FROM staging.shopcore_orders o
LEFT JOIN core.customers c USING (customer_id)
LEFT JOIN core.stores    s USING (store_id);
```

**Name what you just did: a deliberate denormalization.** `customer_country`, `store_name` and
`store_region` are *dimension* attributes copied into the fact, so the everyday question
("revenue by region") reads one table instead of joining three. That convenience is bought
twice over: in storage (58,993 copies of a two-character country code) and in *freshness* —
when a customer moves country, the dim row changes and this table stays wrong until it is
rebuilt. Here the trade is worth it because the whole table is rebuilt from scratch each run;
the rule (SPEC §7.7) is that denormalization needs a stated reason, never a reflex.

Count what the join could NOT match — journal this number; A05 will hold you to it:
```sql
SELECT count(*) FILTER (WHERE customer_id IS NULL)  AS missing_id,
       count(*) FILTER (WHERE customer_id IS NOT NULL
                        AND c.customer_id IS NULL)  AS orphan_id
FROM staging.shopcore_orders o LEFT JOIN core.customers c USING (customer_id);
-- -> 94 missing, 27 orphans (ids beyond the dim — upstream bug, not yours)
```

Then the first aggregate table:
```sql
-- grain: one row = one store per order_date — the GROUP BY is a deliberate grain change,
--        the same move A04 makes with unnest (order grain -> item grain)
CREATE OR REPLACE TABLE core.daily_store_sales AS
SELECT
    order_date, store_id,
    any_value(store_name) AS store_name, any_value(store_region) AS region,
    count(*) AS orders, count(DISTINCT customer_id) AS customers,
    sum(order_total) AS gross_revenue, round(avg(order_total), 2) AS avg_order_value,
    count(*) FILTER (WHERE status = 'cancelled')   AS cancelled_orders,
    count(*) FILTER (WHERE order_total IS NULL)    AS orders_missing_total,
    any_value(_data_date) AS _data_date,  -- the §7.2 pair rides into the rollup too: one
    any_value(_run_id)    AS _run_id      -- delivery per build, so one value each per group
FROM core.orders_enriched
GROUP BY order_date, store_id;
```

Note `orders_missing_total`: aggregates must *carry* their uncertainty, not hide it — whoever
reads `gross_revenue` deserves to know 117 orders contributed nothing to it.

And note the lineage pair survives the `GROUP BY`: the two-column rule covers *every*
staging/core table, rollups included, so `max(_data_date)` is the staleness probe here exactly
as it was on staging. `any_value` is honest only because one build covers one delivery — every
row in a group shares one `_data_date` and one `_run_id`; a rollup that spanned several loads
would have to add them to the `GROUP BY`, or say `max(_data_date)` and mean *latest*.

**Accept when:** enriched count = 58,993 (nothing dropped), miss-split = 94/27,
`daily_store_sales` has 723 rows — why more than 400 stores? Late rows put *two order_dates
plus NULL* in one file. That question is on the A08 exam.

### Task 10 — Parameterize, then run at full scale *(small, then full)*

Hard-coded dates are a dead end — A07/A11 will run *ranges*. Collect the whole build
(Tasks 2–6 plus 9) into a script — every cleaning rule imported from `work/cleaners.py`,
nothing re-pasted:
```python
# work/a03_build_day.py -- usage: python -m work.a03_build_day --date 2026-06-02 --scale small
import argparse, json
import duckdb
from lib.labpaths import data_root, warehouse_path, ensure_dirs
from work.cleaners import (PAYMENT_VALUES, STATUS_VALUES, clean_enum, clean_id,
                           clean_money, clean_order_ts)   # ONE home for the rules (Task 7)

def build_day(con, r: str, day: str) -> None:
    con.execute(f"CREATE OR REPLACE TABLE staging.shopcore_orders_raw AS ...")  # Task 2 — _data_date now comes from `day`
    con.execute(f"CREATE OR REPLACE TABLE staging.shopcore_orders AS ...")      # Task 7's module-built SQL, verbatim
    con.execute(f"CREATE OR REPLACE TABLE ops.cleaning_report AS ...")          # Task 5 (file_date = day)
    ...                                                                         # Tasks 6 + 9

ap = argparse.ArgumentParser()
ap.add_argument("--date", required=True)
ap.add_argument("--scale", choices=["small", "full"], default="small")
args = ap.parse_args()
ensure_dirs(args.scale); root = data_root(args.scale)
con = duckdb.connect(str(warehouse_path(args.scale)))
con.execute("SET memory_limit='8GB'; SET threads=8; SET TimeZone='UTC';")
con.execute(f"SET temp_directory='{root.as_posix()}/tmp';")
build_day(con, root.as_posix(), args.date)
man = json.loads((root / "raw" / "manifest" / f"orders_{args.date}.json").read_text())
got = con.execute("SELECT count(*) FROM staging.shopcore_orders_raw").fetchone()[0]
print(f"{args.date}: raw={got} manifest={man['rows']} match={got == man['rows']}")
```

1. Run it with `python -m work.a03_build_day --date 2026-06-02 --scale small` (the `-m`
   form again — same repo-root reason as the bootstrap) — must reproduce every number from
   Tasks 2–9.
2. Run `--scale full` on the same date. **What to observe:** the CSV is ~350 MB / 1,180,490
   rows; expect roughly 10–30 s total on the baseline machine. Watch python's RAM in Task
   Manager — it stays far under the 8 GB limit because DuckDB streams the CSV (A01's pandas
   lesson, seen from the other side). Full-scale staging lands at 1,179,758 rows = 1,180,490
   − 590 in-file dups − 142 late-collisions. Journal both wall-clocks — A12 will make you 3×
   faster and you'll want the receipt.

**Accept when:** both scales run end-to-end from one command; full raw count matches manifest.

## Definition of Done

- [ ] `staging.shopcore_orders` for 2026-06-02 exists at both scales; small = **58,993** rows, full =
      **1,179,758**; you can explain both deltas vs. the manifests (dups + late collisions).
- [ ] `DESCRIBE staging.shopcore_orders` shows 13 columns and no VARCHAR numbers or dates —
      only items/meta (by design, until A04) and the two enum columns remain VARCHAR.
- [ ] Every staging/core table carries **exactly two** lineage columns — `_data_date` (DATE)
      and `_run_id` (BIGINT) — and no `_source_file`/`_loaded_at`, which are facts about the
      run and wait for A07's ledger. `_run_id` is NULL everywhere (A07 brings it alive), and
      `max(_data_date)` on `staging.shopcore_orders` returns 2026-06-02.
- [ ] `ops.cleaning_report` has 13 rules, every count matching the Verify table, and each rate
      is within noise of SPEC §3.5.
- [ ] `core.customers` (small: 60,000; full: 1.2M), `core.products` (small: 2,000),
      `core.stores` (400 at both scales) built; country
      is ISO-2 or NULL; is_active is BOOLEAN; signup_ts is TIMESTAMP; unit_price is DECIMAL.
- [ ] `core.orders_enriched` (58,993 — joins dropped nothing) and `core.daily_store_sales`
      exist; missing/orphan customer counts journaled.
- [ ] You can state the **grain** of every table built today — one sentence each, "one row =
      one …" — and every DDL carries it as a comment, the ones you write (`core.products`)
      included; you can say which of them is a fact and which are dimensions.
- [ ] `work/cleaners.py` exists and the staging/dim builds go **through it** — proved by
      Task 7's EXCEPT check (0 rows differ from the hand-built version) — and
      `work/a03_build_day.py` imports it, re-pastes nothing.
- [ ] `python -m pytest tests/ -q` → **7 passed**. The `a03-transformation` branch merges to
      `main` only after this line is green — the lab's merge rule from now to A15.
- [ ] `python -m work.a03_build_day --date D --scale S` rebuilds everything from scratch.

## Verify yourself

All numbers are deterministic for **small scale, 2026-06-02** (the generator is seeded per
date, so your file is byte-identical to ours).
```sql
SELECT rule, rows_affected FROM ops.cleaning_report ORDER BY rule;
```

| rule                      | rows_affected | SPEC §3.5 says |
|---------------------------|--------------:|----------------|
| customer_id_float_fixed   |           120 | ~0.2%          |
| customer_id_missing       |            94 | ~0.15%         |
| dup_order_id_removed      |            31 | ~0.05% + late collisions |
| payment_recased           |           204 | ~0.3%          |
| status_junk_to_null       |            71 | ~0.15% (the 'unknown'/empty half) |
| status_recased            |            74 | ~0.15% (the 'PAID '/'Paid' half)  |
| total_decimal_comma_fixed |           187 | ~0.3%          |
| total_dollar_stripped     |           112 | ~0.2%          |
| total_na_or_empty_to_null |           117 | ~0.2%          |
| total_negative_kept       |            52 | ~0.1%          |
| ts_ddmmyyyy_fixed         |           231 | ~0.4%          |
| ts_isot_fixed             |           164 | ~0.3%          |
| ts_unparseable_to_null    |            18 | ~0.02%         |

```sql
SELECT count(*) FROM staging.shopcore_orders;                         -- 58993
SELECT count(*) FROM staging.shopcore_orders WHERE order_ts IS NULL;  -- 18
SELECT order_date, count(*) FROM staging.shopcore_orders GROUP BY 1 ORDER BY 1;
-- 2026-06-01: 883 | 2026-06-02: 58092 | NULL: 18
SELECT max(_data_date) FROM staging.shopcore_orders;                  -- 2026-06-02 (the staleness probe)

-- the two-column rule, asked of the catalog instead of your memory — row grain and rollup:
SELECT table_name, column_name, data_type FROM duckdb_columns()
WHERE (schema_name, table_name) IN (('staging','shopcore_orders'),
                                    ('core','daily_store_sales'))
  AND column_name LIKE '\_%' ESCAPE '\' ORDER BY 1, 2;
-- daily_store_sales _data_date DATE | daily_store_sales _run_id BIGINT
-- shopcore_orders   _data_date DATE | shopcore_orders   _run_id BIGINT
-- exactly two each, neither is a string, and the aggregate is not exempt

SELECT count(*) FROM core.customers;                                  -- 60000
SELECT count(*) FROM core.customers WHERE signup_ts IS NULL;          -- 1175  (~2%)
SELECT count(*) FROM core.customers WHERE is_active IS NULL;          -- 1164  (~2%)
SELECT count(*) FROM core.customers WHERE country   IS NULL;          -- 548 (~1% blank; more = country_map incomplete)
SELECT min(signup_ts), max(signup_ts) FROM core.customers;
-- 2019-01-01 00:00:00 .. 2026-05-31 23:05:17   (epochs decoded to sane dates = TZ pinned right)
SELECT count(*) FROM core.customers WHERE name LIKE '%,%';            -- 0
SELECT count(*) FROM core.products  WHERE unit_price IS NULL;         -- 0 (5 comma-forms fixed)

SELECT count(*) FROM core.orders_enriched;                            -- 58993
SELECT total_bucket, count(*) FROM core.orders_enriched GROUP BY 1 ORDER BY 2 DESC;
-- large 26289 | medium 25287 | small 6234 | huge 1014 | unknown 117 | invalid 52
SELECT count(*) FROM core.daily_store_sales;                          -- 723
```

Full scale, same date: raw = **1,180,490** (manifest), staging = **1,179,758**; roughly
10–30 s wall-clock on the baseline machine. (Numbers identical whether staging was built by
Task 4's hand-written SQL or Task 7's module — that is the whole point of the EXCEPT proof.)

And the tests, from the repo root:
```text
> python -m pytest tests/ -q
.......                                                                  [100%]
7 passed in 1.16s
```

## Common beginner mistakes

- **`CAST` where you meant `TRY_CAST`** — one bad value kills a 59k-row load. The mirror is
  worse: `TRY_CAST` everywhere with no cleaning report — revenue quietly goes NULL. Count, always.
- **De-commaing without killing thousands-dots first.** `replace('1.234,56', ',', '.')` gives
  `'1.234.56'` → NULL. Inner `replace('.','')` first, like the canonical cleaner.
- **`lower()` without `TRIM()`.** `' cod '` lowercases to `' cod '` — still not `'cod'`.
- **Testing empties with `= ''`.** DuckDB's CSV reader delivers empty fields as NULL, so that
  predicate matches nothing; three report rules silently return 0 if you get this wrong.
- **Epoch timestamps without pinning the timezone.** `to_timestamp()` is timezone-aware; skip
  `SET TimeZone='UTC'` and `signup_ts` shifts by your laptop's UTC offset — checks stop matching.
- **Cleaning downstream (core, marts, the BI tool).** Fix a value in three downstream places
  and you own three future bugs. Staging is *the* place; everything after it trusts the types.
- **Running scripts by path instead of `-m`.** `python work/a03_bootstrap.py` or a bare
  `pytest` starts Python's import path in the wrong directory — `ModuleNotFoundError` on
  `lib` or `work`. From the repo root, always `python -i -m work.a03_bootstrap` and
  `python -m pytest tests/ -q`.

## Stretch goals

1. **Fix the SHOUTING names** (1,179 remain, ~2%). Split, recase each word, rejoin:
   `array_to_string(list_transform(string_split(lower(name), ' '), w ->
   upper(substr(w, 1, 1)) || substr(w, 2)), ' ')` applied only `WHERE name = upper(name)`;
   prove the leftover count is 0. (A sneak preview of A04's list functions.)
2. **Promote the enums to mapping tables.** Replace the status/payment CASE whitelists with a
   `staging.shopcore_status_map(variant, canonical)` seeded by INSERTs (junk→NULL rows included), so a
   new upstream variant is one INSERT, not a code change. The edit belongs in
   `work/cleaners.py` (a `clean_enum_via_map` beside `clean_enum`), and
   `tests/test_cleaners.py` grows to pin the new path. Rebuild; the report — and pytest —
   must not change.
3. **Trend the dirt.** Loop `build_day` over 5 v1 dates, appending (not replacing) the
   cleaning report, then query rates per day. They should be flat — that flatness is exactly
   what A05 turns into automated checks.
4. **Price the columns you did not add.** The lineage box argues from *modeling*. Argue from
   *bytes* too, and watch the byte argument fail to matter — which is the real lesson. Write
   your day twice and compare:
   ```python
   con.execute(f"COPY (SELECT * FROM staging.shopcore_orders) "
               f"TO '{R}/tmp/lineage_2col.parquet' (FORMAT parquet);")
   con.execute(f"""COPY (SELECT *,
                           'orders_2026-06-02.csv'  AS _source_file,
                           CAST(now() AS TIMESTAMP) AS _loaded_at
                         FROM staging.shopcore_orders)
                   TO '{R}/tmp/lineage_4col.parquet' (FORMAT parquet);""")
   ```
   Guess the delta before you measure. The naive arithmetic says 58,993 rows × (a 21-char file
   name + an 8-byte timestamp) ≈ **1.7 MB** of new file. Measured, on small scale:
   `lineage_2col.parquet` = **4,278,003 bytes**, `lineage_4col.parquet` = **4,278,538 bytes**.
   The two extra columns cost **535 bytes — 0.013%, about 1/100th of a byte per row**, roughly
   3,200× under the naive estimate. Ask Parquet why, straight from its own footer:
   ```sql
   SELECT path_in_schema, sum(total_compressed_size) AS bytes, any_value(encodings) AS enc
   FROM parquet_metadata('<root>/tmp/lineage_4col.parquet') GROUP BY 1 ORDER BY 2 DESC;
   -- items         1,752,622  PLAIN
   -- ...
   -- _source_file         74  PLAIN_DICTIONARY   <- 58,993 rows of a 21-char string
   -- _loaded_at           57  PLAIN_DICTIONARY
   -- _data_date           53  PLAIN_DICTIONARY
   -- _run_id              29  PLAIN
   ```
   A column with **one distinct value** becomes a one-entry dictionary plus a run of identical
   indices, and that compresses to nothing. `items` — real, varied JSON — costs 1.75 MB in the
   same file: 23,000× more than `_source_file`, for the same 58,993 rows. Push the dictionary
   harder if you like: synthesize `_source_file` with all **69** of the lab's file names
   shuffled across the rows (`'orders_' || strftime(DATE '2026-06-01' + CAST(order_id % 69 AS
   INTEGER), '%Y-%m-%d') || '.csv'`) and rewrite — the file grows to 4,330,968 bytes and
   `_source_file` to 52,403, a 1.2% delta, because the *indices* now need ~7 bits per row
   instead of collapsing into one constant run. Still cheap, and still not the point. (A real
   partitioned lake never even pays this: one day's rows land in one file, so it is the 74-byte
   case, not the 52 KB one.)

   **Write the conclusion in your journal, because it is the one people get backwards:** the
   storage argument for two columns is *weak* — you could afford four. The reason to keep two
   is that `_source_file` and `_loaded_at` describe the run, so the run's ledger row is where
   they are correct, editable in one place, and never able to disagree with themselves across
   1.2M copies. When A07 builds `ops.etl_runs` and joins a core row back to the file that made
   it, you will have paid nothing for the privilege.
