# A04 — Complex data types: JSON, structs, lists
**Topic:** complex types · **Est. time:** 3–4h · **Difficulty:** ⭐⭐⭐
**Requires:** A03 done; month-1 raw data at both scales; your `warehouse.duckdb` from A03.

## Why this matters

Real-world feeds are not flat. An order *contains* items — one order, many products — and CSV
has no way to say that, so shopcore, the e-commerce platform producing this feed, stuffs a JSON
string into a cell. Until you crack
that string open, "revenue by product category" is literally unanswerable. Turning JSON text
into properly typed rows and columns is one of the most common jobs in data engineering, and
doing it in SQL instead of a Python loop is the difference between seconds and hours at scale.

## Background

**Three type worlds.** The `items` column in your staging table is currently a `VARCHAR` — to the
database it is just text, no different from a name. Cast it to `JSON` and DuckDB *parses* it: now
it knows there is an array with objects inside, and you can reach in with path expressions. But
JSON is still loosely typed — every value is "whatever the text says". The final step is casting
to **native nested types**, where the schema is fixed and enforced:

- a **STRUCT** is a value with named, typed fields — like a row inside a cell:
  `{'sku': 'SKU-001965', 'qty': 2, 'unit_price': 114.46}`.
- a **LIST** is a variable-length array of values sharing one type. Written `TYPE[]`.
- combined, one cell holds `LIST<STRUCT>` — exactly what an order's items are. Native types are
  what you want downstream: columnar, compact, fast to filter, stored natively by Parquet.
  JSON is the doorway, not the destination.

**Grain.** The *grain* of a table is what one row means. Your staging table has **order grain**:
one row = one order — strictly, one delivered *version* of an order, since today's un-deduped
day table carries 31 orders twice (Task 3's DDL comment spells that out; A08 folds them).
To analyze products you need **item grain**: one row = one line item.
`unnest` is the operation that explodes a list into rows — it *changes the grain*. Expect the row
count to multiply by the average list length (about 2.5 items per order here). Changing grain on
purpose is modeling; changing it by accident is how people double-count revenue. And a fact is
defined by its grain, so two grains means two facts: A03's modeling box (§7.7) named
`core.orders_enriched` a *fact* at order grain, and Task 3 builds a **second fact table at item
grain** — which is why the lines get their own `core.order_items` table instead of
`sku_1, qty_1, sku_2, …` bolted onto the order row. Measures follow their grain too:
`qty * unit_price` belongs to the item row, `order_total` to the order row.

**Strict vs lenient conversion.** DuckDB gives you two ways from JSON to typed values:

- `CAST(x AS JSON)::STRUCT(...)[]` — **strict**. Anything that does not fit the target type is an
  error and the whole query dies. Good: problems fail loud.
- `json_transform(x, '...spec...')` — **lenient**. Anything that does not fit becomes `NULL`,
  silently. Good: survives messy reality. Bad: silent.

Neither is "the right one". Strict is the right default; lenient is the right tool when the data
has *legitimate* variation — you will hit exactly such a case with `meta` today.

**Three flavors of missing.** In a flat column, "no value" is just NULL. JSON has three ways to
say it: the key holds JSON `null`, the key is **absent** entirely, or upstream wrote a sentinel
string like `"none"`. All three occur in this feed's `meta.utm`, all three mean the same thing
to the business, and part of your job today is counting each one and collapsing them into one
honest NULL.

One more rule you will trip over otherwise: in DuckDB, `unnest` cannot share a `SELECT` with
`GROUP BY`. Explode in a subquery, aggregate outside — you will use this pattern twice today.

## Setup

- A03 finished: month-1 raw data exists at both scales, and `warehouse.duckdb` has your
  `staging` / `core` schemas. Today adds `core.order_items` and your first `marts` table.
- Work at **small scale first** for every task; only Tasks 8–9 move to full scale.
- Start a script `work/a04_complex_types.py` with the usual scaffold:

```python
import duckdb
from lib.labpaths import data_root, warehouse_path

SCALE = "small"                     # develop here; switch to "full" only where a task says so
DAY = "2026-06-02"                  # any v1 day works; verify numbers below assume this one
ROOT = data_root(SCALE)
RAW = (ROOT / "raw" / "orders" / f"orders_{DAY}.csv").as_posix()
PRODUCTS = (ROOT / "raw" / "dims" / "products.csv").as_posix()

con = duckdb.connect(str(warehouse_path(SCALE)))
con.execute(f"SET memory_limit='8GB'; SET threads=8; "
            f"SET temp_directory='{(ROOT / 'tmp').as_posix()}';")
```

- JSON support is built into DuckDB — nothing to install. Keep Task Manager handy for Task 9
  and your `journal.md` open throughout.

## Tasks

### Task 1 — Look at the raw strings (small)

Before parsing anything, look at what you have. Print a few values (build the day table with
the CREATE snippet below first — running this peek before it exists is a Catalog Error):

```python
con.sql("SELECT items, meta FROM shopcore_orders_day LIMIT 5").show()
```

This doc uses its own day table, `shopcore_orders_day` — a **TEMP** table this time: it lives in
your connection, not in the shared `staging` schema, and vanishes when your script exits. That
placement is the point. This is a scratch table for today's exercises, not a pipeline output, so
it skips the §7.2 lineage columns — exactly two of them, `_data_date` and `_run_id` — and §7.2
promises both on every staging/core row, so a table without them has no business sitting in
`staging`. Scratch work stays TEMP; the durable `staging.shopcore_orders_day` arrives in A05
carrying the pair: `_data_date` filled in, `_run_id` still NULL until A07 builds the run ledger
it points at. The rule cuts the other way for Task 3's `core.order_items`: that table *is*
durable, so it stamps the pair itself — `_data_date` straight from `DAY`, because a TEMP parent
has no lineage to inherit. One consequence of TEMP: a fresh session starts without it — re-run
this snippet first. Build it with the snippet below even if your A03 `staging.shopcore_orders`
is still around: it is deliberately *not* deduped, so every
count today matches the manifest (59,024), and it keeps just the four columns you need (`items`
and `meta` stay `VARCHAR` on purpose; parsing them is the assignment). If you reuse
your deduped A03 table instead, expect 58,993 rows and 149,099 item lines — everything else
shifts by the same ~31 duplicate orders.

```python
con.execute(f"""
CREATE OR REPLACE TEMP TABLE shopcore_orders_day AS
SELECT order_id,
       try_strptime(order_ts, ['%Y-%m-%d %H:%M:%S', '%d/%m/%Y %H:%M:%S',
                               '%Y-%m-%dT%H:%M:%S']) AS order_ts,
       items, meta
FROM read_csv('{RAW}', header=true,
    columns={{'order_id':'BIGINT','customer_id':'BIGINT','store_id':'INTEGER',
             'order_ts':'VARCHAR','updated_at':'TIMESTAMP','status':'VARCHAR',
             'payment_method':'VARCHAR','order_total':'VARCHAR',
             'items':'VARCHAR','meta':'VARCHAR'}});
""")
```

**Accept when:** you can say what `items` is (a JSON *array* of *objects*, 1–6 of them) and what
`meta` is (a JSON *object*), and the day table's count equals manifest `rows` (59,024 for small `2026-06-02`).

### Task 2 — Parse `items` — both ways (small)

Convert the string to `LIST<STRUCT>` both ways — strict cast, then lenient transform:

```python
con.sql("""
    SELECT CAST(items AS JSON)::STRUCT(sku VARCHAR, qty INTEGER, unit_price DOUBLE)[] AS item_list
    FROM shopcore_orders_day LIMIT 5
""").show()
con.sql("""                                          -- lenient; same result shape
    SELECT json_transform(items,
           '[{"sku":"VARCHAR","qty":"INTEGER","unit_price":"DOUBLE"}]') AS item_list
    FROM shopcore_orders_day LIMIT 5
""").show()
```

Now the dirt probe. About 0.3% of rows carry `qty`/`unit_price` as JSON *strings*
(`"qty":"2"` instead of `"qty":2`). Count them:

```python
con.sql("""
    SELECT count(*) FROM shopcore_orders_day
    WHERE json_type(items, '$[0].qty') = 'VARCHAR'
""").show()
```

Then check whether the strict cast survived them — count item lists containing a NULL `qty`
after casting (hint: `list_filter(item_list, x -> x.qty IS NULL)`). You should find **zero**:
DuckDB's JSON casts coerce a numeric string like `"2"` into `2`. Note that down — convenient,
but the cast quietly *fixed* data; the `json_type` probe above is your audit trail (the A03
"count every rule" habit again). Contract-wise this dirt is a **gap**, not a violation — the
shopcore contract never pins the inner JSON types (§3.9), which is exactly the kind of silence
you will hunt down formally in A06.

**Accept when:** both forms return identical-looking lists; the string-typed row count is
roughly 0.3% (exactly **161** on small `2026-06-02`); NULL-qty count after strict cast is 0.

### Task 3 — Unnest into `core.order_items` (small)

Change the grain. Explode each order's list into one row per line item, numbering the lines:

```python
con.execute(f"""
-- grain: one row = one line item of one delivered order *version*
-- (this day table is deliberately un-deduped, so the 31 re-sent orders each
--  contribute a second line set: (order_id, line_no) is not yet unique here.
--  One-row-per-order arrives with A08's dedupe; A15's fct_order_items can test it.)
CREATE OR REPLACE TABLE core.order_items AS
SELECT
    order_id,
    generate_subscripts(item_list, 1) AS line_no,     -- 1, 2, 3, ... per order
    unnest(item_list, recursive := true),             -- explodes struct fields into columns
    DATE '{DAY}'         AS _data_date,               -- lineage (§7.2): the delivery's business date
    CAST(NULL AS BIGINT) AS _run_id                   -- lineage: NULL until A07's ledger
FROM (
    SELECT order_id,
           CAST(items AS JSON)::STRUCT(sku VARCHAR, qty INTEGER, unit_price DOUBLE)[] AS item_list
    FROM shopcore_orders_day
);
""")
```

`recursive := true` unpacks the struct's fields into real columns (`sku`, `qty`, `unit_price`);
`generate_subscripts` walks the same list in step with `unnest`, which gives you `line_no`.
The last two columns are the §7.2 pair: a `core` table carries them, and item grain inherits
the delivery date of the orders it exploded.
Now the **conservation check** — after any grain change, prove no rows appeared or vanished:

```python
con.sql("""
    SELECT (SELECT count(*) FROM core.order_items)               AS item_rows,
           (SELECT sum(json_array_length(items))
              FROM shopcore_orders_day)                          AS expected
""").show()
```

**Accept when:** `DESCRIBE core.order_items` shows the 5 typed item columns (no VARCHAR numbers)
plus the two §7.2 lineage columns, 7 in all, and the two counts match exactly — **149,190** on
small `2026-06-02`.

### Task 4 — Know your new table: distributions (small)

Get a feel for the item grain. Items-per-order distribution:

```python
con.sql("""
    SELECT json_array_length(items) AS n_items,
           count(*)                 AS orders,
           round(count(*) * 100.0 / sum(count(*)) OVER (), 1) AS pct
    FROM shopcore_orders_day
    GROUP BY 1 ORDER BY 1
""").show()
```

Now *you* write the equivalent for the `qty` distribution over `core.order_items` (same shape).
Also compute the average items per order — you will need it for Task 9.

**Accept when:** items-per-order runs 1–6 with ~30% at 1; qty runs 1–5 with ~55% at 1;
average items per order is ~2.5 (2.53 on small `2026-06-02`). Note these in your journal.

### Task 5 — Join items to products — meet the orphans (small)

Join `core.order_items` to the products dim on `sku` — your A03 `core.products` or the raw CSV
both work (`sku` and `category` are clean; you don't need the dim's dirty `unit_price`, because
the *transaction* price lives in the item). Count the item lines whose `sku` has no match:

```python
con.sql(f"""
    SELECT count(*) AS orphan_lines,
           round(100.0 * count(*) / (SELECT count(*) FROM core.order_items), 2) AS pct
    FROM core.order_items i
    LEFT JOIN read_csv('{PRODUCTS}', header=true) p USING (sku)
    WHERE ...                                        -- you finish it
""").show()
```

Why LEFT and not INNER? An INNER join would silently *drop* those lines — revenue shrinks and
nobody knows. A LEFT join keeps them so you can count them. These orphan SKUs are real dirt
(products sold but never added to the dim). Do **not** delete or fix them today — detecting and
quarantining unfixable rows is A05's whole job. Today you only measure and report.

**Accept when:** orphan count is roughly 0.2% of item lines (**310** lines, 0.21%, on small
`2026-06-02`), and you can explain why LEFT JOIN was the right choice.

### Task 6 — Parse `meta` — three flavors of missing (small)

Single-field extraction first: `CAST(meta AS JSON)->>'device'` returns VARCHAR
(`json_extract_string(meta, '$.device')` is the same thing spelled out). Group by it and eyeball
the device split. Now `utm` — count each flavor of "no value" separately:

```python
con.sql("""
    SELECT
      count(*) FILTER (WHERE CAST(meta AS JSON)->>'utm' = 'none') AS sentinel_none,
      count(*) FILTER (WHERE json_type(meta, '$.utm') = 'NULL')   AS json_null,
      count(*) FILTER (WHERE json_extract(meta, '$.utm') IS NULL) AS key_absent,
      count(*)                                                    AS total_rows
    FROM shopcore_orders_day
""").show()
```

Read those predicates carefully — they are the lesson. `json_extract` returns SQL NULL **only**
when the key is absent; a stored JSON `null` comes back as a JSON null value (`json_type` says
`'NULL'`), which is *not* SQL NULL. Meanwhile `->>` collapses both cases to SQL NULL — verify:
counting `->>'utm' IS NULL` should give `json_null + key_absent`. That collapsing is what you
want for cleaning: `NULLIF(CAST(meta AS JSON)->>'utm', 'none')` folds all three flavors into one
honest NULL. Run the normalized group-by and look at the result.

One more discovery. Try the strict struct cast on `meta` — it **fails** with
`Object {...} does not have key "utm"`: a strict struct cast demands every field, and ~10% of
rows legitimately have no `utm` key. This is the case where lenient wins:

```python
con.sql("""
    SELECT CAST(meta AS JSON)::STRUCT(device VARCHAR, utm VARCHAR, sess VARCHAR)
    FROM shopcore_orders_day LIMIT 5
""").show()                                   # dies — read the error message
con.sql("""
    SELECT json_transform(meta, '{"device":"VARCHAR","utm":"VARCHAR","sess":"VARCHAR"}') AS m
    FROM shopcore_orders_day LIMIT 5
""").show()                                   # missing keys become NULL fields, no drama
```

Note the trade in your journal: strict for `items` (fixed shape, want loud failures),
lenient for `meta` (optional keys are legitimate). The three-flavors mess itself is a contract
**gap** — the null representation for `utm` is unstated (§3.9) — a candidate for the A06
amendment list rather than a producer bug report.

**Accept when:** the three flavors count **5,930 / 5,981 / 5,945** (sentinel/JSON null/absent,
~10% each) on small `2026-06-02`; you saw the strict cast fail and know why; normalized `utm`
has 4 real sources plus NULL.

### Task 7 — Products: pipe-separated tags and `attrs` JSON (small)

Two more complex-type shapes live in `products.csv`. `tags` is a pipe-separated string
(`"wireless|audio|sale"`) — a poor man's list. Split and explode it; note the rule in action:
`unnest` in the subquery, `GROUP BY` outside:

```python
con.sql(f"""
    SELECT tag, count(*) AS products
    FROM (
      SELECT unnest(string_split(tags, '|')) AS tag
      FROM read_csv('{PRODUCTS}', header=true)
    )
    GROUP BY 1 ORDER BY 2 DESC LIMIT 10
""").show()
```

`attrs` is a JSON object (`{"brand":…,"weight_g":…,"color":…}`). Extract all three fields with
the tools from Task 6 — `weight_g` should end up an INTEGER, not text (`->` returns JSON, `->>`
returns VARCHAR; cast accordingly). Then answer: which brand has the most products?

**Accept when:** you have a top-10 tag list and a brand count table, and `weight_g` is numeric.

### Task 8 — Your first mart: `marts.category_daily_revenue` (small, then full)

A **mart** is a small, purpose-built table that answers business questions directly — the
payoff for the plumbing. Combine everything from today:

```python
con.execute("CREATE SCHEMA IF NOT EXISTS marts;")
con.execute(f"""
CREATE OR REPLACE TABLE marts.category_daily_revenue AS
SELECT
    CAST(s.order_ts AS DATE)              AS order_date,
    COALESCE(p.category, '(unknown sku)') AS category,
    round(sum(i.qty * i.unit_price), 2)   AS revenue,
    ...                                   -- add: units (sum of qty), distinct order count
FROM core.order_items i
JOIN shopcore_orders_day s USING (order_id)
LEFT JOIN read_csv('{PRODUCTS}', header=true) p USING (sku)
GROUP BY 1, 2;
""")
con.sql("SELECT * FROM marts.category_daily_revenue ORDER BY order_date, revenue DESC").show()
```

Look at the `order_date` column — you loaded *one* file, but you get **three** date groups.
Explain each before reading on: yesterday's date (the ~1.5% late correction rows whose
`order_ts` belongs to prior days — same thing you saw in the A02 lake), your day, and NULL (the
~0.02% impossible timestamps your A03 cleaner nulled). One file ≠ one day — journal that; it is
the entire plot of A08. Then switch `SCALE = "full"` and rerun the whole script for the same
day. Expect seconds-to-a-minute territory, not minutes — DuckDB parses JSON in parallel.

**Accept when:** small scale shows 13 categories in each dated group (the tiny NULL group has
fewer), top category `office` at ~734k revenue on `2026-06-02`; full scale runs clean at
roughly 20× (~1.18M rows, ~3M item lines).

### Task 9 — Scale probe: the spike day, and the 200M-row estimate (full)

Everything so far was one ordinary day. Now measure what "the whole history" would mean.

First, on paper. You only have month 1 (45 days) so far, but the feed eventually totals ~82M
order rows across 69 days (SPEC volume table — month 2 arrives in A10). Multiply by your Task 4
average items per order: roughly **200M item rows**. That is what "unnest the whole history"
costs, and why nobody does it as one giant query.

Then run a two-part experiment on the spike day (`2026-06-19`, ~3.64M rows, ~1 GB of CSV),
at **full** scale, with Task Manager open and sorted by memory.

**Part A — stream.** Count the unnested lines without keeping them:

```python
spike = (data_root("full") / "raw" / "orders" / "orders_2026-06-19.csv").as_posix()
V1_COLS = ("{'order_id':'BIGINT','customer_id':'BIGINT','store_id':'INTEGER',"
           "'order_ts':'VARCHAR','updated_at':'TIMESTAMP','status':'VARCHAR',"
           "'payment_method':'VARCHAR','order_total':'VARCHAR',"
           "'items':'VARCHAR','meta':'VARCHAR'}")
con.sql(f"""
    SELECT count(*) FROM (
        SELECT unnest(CAST(items AS JSON)::STRUCT(sku VARCHAR, qty INTEGER, unit_price DOUBLE)[])
        FROM read_csv('{spike}', header=true, columns={V1_COLS})
    )
""").show()
```

Watch python's memory while it runs: it barely moves. On the baseline machine expect roughly
2 seconds and well under half a GB — DuckDB *streams*: parse a chunk, count it, throw it away.

**Part B — materialize.** Now keep the rows (TEMP again, like your day table — scratch results
that vanish when you disconnect):

```python
con.execute(f"""
CREATE OR REPLACE TEMP TABLE spike_items AS
SELECT order_id,
       generate_subscripts(item_list, 1) AS line_no,
       unnest(item_list, recursive := true)
FROM (
    SELECT order_id,
           CAST(items AS JSON)::STRUCT(sku VARCHAR, qty INTEGER, unit_price DOUBLE)[] AS item_list
    FROM read_csv('{spike}', header=true, columns={V1_COLS})
);
""")
```

Same 9.2M rows, but now they have to live somewhere. Expect roughly double the wall-clock and
noticeably more RAM (still under 1 GB on the baseline machine). Journal both runs: wall-clock,
peak RAM, row count (`count(*)` the temp table).

Now extrapolate, in writing: one spike day materialized ≈ 9.2M rows and most of a GB. The full
history is ~200M rows — over 20 spike days at once — plus working memory for any sort or join
on top. That blows past your 8 GB `memory_limit`, and DuckDB starts spilling to
`temp_directory` and gets slow (you will watch that happen, on purpose, in A13). This is why
real pipelines process **per partition** — one day at a time, like your A02 loader — and why
A12 parallelizes those per-day units instead of throwing one giant query at the problem.

**Accept when:** both counts are exactly **9,214,235** (the manifest says 3,643,024 order rows
for the spike day); you recorded wall-clock + peak RAM for stream vs materialize; your journal
states the ~200M estimate and why per-partition processing follows from it.

## Definition of Done

- [ ] `core.order_items` exists for ≥1 day, typed `(order_id, line_no, sku, qty, unit_price)` —
      no VARCHAR numbers — plus the two §7.2 lineage columns (`_data_date` filled, `_run_id` NULL).
- [ ] Conservation check passes: item row count == `sum(json_array_length(items))`.
- [ ] String-typed `qty`/`unit_price` rows counted (~0.3%) and the coercion behavior noted.
- [ ] Orphan-SKU count reported with percentage (~0.2% of item lines), via LEFT JOIN, nothing dropped.
- [ ] The three `utm` missing-flavors counted separately, then normalized to a single NULL.
- [ ] Products `tags` exploded and `attrs` extracted with correct types.
- [ ] `marts.category_daily_revenue` built for your day; the three `order_date` groups explained.
- [ ] Full-scale rerun done; spike-day stream-vs-materialize numbers and the ~200M full-history
      estimate are in your journal.

## Verify yourself

Exact numbers are for **small scale, day `2026-06-02`** — the generator is deterministic, so
yours should match exactly; a different day gives different numbers at similar rates.

| Check | Query sketch | Expect |
|---|---|---|
| Day-table rows | `count(*)` vs manifest | **59,024** |
| Item rows | `count(*)`; equals `sum(json_array_length(items))` | **149,190** |
| String-typed qty rows | `json_type(items,'$[0].qty')='VARCHAR'` | **161** |
| NULL qty after strict cast | filter on `qty IS NULL` | **0** |
| Avg items/order | ratio of the two counts above | **2.53** |
| Orphan item lines | LEFT JOIN products, no match | **310** (0.21%) |
| utm sentinel / JSON null / absent | Task 6 query | **5,930 / 5,981 / 5,945** |
| `->>'utm' IS NULL` | collapses null+absent | **11,926** |
| Devices | group by device | android 20,399 · ios 17,863 · web 14,850 · desktop 5,912 |
| Mart date groups | distinct `order_date` | 3 (2026-06-01, 2026-06-02, NULL) |
| Spike day (small) item lines | Task 9 query with `data_root("small")` | **460,210** |
| Spike day (full) item lines | Task 9, full scale | **9,214,235** |

Full scale: the same `2026-06-02` has roughly 20× everything (exact row count in your full-scale
manifest: 1,180,490). Timings and RAM are machine-dependent; Task 9's numbers are roughly what
the baseline 16 GB / 8-core machine shows.

## Common beginner mistakes

- **Parsing JSON in a Python loop** — `json.loads` over `fetchall()` rows. It works on 59k rows
  and dies of old age on 1.2M×69. The database parses JSON in parallel; let it.
- **`unnest` next to `GROUP BY` in one SELECT** — DuckDB refuses. Explode in a subquery,
  aggregate outside.
- **Joining item grain back to order grain and summing order columns** — join `order_items` to
  orders and `sum(order_total)`: every 3-item order counts its total 3 times. Sum each measure
  at its own grain.
- **Reading `->>` NULL as "the key was absent"** — it also swallows stored JSON `null`. To
  *distinguish* (audits, contracts), use `json_extract` / `json_type`; to *normalize*, the
  collapsing is a feature.
- **Strict-casting objects with optional keys** — worked on five sample rows, dies on row
  50,001 where `utm` is absent. Sampling is not schema; know which fields are optional.

## Stretch goals

- **Sessions.** `meta.sess` is a session id. Count distinct sessions for your day and how many
  contain more than one order. (Very few — 58,995 distinct for 59,024 rows on small `2026-06-02`.)
- **Reconciliation preview.** Compare each order's `sum(qty * unit_price)` from
  `core.order_items` against the cleaned `order_total` from A03; count orders differing by more
  than 0.01. Expect roughly 0.5–0.8% (the deliberate 0.5% mismatch dirt plus late corrections
  that changed totals). Just count today — A05 turns this into a formal check with quarantine.
- **Nested Parquet.** `COPY` a `SELECT order_id, <the strict cast> AS items` to a Parquet file,
  then `DESCRIBE` a `read_parquet` of it: the column comes back as the full `LIST<STRUCT>` type —
  Parquet stores nested types natively, no JSON strings anywhere. Compare the file size against
  the same rows as raw CSV.
