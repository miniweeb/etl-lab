# A15 — dbt capstone

**Topic:** dbt usage · **Est. time:** 6–8h · **Difficulty:** ⭐⭐⭐⭐⭐
**Requires:** A14 done; the full 69-day **canonical lake** at both scales (A10, repaired in A11); `warehouse.duckdb` at both scales; `ETL_LAB_DATA` set.

## Why this matters

For fourteen assignments you built a transformation pipeline with your bare hands: staging, cleaning, incremental loads, tests, backfills, docs-by-journal. Almost every data team wraps exactly that work in a tool called **dbt** — it is the closest thing data engineering has to an industry standard, and it will be on the job description. Today you rebuild your T-layer in dbt and discover the punchline of this whole lab: *there is nothing in dbt you have not already built yourself.* People who learn dbt first treat it as magic and cannot debug it. You learned the machinery first, so for you dbt is just a labor-saving formalization — and when it misbehaves, you will know exactly which hand-built part is hiding under the hood.

## Background

**What dbt is.** dbt (data build tool) runs the **T** of ELT. You write one `SELECT` per output table, plus a little configuration. dbt compiles each `SELECT` into the right DDL/DML (`CREATE VIEW`, `CREATE TABLE`, or the delete+insert dance you wrote in A07), figures out the dependency order, runs everything, and tests the results. It does **not** extract or load — your lake loader, quarantine routing, and runner from A02–A11 keep those jobs.

Five nouns cover most of dbt:

- **model** — one `.sql` file containing one `SELECT`. Its filename is the table/view name.
- **materialization** — *how* a model becomes real: `view` (free until queried), `table` (rebuilt from scratch each run), or `incremental` (only a slice is rebuilt — your A08 strategies, formalized).
- **source** — a declaration of raw data dbt reads but does not build. Ours point at the Parquet lake and the dim CSVs.
- **seed** — a small CSV that dbt loads as a table. Your A03 country mapping table, formalized.
- **test** — a `SELECT` that must return zero rows. **Generic** tests (not_null, unique…) are configured in YAML; **singular** tests are hand-written SQL files. Your A05 check suite, formalized.

**Modeling words, recapped.** You met **grain**, **fact** and **dimension** in A03, where you designed the first core tables, and you watched grain change under `unnest` in A04. Today's models simply wear the names on their faces: `fct_orders` and `fct_order_items` are facts, `dim_customers`/`dim_products`/`dim_stores` are the dimensions they reference, and marts join the two — that fact-in-the-middle, dimensions-around-it shape is the classic **star schema**. If any of that is hazy, reread A03's modeling box before you start; this doc assumes it and does not re-teach it.

**One modeling word you have not met: additive.** Task 14 will ask you to apply it, so meet it here, on the rollup you already built in A03 — `core.daily_store_sales`. A measure is **additive** when a rollup is recoverable from the stored values alone: sum its `gross_revenue` or `orders` across stores, days or months and the answer still means what it says. `avg_order_value` and `customers` are **non-additive** and fail differently. An average of per-store averages weights a three-order store exactly like a three-thousand-order one, so it is not the average order value of anything. And a customer who ordered at two stores is counted twice the moment you add the column up — summing distinct counts roughly doubles the true number on this data. Two rules follow, and both are already visible in your own models: **store the numerator and the denominator, divide at read time** — which is precisely why A03 put `orders_missing_total` in that table, so the honest denominator survives the `GROUP BY` — and **recompute a distinct count from the fact**, never by summing a rollup. Full treatment with the lab's measured numbers: `guides/data_modeling.md` §2.

**What A15 adds: facts come in types.** You have built two of the three.

- **Transaction fact** — one row per event. `fct_orders` (one row per order) and `fct_order_items` (one row per order line) are both this, and it is the kind you reach for by default.
- **Periodic-snapshot / aggregate fact** — one row per entity per period. `marts.daily_store_sales` is one: one row per (order_date, store_id, currency). Every number in it is recomputable from `fct_orders`, which is what makes a derived fact safe to throw away and rebuild.
- **Accumulating-snapshot fact** — one row per process instance, one *column* per milestone, updated in place as the process advances. The order lifecycle would be exactly this: one row per order with `created_ts`, `paid_ts`, `shipped_ts`, `delivered_ts`, `refunded_ts` and the gaps between them — the table that answers "how long from paid to delivered?". This lab **names it and does not build it**, and the reason matters more than the name: the feed ships only the *current* `status` as of `updated_at`, never a timestamp per milestone, so milestones would have to be reconstructed from the correction history (A08's append-only log) — and at small scale only about 1.5% of orders are ever observed more than once. Nearly every milestone column would be NULL. A fact type is only as good as the events you actually receive.

**The trick that makes it work: `ref()` and `source()`.** Inside a model you never write a table name directly — you write `{{ ref('stg_shopcore__orders') }}` or `{{ source('shopcore', 'orders') }}`. The double braces are **Jinja**, a templating language: dbt replaces the call with the real name at compile time. Because dbt now knows who reads whom, it derives the **DAG** (directed acyclic graph — the dependency map of your models) and always builds parents before children. Remember A11 Task 8, where you reasoned out that marts must rerun after core, in that order? `ref()` is that reasoning, automated.

**dbt + DuckDB.** The `dbt-duckdb` adapter runs all compiled SQL inside one DuckDB process against `warehouse.duckdb`. One process means one writer — the A12 lock problem cannot happen *within* a dbt run. It also means dbt cannot run while your Python REPL holds the same database file open. Close those first.

**Three words you will hear at work.** **Metadata** is data *about* your data: a column's type, its description, who owns the table, when it last loaded. A **data catalog** is the searchable place that metadata lives, so someone who did not build a table can find it and decide whether to trust it. **Lineage** is the part that answers "where did this come from, and what breaks if I change it?" — and it comes in three levels (§7.6): row-level (`_data_date`/`_run_id` → `ops.etl_runs`, which you built in A03/A07), table-level (the dbt DAG), and column-level (one field traced through every expression that reads it). Tasks 10–13 build all three into your project, and you will be surprised how much of it you already have.

**What today looks like.** You will declare sources over your lake, port your A03/A10 cleaning into staging views, build dims and two incremental facts, wire up tests, prove windowed idempotent rebuilds with `--vars`, run it all at full scale, write real descriptions for every model, and generate a documentation site — which, said plainly, is a **data catalog** with a clickable lineage graph you will then use to answer a live change request. Then, at the very end, the one thing fourteen assignments never asked of you: you *design* a table instead of implementing one. Every step names the assignment where you built the same thing by hand.

## Setup

- Venv active, terminal at the repo root. Install the last tool of the lab:

```powershell
pip install dbt-duckdb
dbt --version    # expect dbt Core ~1.12, duckdb plugin ~1.11 (or newer)
```

- This doc was verified against dbt Core 1.12.2 / dbt-duckdb 1.11.0 (DuckDB 1.5.x underneath). Newer versions should behave the same; if a flag moves, the error message will say so.
- The project lives **in the repo** at `dbt_lab/` (it is code, so it is versioned — unlike data). Add these lines to `.gitignore`: `dbt_lab/target/`, `dbt_lab/logs/`, `dbt_lab/dbt_packages/`.
- Git ritual (§7.4), one last time: `git switch -c a15-dbt`, commit after each numbered task (`A15 task 5: fct_orders incremental`), merge to `main` when the Definition of Done is green — `guides/git_basics.md` if any of that is rusty.
- dbt reads `ETL_LAB_DATA` (A00). If you relied on the `<repo>/data` fallback, set the env var now — dbt's `env_var()` errors when it is missing, on purpose.
- **Run every dbt command from inside `dbt_lab/`** — dbt looks for its config where you stand.
- Scale rule, one last time: develop on **small** (Tasks 1–8), run **full** once it is green (Task 9). The catalog and lineage work (Tasks 10–13) reads your *project*, not your data, so either target does; Tasks 14–15 are pen-and-paper, with one optional build at the end. Close any other process holding either `warehouse.duckdb` before you run dbt (single writer, A12).

## Tasks

### Task 1 — Scaffold the project and connect *(small, ~30 min)*

Do not run `dbt init` — it scaffolds a demo project; we build ours deliberately, three files. First `dbt_lab/dbt_project.yml`, the project's identity card:

```yaml
name: etl_lab_dbt
version: "1.0.0"
profile: etl_lab_dbt

model-paths: ["models"]
seed-paths: ["seeds"]
test-paths: ["tests"]
macro-paths: ["macros"]

vars:
  start_date: "2026-06-01"   # incremental window defaults: whole history = safe
  end_date: "2026-08-08"

models:
  etl_lab_dbt:
    staging:
      +materialized: view
      +schema: staging
    core:
      +materialized: table
      +schema: core
    marts:
      +materialized: table
      +schema: marts

seeds:
  etl_lab_dbt:
    +schema: staging
```

Read the `models:` block as policy: everything in `models/staging/` becomes a view in schema `staging`, everything in `models/core/` a table in `core`, and so on — the A03 layer convention, now enforced by folder structure. The `vars:` are runtime parameters (your A11 runner's `--start/--end`); more in Task 5.

Next `dbt_lab/profiles.yml` — *where* to build. Project says what, profile says where; they are separate files because the what is shared and the where is per-machine:

```yaml
etl_lab_dbt:
  target: small            # dev on small — the lab rule since A00
  outputs:
    small:
      type: duckdb
      path: "{{ env_var('ETL_LAB_DATA') }}/small/warehouse/warehouse.duckdb"
      threads: 4
      settings:
        memory_limit: 8GB
        threads: 8
        timezone: UTC
        temp_directory: "{{ env_var('ETL_LAB_DATA') }}/small/tmp"
    full:
      type: duckdb
      path: "{{ env_var('ETL_LAB_DATA') }}/warehouse/warehouse.duckdb"
      threads: 4
      settings:
        memory_limit: 8GB
        threads: 8
        timezone: UTC
        temp_directory: "{{ env_var('ETL_LAB_DATA') }}/tmp"
```

Two targets = your two scales; `--target full` is the whole switch. Note **two different `threads`**: the top-level `threads: 4` is how many *models* dbt builds concurrently; `settings.threads: 8` is DuckDB's core count per query (§1 convention). And note what you do *not* see: no N-workers × memory_limit budget math. One DuckDB process, one 8 GB limit — dbt lives inside the A12 single-process alternative you measured. `settings.timezone: UTC` pins epoch decoding, same reason as A03 Task 6.

Last, a five-line macro, `dbt_lab/macros/schema_names.sql`. By default dbt names custom schemas `<target_schema>_<custom_schema>` (you would get `main_staging`). We want our A03 names back:

```sql
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
```

Now prove the plumbing:

```powershell
cd dbt_lab
dbt debug
```

**Accept when:** `dbt debug` ends with `All checks passed!` — and `dbt debug --target full` passes too.

### Task 2 — Sources: point dbt at data it does not own *(small, ~30 min)*

A **source** is a contract-like declaration: "this data exists, here is where, here is how fresh it must be." Sound familiar? It is a thin version of your A06 YAML contract — and it is named like one, too: everything below is shipped by **shopcore**, the e-commerce platform, so the dbt source is named `shopcore`, the same producer-system name `contracts/shopcore_orders_daily.contract.yaml` carries. With dbt-duckdb, a source table can be an `external_location` — any `read_parquet`/`read_csv` expression, so no import step is needed. Create `dbt_lab/models/sources.yml`:

```yaml
version: 2

sources:
  - name: shopcore
    description: >
      Every feed from shopcore, the e-commerce platform: the orders feed (as
      the canonical Parquet lake built in A10 and repaired in A11) plus the
      dimension CSVs exactly as delivered (dirty on purpose).
    meta:
      external_location: "read_csv('{{ env_var('ETL_LAB_DATA') }}{{ '/small' if target.name == 'small' else '' }}/raw/dims/{name}.csv', header=true, all_varchar=true)"
    tables:
      - name: orders
        description: One row per order event, canonical schema, all three eras.
        meta:
          external_location: "read_parquet('{{ env_var('ETL_LAB_DATA') }}{{ '/small' if target.name == 'small' else '' }}/lake/orders_v=*/*/*.parquet', hive_partitioning=true)"
        loaded_at_field: updated_at
        freshness:
          warn_after: {count: 24, period: hour}
          error_after: {count: 7, period: day}
      - name: customers
      - name: products
      - name: stores
```

Four things to understand, not just paste:

- One producer, one source. shopcore ships four feeds, so they are four *tables* under one `shopcore` source — not four sources. That keeps `source('shopcore', ...)` reading like the org chart: the name answers "who do I page?" exactly like the contract's `producer:` block.
- The `{{ ... }}` Jinja picks the small or full path from the active target — one YAML, both scales. The single-brace `{name}` in the source-level `external_location` is a dbt-duckdb placeholder that fills in each table's name: one line serves all three dim CSVs. `orders` declares its own `external_location`, and table-level `meta` wins over source-level — that is how one source spans a Parquet lake and three CSVs. (Do not add other literal `{ }` braces inside `external_location` — that field treats them as placeholders.)
- `all_varchar=true` reads every dim column as text. Deliberate: A03 taught you that a cleaning layer reads dirty columns as strings so it can *see* what it fixes. Staging will do the visible, counted casting.
- `orders_v=*` matches your A10 versioned lake directory. If yours has a different name and the old one is still in `lake/` (not `.trash`), adjust the glob.

Probe a source without writing a model — and meet source **freshness**, dbt's "is the feed alive?" check (your A06 freshness clause, executable):

```powershell
dbt show --inline "select count(*) as lake_rows from {{ source('shopcore','orders') }}"
dbt source freshness
```

At small scale the count is exactly **4,116,024** (manifest total 4,116,840 minus the 816 unparseable-timestamp rows your A10/A11 pipeline dropped and counted — your identity from A10 Task 7, reappearing). Freshness computes `max(updated_at)` — small scale: `2026-08-10 23:31:14` — and compares it to *now*, so your result depends on the day you run it: within 7 days of that stamp you get **WARN**, later you get **ERROR**. Both are correct: the feed really did stop on 2026-08-08 (last corrections landed on the 10th). A dead feed *should* go red — in production this check pages someone before a dashboard silently flatlines.

**Accept when:** the inline count matches, and you can explain in one sentence why your freshness status (WARN or ERROR) is the right answer today.

### Task 3 — Seed + staging views *(small, ~45 min)*

**Seed first.** Your A03 country conformance table becomes `dbt_lab/seeds/country_map.csv` — a seed is exactly "small mapping table, versioned with the code":

```csv
variant,iso2
us,US
usa,US
united states,US
vn,VN
vietnam,VN
viet nam,VN
gb,GB
uk,GB
united kingdom,GB
```

That is the starter. **Extend it from your A03 `staging.shopcore_country_map`** (DE/FR/JP/AU/CA/BR/IN variants — you already discovered them) until the Verify count passes.

> **Naming box — §7.1, spelled in dbt.** Same rules, different dialect. In the warehouse, staging tables are source-prefixed (`staging.shopcore_orders`) because a shared namespace must say *whose* data every name is. dbt's spelling of that rule: the source is named `shopcore` (Task 2) and staging models are `stg_<source>__<entity>` — `stg_shopcore__orders`, with a **double underscore** between source and entity so the name still parses when the entity has underscores of its own (`stg_shopcore__order_items`). core and marts keep their conformed short names (`dim_customers`, `fct_orders`, `daily_store_sales`) — earned the same way §7.1 lets `core.orders` be short.

**Staging views.** Four models under `dbt_lab/models/staging/`. First `stg_shopcore__orders.sql` — thin, because A10 already did the era alignment and heavy cleaning before the lake. What remains: the enum cleanup (the lake kept raw status/payment values) and dropping the technical `orders_v` partition column by simply not selecting it:

```sql
select
    order_id,
    order_id_num,
    customer_id,
    store_id,
    order_ts,
    updated_at,
    case when lower(trim(status)) in
              ('created','paid','shipped','delivered','cancelled','refunded')
         then lower(trim(status)) end                       as status,
    nullif(lower(trim(payment_method)), '')                 as payment_method,
    order_total,
    currency,
    discount_amount,
    channel,
    loyalty_tier,
    items,
    meta,
    order_date
from {{ source('shopcore', 'orders') }}
```

(If your A10 canonicalizer already cleaned the enums, this cleaner is idempotent — clean in, clean out. Harmless both ways.)

Then `stg_shopcore__customers.sql` — your A03 Task 6 relocated, with one deliberate difference: per the data-classification clause you drafted in A06 (emails are PII), the model keeps `md5(lower(trim(email)))` as `email_hash` and never selects the raw email — join and dedupe capability preserved, exposure gone. Note the seed joined via `ref()`, which puts `country_map` into the DAG:

```sql
with src as (
    select * from {{ source('shopcore', 'customers') }}
),

cleaned as (
    select
        cast(customer_id as bigint)                          as customer_id,
        case when name like '%,%'    -- "NGUYEN, Linh" -> "Linh Nguyen"
             then trim(split_part(name, ',', 2)) || ' ' ||
                  upper(substr(trim(split_part(name, ',', 1)), 1, 1)) ||
                  lower(substr(trim(split_part(name, ',', 1)), 2))
             else trim(name) end                             as name,
        md5(lower(trim(email)))                              as email_hash,
        lower(trim(country))                                 as country_variant,
        city,
        case when nullif(trim(signup_ts), '') is null then null
             when regexp_full_match(signup_ts, '[0-9]+')     -- epoch seconds
                 then cast(to_timestamp(cast(signup_ts as bigint)) as timestamp)
             else try_strptime(signup_ts, ['%Y-%m-%d %H:%M:%S', '%d/%m/%Y'])
        end                                                  as signup_ts,
        case when lower(trim(is_active)) in ('true','1','yes','y','t') then true
             when lower(trim(is_active)) in ('false','0','no','n','f') then false
        end                                                  as is_active
    from src
)

select
    c.customer_id, c.name, c.email_hash,
    m.iso2 as country,
    c.city, c.signup_ts, c.is_active
from cleaned c
left join {{ ref('country_map') }} m on c.country_variant = m.variant
```

**You write** `stg_shopcore__products.sql` and `stg_shopcore__stores.sql`: products = select from `{{ source('shopcore', 'products') }}`, clean `unit_price` with the canonical A03 money cleaner into `DECIMAL(10,2)`, pass `tags`/`attrs` through untouched; stores = cast `store_id` to INTEGER and `opened_date` to DATE, rest as-is. Then build just this layer (a **selector** — dbt's version of your runner running a subset):

```powershell
dbt build --select staging country_map
```

**Accept when:** 5 of 5 pass; `staging.stg_shopcore__customers` has 60,000 rows with `country` NULL for exactly 548 (more NULLs = seed incomplete); `stg_shopcore__products.unit_price` has zero NULLs.

### Task 4 — Core dimensions *(small, ~20 min)*

The staging views hold the logic; dims persist it. A view is free until queried — but every fact join will hit these, so pay the build cost once: that is the whole reason `models/core/` is `+materialized: table`. `dim_customers.sql` is honestly boring, and that is the design (its one job is the materialization boundary):

```sql
select * from {{ ref('stg_shopcore__customers') }}
```

`dim_products.sql` carries your A04 complex-type work — pipe-tags to a LIST, attrs JSON to real columns:

```sql
select
    sku,
    product_name,
    category,
    subcategory,
    unit_price,
    string_split(tags, '|')                                  as tags,
    cast(attrs as json)->>'brand'                            as brand,
    try_cast(cast(attrs as json)->>'weight_g' as double)     as weight_g,
    cast(attrs as json)->>'color'                            as color
from {{ ref('stg_shopcore__products') }}
```

**You write** `dim_stores.sql` (one line, same pattern as dim_customers). Build with `dbt build --select core` — staging is *not* rebuilt (views are already there), but dbt still knows the order thanks to `ref()`.

**Accept when:** `core.dim_customers` = 60,000, `core.dim_products` = 2,000 with `tags` typed `VARCHAR[]`, `core.dim_stores` = 400.

### Task 5 — `fct_orders`: your A07/A08 machinery, formalized *(small, ~45 min)*

The centerpiece. Read every comment — each one names the assignment where you built the same thing by hand. `dbt_lab/models/core/fct_orders.sql`:

```sql
-- One row per order, latest correction wins. This IS your A07 transaction and
-- your A08 strategy (c): dbt's delete+insert deletes every order_date present
-- in the batch, then inserts the batch — a partition refresh by date.
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='order_date'
) }}

with batch as (

    select * from {{ ref('stg_shopcore__orders') }}
    {% if is_incremental() %}
    -- the A11 runner's --start/--end, reborn as dbt vars
    where order_date between date '{{ var("start_date") }}'
                         and date '{{ var("end_date") }}'
    {% endif %}

),

latest as (
    -- A08 strategy (a): dedupe on read, newest updated_at wins
    select *,
           row_number() over (partition by order_id_num
                              order by updated_at desc) as rn
    from batch
)

select
    order_id, order_id_num, customer_id, store_id,
    order_ts, updated_at, status, payment_method,
    order_total, currency, discount_amount, channel, loyalty_tier,
    items, meta, order_date
from latest
where rn = 1
```

How the incremental lifecycle works — this is the paragraph to actually learn:

- **First run** (table absent): `is_incremental()` is false, the date filter vanishes, dbt runs `CREATE TABLE` over all history. Your A11 full backfill.
- **Later runs**: `is_incremental()` is true. The batch is only `[start_date, end_date]`; dbt materializes it to a temp table, then — because `unique_key='order_date'` with `incremental_strategy='delete+insert'` — deletes every `order_date` present in the batch from the target and inserts the batch, transactionally. Open your A07 script next to this: `BEGIN; DELETE day-range; INSERT; COMMIT;`. Same machine. dbt just writes the boilerplate.
- **Why the window is safe**: the lake already holds late corrections *inside the correct `order_date` partitions* (you proved that in A02 and fixed the ids in A10), so rebuilding a date range from the lake is complete by construction. The dedupe window then keeps only the newest `updated_at` per `order_id_num` — corrections win, exactly like your A08 reconciliation demanded.

Build it (first run = full history) and then *look at what dbt actually ran* — the compiled SQL is saved under `dbt_lab/target/run/etl_lab_dbt/models/core/fct_orders.sql`. On this first run it is a plain `CREATE TABLE` over all history — your A11 full backfill, no delete+insert anywhere, because `is_incremental()` was false. After Task 8's windowed run, reopen the same file: the A07 `DELETE`+`INSERT` dance will be sitting in it. See both with your own eyes.

```powershell
dbt build --select fct_orders
```

**Accept when:** the model builds green (roughly 10–60 s at small scale on the baseline machine) and `core.fct_orders` has **4,053,208** rows — 4,116,024 lake rows minus 62,816 folded duplicate/correction rows. Journal that delta: it is the across-files dedupe you measured in A08.

### Task 6 — `fct_order_items` and the marts *(small, ~45 min)*

Items next — A04's unnest, placed *downstream of the deduped fact* so no item is ever counted twice. `models/core/fct_order_items.sql`:

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='order_date'
) }}

with orders as (

    select order_id, order_id_num, order_date, store_id, customer_id,
           currency, items
    from {{ ref('fct_orders') }}
    {% if is_incremental() %}
    where order_date between date '{{ var("start_date") }}'
                         and date '{{ var("end_date") }}'
    {% endif %}

)

select
    order_id,
    order_id_num,
    order_date,
    store_id,
    customer_id,
    currency,
    generate_subscripts(items, 1)      as line_no,
    unnest(items, recursive := true)   -- -> sku, qty, unit_price, disc
from orders
where items is not null
```

(`generate_subscripts` is the one new SQL function today: it emits 1,2,3… in lockstep with `unnest`, giving each item its line number.)

Then two marts. `models/marts/daily_store_sales.sql` is given, with one A10 scar folded in — **currency joins the grain**, because summing USD and VND in one number is a ×25,000 lie you have already been burned by:

```sql
select
    o.order_date,
    o.store_id,
    o.currency,
    any_value(s.store_name)                          as store_name,
    any_value(s.region)                              as region,
    count(*)                                         as orders,
    count(distinct o.customer_id)                    as customers,
    sum(o.order_total)                               as gross_revenue,
    round(avg(o.order_total), 2)                     as avg_order_value,
    count(*) filter (where o.status = 'cancelled')   as cancelled_orders,
    count(*) filter (where o.order_total is null)    as orders_missing_total
from {{ ref('fct_orders') }} o
left join {{ ref('dim_stores') }} s using (store_id)
group by 1, 2, 3
```

**You write** `models/marts/category_daily_revenue.sql`: from `{{ ref('fct_order_items') }}`, LEFT JOIN `{{ ref('dim_products') }}` on sku, grain = (order_date, category, currency), measures = distinct orders, `sum(qty)` units, `sum(qty * unit_price)` gross_revenue. LEFT JOIN on purpose — orphan SKUs (A04's 0.2%) must land visibly in a NULL category, not vanish.

```powershell
dbt build
```

**Accept when:** everything is green; `core.fct_order_items` = **10,253,421** rows (≈2.53 items/order — sanity vs. A04's estimate); `marts.daily_store_sales` = **50,282**; `marts.category_daily_revenue` = **1,990**, of which **143** rows have NULL category. If NULL-category rows are missing, you inner-joined.

### Task 7 — Tests: the A05 suite comes home *(small, ~45 min)*

Generic tests live in YAML next to the models. Create `dbt_lab/models/core/schema.yml`:

```yaml
version: 2

models:
  - name: fct_orders
    description: One row per order, canonical schema, latest correction applied.
    columns:
      - name: order_id
        data_tests: [not_null, unique]
      - name: order_id_num
        data_tests: [not_null]
      - name: order_date
        data_tests: [not_null]
      - name: status
        data_tests:
          - accepted_values:
              arguments:
                values: ['created', 'paid', 'shipped', 'delivered',
                         'cancelled', 'refunded']
      - name: store_id
        data_tests:
          - not_null
          - relationships:
              arguments:
                to: ref('dim_stores')
                field: store_id
      - name: customer_id
        data_tests:
          - relationships:
              arguments:
                to: ref('dim_customers')
                field: customer_id
              config:
                severity: warn   # ~0.05% orphan ids are documented dirt (A05)

  - name: dim_customers
    columns:
      - name: customer_id
        data_tests: [not_null, unique]

  - name: dim_products
    columns:
      - name: sku
        data_tests: [not_null, unique]
```

(Syntax notes for dbt ≥ 1.12: the key is `data_tests:`, and tests that take parameters nest them under `arguments:`.) Each entry compiles to a `SELECT` that must return zero rows — `dbt test` even saves them under `target/compiled/`, and they read exactly like your A05 queries, because they are your A05 queries. And like A05, the rules are not invented here: generic tests are the contract's per-column schema promises made executable in dbt — `accepted_values` is `allowed_values`, `relationships` is `references`, straight out of the `schema:` block of `contracts/shopcore_orders_daily.contract.yaml` (the `quality:` block's tolerances return in the singular test below). The one design decision worth staring at is `severity: warn` on the customer relationship: you *know* orphan customer_ids exist (~0.05%, documented since A05). A test that fails on documented dirt trains people to ignore red. Warn = "known, watched"; error = "stop the line." That is your A05 threshold philosophy expressed in dbt — tolerances come from the contract, not perfection.

(The one `description:` above is a placeholder-grade stub, and Task 10 comes back to fix it. Tests first, descriptions second, is fine as a working order — shipping the stub is not.)

**Name the successor** (§7.5, the last one). dbt's built-in generic tests are deliberately few. The community package **dbt-expectations** ports Great Expectations' assertion library into dbt tests — distribution shape, string patterns, row-count-between, column-type checks — installed via `packages.yml` + `dbt deps`. We do not install it: your four generic tests plus one singular test already express every rule the contract states, and a package that adds fifty assertions you have not justified is how test suites turn into noise. Know the name; reach for it when a real rule needs a shape dbt Core cannot say.

One check cannot be expressed column-by-column: the items-sum reconciliation. That is a **singular test** — plain SQL in `dbt_lab/tests/assert_order_totals_reconcile.sql`:

```sql
-- A05's cross-field check as a singular test, era-blind thanks to A10:
-- order_total = items_total - discount_amount holds for v1/v2/v3 alike,
-- because v1 rows were canonicalized with discount_amount = 0.
-- ~0.5% mismatch is documented dirt; fail only past the contract's
-- total_vs_items_mismatch tolerance (1% — the same number A05 loaded from the YAML).
with items_sum as (
    select order_id, sum(qty * unit_price) as items_total
    from {{ ref('fct_order_items') }}
    group by 1
),

checked as (
    select o.order_id, o.order_total, o.discount_amount, i.items_total
    from {{ ref('fct_orders') }} o
    join items_sum i using (order_id)
    where o.order_total is not null
)

select
    count(*) filter (
        where abs(order_total - (items_total - discount_amount)) > 0.01
    )                          as bad_rows,
    count(*)                   as checked_rows
from checked
having bad_rows > 0.01 * checked_rows
```

Zero rows = pass. The `HAVING` is the threshold: on the baseline data ~0.6% of orders mismatch (documented), well under the 1% tolerance — the test passes *while the dirt exists*, which is precisely the A05 lesson about thresholds versus perfection. Note it is **era-blind**: one formula for v1/v2/v3, possible only because your A10 canonicalization set `discount_amount = 0` for v1. Savor that a second time.

```powershell
dbt build
```

`dbt build` runs seeds, models, and tests in DAG order — and tests *gate*: if `fct_orders` failed a test, downstream models would be skipped, like your runner refusing to build marts on a red day.

**Accept when:** the summary reads `PASS=24 WARN=1 ERROR=0 SKIP=0 … TOTAL=25`, and the one WARN is the customer relationship with **2,056** orphan rows (small scale — journal that number).

### Task 8 — The incremental backfill drill *(small, ~30 min)*

Now prove the property this lab has been hammering since A07: **safe to rerun, any window, any number of times.** Journal a timing for each run:

```powershell
# 1) baseline: full build (you have this from Task 7 — roughly 20-90 s at small scale)
# 2) a 3-day window, the A11 --start/--end pattern:
dbt build --vars "{start_date: 2026-08-01, end_date: 2026-08-03}"
# 3) the same command AGAIN — the A07 double-run test:
dbt build --vars "{start_date: 2026-08-01, end_date: 2026-08-03}"
```

Then check state after run 3, in any DuckDB session (close it before the next dbt run):

```sql
SELECT count(*), count(DISTINCT order_id) FROM core.fct_orders;   -- 4053208, 4053208
SELECT order_date, count(*) FROM core.fct_orders
WHERE order_date BETWEEN DATE '2026-08-01' AND DATE '2026-08-03'
GROUP BY 1 ORDER BY 1;    -- 2026-08-01: 46273 · 08-02: 45874 · 08-03: 56940
```

Counts identical after a re-run = delete+insert idempotency, the thing you built by hand in A07 and here got for one line of `config()`. This is also the moment Task 5 promised: reopen `target/run/etl_lab_dbt/models/core/fct_orders.sql` — the compiled file now holds the `DELETE`+`INSERT` pair. The windowed run should land in roughly a third of the full-build time at small scale (the dims and marts still rebuild fully — only the facts are windowed; at full scale the gap grows much larger because the facts dominate). Two variations to try and journal:

```powershell
dbt build --select fct_orders+ --vars "{start_date: 2026-08-01, end_date: 2026-08-03}"
dbt build --full-refresh      # drops and rebuilds incrementals from scratch
```

`fct_orders+` (the `+` means "and everything downstream") is the surgical version — dims and staging untouched. `--full-refresh` is your A11 `--force`: a restatement, with everything that word implied about warning consumers first.

One trap to write in your journal before it bites: **the late-data allowance still applies.** When tomorrow's file D lands in the lake, partitions D−7…D changed (A06 contract, measured in A08). The correct daily window is `start_date = D−7, end_date = D` — never just D.

**Accept when:** both windowed runs are green with stable counts, timings journaled, and the D−7 rule is written down.

### Task 9 — The real thing: full scale *(full, ~30–60 min, mostly waiting)*

One flag. That is the payoff of the two-target profile:

```powershell
dbt debug --target full
dbt build --target full
```

Expect roughly 10–30 minutes on the baseline machine for the first full build (~82M order rows in the lake, ~200M item rows through unnest — watch Task Manager: memory stays under the 8 GB limit, spill may appear in `<DATA_ROOT>/tmp`, exactly as A13 taught you to expect). Then run the 3-day window at full scale, twice, and journal all three wall-clocks — full build vs. windowed vs. windowed-again is the clearest before/after of the whole lab.

**Accept when:** full-scale `dbt build --target full` is green (same 1 WARN); windowed rerun is green with stable counts; three timings in the journal.

### Task 10 — Describe it: the metadata a catalog catalogues *(both, ~30 min)*

Everything you have built so far is *data*. **Metadata** is the data about it: types, tests, freshness, dependencies — and the one part no tool can produce for you, what a column actually *means*. dbt collects the mechanical metadata for free. The meanings you write yourself, and `schema.yml` is where they go.

Go back to `models/core/schema.yml` and give every model, and every column that carries meaning someone could get wrong, a real `description:`. Here is `fct_orders` done properly — merge these `description:` fields into the block you already have, keeping every column and test already there, then do the rest yourself:

```yaml
models:
  - name: fct_orders
    description: >
      One row per order, canonical schema, latest correction applied. Built from
      the lake by keeping the highest `updated_at` per `order_id_num`, so an order
      corrected in a later file appears once, in its final state. Rebuilt by date
      window (delete+insert), so any date range is safe to reload.
    columns:
      # order_id_num, status and store_id are omitted here for brevity — keep
      # your Task 7 entries for them, tests and all, or TOTAL drops 25 -> 21.
      - name: order_id
        description: >
          Canonical order key, `ORD-` plus a zero-padded 10-digit number. Stable
          across all three schema eras — v1/v2 numeric ids were reformatted into
          this shape in A10 so that v3 corrections match v1 history.
        data_tests: [not_null, unique]
      - name: order_date
        description: >
          Business date of the order, derived from the cleaned `order_ts`. This is
          the partition key of the lake and the delete+insert key of this model —
          reloading a date range replaces exactly these rows.
        data_tests: [not_null]
      - name: customer_id
        description: >
          Customer who placed the order; foreign key into `dim_customers`. NULL is
          meaningful — the contract says it means guest checkout, not missing data.
          About 0.05% of ids are orphans (documented dirt since A05), which is why
          the relationship test warns instead of failing.
        data_tests:
          - relationships:
              arguments:
                to: ref('dim_customers')
                field: customer_id
              config:
                severity: warn
      - name: currency
        description: >
          ISO currency code of `order_total` and the item prices. Never sum across
          currencies — VND amounts are ~25,000× USD ones, which is why every mart
          in this project carries currency in its grain.
```

Read what those sentences are doing. Not one of them says "the order id" or "the order date". **A description that only restates the column name is worse than no description at all**: it makes the catalog look documented while telling the reader nothing, and nobody ever re-opens a field that already has text in it. Every description above carries something the name cannot — a *decision* (why the id has that shape), a *constraint* (never sum across currency), or a *warning* (NULL means guest checkout, not a bug).

And note where `fct_orders` *starts*: "One row per order" — its grain — before a word about what the table is for. That is the rule, not a house style (§7.7): **every model description opens with its grain sentence**, the same "one row = one …" A03 made you write as a comment on the DDL. The comment is read by whoever edits the model; the catalog is where everybody else meets the table, and a stranger deciding whether it answers their question needs to know what one row is before they need anything else. Grain first, then the thing the name cannot say.

Two new files, **descriptions only, no tests**, so your `TOTAL=25` does not move. First `models/marts/schema.yml`, where the audience is least likely to have read your SQL:

```yaml
version: 2

models:
  - name: daily_store_sales
    description: >
      One row per (order_date, store_id, currency) — store performance by day:
      order counts, distinct customers, revenue and cancellations. Currency is in
      the grain deliberately — summing USD and VND into one number is a ×25,000
      lie, the A10 scar this mart refuses to repeat.
    columns:
      - name: gross_revenue
        description: >
          Sum of `fct_orders.order_total` for the day, store and currency. Includes
          cancelled and refunded orders; net revenue is a different question and
          would be a different mart.
      - name: orders_missing_total
        description: >
          Orders whose total could not be parsed to a number. Surfaced as a column,
          not hidden — it is the denominator caveat for `avg_order_value`.
```

**You write** `models/staging/schema.yml` the same way: the four `stg_shopcore__*` models, plus the `country_map` seed under a `seeds:` key in the same file. Say what the layer *is* — thin, because A10 aligned the three eras before anything reached the lake.

Two facts belong in descriptions specifically because no reader can infer them from the SQL: `stg_shopcore__customers.email_hash` exists **because of** A06's data-classification clause (the raw address is deliberately not selected, not accidentally missing), and `dim_products.unit_price` is today's list price while `fct_order_items.unit_price` is what that order was actually charged. Both are exactly the wrong-query-waiting-to-happen that a catalog exists to prevent.

**Accept when:** every model and the seed carry a real `description:` whose **first sentence states its grain**; `dbt build` still ends `PASS=24 WARN=1 ERROR=0 … TOTAL=25` (descriptions are not tests); and you can point at three descriptions that would have saved a colleague a wrong query.

### Task 11 — `dbt docs generate` **is** a data catalog *(both, ~15 min)*

```powershell
dbt docs generate
dbt docs serve      # opens http://localhost:8080 — Ctrl+C to stop
```

`generate` ends with a line worth actually reading:

```
Building catalog
Catalog written to ...\dbt_lab\target\catalog.json
```

dbt uses the word itself, and it means it. **A data catalog is the searchable inventory of what data exists, what it means, and whether you can trust it** — and the site you just built is one: every table and column, its real type, your descriptions, the tests guarding it, source freshness, the DAG, and the compiled SQL behind each model. Two files feed it: `target/manifest.json` holds what *you* declared (descriptions, tests, dependencies) and `target/catalog.json` holds what the *warehouse* reports (actual column types and stats). The site is the join of the two — which is why Task 10 had to come first: `docs generate` publishes your metadata, it does not invent it.

Click into `fct_orders`, then `daily_store_sales`. Ask the three questions a stranger asks — *what is this, can I trust it, who do I ask* — and notice you can now answer all three without opening a `.sql` file. That is the entire product.

The commercial versions are **DataHub**, **OpenMetadata**, **Unity Catalog** and **AWS Glue Data Catalog**; the lab installs none of them for the same reason it skipped Great Expectations (§7.5) — own backend, own config, a learning curve that would bury the lesson. What they add over `dbt docs` is coverage and automation: everything in the company rather than everything in one dbt project (the OLTP source, the S3 buckets, the BI dashboards), usage statistics, stewardship workflows, access policies, and column-level lineage (Task 13). dbt does reach past its own DAG in one way worth knowing: `exposures:` let you declare the dashboards and reports that read your marts, so "what breaks?" can name a dashboard and not just a table.

And notice what your catalog already carries from the rest of the lab (§7.6): ownership and escalation live in the A06 contract, the classification decision shows up as `email_hash`, quality standards show up as tests with contract tolerances, row-level provenance is `_data_date`/`_run_id` pointing at `ops.etl_runs`, and the audit trail of who ran what when is that same ledger. Governance is not a product you buy. It is a set of decisions, and you have made all of them; the catalog is just where they finally become findable.

**Accept when:** the site is served, and — from the site alone, no SQL — you can say what `orders_missing_total` counts and which two tests guard `fct_orders.order_id`.

### Task 12 — Table-level lineage and the impact-analysis drill *(both, ~30 min)*

That clickable graph has a proper name: **table-level lineage** (model-level lineage, same thing). It answers *what feeds this table, and what breaks if its source changes* — and today the question arrives for real.

**The scenario.** shopcore posts to `#shopcore-data-changes`, the channel named in the contract's `change_management` block: *"Planning to drop `channel` from the orders export. Any objections? Need an answer this week."* Dropping a column is a breaking change — the contract owes you 30 days' notice and a MAJOR version bump (A10's amendment discipline). But before you quote a clause you need the engineering answer: **what actually breaks?**

Do not grep. Ask the DAG:

```powershell
dbt ls --quiet --select "source:shopcore.orders+"
```

The `+` suffix means "and everything downstream". Real output — identical on either target, because this reads your project structure, not your data:

```
etl_lab_dbt.marts.category_daily_revenue
etl_lab_dbt.marts.daily_store_sales
etl_lab_dbt.core.fct_order_items
etl_lab_dbt.core.fct_orders
etl_lab_dbt.staging.stg_shopcore__orders
source:etl_lab_dbt.shopcore.orders
etl_lab_dbt.core.accepted_values_fct_orders_status__created__paid__shipped__delivered__cancelled__refunded
etl_lab_dbt.assert_order_totals_reconcile
etl_lab_dbt.core.not_null_fct_orders_order_date
etl_lab_dbt.core.not_null_fct_orders_order_id
etl_lab_dbt.core.not_null_fct_orders_order_id_num
etl_lab_dbt.core.not_null_fct_orders_store_id
etl_lab_dbt.core.relationships_fct_orders_customer_id__customer_id__ref_dim_customers_
etl_lab_dbt.core.relationships_fct_orders_store_id__store_id__ref_dim_stores_
etl_lab_dbt.core.unique_fct_orders_order_id
```

Fifteen nodes: five models, the source itself, and nine tests. (`--quiet` prints nodes only — without it dbt's three startup log lines land on stdout too and ruin any `Measure-Object` you pipe it into.) Narrow it when you only want the build list:

```powershell
dbt ls --quiet --select "source:shopcore.orders+" --resource-type model
```

Now write the reply. Three parts, and none of them is "sure, go ahead":

1. **Blast radius, from the DAG.** Five models and nine tests sit downstream of this source — name them. That is the list you would have had to assemble by memory before today.
2. **The contract.** Column removal is MAJOR under `change_management`: 30 days' notice, a version bump, a migration note, announced on that channel. Your A06 pre-load gate will fail the first file that drops the column — which is the *correct* behaviour, per the contract's own "fail loudly rather than load garbage" policy.
3. **What you need back.** An effective date, so you can retire the column from staging *after* the last file that still carries it — the same era-boundary handling you built in A10.

Journal the answer. Then journal the honest limitation, because it is the entire reason Task 13 exists: `dbt ls` told you which models **read the orders source**. It did not tell you which models read the **`channel` column**. Table-level lineage always over-reports — safe, but blunt.

**Accept when:** the impact list is in your journal with all five models named, the reply cites `change_management` by name, and you have written down why that list is an upper bound rather than an answer.

### Task 13 — Column-level lineage, by hand *(both, ~20 min)*

**Column-level lineage** tracks a single field through every expression that reads it. dbt Core's docs site does not draw it — the DAG stops at model boundaries. Commercial catalogs (and dbt's own paid Explorer) parse each model's SQL to derive it automatically. Today you do it the way you will do it on any system that lacks the feature: by reading four files.

Trace `order_total`, and write down each hop with its expression:

| # | Where | What happens to it |
|---|---|---|
| 1 | `shopcore.orders` (the lake) | `DECIMAL(14,2)`, already clean — A03's `$` / `N/A` / decimal-comma cleaner ran before the Parquet was written |
| 2 | `stg_shopcore__orders.order_total` | passed through untouched |
| 3 | `fct_orders.order_total` | survives only on the winning row of the dedupe window — a correction *replaces* this value |
| 4 | `marts.daily_store_sales.gross_revenue` | `sum(order_total)` per (order_date, store_id, currency) |
| 4′ | `marts.daily_store_sales.avg_order_value` | `round(avg(order_total), 2)` — same column, second measure |
| 4″ | `marts.daily_store_sales.orders_missing_total` | `count(*) filter (where order_total is null)` |
| 4‴ | `tests/assert_order_totals_reconcile.sql` | compared against `sum(qty * unit_price) − discount_amount` |

Now the payoff. `marts.category_daily_revenue` was in Task 12's list of five — and it appears **nowhere** in this table. It computes revenue from `qty * unit_price` on order lines; `order_total` never reaches it. Table-level lineage said "at risk". Column-level lineage says "untouched".

Do the same for the column shopcore actually asked about, `channel`. It reaches `stg_shopcore__orders`, then `fct_orders` — and stops. No mart selects it. No test asserts on it. So the true answer is: *two models to edit, no mart output changes, no test changes* — a twenty-minute job, not a project. That is a very different email from the one the DAG alone would have made you write, and it is the difference between an engineer who can be trusted with a change request and one who answers "everything might break" every time.

**Accept when:** both traces are in your journal, and you can state in one sentence what column-level lineage buys you over the DAG — and what it costs you (you did it by hand; a catalog does it by parsing every model's SQL, which is precisely the automation you are paying for).

### Task 14 — Design a table that does not exist yet *(both, ~40 min)*

Fourteen assignments handed you a target and asked you to hit it. Real work does not arrive that way: someone asks a question, and the shape of the table that answers it is *your* decision. That decision is the part of this job that does not automate, so you make one here, deliberately, before writing a line of SQL.

**The request.** From the merchandising lead, in the same channel where shopcore posted yesterday:

> "Which products get returned most, by region and month? I'd want to see it every month."

**Write the design first — on paper, no SQL.** Prose and a small table in `journal.md`, under these six headings. This is the deliverable; the model is optional and worth less.

1. **Grain** — one sentence, "one row = one …". Everything else follows from it.
2. **Keys** — what identifies a row; which are natural, which the warehouse controls, and whether you need a new one at all (A10's ladder).
3. **Dimensions** — which existing dims answer "by region" and "by month", and whether either forces a *new* dimension.
4. **Measures** — the numbers, and for each: **additive** over every dimension in the grain, or not? Say why. (The word is defined in this doc's Background; the full treatment is `guides/data_modeling.md` §2.)
5. **Refresh strategy** — full rebuild or incremental; if incremental, keyed on what, over what window (remember the late-data rule you wrote down in Task 8).
6. **Reuse** — which existing models it stands on, and what new models it forces you to add.

And one line above all six: **what the question actually means**, because as asked it is not answerable. Three words in it are ambiguous, and spotting them is half the exercise. Do not read past the horizontal rule below until those six headings are filled in — the reference answer looks obvious once you have read it, and that feeling teaches nothing.

---

**Reference answer.** It argues rather than declares, because a design question with one right answer is not a design question. Differing from it is *fine* — provided you name the trade-off you took.

**First: the question is not answerable as asked.** Three ambiguities, and pinning them down is the design work juniors skip:

- **"returned"** — this feed has no return event. The only return signal is `status = 'refunded'` on the order, and the contract is explicit: refunds arrive as `status=refunded` with a **positive** `order_total` (`business_rules.refunds` — the sign is implied by the status, which is exactly why a negative total is a violation and not a refund). So "returned" means *"was a line on an order whose latest status is refunded"*. Put that sentence at the top of your design and into the model's `description:`, because your reader will otherwise assume something else.
- **"most"** — most *units*, most *orders*, or highest *rate*? Three rankings, and they disagree. Carry the ingredients for all three; rank in the query, not in the table.
- **"region"** — whose? `dim_stores.region` (NA / EU / APAC / LATAM — the store the order was placed at), or the customer's country in `dim_customers`? Both exist in your project; only one of them is *called* region, so that is the one — and you say so out loud rather than choosing silently.

**Grain: one row per (order_month, region, sku).** The question is per *product*, and product only exists at item grain — `fct_orders` has no `sku` at all. That is A04's lesson arriving as a design constraint: you do not pick the grain, the question does.

**The real fork: reuse the item fact, or build a returns fact?** Three defensible answers; the argument matters more than the pick.

- **(a) A mart over `fct_order_items`, filtered to refunded orders.** Cheapest: no new fact, no new load path, nothing extra to backfill or keep in sync. The cost is a join — look again at Task 6 and notice `fct_order_items` does not carry `status`; it selects `order_id`, `order_id_num`, `order_date`, `store_id`, `customer_id`, `currency` and the unnested item fields. So every returns question pays a join back to `fct_orders`. Fine for one mart built once a day. Irritating by the tenth.
- **(b) A separate `fct_returns` at item grain.** This wins the day returns acquire attributes the order fact cannot hold — a return *reason*, a restocking flag, a return date distinct from the order date. Those are facts about the return, not about the order, and hanging them off `fct_orders` would be the same mistake §7.2 taught you about `_source_file`: a fact about one thing stored on the rows of another. But this feed ships none of them today, so `fct_returns` would be `fct_order_items` with rows removed — a second copy of data you already have and a second thing to rebuild every morning. Not yet. Design for the data you have, and name the trigger that would change your mind.
- **(c) Push `status` down onto `fct_order_items`.** One denormalized column and the join disappears for every future returns question. The cost is the one A03 named for `orders_enriched` — a copy that goes stale when its source changes — except here it barely applies: the item fact is rebuilt by the same `delete+insert` window as the order fact it reads from, so the copy cannot drift. If you argued (c) you argued well; it is the cheapest repair to (a)'s only real flaw.

**Keys: nothing new.** `sku` is `dim_products`' natural key, `store_id` is `dim_stores`', and both already flow through the item fact — no lookup to build, no surrogate to mint. The mart's own key *is* its grain: `(order_month, region, sku)` is unique by construction of the `GROUP BY`, which is what "the grain is the key" means for an aggregate. Note the key you deliberately did not add: a surrogate earns its keep when other tables reference this one, and nothing references a mart.

**Dimensions: also nothing new.** Region arrives through `dim_stores`, joined on the `store_id` the item fact already carries. The urge to invent a `dim_regions` is worth refusing out loud: region is an *attribute of a store*, not an entity with a life of its own. Product name and category arrive through `dim_products`.

**And month is not a dimension here — it is a rollup:** `date_trunc('month', order_date)`. There is no `dim_date` in this project, and you can answer "by month" without one, which is precisely why it was never built. Real warehouses often do build a date dimension, for the things `date_trunc` cannot compute: fiscal periods, holidays, week-of-year, "was this a promo day". When merchandising comes back asking for *fiscal* months, that is when you build it. What you must not do is store a `month` column on the *fact* — it is derivable from `order_date`, so it is a second copy of a fact, and second copies drift.

**Measures — and the trap this whole exercise exists for.** Four ingredients, and one number you must refuse to store:

| measure | expression | additive over the grain? |
|---|---|---|
| `returned_units` | `sum(qty) filter (where status = 'refunded')` | **yes** |
| `sold_units` | `sum(qty)` | **yes** |
| `returned_lines` | `count(*) filter (where status = 'refunded')` | **yes** |
| `returned_orders` | `count(distinct order_id) filter (…)` | **no** — see below |
| `return_rate` | `returned_units / sold_units` | **no — do not store it** |

A **ratio is not additive**, and the test for additivity is not "does the number look wrong" — it is: *can I recover a rollup from the stored values alone?* For `returned_units`, yes: add them up. For `return_rate`, no. EU's June rate is not the sum of its per-SKU rates, and it is not their average either, unless every SKU happened to sell exactly the same number of units. A stored rate cannot be rolled up at all once its denominator is gone. So store the numerator and the denominator, and compute the rate in the query:

```sql
SELECT round(sum(returned_units) * 1.0 / sum(sold_units), 6) AS pooled_rate,
       round(avg(returned_units * 1.0 / sold_units), 6)      AS avg_of_cell_rates
FROM marts.monthly_product_returns
WHERE sold_units > 0;
```

Run it and read the result honestly: on this feed the two land within a fraction of a percent of each other, because the generator refunds every product at the same underlying rate and the cells are all roughly the same size. **That near-agreement is a property of this data, not of ratios** — and it is the most dangerous kind of luck, the kind that lets a wrong habit pass its first test. A real catalogue is lumpy: one clearance SKU with nine units sold and one returned reads as 11%, and in an unweighted average it outweighs a bestseller with thirty thousand. Only `pooled_rate` ever answers "what fraction of units come back".

`returned_orders` is the subtler one, and it is subtle *because of the grain you chose*: a refunded order lands in one row per distinct product it contains, so summing `returned_orders` across SKUs counts a three-product order three times. Expect the sum over all cells to overshoot the true distinct count by roughly 2.5× — which is, of course, the average items per order (A04's number, back again). Keep the column, since it answers "how many orders" *within* a cell, and label it non-additive in the description. Then look at `daily_store_sales.customers` (`count(distinct customer_id)`) and notice it has had exactly this property since Task 6: sum it over seven days and you do not get the week's distinct customers. You just learned to read your own model.

**One measure you do not need, and why that is interesting:** no money. Counts and units are currency-free, so — unlike every other mart in this project — **currency does not belong in this grain**. Add `returned_value` tomorrow and it belongs instantly, for the ×25,000 reason `daily_store_sales` carries it. Grain follows measures.

**The honest caveat, which is a modeling limit and not a bug.** The refund status sits on the *order*, not the line. A refunded three-line order makes all three products look returned; if only the headphones came back, this mart still blames the cable and the case. No SQL fixes that — the fix is line-level return events from the producer, i.e. an A06 contract amendment with a notice period, not an afternoon's work. Write the caveat into the mart's `description:`; it is the precise "wrong query waiting to happen" that Task 10 says a catalog exists to prevent. (One warning about the synthetic feed: it assigns refunds without regard to product, so the *leaderboard* you get is noise. The shape of the answer is the lesson; the ranking is not.)

**Refresh strategy.** The mart is derived, so it inherits the facts' window. Two defensible answers:

- **Full rebuild as a `table` every run** — what `daily_store_sales` does. The output is one row per (month, region, sku): tens of thousands of rows, seconds to build. "The simplest thing that works" is a real answer here, not a cop-out.
- **Incremental, `delete+insert` on `order_month`** — matching the facts, worth it only once the full rebuild stops being cheap. If you pick it, watch what the late-data rule does: a file for day D corrects days D−7…D (the A06 contract window, measured in A08), so any month those days touch is dirty and must be recomputed *whole*. The refresh key and the grain turn out to be the same decision — usually a sign the grain was right.

**Reuse.** `fct_order_items` (grain + sku), `fct_orders` (status), `dim_stores` (region), `dim_products` (category). Zero new sources, zero new staging models, zero edits to existing models. That ratio is the tell of a good design: a request that forces a new *source* is usually a different question wearing this one's clothes.

**Optional build (~20 min).** If you want it in the project, it is one model — `dbt_lab/models/marts/monthly_product_returns.sql`:

```sql
-- Grain: one row per (order_month, region, sku).
-- "Returned" = a line on an order whose latest status is 'refunded'. The signal
-- is order-level, so every line of a refunded order counts — see the description.
with lines as (

    select
        cast(date_trunc('month', i.order_date) as date) as order_month,
        s.region                            as region,
        i.sku                               as sku,
        i.order_id                          as order_id,
        i.qty                               as qty,
        o.status                            as status,
        p.category                          as category
    from {{ ref('fct_order_items') }} i
    join      {{ ref('fct_orders') }}   o using (order_id)
    left join {{ ref('dim_stores') }}   s on s.store_id = i.store_id
    left join {{ ref('dim_products') }} p on p.sku      = i.sku

)

select
    order_month,
    region,
    sku,
    any_value(category)                                          as category,
    count(*)                 filter (where status = 'refunded')  as returned_lines,
    coalesce(sum(qty) filter (where status = 'refunded'), 0)     as returned_units,
    count(distinct order_id) filter (where status = 'refunded')  as returned_orders,
    count(*)                                                     as sold_lines,
    sum(qty)                                                     as sold_units
from lines
group by 1, 2, 3
```

Three details worth the ten seconds. `cast(... as date)` because `date_trunc` hands back a TIMESTAMP and a month column that prints `2026-06-01 00:00:00` invites the question "00:00:00 of what?". `coalesce(…, 0)` because a cell with no returns has *zero* returns, not unknown ones — a NULL there would quietly drop the row out of every average someone runs. And `category` is denormalized in with `any_value` for the same reason `daily_store_sales` carries `store_name` — convenience, paid for with a rebuild when the dim changes — while the LEFT JOINs are deliberate: A04's ~0.2% orphan SKUs must land in a visible NULL category, not vanish into an inner join.

**If you build it, your counts move.** A thirteenth buildable takes the `dbt build` summary from `TOTAL=25` to `TOTAL=26` (`PASS=25 WARN=1`); every number in the Definition of Done and Verify-yourself sections below is stated for the project *without* this optional model. And add its `description:` to `models/marts/schema.yml` — grain in the first line, the order-level-signal caveat in the second — or you have just shipped the undocumented table Task 10 spent half an hour arguing against.

**Accept when:** the six headings are filled in *in your journal, written before you read the reference answer*, and you can defend one place where your design differs from it by naming the trade-off you accepted. Agreement is not the pass condition; a named trade-off is.

### Task 15 — The map back *(both, ~20 min)*

Close the loop. Copy this table into `journal.md`, check every row against something you actually did today, and **add at least two rows of your own** (candidates: `dbt show`, selectors, `target/compiled/`, the schema macro):

| dbt feature | Where you built it by hand |
|---|---|
| `source` + `external_location` | A01/A02 — `read_csv`/`read_parquet` over the lake |
| source freshness | A05 reconciliation + the A06 contract's freshness clause |
| seed (`country_map`) | A03 — the conformance mapping table |
| staging views (rename/cast/clean) | A03 staging + A10 canonicalizer |
| `ref()` — the dependency graph | A11 Task 8 — "marts rerun after core, in this order" |
| materializations (view/table) | A03 — layer design, what persists vs. derives |
| incremental, `delete+insert` by date | A07 `BEGIN;DELETE;INSERT;COMMIT` + A08 strategy (c) |
| dedupe window in the fact | A08 strategy (a) — latest `updated_at` wins |
| `var("start_date")/var("end_date")` | A11 runner `--start/--end` |
| `--full-refresh` | A11 `--force` (restatement, with comms) |
| generic tests (not_null/unique/…) | A05 check suite |
| contract schema promises (`allowed_values`/`references`) → `schema.yml` tests | A05/A06 — the contract YAML's per-column clauses, executable |
| `severity: warn` / thresholds | A05 — documented dirt vs. hard failure |
| singular test (items reconciliation) | A05 cross-field check, era-blind via A10 |
| `--target small/full` | A00 — the dev-on-small rule |
| `threads` + one-process DuckDB | A12 — budget math and the single-writer rule |
| `schema.yml` `description:` (metadata) | your journal + the A06 contract's per-column semantics — now attached to the columns themselves |
| `dbt docs generate` = a **data catalog** | nothing yet — this is the first time the lab's metadata became searchable by someone who did not build it |
| the DAG = **table-level lineage** | the "what feeds what" picture you have carried in your head since A02 — now generated, clickable and queryable |
| `dbt ls --select source:…+` (impact analysis) | A06 `change_management` — answering "what breaks" before agreeing to a change |
| **column-level lineage** (by hand) | the A03 → A10 cleaner chain; nothing in the lab draws it, which is what commercial catalogs sell |
| **row-level provenance** | A03/A07 — `_data_date` + `_run_id` → `ops.etl_runs`. The level dbt does *not* model: it lives on your rows and your ledger, not in the DAG |

**Accept when:** the table is in your journal with your two added rows, and for every row you can say one sentence about the hand-built version without looking it up. The last row is the one to dwell on: dbt gave you two of §7.6's three levels of lineage for free and cannot give you the third — a dbt DAG will never tell you which *run* wrote a particular row.

## Definition of Done

- [ ] `dbt_lab/` exists in the repo with `dbt_project.yml`, `profiles.yml`, the schema-name macro, the `shopcore` source (orders + 3 dim tables), seed, 4 staging views (`stg_shopcore__*`), 5 core models, 2 marts, three `schema.yml` files (staging/core/marts), 1 singular test; `target/`, `logs/`, `dbt_packages/` gitignored.
- [ ] `dbt debug` passes on both targets.
- [ ] `dbt source freshness` runs; you can explain its WARN/ERROR status.
- [ ] Small scale: `dbt build` fully green — `PASS=24 WARN=1 ERROR=0`, the WARN being the documented orphan-customer relationship (2,056 rows).
- [ ] `core.fct_orders` = 4,053,208 rows and you can explain the delta vs. 4,116,024 lake rows.
- [ ] 3-day windowed `dbt build --vars ...` run **twice**: green both times, counts stable, visibly faster than full — timings journaled.
- [ ] Full scale: `dbt build --target full` green; windowed rerun green; wall-clocks journaled.
- [ ] Every model and the seed carry a **real** `description:` whose **first sentence is that model's grain** (§7.7 rule 1) — no restated column names, no placeholders — and `dbt build` still ends `TOTAL=25`.
- [ ] `dbt docs generate` succeeded, the site is served, and you can say why it *is* a data catalog and name two commercial equivalents.
- [ ] Impact analysis answered from `dbt ls --quiet --select "source:shopcore.orders+"`: five downstream models named, the reply cites the contract's `change_management` clause, and the journal records why the list is an upper bound.
- [ ] Column-level traces of `order_total` and `channel` in the journal, including why `category_daily_revenue` is a false positive of the table-level list.
- [ ] The returns model is **designed in the journal before any SQL is written**, under all six headings — grain, keys, dimensions, measures (each marked additive or not), refresh strategy, and the existing models it reuses — preceded by the one line that says what "returned" means in this feed.
- [ ] You can defend one point where your design differs from the reference answer by naming the trade-off you took (or, if it matches, name the trade-off you accepted anyway).
- [ ] The feature-to-assignment mapping table is in your journal with ≥2 rows of your own, including the row-level-provenance row dbt cannot supply.

## Verify yourself

All numbers are for **small scale** with the canonical lake exactly as A10/A11 left it (kept + dropped = manifest, 816 dropped total), and for the project *without* Task 14's optional mart. Timestamps and counts are deterministic; wall-clocks are hedged.

```powershell
dbt show --inline "select count(*) as n from {{ source('shopcore','orders') }}"  # 4116024
dbt source freshness    # max_loaded_at 2026-08-10 23:31:14 -> WARN (or ERROR once
                        # you are more than 7 days past that stamp — both correct)
```

```sql
SELECT count(*) FROM core.fct_orders;                          -- 4053208
SELECT count(*) FROM core.fct_order_items;                     -- 10253421
SELECT count(*) FROM marts.daily_store_sales;                  -- 50282
SELECT count(*) FROM marts.category_daily_revenue;             -- 1990
SELECT count(*) FROM marts.category_daily_revenue
WHERE category IS NULL;                                        -- 143  (orphan SKUs, visible)
SELECT count(*) FROM core.dim_customers WHERE country IS NULL; -- 548  (else: seed incomplete)

-- the singular test's own math (documented dirt, under threshold):
WITH s AS (SELECT order_id, sum(qty*unit_price) AS t
           FROM core.fct_order_items GROUP BY 1)
SELECT count(*) FILTER (WHERE abs(o.order_total - (s.t - o.discount_amount)) > 0.01)
       AS bad, count(*) AS checked
FROM core.fct_orders o JOIN s USING (order_id)
WHERE o.order_total IS NOT NULL;                               -- 24116 bad / 4045048 (~0.6%)
```

Catalog and lineage — these depend only on your project, not on the data, so they are exact:

```powershell
dbt ls --quiet --select "source:shopcore.orders+" | Measure-Object -Line   # 15
#   5 models + 1 source + 9 tests. Without --quiet you get 18: dbt's three
#   startup log lines land on stdout too.

dbt ls --quiet --select "source:shopcore.orders+" --resource-type model
#   etl_lab_dbt.marts.category_daily_revenue
#   etl_lab_dbt.marts.daily_store_sales
#   etl_lab_dbt.core.fct_order_items
#   etl_lab_dbt.core.fct_orders
#   etl_lab_dbt.staging.stg_shopcore__orders

dbt docs generate       # ends: "Catalog written to ...\dbt_lab\target\catalog.json"
```

Descriptions are not tests — confirm your Task 10 writing changed the catalog and nothing else: `dbt build` must still end `TOTAL=25`, while `target/manifest.json` gains a non-empty `description` for all 12 buildables.

`dbt build` summary: `PASS=24 WARN=1 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=25` — 12 buildables (1 seed + 4 views + 5 tables/incrementals + 2 marts) + 13 data tests. The WARN reports **2,056** orphan customer_ids. Windowed-rerun day counts: `2026-08-01: 46,273 · 08-02: 45,874 · 08-03: 56,940`, byte-stable across reruns. On the baseline machine expect roughly: small full build well under 2 minutes, small windowed run a fraction of that; full scale roughly 10–30 minutes for the first build.

## Common beginner mistakes

- **Running dbt from the wrong directory.** dbt looks for `dbt_project.yml` and `profiles.yml` where you stand. `cd dbt_lab` first; "Could not find profile" almost always means "wrong folder", not "broken YAML".
- **Writing real table names instead of `ref()`/`source()`.** `from core.fct_orders` works today — and silently falls out of the DAG: wrong build order, missing lineage, broken docs. If you type a schema name inside a model, stop.
- **Dropping the `is_incremental()` guard.** The model still runs green — while quietly restating all 69 days every single run. If a "3-day" run takes as long as the full build, read `target/run/.../fct_orders.sql` and see what you actually executed (A14's rule: measure, don't guess).
- **Windowing `[D, D]` on a daily run.** File D corrects days D−7…D−1 (A06 contract, A08 measurement). The window is `[D−7, D]`. dbt formalized your pipeline, not your calendar math — that part is still on you.
- **"Fixing" a red test by deleting it.** The orphan-customer warn and the 0.6% reconciliation mismatch are *documented dirt* — classify (warn + threshold), don't silence. A test that fails on known dirt gets tuned; a test that fails on new breakage gets investigated. Deleting is neither.
- **Editing dbt-owned tables by hand.** Any manual `UPDATE core.fct_orders` lives exactly until the next `dbt build` clobbers it. State belongs in models, fixes belong in model SQL — the same "fix it in staging, not downstream" rule as A03.
- **Filling `description:` with the column name again.** `order_date: "the order date"` is worse than leaving it blank: blank is visibly a gap someone will fill, while a stub looks finished forever. A description earns its place by carrying a decision, a constraint or a warning — the things your SQL cannot say out loud. The same trap catches lineage answers: quoting `dbt ls` as "these five models break" without checking which of them touch the *column* is over-reporting dressed up as diligence.

## Stretch goals

1. **Snapshot a mutable dim.** `dbt snapshot` is SCD Type 2 — it keeps history rows with valid_from/valid_to when a dimension changes. Snapshot `dim_customers` (`strategy: check`, `check_cols: ['country', 'is_active']`), hand-edit one customer in a copy of the CSV, re-snapshot, and find both versions. Connect it to A08's "mutable entities" discussion.
2. **`store_failures`.** Add `config: {store_failures: true}` to a test and find the failing rows dbt saved as a table — then compare with your A05 quarantine design: what does yours capture that dbt's doesn't? (Hint: `reject_reason` and `_rejected_at` on the row, plus the §7.2 pair `_data_date`/`_run_id` — and through `_run_id`, a `JOIN ops.etl_runs` that names the file, the load time, the status and the row count of the run that produced the reject. dbt's failure table knows which *test* failed; yours knows which *delivery* did.)
3. **Publish a mart back to the lake.** dbt-duckdb supports `materialized='external'`, writing a model as Parquet instead of a warehouse table (see the dbt-duckdb README). Materialize `daily_store_sales` under `<DATA_ROOT>/lake/marts/` and read it from a fresh DuckDB session — your A09 "versioned dirs + view repoint" pattern is how you'd make that swap atomic.
4. **Take the ritual to a remote.** Create a free private repo on GitHub/GitLab — or the local bare-repo stand-in from `guides/git_basics.md` §8, no account needed — push `main` and your `a15-dbt` branch, open a pull request to yourself, and read your own diff like a reviewer before merging. Then journal the punchline: branch → commit per task → read the diff → merge on green is exactly what you have practiced since A00 — the team PR workflow is that ritual plus one reviewer, and you have now run it end to end.
