# A10 — Schema evolution: month 2 arrives
**Topic:** schema evolution · **Est. time:** 4h · **Difficulty:** ⭐⭐⭐⭐
**Requires:** A09 done; month-1 lake + `warehouse.duckdb` in place; contract gate from A06; the idempotent per-day loader from A07. Month-2 raw data is generated in Task 1 of this assignment.

## Why this matters

The upstream team at shopcore, the e-commerce platform, just emailed: "We shipped export v2 on July 16 and v3 on August 1. Should be backwards compatible!" It is not. It never is. Schema changes from producers are the number one silent killer of real pipelines: the lucky changes crash your loader at 2 a.m., and the unlucky ones load *wrong data without any error* until someone questions a dashboard weeks later. Today you learn to detect a schema change, classify how dangerous it is, and fold three schema versions into one **canonical schema** so the rest of your pipeline never has to care which era a row came from.

## Background

**Schema evolution** means: the structure of your input data changes over time while your pipeline keeps running. Columns get added, renamed, retyped, reordered. Your history does not change — so from today on, your pipeline must handle *all* versions at once, forever.

Our feed now has three **eras** (an era = a date range with one stable schema):

| Era | Dates | What changed |
|-----|-------|--------------|
| v1 | 2026-06-01 → 2026-07-15 | The 10 columns you know from A01–A09. |
| v2 | 2026-07-16 → 2026-07-31 | Three columns **appended**: `currency`, `discount_amount`, `channel`. Also a rule change: `order_total` now equals items total **minus discount**. |
| v3 | 2026-08-01 → 2026-08-08 | `order_id` becomes VARCHAR (`ORD-0000000123`); `payment_method` **renamed** to `payment_type`; items structs gain a `disc` field; new column `loyalty_tier`; and the column **order** changed. |

Not all changes are equally dangerous. Classify every change on two axes.

*Compatibility:* an **additive** change (new column at the end) is safe-ish — old readers can ignore it. A **rename** is breaking — to a program, `payment_type` is simply a new column, and `payment_method` has vanished. A **type change** is breaking — `'ORD-…'` will not cast to BIGINT. A **nested change** (new field inside the `items` JSON) is subtle — nothing fails until code that parses the JSON meets the new field, or silently drops it.

*Loudness:* a **loud** failure stops your pipeline with an error. A **silent** one loads garbage that looks fine. Loud failures are gifts. The rename is the scary one: a reader using `union_by_name` gets *both* payment columns, each half NULL, and nothing errors. You will see this with your own eyes in Task 3.

The professional answer is a **canonical schema**: one target schema that every era can be mapped into. You write one small **normalizer** per era (a SELECT that renames, casts, and fills in what's missing), and everything downstream — staging, core, marts, contracts — sees exactly one shape. Two design rules:

1. **Backfill missing columns with the right kind of nothing.** v1 rows have no `currency` — but every v1 order *was* in USD, so the value is known: default `'USD'`. v1 rows have no `channel` — and we genuinely don't know it: `NULL`. Known value → default; unknown value → NULL. Never invent data.
2. **Canonicalize identifiers.** Late corrections (A08) cross the era boundary: a v3 file carries corrections to orders that live in your lake as BIGINT ids. `'ORD-2070012345'` will never equal `2070012345` in a join — the correction silently misses, and an upsert would insert it as a brand-new order. Double counting, no error. So the canonical schema keeps both: `order_id` VARCHAR in the `ORD-…` form, and `order_id_num` BIGINT for matching history.

A nice side effect of canonicalization: after v1 rows get `discount_amount = 0`, the *new* v2 reconciliation rule `order_total ≈ items_total − discount_amount` becomes true for **all** eras. One formula for all history — that is the whole point.

## Setup

- Open a PowerShell in the repo, venv activated. Have `journal.md` and your `work/` scripts from A02/A07/A09 handy.
- **Develop every step on `small` first.** Only Task 7 touches full scale.
- Full scale month 2 needs roughly 8–9 GB more raw CSV plus a few GB of Parquet — check free disk first.
- Connection settings, as always:

```python
import duckdb
from lib.labpaths import data_root

ROOT = data_root("small")                      # switch to "full" only in Task 7
con = duckdb.connect(str(ROOT / "warehouse" / "warehouse.duckdb"))
con.execute(f"SET memory_limit='8GB'; SET threads=8; "
            f"SET temp_directory='{(ROOT / 'tmp').as_posix()}';")
```

## Tasks

### Task 1 — Generate month 2 (small, and start full in the background)

The "upstream team" ships the new export versions the moment you run:

```powershell
python datagen/generate.py --scale small --month 2
# in a SECOND terminal, start the big one now so it's ready for Task 7:
python datagen/generate.py --scale full --month 2     # roughly 5–10 min
```

**Accept when:** `<DATA_ROOT>/small/raw/orders/` contains **69** files (2026-06-01 … 2026-08-08) and `small/raw/manifest/` contains 69 JSONs.

### Task 2 — Break your loader on purpose (small)

Point your A02/A07 per-day loader at the first v2 day, then at a v3 day:

```python
load_day("2026-07-16")     # your existing loader, v1 columns= schema
load_day("2026-08-01")
```

Both fail. Paste the exact error into your journal. Read it carefully: DuckDB says *"It was not possible to automatically detect the CSV parsing dialect"*. Nothing about "3 new columns". This is the lesson: **schema drift rarely announces itself** — you gave `read_csv` 10 columns, the file has 13, and the sniffer gives up with an error that *sounds* like a delimiter problem. Now run your A06 contract gate on the same file and compare: the gate names the actual problem (unexpected columns) because you built it to. That difference — cryptic crash vs. actionable message — is why contracts exist.

**Accept when:** journal has both error texts plus one sentence on why the raw error is misleading and the gate's is not.

### Task 3 — Map and classify the drift (small)

First, find *when* the schema changed. Sniffed types wobble file-to-file, so compare the one thing that is cheap and stable: the header line.

```python
from pathlib import Path
sigs = {}
for f in sorted((ROOT / "raw" / "orders").glob("orders_*.csv")):
    header = open(f, encoding="utf-8").readline().strip()
    sigs.setdefault(header, []).append(f.stem[-10:])
for h, dates in sigs.items():
    print(f"{len(dates):3d} files  {dates[0]} .. {dates[-1]}\n     {h[:100]}")
```

You should see exactly 3 headers: 45 files, then 16, then 8 — the era boundaries. Next, explore what a "just union everything" read produces (one file per era; `union_by_name=true` matches columns by name across files):

```sql
SELECT * FROM read_csv(
  ['<DATA_ROOT>/small/raw/orders/orders_2026-06-02.csv',
   '<DATA_ROOT>/small/raw/orders/orders_2026-07-16.csv',
   '<DATA_ROOT>/small/raw/orders/orders_2026-08-01.csv'],
  union_by_name=true, filename=true);
```

Run `DESCRIBE` on that and study it. Three observations to write down:

- `order_id` came out VARCHAR — the v3 strings forced a promotion. Your BIGINT history won't join it.
- `payment_method` **and** `payment_type` both exist, each NULL for the other's files. `union_by_name` cannot know they are the same column. It papers over additive changes and *hides* renames.
- v1 rows show NULL for `currency` etc. — automatic NULL padding (count per file with `count(currency)` grouped by `filename` to see it).

Finish with a classification table in your journal — one row per change, e.g.:

| Change | Class | Loud or silent? | Why |
|--------|-------|-----------------|-----|
| `currency` etc. appended | additive | loud (only because our loader pins 10 columns) | … |
| `payment_method` → `payment_type` | rename | **silent** under union_by_name | … |
| `order_id` BIGINT → VARCHAR | type change | … | … |
| `items` gains `disc` | nested add | … | … |
| `loyalty_tier` added | additive | … | … |
| column order changed in v3 | reorder | **silent** for positional readers | see Task 5! |

> **This drift is also a process failure.** Open `contracts/shopcore_orders_daily.contract.yaml` and read the `change_management` block: a rename or type change is a MAJOR change requiring **30 days' notice** in `#shopcore-data-changes` plus a semver MAJOR bump and a migration note; even the v2 additions needed 7 days. shopcore shipped both eras with a cheery email after the fact. Add one line to your journal's incident write-up citing the clause (`change_management.notice_breaking`) — that citation is what turns "the data broke" into a producer incident with an owner.

**Accept when:** the header script prints 45/16/8 with the right boundary dates, the table has all six rows filled, and the journal cites the violated change-management clause.

### Task 4 — Define the canonical schema (design step, no data)

This is the schema every era will be normalized into. Copy it into your journal and write one line of justification next to every decision:

| Column | Type | v1 rows get | v2 rows get | v3 rows get |
|--------|------|------------|-------------|-------------|
| `order_id` | VARCHAR | `'ORD-' + 10-digit` built from the number | same | as delivered |
| `order_id_num` | BIGINT | the original id | same | prefix stripped, cast |
| `customer_id` … `updated_at` | as in A03 | unchanged | unchanged | unchanged |
| `payment_method` | VARCHAR | as delivered | as delivered | **from `payment_type`** |
| `order_total` | DECIMAL(14,2) | A03 cleaner | same | same |
| `currency` | VARCHAR | `'USD'` (known!) | as delivered | as delivered |
| `discount_amount` | DECIMAL(14,2) | `0` (known: no discounts existed) | as delivered | as delivered |
| `channel` | VARCHAR | `NULL` (unknown) | as delivered | as delivered |
| `loyalty_tier` | VARCHAR | `NULL` | `NULL` | as delivered |
| `items` | `STRUCT(sku VARCHAR, qty INTEGER, unit_price DOUBLE, disc DOUBLE)[]` | `disc` comes out NULL | same | full |
| `meta` | VARCHAR (parsed further in A04 logic) | unchanged | unchanged | unchanged |
| `order_date` | DATE | derived from cleaned `order_ts` | same | same |

Note the two id columns (Background rule 2), and that `discount_amount = 0` for v1 is what makes one reconciliation rule work everywhere. Casting v1 items JSON into the struct *with* `disc` simply yields `disc = NULL` — a nested column backfill for free.

> **Name what you just decided: this is the key question.** `order_id` is a **natural key** — a key that arrives with the data, minted by the producer. (Open the contract and read `order_id`'s `comment:`. shopcore calls it "shopcore surrogate key" — surrogate *for shopcore*. From where you sit it is inherited, which is why A03 called it natural: meaningful, free, and **outside your control**.) You have just watched a natural key change type underneath you. The `order_id` VARCHAR + `order_id_num` BIGINT pair above is not a patch — it is a **warehouse-controlled canonical key**: shopcore still supplies the meaning (*which* order), your warehouse owns the format. Three rungs on that ladder, and you should be able to argue all three:
>
> - **(a) Use the producer's key raw.** Simplest thing that works: no code, and every join reads exactly like the source. It just broke, on two counts — the type changed, and the late corrections stop matching history (Task 6 measures it: 0 of 705).
> - **(b) Canonicalize it — what you are doing here.** One deterministic expression per era. The format is yours, the meaning stays shopcore's, joins against BIGINT history keep working because you kept `order_id_num`, and it is cheap: no extra state, no lookup, and any engineer can re-derive the key by eye.
> - **(c) A true surrogate key.** The warehouse mints its own id — a sequence, or a hash of the natural key — and keeps the natural key as an ordinary attribute. Downstream is then fully decoupled: shopcore can rename, retype or re-prefix the id and your `core.orders` key never moves. The costs are real: a **key lookup on every load** (given this order, what is its surrogate — and late rows mean that lookup hits history, not just today's batch), and one more hop every time you debug against the source, because your id exists nowhere in shopcore's system. Note what it does *not* buy: the lookup still has to match `'ORD-0000000123'` to `123`, so the canonicalization on this page does not disappear — it moves inside the key assignment.
>
> Which rung is right depends on one thing: **how much you trust the producer's key to stay put**. Most teams pick (b) and are fine; teams burned twice pick (c). Today you have evidence rather than an opinion — write it down while it is fresh.

Now is the moment for `guides/data_modeling.md` §3 (Keys) — it carries this ladder alongside
composite keys and degenerate dimensions, with this assignment as its worked example.

**Accept when:** every default has a written "known value" or "unknown" justification, and the journal has one paragraph naming your key as a warehouse-controlled canonical key and arguing it against (a) and (c) — citing the drift you classified in Task 3 as the evidence, not a preference.

### Task 5 — Per-era readers and aligners (small)

Two layers, kept separate on purpose: an **aligner** per era does *schema* work only (rename, ids, missing columns) and outputs the same 15 raw-ish columns for every era; then ONE shared canonicalizer (Task 6) does the *value* cleaning you built in A03. Era logic stays in one thin layer.

Start `work/a10_canonical.py`:

```python
from datetime import date

def era_of(d: date) -> str:
    if d <= date(2026, 7, 15): return "v1"
    if d <= date(2026, 7, 31): return "v2"
    return "v3"

V1_COLS = {'order_id':'BIGINT','customer_id':'BIGINT','store_id':'INTEGER',
           'order_ts':'VARCHAR','updated_at':'TIMESTAMP','status':'VARCHAR',
           'payment_method':'VARCHAR','order_total':'VARCHAR',
           'items':'VARCHAR','meta':'VARCHAR'}
V2_COLS = dict(V1_COLS, **{'currency':'VARCHAR',
                           'discount_amount':'DECIMAL(14,2)','channel':'VARCHAR'})
V3_COLS = { ... }   # YOU write this one — read the warning below first
```

> **Warning — `columns=` maps by POSITION, not by name.** The header row is only skipped, not matched. v3 moved `currency/discount_amount/channel/loyalty_tier` *before* `items/meta`. If you list v3 columns in the v2 order, DuckDB will happily load the string `USD` into your `items` column — every column is VARCHAR-ish, so **no error is raised**. This is the silent misload from your Task 3 table. Build `V3_COLS` from the actual header you printed in Task 3, in file order, and spot-check two values after reading.

Now the aligners. v1 is given; v3 is given because it holds the two new tricks; **you write v2** (start from v1 — exactly three lines change: the three backfilled defaults become pass-through columns):

```python
ALIGN = {
"v1": """SELECT
    printf('ORD-%010d', order_id) AS order_id,
    order_id                      AS order_id_num,
    customer_id, store_id, order_ts, updated_at, status,
    payment_method, order_total,
    'USD'                    AS currency,
    CAST(0 AS DECIMAL(14,2)) AS discount_amount,
    CAST(NULL AS VARCHAR)    AS channel,
    CAST(NULL AS VARCHAR)    AS loyalty_tier,
    items, meta
  FROM src""",
"v2": """ ... """,          # YOU: real currency/discount/channel, NULL loyalty_tier
"v3": """SELECT
    order_id,
    CAST(replace(order_id, 'ORD-', '') AS BIGINT) AS order_id_num,
    customer_id, store_id, order_ts, updated_at, status,
    payment_type AS payment_method,
    order_total, currency, discount_amount, channel, loyalty_tier,
    items, meta
  FROM src""",
}

def open_day(con, day: str, era: str, raw_dir):
    f = (raw_dir / f"orders_{day}.csv").as_posix()
    cols = {"v1": V1_COLS, "v2": V2_COLS, "v3": V3_COLS}[era]
    con.execute(f"CREATE OR REPLACE VIEW src AS "
                f"SELECT * FROM read_csv('{f}', header=true, columns={cols})")
    con.execute(f"CREATE OR REPLACE VIEW aligned AS {ALIGN[era]}")
```

**Accept when:** for one day per era (say 06-02, 07-16, 08-01), `DESCRIBE SELECT * FROM aligned` returns the *identical* 15 columns in the identical order, and a v3 spot check shows `order_id_num` as a plain number and real values in `items`.

### Task 6 — One canonical staging, and the proof it works (small)

The shared canonicalizer — your A03 cleaners, applied once, era-blind (the timestamp and amount cleaners below are §4/A03 material, unchanged):

```python
TS = ("try_strptime(order_ts, ['%Y-%m-%d %H:%M:%S',"
      "'%d/%m/%Y %H:%M:%S','%Y-%m-%dT%H:%M:%S'])")
CANON = f"""
SELECT
  order_id, order_id_num, customer_id, store_id,
  {TS} AS order_ts, updated_at, status, payment_method,
  TRY_CAST(NULLIF(
    CASE WHEN order_total LIKE '%,%'
         THEN replace(replace(order_total, '.', ''), ',', '.')
         ELSE replace(TRIM(order_total), '$', '') END,
    'N/A') AS DECIMAL(14,2)) AS order_total,
  currency, discount_amount, channel, loyalty_tier,
  TRY_CAST(CAST(items AS JSON) AS
    STRUCT(sku VARCHAR, qty INTEGER, unit_price DOUBLE, disc DOUBLE)[]) AS items,
  meta,
  CAST({TS} AS DATE) AS order_date
FROM aligned
WHERE {TS} IS NOT NULL
"""
```

The `WHERE` drops rows whose timestamp cannot be parsed at all (the ~0.02% impossible dates) — they have no `order_date`, so they cannot be partitioned. **Count them, every run** (`SELECT count(*) FROM aligned WHERE {TS} IS NULL`) — a dropped row you didn't count is a lie in your row math. If your A05 quarantine is wired into the loader, route them there instead.

Land one day per era in `staging` — `open_day`, then `CANON`, then the **two §7.2 lineage columns**. `CANON` is era-blind *and* delivery-blind on purpose: it never learns which file it is reading, so the `day` you handed `open_day` is what stamps them.

```python
con.execute(f"""CREATE OR REPLACE TABLE staging.shopcore_orders_canon AS
  SELECT *, DATE '{day}'         AS _data_date,   -- which delivery this row came from
            CAST(NULL AS BIGINT) AS _run_id       -- alive once A11's runner registers a run
  FROM ({CANON})""")
```

Two, and that is the whole list (§7.2) — the file name, the load time and the status are facts about the *run*, not about the order, and they live on the `ops.etl_runs` row `_run_id` points at. Eighteen columns out: the sixteen canonical ones from Task 4, plus the pair. Now verify three things:

1. **Counts.** kept + dropped must equal the manifest `rows` exactly. Small scale: 06-02 → 59,024 · 07-16 → 59,613 · 08-01 → 47,008.
2. **One reconciliation rule for all eras.** For each of the three days:

```sql
SELECT count(*) FILTER (abs(order_total - (items_sum - discount_amount)) <= 0.01) AS ok,
       count(*) AS total
FROM (SELECT order_total, discount_amount,
             (SELECT sum(i.qty * i.unit_price) FROM unnest(items) AS t(i)) AS items_sum
      FROM staging.shopcore_orders_canon
      WHERE order_total IS NOT NULL AND items IS NOT NULL);
```

   Expect roughly 99.4% ok on *every* era — the ~0.5% mismatch dirt (§A05 territory), nothing more. The v1 days pass the v2-era formula only because you set `discount_amount = 0`. Savor that.
3. **The correction join — the punchline of this assignment.** The 08-01 file carries ~705 late corrections (its manifest's `late_rows`) to orders that exist in late-July files as BIGINT ids. Prove that raw ids never match and canonical ids all do:

```sql
-- hist = order_id_num from your canonical staging of 07-25..07-31
-- v3day = the raw 08-01 file read with V3_COLS
SELECT count(*) FROM v3day v JOIN hist h
  ON v.order_id = CAST(h.order_id_num AS VARCHAR);          -- naive:      0
SELECT count(DISTINCT v.order_id) FROM v3day v JOIN hist h
  ON CAST(replace(v.order_id,'ORD-','') AS BIGINT) = h.order_id_num;  -- canonical: 705
```

Zero matched corrections means every one of them would have upserted as a *new* order in A08's MERGE — silent double counting. This is why `order_id_num` exists.

**Accept when:** all three checks pass at small scale with the numbers above (0 and 705 exactly), and `DESCRIBE staging.shopcore_orders_canon` ends in exactly `_data_date DATE` and `_run_id BIGINT`.

### Task 7 — Rebuild the whole lake in canonical schema (small first, then full)

Now make history and future look the same. Plan: migrate month 1 from your **existing lake Parquet** (no need to re-parse 45 CSVs — migrating stored data is its own skill), load month-2 days from raw CSV, everything into a **new versioned directory**, then swap the reader over — the A09 pattern, pattern 3.

**(a) Migrate month 1** — small scale. `DESCRIBE` your current lake first: it should be v1-shaped plus `order_date`. Then it *is* a v1 source — reuse the machinery:

```python
ver = "orders_v=20260814"        # or a timestamp; keep the old dir until verified
con.execute(f"""CREATE OR REPLACE VIEW src AS
  SELECT * FROM read_parquet('{(ROOT/'lake'/'orders').as_posix()}/*/*.parquet',
                             hive_partitioning=true)""")
con.execute(f"CREATE OR REPLACE VIEW aligned AS {ALIGN['v1']}")
con.execute(f"""COPY ({CANON}) TO '{(ROOT/'lake'/ver).as_posix()}'
  (FORMAT parquet, PARTITION_BY (order_date), APPEND, ROW_GROUP_SIZE 122880)""")
```

(If your A02 lake already stores a cleaned TIMESTAMP for `order_ts`, replace the `try_strptime` with a pass-through — adapt, don't cargo-cult.)

Note what the lake does *not* get: the §7.2 pair stays behind in `staging`. Parquet under `lake/` is re-derivable storage, not a warehouse table, and the month-1 half is a bulk migration of files A02 wrote — their delivery dates are gone, and `order_date` is not a substitute (late rows are exactly the rows whose file date differs). A uniformly lineage-free lake beats one that is stamped on the half you happen to know. The pair lands on `core.orders` in A11, where each day is loaded under a real `run_id`.

**(b) Load month 2 day by day** — small scale. Loop 2026-07-16 → 2026-08-08: `open_day` → `COPY ({CANON}) TO ... (… PARTITION_BY (order_date), APPEND, …)`, logging each day to `ops.etl_runs` like A07 taught you — `rows` holds kept; the ledger has no column for dropped, so keep a running dropped total in your script or journal ((d)'s identity check needs it).

> **Why `APPEND` and not `OVERWRITE_OR_IGNORE`:** each day's COPY names its files `data_0.parquet…` from scratch. Late rows make day D write into partitions D−7…D — the same folders earlier days wrote. With `OVERWRITE_OR_IGNORE`, day D's `data_0.parquet` **silently replaces** an earlier file in a shared partition. We tested it so you don't have to: the 4-million-row lake came out at 45 thousand rows, no error anywhere. `APPEND` generates unique filenames instead. Look inside a boundary partition like `order_date=2026-07-14/` afterwards: several Parquet files, one per writer — that is what correct looks like.

**(c) One day will refuse to load.** `orders_2026-07-22.csv` fails with another cryptic sniffing error. Do **not** fix it today. Let your try/except record it as `failed` in `ops.etl_runs`, journal one line ("07-22 unreadable, deferred"), and move on — diagnosing and repairing that file is the centerpiece of A11. Its partition folder will exist anyway: days 07-23…07-29 deliver late corrections *into* 2026-07-22.

**(d) Verify, then swap.** The row math must close before you publish anything to readers:

```sql
SELECT sum(rows) FROM read_json_auto('<DATA_ROOT>/small/raw/manifest/orders_*.json')
WHERE file != 'orders_2026-07-22.csv';                      -- small: 4,058,440
SELECT count(*), count(DISTINCT order_date)
FROM read_parquet('<DATA_ROOT>/small/lake/orders_v=20260814/*/*.parquet',
                  hive_partitioning=true);                  -- small: 4,057,637 · 69
```

lake rows + your logged dropped total (803 at small scale) must equal the manifest sum **exactly** — if your month-1 lake had already dropped a few unparseable-timestamp rows back in A02, those belong on the dropped side of the identity; the sum must still close. 69 partitions even though only 68 files loaded — explain why in your journal. Then create the `core.lake_orders` view — A09's cleanup dropped it, so this is a create, not a repoint, but it is the same A09 catalog move (atomic for warehouse readers) — and retire the old dir to `.trash`:

```sql
CREATE OR REPLACE VIEW core.lake_orders AS
SELECT * EXCLUDE (orders_v)
FROM read_parquet('<DATA_ROOT>/small/lake/orders_v=20260814/*/*.parquet',
                  hive_partitioning=true);
```

(`EXCLUDE (orders_v)`: the `orders_v=…` directory name looks like a Hive partition, so `hive_partitioning=true` invents an `orders_v` column. Harmless, but keep the canonical shape clean.)

**(e) Full scale.** Switch `data_root("full")` and rerun (a)–(d). Sequential month-2 loading plus the migration takes roughly 20–45 minutes on the baseline machine — watch Task Manager while it runs (steady CPU, disk-bound phases) and journal the wall-clock; A12 will make this number embarrassing. Expected counts: run the same manifest-sum query against the full manifests — your identity must close the same way (the totals are roughly 20× small scale).

**Accept when:** small-scale identity closes exactly; full-scale identity closes; `core.lake_orders` view serves all 69 dates in one schema; 07-22 is recorded as failed, not fixed.

### Task 8 — Update the contract, close the loop (small)

Your A06 gate still describes v1 — it now (correctly) rejects two-thirds of the feed. Evolve the contract, not just the code. The contract is the *real* file, `contracts/shopcore_orders_daily.contract.yaml` (currently at 1.3.0 — yes, still: your A06 v1.4.0 stayed `proposed`, shopcore never countersigned it. Fold its locale clause into the 2.0.0 amendment so it finally ships — proposals that miss one train catch the next breaking change) — amend it, version-controlled, the way the producer should have:

- **Amendment → 2.0.0** — v2 era: the three appended columns with nullability, and the **new** reconciliation rule `order_total = items_total − discount_amount`. The columns alone would be a minor bump, but rewriting the meaning of an existing business rule breaks every consumer that reconciles — that makes the whole change **MAJOR** (1.3.0 → 2.0.0), which is exactly why the 30-day-notice clause applied.
- **Amendment → 3.0.0** — v3 era: rename, id type change, `loyalty_tier`, items `disc`, new column order. Breaking on four counts → another **major** bump.

Keep the earlier eras loadable: each amendment carries its own `effective_from`, so the gate can hold all three versions of the one contract (by git tag, or three dated copies in scratch) and validate a file against the version in force on its date.
- Teach the gate to pick the contract by file date (era boundaries) — or better, by matching the header against each contract and *reporting* if date and header disagree.
- Update your A05 check suite: the cross-field check becomes the one canonical formula (it runs on canonical staging, so no era branches needed).

> **07-22 needs one more gate skill.** Run your unmodified gate on `orders_2026-07-22.csv` first and read the report. `sniff_csv` does not raise on the corrupt file — it degenerates: the broken lines poison dialect detection, the sniffer settles on a delimiter that isn't even in the data, and it returns ONE column whose *name* is the entire 13-column header line. Your gate then dutifully reports "missing column(s): all of them" plus one unexpected mega-column — a schema-drift verdict for a file that is not drifted, it is unreadable. Teach the gate the difference: a sniffed schema that collapses to a single column with commas (or quotes) in its name is a structural failure — report it against `quality.structural` (producer incident), not against the schema block. Wrapping the gate's sample read in try/except is the other half of unreadability detection.

Run the gate over all 69 small files. **Accept when:** 68 green; `orders_2026-07-22.csv` fails as *unreadable* (not as a contract violation) — note that distinction in the journal for A11.

### Task 9 — Explain it back (journal)

Three sentences minimum: which changes were breaking and why; which change was the most dangerous despite not being "breaking" on paper (hint: it never raised an error); and what you would ask the upstream team to do differently next time (version field in the file? announce before shipping? both?).

## Definition of Done

- [ ] Month 2 generated at both scales; 69 raw files + 69 manifests each.
- [ ] Journal has the Task 2 error texts and the six-row drift classification table.
- [ ] Canonical schema table with a justification per default (Task 4).
- [ ] Journal paragraph naming the canonical key for what it is, and arguing it against the raw-producer-key and true-surrogate alternatives (Task 4).
- [ ] `work/a10_canonical.py` with `V3_COLS`, all three aligners, shared canonicalizer.
- [ ] One day per era: kept + dropped = manifest `rows` exactly; reconciliation ≈ 99.4% with the single canonical formula; `staging.shopcore_orders_canon` carries `_data_date` + `_run_id` and no more (§7.2).
- [ ] Correction join proof: naive = 0, canonical = 705 (small scale).
- [ ] Canonical lake at both scales; row identity closes exactly; 69 partitions; `core.lake_orders` view re-created (the A09 view pattern); old dir in `.trash`.
- [ ] 2026-07-22 recorded as failed in `ops.etl_runs` and journaled — **not** fixed.
- [ ] `shopcore_orders_daily.contract.yaml` amendments 2.0.0 + 3.0.0 written; gate green on 68 files, 07-22 flagged unreadable.
- [ ] Task 9 explanation written.

## Verify yourself

```powershell
(Get-ChildItem "$env:ETL_LAB_DATA\small\raw\orders").Count    # 69
```

```sql
-- era census from the manifests (small scale — exact, the generator is deterministic)
SELECT era, count(*) AS files, sum(rows) AS rows
FROM read_json_auto('<DATA_ROOT>/small/raw/manifest/orders_*.json')
GROUP BY era ORDER BY era;
-- v1  45  2,773,566
-- v2  16    907,584
-- v3   8    435,690
```

- Header detector (Task 3): 3 headers, 45 / 16 / 8 files, boundaries 07-15/07-16 and 07-31/08-01.
- Staging days (small): 06-02 → 59,024 · 07-16 → 59,613 · 08-01 → 47,008 (kept + dropped).
- Correction join (small): 0 naive, 705 canonical — 705 is `late_rows` in `orders_2026-08-01.json`; at full scale expect exactly that manifest's `late_rows` instead.
- Canonical lake (small): 4,057,637 rows + 803 dropped = 4,058,440 = manifest sum excl. 07-22; 69 distinct `order_date` values.
- Full scale: same queries against full manifests; identity must close exactly; totals roughly 20× small.

## Common beginner mistakes

- **Trusting the error message.** "Could not detect the CSV dialect" almost never means the delimiter changed — check the column count first. Drift usually surfaces as a *confusing* error, not an honest one.
- **Listing v3 `columns=` in the old order.** Mapping is positional; you'll load `USD` into `items` with zero errors. Always spot-check two values after writing a new explicit schema.
- **Treating `union_by_name=true` as the fix.** It is an exploration tool. It leaves the rename as two half-NULL columns and quietly promotes your key to VARCHAR.
- **Backfilling with the wrong nothing.** `discount_amount = NULL` for v1 breaks the unified reconciliation (NULL propagates); `channel = 'web'` invents data. Known value → default, unknown → NULL.
- **Forgetting corrections cross the era boundary.** Without `order_id_num`, every v3 late row becomes a new order in an upsert — double counting with no error anywhere.
- **Using `OVERWRITE_OR_IGNORE` for per-day partitioned writes.** Shared partitions (late rows!) get silently clobbered by the next day's `data_0.parquet`. Use `APPEND`, then count rows against manifests — the identity check is what catches this class of bug.

## Stretch goals

1. **The VND bomb.** ~1% of v2+ rows are VND with amounts ×25,000. Compute daily `avg(order_total)` for month 2 raw vs. normalized (`/ 25000` for VND): on small 2026-07-17 the naive average is roughly 30,589 vs roughly 124 normalized — a ×246 lie from 1% of rows. Chart both series in your journal.
2. **Era autodetect.** Files can arrive misnamed. Make `era_of` read the header line and match it against the known era headers, using the filename date only as a cross-check that *warns* on disagreement.
3. **Cross-era load strategies.** Re-run your A08 three-strategy comparison on a window spanning 07-29…08-03, keyed on `order_id_num`. All three must still agree — canonical keys are what make dedupe possible across eras.
