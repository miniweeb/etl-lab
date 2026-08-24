# A01 — First contact: explore 24 GB of CSV, load one day to Parquet
**Topic:** exploration, schema, file formats · **Est. time:** 3h · **Difficulty:** ⭐⭐
**Requires:** A00 done; month-1 raw data generated at both scales (45 CSV files per scale, era v1 only).

## Why this matters

Most pipeline disasters start the same way: someone wrote code against data they had
never actually looked at. Today you do the opposite — you interrogate the raw feed
before moving a single byte. You will also feel, in your own RAM, why "just load it
with pandas" stops working at this size, and why every serious data platform converts
text files into a columnar format early. That one conversion is the foundation of
everything you build in A02–A15.

## Background

**A CSV is text pretending to be a table.** The file on disk is just characters:
`1520021227,"47593",308,...`. There are no types. Nothing in the file says
`customer_id` is a number — that idea lives only in your head, or in code. Every
single time you query a CSV, the engine must re-parse every character of every row,
even if you only wanted one column. That is why CSV is fine as an *exchange* format
and terrible as a *working* format.

**Schema** is the formal answer to "what columns exist and what type is each one?".
You can get a schema two ways. *Auto-detect* (also called sniffing): the engine reads
a sample of rows and guesses. *Explicit*: you declare every column and type yourself.
Auto-detect is great for exploration and dangerous in pipelines — it guesses from a
sample, so a few weird rows can flip a column's type, and tomorrow's file may be
guessed differently than today's. Production pipelines declare their schema. Today
you will catch auto-detect making a real mistake on this dataset.

**Two ways to process a big file.** pandas *materializes*: `read_csv` parses the
whole file into RAM as a DataFrame before you can touch row one. Memory needed grows
with file size, and a parsed table in RAM is usually *larger* than the file on disk
(numbers unpack, strings carry overhead). DuckDB *streams*: it reads the file in
chunks, keeps only the small piece it currently needs, and throws each chunk away
once processed. A count or an aggregate over a 350 MB file needs only a few hundred
MB of working memory — and roughly the same for a 24 GB folder. You will measure
both today using **RSS** (Resident Set Size — the actual physical RAM a process is
using; Task Manager's "Memory" column shows it).

**Parquet** is what data engineers convert CSV into. Three properties matter:
it is *columnar* (each column stored separately, so a query touching 2 of 10 columns
reads ~20% of the file), *typed* (the schema is stored in the file — parse once,
never again), and *compressed* (similar values sit next to each other, which
compresses well). Inside, a Parquet file is split into **row groups** — horizontal
chunks of ~100k rows. Each row group stores min/max statistics per column, so an
engine can skip whole row groups that cannot possibly match your filter. You will
look at these with your own eyes today.

**The manifest.** Next to every orders file shopcore, the e-commerce platform that produces this feed, wrote
`raw/manifest/orders_YYYY-MM-DD.json` stating how many rows and bytes they exported.
This is your ground truth. Rule for the rest of the lab: after any load, reconcile
your count against the producer's claim. Never assume a file arrived complete.

## Setup

- Have open: a terminal in the repo root (venv active), **Task Manager** on the
  Details tab (find the `Memory` column — sort by it), and your `journal.md`.
- Data: month-1 raw data at both scales, generated in A00. Small scale is
  `<DATA_ROOT>/small/...`, full is `<DATA_ROOT>/...`. Each task below says which
  scale to use. Rule of the lab: try everything on small first.
- Scripts you write go in `work/` (e.g. `work/a01_explore.py`). Run them from the
  repo root **as modules**: `python -m work.a01_explore` (`-m` puts the repo root
  on Python's import path — the reason `from lib.labpaths import ...` works;
  `python work\a01_explore.py` dies with `ModuleNotFoundError`, because Python
  puts the script's *own* folder on the path, not the folder you ran it from).
- Every DuckDB connection today starts like this (leave RAM for the OS):

```python
import duckdb
from lib.labpaths import raw_orders_dir, manifest_dir, tmp_dir

con = duckdb.connect()   # in-memory scratch connection; no database file yet
con.execute("SET memory_limit='8GB'; SET threads=8;")
tmp_dir('full').mkdir(parents=True, exist_ok=True)   # DuckDB won't create missing folders for COPY (Task 6)
con.execute(f"SET temp_directory='{tmp_dir('full').as_posix()}';")
```

- In PowerShell snippets, `$root` is your data root:

```powershell
$root = $env:ETL_LAB_DATA        # e.g. F:\etl_lab
```

## Tasks

### Task 1 — Walk the raw folder (full scale, PowerShell)

Before code: look at what you have. List the order files with sizes:

```powershell
Get-ChildItem "$root\raw\orders" | Sort-Object Name |
  Format-Table Name, @{n='MB'; e={[math]::Round($_.Length/1MB,1)}}

Get-ChildItem "$root\raw\orders" | Measure-Object Length -Sum |
  ForEach-Object { "{0} files, {1:N1} GB" -f $_.Count, ($_.Sum/1GB) }
```

You should see 45 files (2026-06-01 … 2026-07-15) totalling roughly 15–16 GB.
The title of this assignment says 24 GB — the rest arrives in A10, when the upstream
team ships month 2. Two files are much bigger than the others. Find them:

```powershell
Get-ChildItem "$root\raw\orders" | Sort-Object Length -Descending |
  Select-Object -First 3 Name, @{n='MB'; e={[math]::Round($_.Length/1MB,0)}}
```

Those are flash-sale days at roughly 3× normal volume — they will haunt every
performance assignment later.
Now pick **your day** for the rest of this assignment: any ordinary weekday in June,
not a flash-sale day. This doc uses `2026-06-02` in examples, and the "Verify
yourself" numbers below are for that date — picking the same one makes checking
easier. Look at the producer's claim and the first raw lines of your day:

```powershell
Get-Content "$root\raw\manifest\orders_2026-06-02.json"
Get-Content "$root\raw\orders\orders_2026-06-02.csv" -TotalCount 3
```

Do NOT open the CSV in Excel (see Common beginner mistakes for why).

**Accept when:** journal has the file count, total GB, the two flash-sale dates,
your chosen day, and its manifest `rows` and `bytes` values.

### Task 2 — Count one day, check it against the manifest (small first, then full)

The simplest possible reconciliation: does the row count match what the producer
claims? Start on the **small** scale:

```python
import json
from lib.labpaths import raw_orders_dir, manifest_dir

DAY = "2026-06-02"
for scale in ("small", "full"):        # small runs instantly; full takes seconds
    csv_path = raw_orders_dir(scale) / f"orders_{DAY}.csv"
    n = con.execute("SELECT count(*) FROM read_csv(?)", [str(csv_path)]).fetchone()[0]
    claimed = json.loads((manifest_dir(scale) / f"orders_{DAY}.json").read_text())
    print(f"{scale:5}: counted {n:,}  manifest says {claimed['rows']:,}  "
          f"match={n == claimed['rows']}")
```

Both scales must match exactly. Note how fast the full-scale count is (~350 MB in
about a second) — hold that thought for Task 4.

**Accept when:** both scales print `match=True`, and you can say in one sentence why
this check will run after *every* load for the rest of the lab.

### Task 3 — DESCRIBE and SUMMARIZE: interrogate the schema (small first, then full)

Let DuckDB guess the schema, then catch the guess being wrong.

```sql
DESCRIBE SELECT * FROM read_csv('<DATA_ROOT>/small/raw/orders/orders_2026-06-02.csv');
```

Read the inferred types carefully. Three things deserve suspicion:

1. `customer_id` comes back as `DOUBLE`. Customer *ids* with decimal points?
   Find out why (gaps left on purpose — write these yourself):
   - re-read with `read_csv(..., all_varchar=true)` and count rows
     `WHERE customer_id LIKE '%.0'`. Look at a few with `LIMIT 5`.
   - This float-form (`"47593.0"`) is classic damage from a producer that
     round-tripped ids through a float type (Excel is a common culprit). A few
     thousandths of the rows are enough to flip auto-detect's guess for the
     *entire column* — this is why pipelines don't sniff.
2. `order_ts` stays `VARCHAR` while `updated_at` becomes a timestamp. If a
   timestamp-looking column is inferred as text, some values didn't parse.
3. `items` and `meta` are `VARCHAR` — JSON stuffed into CSV fields. Leave them as
   text for now; unpacking them properly is A04's whole job.

The authoritative answers to what each column *should* be — types, ranges, allowed
values — live in the data contract in `contracts/`, which you study in A06.

Now profile the day. `SUMMARIZE` computes min/max/uniques/nulls per column:

```sql
SUMMARIZE SELECT * FROM read_csv('<DATA_ROOT>/small/raw/orders/orders_2026-06-02.csv');
```

Hunt for stories in the output (write your findings down):

- `order_ts` min and max. Both are lies-in-plain-sight: the min starts with
  `01/06/2026...` (a day-first format, sorting first *as text*) and the max is an
  *impossible* date (look closely). Dirty values, parseable structure — this feed's
  signature. You will clean all of it in A03.
- `order_id` min is below your day's id range. Order ids encode the day
  (`days since 2026-01-01` × 10,000,000 + sequence — so 2026-06-02 starts at
  1,520,000,000). Ids from the *previous* day are in this file. The manifest calls
  them `late_rows` — corrections to earlier days. Park the thought; it becomes the
  core problem of A02 and A08.
- `customer_id` null_percentage is not 0.00 — some orders have no customer id at all.

Then run both statements again on the **full** file (takes a few seconds). Same
shapes, ~20× the rows.

**Accept when:** journal has at least three written observations, including an
explanation of the `DOUBLE` inference in your own words.

### Task 4 — The pandas trap (full scale, one file, with a guard)

Now the lesson you feel in your hardware. You will load ONE full-scale day with
pandas while watching RAM, then *calculate* — not run — what the whole feed would
need. Save as `work/a01_pandas_trap.py`:

```python
import os, time
import pandas as pd
import psutil
from lib.labpaths import raw_orders_dir

GB = 1024 ** 3
if psutil.virtual_memory().available < 6 * GB:
    raise SystemExit("Less than 6 GB RAM free - close some apps first.")

csv_path = raw_orders_dir("full") / "orders_2026-06-02.csv"
proc = psutil.Process(os.getpid())

print(f"RSS before: {proc.memory_info().rss / GB:.2f} GB")
t0 = time.perf_counter()
df = pd.read_csv(csv_path)                    # whole file -> RAM, no way around it
print(f"loaded {len(df):,} rows in {time.perf_counter() - t0:.0f}s")
print(f"RSS after:  {proc.memory_info().rss / GB:.2f} GB")
print(f"DataFrame alone: {df.memory_usage(deep=True).sum() / GB:.2f} GB")
```

Run it with `python -m work.a01_pandas_trap` (the `-m` form from Setup) and watch
`python.exe` in Task Manager while it runs — you will see the memory climb as
the file parses. On the baseline machine expect the process to end up somewhere
around 0.5–2 GB for this one 350 MB file (the exact number depends on your pandas
version; older versions store every string as a Python object and land far higher).
Either way the ratio is the point: **in RAM, the parsed table costs about 2× or more
its size on disk.**

Now the arithmetic (journal, not code): month 1 is ~15.7 GB of CSV. At ≥2× on-disk
size, loading all 45 files pandas-style needs 30+ GB of RAM. The machine has 16 GB.
When month 2 arrives (~24 GB total), it's 45+ GB. This is not "slow" — it is
*impossible*, and the OS will kill or thrash the process long before the end.

Contrast with streaming. Append to the same script:

```python
import duckdb
con = duckdb.connect()
con.execute("SET memory_limit='8GB'; SET threads=8;")
t0 = time.perf_counter()
n = con.execute("SELECT count(*) FROM read_csv(?)", [str(csv_path)]).fetchone()[0]
print(f"DuckDB counted {n:,} rows in {time.perf_counter() - t0:.1f}s")
print(f"RSS now: {proc.memory_info().rss / GB:.2f} GB")
```

Same file, same machine: RSS barely moves (tens to ~200 MB), because DuckDB never
holds the whole file — chunks stream through and are discarded. This is why the
rest of the lab is built on DuckDB, and pandas appears only when a trap is the
lesson.

**Accept when:** journal has pandas peak RSS (from Task Manager) and DataFrame size,
the DuckDB RSS for comparison, and your written 45-file estimate with the
conclusion.

### Task 5 — Explicit schema: read like a production pipeline (small first, then full)

Stop letting the engine guess. Declare era v1's schema yourself — this exact form
is the lab's canonical v1 read; you will reuse it in A02:

```sql
SELECT count(*) FROM read_csv('<DATA_ROOT>/small/raw/orders/orders_2026-06-02.csv',
    header=true,
    columns={'order_id':'BIGINT','customer_id':'BIGINT','store_id':'INTEGER',
             'order_ts':'VARCHAR','updated_at':'TIMESTAMP','status':'VARCHAR',
             'payment_method':'VARCHAR','order_total':'VARCHAR',
             'items':'VARCHAR','meta':'VARCHAR'});
```

Two deliberate choices to understand, not memorize:

- `customer_id` is declared `BIGINT`. The reader converts the float-forms
  (`"47593.0"` → 47593) and turns empties into NULL. The column is a proper id
  again — auto-detect's `DOUBLE` mistake is gone. (Full id cleanup rules,
  including orphans, are A03's.)
- `order_ts` and `order_total` are declared `VARCHAR` **on purpose**. They contain
  dirty values; if you declared `TIMESTAMP`/`DECIMAL` the read would fail or lose
  rows. Production habit: land dirty columns as text, clean them in a staging step
  (A03), so the raw read never silently drops data.

Measure how much dirt you just protected yourself from:

```sql
SELECT count(*) FROM read_csv('<DATA_ROOT>/small/raw/orders/orders_2026-06-02.csv',
    header=true, columns={...same as above...})
WHERE TRY_CAST(order_ts AS TIMESTAMP) IS NULL;
```

(`TRY_CAST` returns NULL instead of erroring when a cast fails — your main tool for
probing dirty columns.) Look at 5 offending values with a `LIMIT 5` variant: you'll
see day-first dates and the impossible date from Task 3. Roughly 0.4% of rows.
Repeat the count on the **full** file. Try the same probe on `order_total` too.

**Accept when:** explicit-schema count still equals the manifest for both scales,
and journal has the dirty `order_ts` count for both scales with two example values.

### Task 6 — Load one day to Parquet and race it (full scale)

Convert your day: CSV text in, columnar typed file out. Write it to the scratch
area — the real lake with its partition layout is A02's job:

```sql
COPY (
    SELECT * FROM read_csv('<DATA_ROOT>/raw/orders/orders_2026-06-02.csv',
        header=true,
        columns={'order_id':'BIGINT','customer_id':'BIGINT','store_id':'INTEGER',
                 'order_ts':'VARCHAR','updated_at':'TIMESTAMP','status':'VARCHAR',
                 'payment_method':'VARCHAR','order_total':'VARCHAR',
                 'items':'VARCHAR','meta':'VARCHAR'})
) TO '<DATA_ROOT>/tmp/orders_2026-06-02.parquet' (FORMAT parquet);
```

Compare file sizes (PowerShell or explorer): expect roughly 4× smaller — ~350 MB of
CSV becomes ~90 MB of Parquet, with the schema now stored *inside* the file.

Now race the two formats. Time each query with a tiny helper, run every query
**twice** and record the second run (the first pays a one-time OS disk-cache cost —
comparing a cold run to a warm one is a classic benchmarking sin):

```python
import time

def bench(label, sql):
    t0 = time.perf_counter()
    result = con.execute(sql).fetchall()
    print(f"{label:<32}{(time.perf_counter() - t0) * 1000:8.0f} ms   {result[0]}")
```

Race these three on the CSV (explicit-schema read) vs the Parquet file
(`read_parquet('<DATA_ROOT>/tmp/orders_2026-06-02.parquet')`):

1. `SELECT count(*) ...`
2. `SELECT count(*) ... WHERE store_id = 42`
3. `SELECT status, count(*) ... GROUP BY 1 ORDER BY 2 DESC`

Fill a small table in your journal: query × format × ms. On the baseline machine the
CSV scans land around a second or a few each; Parquet answers in tens of
milliseconds — expect at least 10× on the filter and aggregate, and something
absurd on `count(*)`. Why is Parquet `count(*)` near-instant? Because the row count
sits in the file's metadata — no data pages are read at all. Why is the filter so
fast? Partly columnar reading (only `store_id` is touched), partly the next task.

**Accept when:** both files exist, the size ratio and the 3×2 timing table are in
your journal, second-run numbers.

### Task 7 — Look inside the Parquet file (full scale)

Proof, not folklore. DuckDB can show you Parquet's internal structure:

```sql
SELECT row_group_id, row_group_num_rows
FROM parquet_metadata('<DATA_ROOT>/tmp/orders_2026-06-02.parquet')
WHERE column_id = 0;
```

Expect ~10 row groups of up to ~123k rows each (DuckDB's default row-group size is
122,880 rows). Your small-scale day would fit in a single row group. Now the
statistics that make skipping possible:

```sql
SELECT path_in_schema, stats_min_value, stats_max_value, total_compressed_size
FROM parquet_metadata('<DATA_ROOT>/tmp/orders_2026-06-02.parquet')
WHERE row_group_id = 0 AND path_in_schema IN ('order_id', 'status', 'items');
```

Every row group carries min/max per column. A query with
`WHERE order_id = 1520000042` can compare against each group's min/max and skip
groups that cannot contain it — *not reading* is the fastest IO there is. Notice
also `total_compressed_size`: `items` (the JSON blob) dwarfs every other column, and
columnar means a query ignoring `items` never pays for those bytes. In A02 you
extend this same skipping idea from row groups to whole folders.

Finish the assignment in your journal: in your own words, three bullets on why
columnar + typed beats row-text. If you can't write them without scrolling up,
re-run Tasks 6–7.

**Accept when:** row-group count and one column's min/max are in the journal, plus
your three bullets.

## Definition of Done

- [ ] Journal lists file count (45), total size, both flash-sale dates, and your
      chosen day with its manifest `rows`/`bytes`.
- [ ] DuckDB `count(*)` equals manifest `rows` exactly, at both scales.
- [ ] Auto-detect's mistakes explained in writing: `customer_id` → DOUBLE (why),
      `order_ts` stuck as VARCHAR (why).
- [ ] pandas one-day RSS measured and recorded; written estimate showing month-1
      via pandas exceeds 16 GB RAM; DuckDB streaming RSS recorded for contrast.
- [ ] Explicit-schema read returns the same count as the manifest; dirty
      `order_ts` count recorded for both scales.
- [ ] `orders_2026-06-02.parquet` (or your day) exists in `<DATA_ROOT>/tmp/`;
      size ratio and 3-query timing table in journal (second-run numbers).
- [ ] Row-group count and one column's min/max stats recorded.
- [ ] Three own-words bullets: why columnar + typed beats text.

## Verify yourself

Numbers below are for day `2026-06-02` (the generator is deterministic — same date,
same file, byte for byte, on every machine). If you picked another day, check
counts against that day's manifest instead.

```
count(*), small  :  59,024        (manifest rows: 59,024)
count(*), full   :  1,180,490     (manifest rows: 1,180,490; bytes: 359,260,066)
manifest late_rows/dup_rows, full :  17,707 / 590
dirty order_ts (TRY_CAST IS NULL), small :  249      (~0.4%)
dirty order_ts (TRY_CAST IS NULL), full  :  4,899    (~0.4%)
customer_id LIKE '%.0' (all_varchar), small / full :  120 / 2,390   (~0.2%)
DESCRIBE auto-detect: customer_id DOUBLE, order_ts VARCHAR, items VARCHAR
Parquet size, full day  :  ~92 MB from ~343 MB CSV  (~3.5-4x; codec-dependent)
Row groups, full day    :  10   (small day: 1)
Parquet count(*)        :  ~1 ms;  CSV count(*): roughly a second or more
```

Sanity checks that should hold on ANY v1 day: dirty `order_ts` ≈ 0.4% of rows;
float-form `customer_id` ≈ 0.2%; `SUMMARIZE` max of `order_ts` is an impossible
date; min `order_id` belongs to the previous day (late corrections) — except on
2026-06-01, the feed's first day, which has no earlier day to correct.

## Common beginner mistakes

- **Opening the CSV in Excel** "just to look". Excel truncates at 1,048,576 rows,
  reformats dates, and strips leading zeros — and if you save, the file is ruined.
  The `customer_id` float-forms in this feed are exactly the kind of scar such
  round-trips leave. Use `Get-Content -TotalCount` or DuckDB `LIMIT`.
- **Trusting auto-detect in pipeline code.** It guesses from a sample; today it
  made `customer_id` a DOUBLE. Sniff when exploring, declare schemas when loading.
- **Running the pandas estimate instead of calculating it.** Loading 45 files into
  one process will freeze or kill your machine. The guard in Task 4 exists for a
  reason; the estimate is arithmetic, not an experiment.
- **Benchmarking cold vs warm.** First read of a file pays the disk; second comes
  from OS cache. Compare second runs to second runs (A14 formalizes this).
- **Writing the Parquet file into `lake/` or onto a OneDrive-synced path.** The
  lake has a specific layout that A02 builds; scratch output goes to
  `<DATA_ROOT>/tmp/`. And a sync client hashing your 24 GB is misery.
- **Reading "match=True" as "data is good".** The row count matches and the values
  are still ~3% dirty. Structure parses; values lie. A03 and A05 deal with that.

## Stretch goals

1. **Count the whole month in one query.** Point `read_csv` at
   `'<DATA_ROOT>/small/raw/orders/orders_*.csv'` with `filename=true` and
   `all_varchar=true`, group by `parse_filename(filename)`. Spot the weekend dip
   (~×0.75) and the 2026-06-19 spike (~×3) in the counts. Small scale — then decide
   for yourself whether you dare run it at full scale (it streams — it works, and
   the wall-clock is your first taste of A12's problem).
2. **Compression codecs.** Rewrite your day with
   `(FORMAT parquet, COMPRESSION zstd)` and compare size to the default (snappy).
   Expect roughly 92 MB → ~51 MB. What might the trade-off be? (A14 measures it.)
3. **Pick your poison, quantify it.** Choose one more dirt pattern from the
   `SUMMARIZE` output (junk `status` values, `order_total` with `$` or `N/A`),
   count it with `TRY_CAST`/`LIKE` probes, and note the % — you are building the
   intuition A03's cleaning catalog is made of.
