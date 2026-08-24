# A02 — Build the Hive-partitioned Parquet lake

**Topic:** partitioning · **Est. time:** 3h · **Difficulty:** ⭐⭐
**Requires:** A01 done; month-1 raw data at both scales (generated in A00). Month 2 does not exist yet — that is on purpose.

## Why this matters

In A01 you turned one day of CSV into one Parquet file. Real pipelines hold months of
history, and almost every question asked of them is date-shaped: "yesterday's
revenue", "reload last Tuesday". If history sits in one big pile, every question —
even about a single day — pays to read everything. Partitioning is how a lake answers
date-sized questions at date-sized cost. Every serious tool (Spark, Hive, BigQuery,
Iceberg, Delta) is built on this idea; today you build it with plain folders and
DuckDB, so you understand exactly what those tools automate for you.

## Background

**A partition is a folder whose name carries data.** The layout we build is called
the *Hive layout* (after Apache Hive, which popularized it): one subfolder per value
of a chosen column, named `column=value`.

```
<DATA_ROOT>/lake/orders/
  order_date=2026-06-01/     <- the folder name IS data
    <uuid>.parquet
  order_date=2026-06-02/
    ...
```

The folder name is a promise: every row under `order_date=2026-06-19/` has that order
date. A query engine can compare the folder *names* to your `WHERE` clause and skip
whole folders without opening a single file. That skip is **partition pruning** — a
poor man's index, and the biggest performance lever in any file-based lake.

**Why partition by date?** Pick the partition column to match how data *arrives* and
how it is *asked for*. Our feed from shopcore, the e-commerce platform, arrives
daily, corrections and reloads are
day-ranged, and most queries filter on time. Date is the obvious key.

**Event time is not arrival time.** `orders_2026-06-10.csv` is named for the day it
*arrived*. Inside it, roughly 1.5% of rows are late corrections: orders that
*happened* on one of the seven previous days and got a status update today. We
partition by when the order happened — `order_date` derived from `order_ts` — not by
which file delivered it, so one input file writes into up to **8** partition folders.
This behavior is contractual, not a bug: the `shopcore_orders_daily` contract's
`business_date` and `late_corrections` clauses promise exactly this — the file for
day D may re-send orders up to 7 days old. You will see this in Task 4; A08 deals
with the consequences.

**A partition folder holds files, and filenames matter.** Each `COPY` writes files
into every folder it touches. Two writes, same filename: the second silently replaces
the first. You step on this rake deliberately in Task 2 — better here than in prod.

**Granularity.** Finer partitions = better pruning but more, smaller files, and every
file costs a footer plus an OS file-open. Rule of thumb: partition files of
tens-to-hundreds of MB. Daily is right here; date *and* store would mean thousands
of kilobyte-sized files per month — stretch goal 1 lets you feel that.

**We are not cleaning data today.** A03 owns cleansing. We parse `order_ts` just
enough to know *which day a row belongs to*. About 0.02% of rows have impossible
timestamps (`2026-06-31 12:00:00`); they parse to NULL, and DuckDB routes them into
`order_date=__HIVE_DEFAULT_PARTITION__` — visible, not vanished. A05 quarantines
them properly.

## Setup

- Venv active, `ETL_LAB_DATA` set (A00). Everything until Task 6 runs on **small**.
- No `warehouse.duckdb` today — a lake is just files. An in-memory connection
  (`duckdb.connect()`) with the standard settings (`memory_limit='8GB'`, `threads=8`,
  `temp_directory=<DATA_ROOT>/tmp`) is all we need; the Task 2 script sets them.
  This job streams and never nears the limit — set them anyway, lab habit.
- Open: editor, PowerShell at the repo root, Task Manager (Details tab, find
  `python.exe`), `journal.md`. Sanity-check the raw data:

```powershell
(Get-ChildItem "$env:ETL_LAB_DATA\small\raw\orders").Count    # expect 45 files
```

(45 files = month 1 = era v1 = `2026-06-01` … `2026-07-15`. Month 2 does not exist
yet — A10 generates it. If you have more than 45, load only these 45 today.)

## Tasks

### Task 1 — Look inside one file before moving anything (small)

Never route data you have not looked at. We claim one file carries eight days of
event time — verify that on `orders_2026-06-10.csv`. The timestamp column is dirty
(three formats plus a few impossible dates — A01 found two of these; the third,
ISO-T, hid from A01's `TRY_CAST` probe because a plain `TIMESTAMP` cast accepts
it), so a plain `CAST` would
crash. `try_strptime` tries formats in order and returns NULL when none fit:

```python
import duckdb
import os
from pathlib import Path

ROOT = Path(os.environ.get("ETL_LAB_DATA", "data")) / "small"
SRC = ROOT / "raw" / "orders" / "orders_2026-06-10.csv"

TS_FORMATS = "['%Y-%m-%d %H:%M:%S', '%d/%m/%Y %H:%M:%S', '%Y-%m-%dT%H:%M:%S']"

con = duckdb.connect()
rows = con.execute(f"""
    SELECT CAST(try_strptime(order_ts, {TS_FORMATS}) AS DATE) AS order_date,
           count(*) AS n
    FROM read_csv('{SRC.as_posix()}',
        header=true,
        columns={{'order_id':'BIGINT','customer_id':'BIGINT','store_id':'INTEGER',
                 'order_ts':'VARCHAR','updated_at':'TIMESTAMP','status':'VARCHAR',
                 'payment_method':'VARCHAR','order_total':'VARCHAR',
                 'items':'VARCHAR','meta':'VARCHAR'}})
    GROUP BY 1 ORDER BY 1
""").fetchall()
for r in rows:
    print(r)
```

Expected (small scale is deterministic, so these exact numbers):

```
(datetime.date(2026, 6, 3), 41)      <- late corrections, 7 days back
   ... counts growing toward yesterday: 66, 110, 122, 154, 196 ...
(datetime.date(2026, 6, 9), 222)
(datetime.date(2026, 6, 10), 59857)  <- the file's own day
(None, 14)                           <- impossible timestamps
```

Why three formats? Remove the ISO-T one and re-run: the NULL bucket jumps about
tenfold — those 0.3% ISO-T rows are good orders we must not lose to lazy parsing.

**Acceptance:** the counts sum to `rows` in
`<DATA_ROOT>/small/raw/manifest/orders_2026-06-10.json`; you can say in one sentence
where each group (prior days / own day / NULL) comes from.

### Task 2 — Step on the filename rake (small, deliberate failure)

Create `work/build_lake.py` from the listing below. Note the two constants marked
`# Task 2` / `# Task 3` — start with the Task 2 values.

```python
"""A02 - build the Hive-partitioned Parquet lake, one day at a time."""
import os
import time
from datetime import date, timedelta
from pathlib import Path

import duckdb

# Where is the data? Same rule as lib/labpaths.py: env var, else <repo>/data.
BASE = Path(os.environ.get("ETL_LAB_DATA", Path(__file__).resolve().parents[1] / "data"))
SCALE = "small"                          # develop on small, switch to "full" in Task 6
ROOT = BASE / "small" if SCALE == "small" else BASE

RAW_ORDERS = ROOT / "raw" / "orders"
LAKE_ORDERS = ROOT / "lake" / "orders"
TMP = ROOT / "tmp"

FIRST_DAY = date(2026, 6, 1)
LAST_DAY = date(2026, 6, 2)              # Task 2: two days.  Task 3: date(2026, 7, 15)
PARTITION_MODE = "OVERWRITE_OR_IGNORE"   # Task 2: this.      Task 3: "APPEND"

TS_FORMATS = "['%Y-%m-%d %H:%M:%S', '%d/%m/%Y %H:%M:%S', '%Y-%m-%dT%H:%M:%S']"
V1_COLUMNS = """{'order_id':'BIGINT','customer_id':'BIGINT','store_id':'INTEGER',
     'order_ts':'VARCHAR','updated_at':'TIMESTAMP','status':'VARCHAR',
     'payment_method':'VARCHAR','order_total':'VARCHAR',
     'items':'VARCHAR','meta':'VARCHAR'}"""

con = duckdb.connect()   # in-memory: the lake is plain files, no database needed yet
con.execute(f"SET memory_limit='8GB'; SET threads=8; "
            f"SET temp_directory='{TMP.as_posix()}';")


def load_day(day: date) -> int:
    """Read one raw CSV, derive order_date, write into the partitioned lake."""
    src = RAW_ORDERS / f"orders_{day}.csv"
    result = con.execute(f"""
        COPY (
            SELECT *,
                   try_strptime(order_ts, {TS_FORMATS}) AS order_ts_clean,
                   CAST(try_strptime(order_ts, {TS_FORMATS}) AS DATE) AS order_date
            FROM read_csv('{src.as_posix()}', header=true, columns={V1_COLUMNS})
        ) TO '{LAKE_ORDERS.as_posix()}'
        (FORMAT parquet, PARTITION_BY (order_date), {PARTITION_MODE},
         ROW_GROUP_SIZE 122880);
    """)
    return result.fetchone()[0]          # COPY reports how many rows it wrote


if __name__ == "__main__":
    LAKE_ORDERS.mkdir(parents=True, exist_ok=True)
    TMP.mkdir(parents=True, exist_ok=True)

    day = FIRST_DAY
    t_total = time.perf_counter()
    while day <= LAST_DAY:
        t0 = time.perf_counter()
        rows = load_day(day)
        print(f"{day}  {rows:>9,} rows  {time.perf_counter() - t0:6.2f} s")
        day += timedelta(days=1)
    print(f"TOTAL {time.perf_counter() - t_total:8.1f} s")
```

`OVERWRITE_OR_IGNORE` looks like the natural choice — and it is fine for a
one-shot full rewrite. Watch what it does in a loop. Run
`python work\build_lake.py`: it prints `60,703` rows for 06-01 and `59,024` for
06-02, so the lake must hold 119,727. Check that (`hive_partitioning=true` turns the
folder names back into a real `order_date` column):

```python
g = (ROOT / "lake" / "orders").as_posix() + "/*/*.parquet"
con.execute(f"SELECT count(*) FROM read_parquet('{g}', hive_partitioning=true)")
```

You get **59,024**. A full day is gone and nothing errored. The crime scene:

```powershell
Get-ChildItem "$env:ETL_LAB_DATA\small\lake\orders\order_date=2026-06-01"
```

One file, `data_0.parquet`, a few dozen KB. Day 1's COPY wrote that file with 60,686
rows (17 of its 60,703 had impossible timestamps and went to the NULL partition
instead). Day 2's COPY carried 885 late rows for 06-01, wrote into the same folder —
named `data_0.parquet` again — and silently replaced a full day with 885 corrections.
`OVERWRITE_OR_IGNORE` means "don't complain if files are in the way": fine for a
one-shot write, poison for a loop that touches the same partition twice.

**Acceptance:** count query returns 59,024; the 06-01 partition holds exactly 885
rows (`WHERE order_date = DATE '2026-06-01'`); you can explain the loss in one
sentence without re-reading the text above.

### Task 3 — The real loader: APPEND, all 45 days (small)

The fix is one word. `APPEND` gives every written file a unique (UUID) name, so
writes into an existing folder *add* instead of *replace*. In `work/build_lake.py`:

```python
LAST_DAY = date(2026, 7, 15)             # all of month 1, era v1
PARTITION_MODE = "APPEND"
```

Throw away the poisoned lake first (APPEND on top of wrong data = bigger wrong data):

```powershell
Remove-Item -Recurse -Force "$env:ETL_LAB_DATA\small\lake\orders"
python work\build_lake.py
```

Watch the 45 timing lines scroll by, then create `work/verify_lake.py` — a habit you
keep all lab long: the manifests say what was written; the lake must agree.

```python
"""Reconcile the lake against the generator manifests."""
import json
import os
from datetime import date, timedelta
from pathlib import Path

import duckdb

BASE = Path(os.environ.get("ETL_LAB_DATA", Path(__file__).resolve().parents[1] / "data"))
SCALE = "small"
ROOT = BASE / "small" if SCALE == "small" else BASE

manifest_total = 0
day, last = date(2026, 6, 1), date(2026, 7, 15)
while day <= last:
    m = json.loads((ROOT / "raw" / "manifest" / f"orders_{day}.json").read_text())
    manifest_total += m["rows"]
    day += timedelta(days=1)

con = duckdb.connect()
lake_glob = (ROOT / "lake" / "orders").as_posix() + "/*/*.parquet"
lake_total = con.execute(
    f"SELECT count(*) FROM read_parquet('{lake_glob}', hive_partitioning=true)"
).fetchone()[0]

print(f"manifests say: {manifest_total:,}")
print(f"lake contains: {lake_total:,}")
print("MATCH" if manifest_total == lake_total else "MISMATCH - investigate!")
```

**Acceptance:** `MATCH`, 2,773,566 rows on both lines (standard small dataset — if
yours differs slightly, your manifests are the truth). Journal the wall-clock; the
small loop takes well under a minute on the baseline machine. The loop is strictly
one day after another — deliberately naive. A12 makes it fast; A07 makes it
re-runnable.

### Task 4 — Read the tree like an engineer (small)

A partitioned lake is self-describing — a directory listing audits pipeline behavior
before you run a single query. Survey yours:

```powershell
$lake = "$env:ETL_LAB_DATA\small\lake\orders"
(Get-ChildItem $lake -Directory).Count                              # folders
(Get-ChildItem $lake -Recurse -Filter *.parquet).Count              # files
Get-ChildItem $lake -Directory | ForEach-Object {
    "{0}  {1}" -f $_.Name, (Get-ChildItem $_.FullName -Filter *.parquet).Count }
Get-ChildItem $lake -Directory | Select-Object Name, @{n='MB';e={[math]::Round(
    (Get-ChildItem $_.FullName -Filter *.parquet |
     Measure-Object Length -Sum).Sum/1MB,1)}} |
    Sort-Object MB -Descending | Select-Object -First 5
```

You loaded 45 files, yet you get **46 folders** and **377 files**. Three anomalies —
work each out before reading on:

1. **The 46th folder**, `order_date=__HIVE_DEFAULT_PARTITION__`: the NULL-date rows
   (impossible timestamps, ~0.02%, 553 rows total, 45 small files — every day
   contributed a few). Parked, not lost; A05 quarantines them properly.
2. **Most date folders hold 8 files**: the day's own write plus late-correction
   writes from each of the next 7 days. The last week tapers 7, 6, … 1
   (`2026-07-15` has no later files to correct it — *yet*; month 2 will, in A10).
3. **Two folders ~3× their neighbors**: `2026-06-19` and `2026-07-11`, the flash-sale
   spike days. Data skew, visible in the filesystem. They will torment you in A12–A14.

Also note: the same `order_id` can now exist in two partitions (original + late
correction). The lake is an *append-only log*, not final truth — deduplicating it is
A08's whole topic.

**Acceptance:** all three anomalies explained in your journal, in your own words.

### Task 5 — Prove the pruning (small)

Collect the payoff: the same aggregate with a prunable filter (partition column) and
a non-prunable one (data column). Query A — partition filter:

```sql
SELECT count(*) AS orders, round(avg(store_id), 2) AS avg_store
FROM read_parquet('<DATA_ROOT>/small/lake/orders/*/*.parquet', hive_partitioning=true)
WHERE order_date = DATE '2026-06-19';
```

Query B — your turn: same aggregate, but filter on the *data* column
`order_ts_clean` (hint: `WHERE CAST(order_ts_clean AS DATE) = …`). Both must return
identical results, since `order_date` was *derived* from it. Run each 3 times (first
run warms the OS cache; keep the median — the A01 habit), then compare plans:

```python
print(con.execute("EXPLAIN " + query_a).fetchall()[0][1])
print(con.execute("EXPLAIN " + query_b).fetchall()[0][1])
```

Query A's `READ_PARQUET` box holds the proof:

```
File Filters: (order_date = '2026-06-19'::DATE)
Scanning Files: 8/377
```

8 files opened out of 377 — 369 skipped by folder name alone. Query B shows a plain
`Filters:` on the timestamp and opens every file. On small scale expect roughly
20 ms vs 60 ms; pruning saves *reading*, so the win grows with data size — Task 6
makes that concrete.

**Acceptance:** both queries return `(180183, 267.17)` (small scale); both `EXPLAIN`
outputs in your journal with the `Scanning Files` line highlighted; timings recorded.

### Task 6 — Now for real: full scale (full)

Flip `SCALE = "full"` in both scripts. Month 1 at full scale is roughly 16 GB of
CSV — before running, predict in your journal: total rows (hint: small × 20), lake
size, loop time. Then run `python work\build_lake.py` and observe while it grinds
(this is the lesson, do not skip it):

- **Task Manager:** `python.exe` memory stays modest (roughly a few hundred MB to
  ~2 GB) even though each file is ~350 MB — DuckDB streams: read a chunk, write a
  chunk. Compare with A01's pandas trap.
- **Per-day timings:** roughly 5–20 s each on the baseline machine; `2026-06-19` and
  `2026-07-11` take ~3× their neighbors — skew again.
- **The whole loop:** roughly 5–20 minutes of sequential grinding, depending on your
  disk. Find it annoying — that annoyance is A12's motivation. Do not parallelize
  anything today.

Afterwards: `work/verify_lake.py` (with `SCALE = "full"`) must print `MATCH`
(roughly 55M rows; your manifests are the exact truth). Re-run Task 5's queries on
the full lake: the gap is now dramatic — Query B reads a multi-GB lake, Query A one
spike day.

**Acceptance:** full-scale `MATCH`; journal has predictions vs actuals, total
wall-clock, per-day pattern, and full-scale pruning timings.

## Definition of Done

- [ ] `work/build_lake.py` exists, uses `APPEND` mode, explicit v1 schema, and
      3-format `try_strptime` for `order_date`.
- [ ] Small lake rebuilt clean: `work/verify_lake.py` prints `MATCH` (2,773,566 rows).
- [ ] You can explain from memory: why 46 folders and not 45; why most partitions
      hold 8 files; why two folders are ~3× bigger.
- [ ] Pruned and unpruned query return identical results; `EXPLAIN` evidence
      (`Scanning Files: 8/377`) captured in journal.
- [ ] Full-scale lake built and verified `MATCH`; journal updated with wall-clock for
      the sequential loop (A12 will beat it), lake-vs-CSV sizes, pruning timings.

## Verify yourself

```powershell
(Get-ChildItem "$env:ETL_LAB_DATA\small\lake\orders" -Directory).Count
# 46
(Get-ChildItem "$env:ETL_LAB_DATA\small\lake\orders" -Recurse -Filter *.parquet).Count
# 377
[math]::Round((Get-ChildItem "$env:ETL_LAB_DATA\small\lake\orders" -Recurse -Filter *.parquet |
  Measure-Object Length -Sum).Sum/1MB)
# roughly 210-230 (vs ~840 MB of CSV -- roughly 4x smaller)
python work\verify_lake.py
# manifests say: 2,773,566
# lake contains: 2,773,566
# MATCH
```

Spike-day count (small scale, deterministic):

```sql
SELECT count(*) FROM read_parquet('<DATA_ROOT>/small/lake/orders/*/*.parquet',
    hive_partitioning=true)
WHERE order_date = DATE '2026-06-19';
-- 180183      (full scale: roughly 3.6M)
```

Full scale: same checks with the `small` path segment removed and `SCALE = "full"`.
Folder count identical (46 — partitioning depends on dates, not volume); file count
377 or somewhat higher (DuckDB may split a large partition into several files per
write); sizes roughly 20× larger; `MATCH` against full manifests.

## Common beginner mistakes

- **`OVERWRITE_OR_IGNORE` inside a loop.** Task 2 proved it silently destroys data.
  One-shot rewrite of everything: fine. Incremental loop: `APPEND`.
- **Re-running the loop without wiping the lake.** `APPEND` means append: run the
  loop twice and every row exists twice (verify prints `MISMATCH … 5,547,132`).
  For now: wipe and rebuild. Safe re-runs are A07's topic.
- **Building the lake inside OneDrive or the repo.** The sync client will happily
  upload thousands of parquet files while you work. Data lives under `ETL_LAB_DATA`
  on a fast local disk — A00 rules.
- **`CAST(order_ts AS TIMESTAMP)` instead of `try_strptime`.** Crashes on the first
  `DD/MM/YYYY` row with a `Conversion Error`. And a 2-format list quietly drops the
  0.3% ISO-T rows into the NULL partition — parse all three (full cleaning: A03).
- **Filtering with `WHERE order_ts LIKE '2026-06-19%'`.** No pruning (not the
  partition column) *and* wrong results: `19/06/2026`-format rows don't match the
  string. Filter on `order_date`.
- **Globbing one level too shallow.** `lake/orders/*.parquet` finds nothing
  (`IO Error: No files found`) — files live inside the `order_date=…` folders. Use
  `lake/orders/*/*.parquet`.

## Stretch goals

1. **Feel the small-files problem.** Copy one day into a scratch folder with
   `PARTITION_BY (order_date, store_id)`: it explodes into ~1,100 folders of ~6 KB
   files. Query it, feel the sluggishness, delete it, journal one sentence on
   choosing partition columns.
2. **Forensics with `filename=true`.** `read_parquet(…, filename=true)` adds each
   row's source file as a column. Group the `order_date=2026-06-01` partition by
   `parse_filename(filename)`: one 60,686-row file (the day's own load) and seven
   shrinking files (885 → 28 rows) — late-correction writes from days 2–8, fading
   with distance. Cross-check the 885 against `late_rows` in the 06-02 manifest.
3. **Predict before you measure.** Formalize your Task 6 predictions: write down the
   scaling factors you would use (rows ×20, bytes ×~20, time ×?), check each against
   reality, and explain your worst miss.
