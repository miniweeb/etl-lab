# A12 — Parallelization: 69 days on 8 cores

**Topic:** parallelization · **Est. time:** 4h · **Difficulty:** ⭐⭐⭐⭐
**Requires:** A11 done (full backfill green, baseline wall-clock in your journal); months 1+2 raw data at both scales; your A10 per-era canonical staging.

## Why this matters

Your A11 backfill worked, but it took tens of minutes — and it will run again
every time history needs a rebuild. Real teams parallelize backfills constantly,
and teams that do it badly take down their own machines: eight workers that each
believe they own all the RAM freeze a server just as happily as your laptop.
Today you learn the capacity math first, then the code. The math is the lesson.

## Background

You have 69 independent days and 8 CPU cores. A11 did them one at a time.
Obvious fix: several at once. Three ideas first.

**Your baseline is faster than you think.** A11 was "sequential" but not
single-core: with `SET threads=8`, DuckDB already spreads each day's SQL across
all 8 cores. A pool adds parallelism *across* days — it fills the gaps where one
day can't keep 8 cores busy (Python overhead, disk waits, small query steps).
So the honest comparison is a pool versus "one process with 8 threads", and the
honest expectation is a real speedup, not a miracle 8×.

**Processes, not threads.** A *thread* is a worker inside your program that
shares its memory; a *process* is a separate program with its own memory. We use
processes: each gets a private DuckDB with its own `memory_limit`, and a crash
in one cannot corrupt the others. On Windows a new process starts by *spawn*:
the child launches a fresh Python and **re-imports your script file**. Hence two
rules: pool-starting code sits under `if __name__ == "__main__":` (or every
child spawns children of its own), and worker functions must be top-level in
the file (the child finds them by name).

**The budget.** The machine does not grow because you asked for more workers.
Two hard constraints on the baseline machine (SPEC §1: 16 GB RAM, 8 cores):

```
workers × threads_per_worker                              ≤ 8 cores
workers × memory_limit_per_worker + ~4 GB (OS, browser…)  ≤ 16 GB
```

Nothing enforces these totals for you. DuckDB's `memory_limit` is **per
process**, and its default is ~80% of machine RAM — *per process*. Eight workers
on defaults collectively "promise" themselves ~100 GB on a 16 GB machine. That
promise gets settled with swapping, spilling, or a dead machine.

**The single-writer rule.** A DuckDB database file can be open for writing by
**one process at a time**; a second process gets an IO error — even a read-only
open fails while a writer holds the file. So workers never touch
`warehouse.duckdb`. The standard pattern: each worker writes its **own** Parquet
file (nothing shared, nothing to lock), then one *committer* process merges.

Finally, not all days are equal: the spike days (`2026-06-19`, `2026-07-11`) are
~3× bigger, and in a pool the slowest task sets the finish line. A big day
scheduled last leaves 7 idle cores waiting on it — a *straggler*.

## Setup

- All 69 raw files at both scales:

```powershell
(Get-ChildItem "$env:ETL_LAB_DATA\small\raw\orders\orders_*.csv").Count   # 69
(Get-ChildItem "$env:ETL_LAB_DATA\raw\orders\orders_*.csv").Count         # 69
```

- Open: Task Manager (Performance tab — CPU *and* Memory), two terminals, `journal.md`.
- The usual `SET memory_limit='8GB'; SET threads=8;` is a budget for **one**
  process. Today you split it: every worker sets its own smaller share.
- Develop on **small**; go full scale only where a task says so.

## Tasks

### Task 1 — Do the budget math (paper, ~10 min)

Before any code, fill this table into your journal. Keep ~4 GB back for the OS.

| workers | threads each | memory_limit each   | total threads | total promised RAM | legal? |
|---------|--------------|---------------------|---------------|--------------------|--------|
| 1       | 8            | 8 GB                | 8             | 8 GB               | yes    |
| 2       | 4            | 5 GB                | 8             | 10 GB              | yes    |
| 4       | 2            | 3 GB                | 8             | 12 GB              | yes    |
| 8       | 1            | 1500 MB             | 8             | 12 GB              | yes    |
| 8       | 8            | 1500 MB             | 64            | 12 GB              | ?      |
| 8       | 1            | default (~12.8 GB)  | 8             | ~102 GB            | ?      |

**Accept when:** the last two rows carry a one-sentence explanation of *which*
constraint each violates (one wastes cores on switching; one promises RAM that
does not exist).

### Task 2 — Break the single-writer rule on purpose (small, ~10 min)

See the lock error once, in a controlled way, so you recognize it forever.

```powershell
# Terminal 1 — open the small warehouse and hold it 60 s (a pretend long load)
python -c "import duckdb, os, time; p = os.environ['ETL_LAB_DATA'] + r'\small\warehouse\warehouse.duckdb'; con = duckdb.connect(p); time.sleep(60)"

# Terminal 2 — while Terminal 1 still sleeps:
python -c "import duckdb, os; p = os.environ['ETL_LAB_DATA'] + r'\small\warehouse\warehouse.duckdb'; duckdb.connect(p)"
```

Terminal 2 dies with an `IOException`: *"The process cannot access the file
because it is being used by another process. File is already open in …python.exe
(PID …)"*. Try Terminal 2 again with `duckdb.connect(p, read_only=True)` — it
**also** fails while a writer holds the file. Paste both errors into your journal.

**Accept when:** you can state the rule from memory: one writer per DuckDB file;
workers therefore write Parquet, never the warehouse.

### Task 3 — Build the parallel stage (small)

Create `work/parallel_runner.py`. Workers only *stage*: raw CSV in, one clean
typed Parquet file per day out, into `<DATA_ROOT>/small/tmp/backfill_stage/`.

```python
"""work/parallel_runner.py — A12: stage many days in parallel.
Workers ONLY write parquet. They never open warehouse.duckdb."""
import argparse, json, sys, threading, time
from datetime import date, timedelta
from multiprocessing import Pool
from pathlib import Path
import duckdb, psutil

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))   # repo root
from lib.labpaths import manifest_dir, raw_orders_dir, tmp_dir

SCALE = "small"   # module-level on purpose: spawned workers re-import this file
                  # and see it. Parsed CLI args would NOT reach them by magic.
RAW, MANIFESTS = raw_orders_dir(SCALE), manifest_dir(SCALE)
STAGE = tmp_dir(SCALE) / "backfill_stage"

V1_SCHEMA = """{'order_id':'BIGINT','customer_id':'BIGINT','store_id':'INTEGER',
    'order_ts':'VARCHAR','updated_at':'TIMESTAMP','status':'VARCHAR',
    'payment_method':'VARCHAR','order_total':'VARCHAR','items':'VARCHAR','meta':'VARCHAR'}"""

def stage_sql(day: str, csv_path: Path, out_path: Path) -> str:
    # Minimal v1 cleaner (SPEC §4) so the scaffold runs as-is on month-1 days.
    # TODO(you): replace the SELECT with your A10 per-era canonical staging —
    # era comes from the day's manifest; 2026-07-22 needs store_rejects=true (A11).
    # Keep the _data_date stamp whatever you paste in — §7.2 lineage.
    return f"""
    COPY (
        WITH typed AS (
            SELECT order_id, customer_id, store_id,
                try_strptime(order_ts, ['%Y-%m-%d %H:%M:%S', '%d/%m/%Y %H:%M:%S', '%Y-%m-%dT%H:%M:%S']) AS order_ts,
                updated_at, status, payment_method,
                TRY_CAST(NULLIF(
                    CASE WHEN order_total LIKE '%,%'
                         THEN replace(replace(order_total, '.', ''), ',', '.')
                         ELSE replace(TRIM(order_total), '$', '') END,
                    'N/A') AS DECIMAL(14,2)) AS order_total,
                items, meta
            FROM read_csv('{csv_path.as_posix()}', header=true, columns={V1_SCHEMA})
        )
        SELECT *, CAST(order_ts AS DATE) AS order_date,
               DATE '{day}' AS _data_date        -- §7.2: the delivery's business date
        FROM typed
    ) TO '{out_path.as_posix()}' (FORMAT parquet);"""

def load_day(task):
    """Runs in a worker process. Must be top-level (spawn pickles it by name)."""
    day, cfg = task
    try:
        con = duckdb.connect()   # in-memory DB, private to this worker
        con.execute(f"SET threads={cfg['threads']}; SET memory_limit='{cfg['mem']}'; "
                    f"SET temp_directory='{tmp_dir(SCALE).as_posix()}';")
        t0 = time.perf_counter()
        con.execute(stage_sql(day, RAW / f"orders_{day}.csv", STAGE / f"orders_{day}.parquet"))
        con.close()
        return day, "OK", time.perf_counter() - t0, ""
    except Exception as e:
        return day, "FAIL", 0.0, f"{type(e).__name__}: {e}"[:200]

def daterange(start, end):
    d, e = date.fromisoformat(start), date.fromisoformat(end)
    while d <= e:
        yield d.isoformat()
        d += timedelta(days=1)

def biggest_first(days):
    key = lambda day: json.loads((MANIFESTS / f"orders_{day}.json").read_text())["bytes"]
    return sorted(days, key=key, reverse=True)

def track_peak_rss(stop, out):
    """Every 0.5 s, sum RSS of this process + all children; remember the peak."""
    me, peak = psutil.Process(), 0
    while not stop.is_set():
        total = 0
        for p in [me] + me.children(recursive=True):
            try:
                total += p.memory_info().rss
            except psutil.NoSuchProcess:
                pass                       # a worker exited mid-scan; fine
        peak = max(peak, total)
        out["peak_gb"] = peak / 2**30
        time.sleep(0.5)

if __name__ == "__main__":                 # REQUIRED on Windows (spawn)
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2026-06-01")
    ap.add_argument("--end", default="2026-07-15")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--threads", type=int, default=2)   # per worker
    ap.add_argument("--mem", default="3GB")             # per worker
    ap.add_argument("--order", choices=["date", "size"], default="date")
    args = ap.parse_args()

    STAGE.mkdir(parents=True, exist_ok=True)
    days = list(daterange(args.start, args.end))
    if args.order == "size":
        days = biggest_first(days)
    tasks = [(d, {"threads": args.threads, "mem": args.mem}) for d in days]

    stop, stats = threading.Event(), {}
    threading.Thread(target=track_peak_rss, args=(stop, stats), daemon=True).start()
    t0, failed = time.perf_counter(), []
    with Pool(processes=args.workers) as pool:
        for day, status, secs, err in pool.imap_unordered(load_day, tasks):
            print(f"  {day}  {status:4}  {secs:6.1f}s  {err}")
            if status == "FAIL":
                failed.append(day)         # continue-on-error, like A11
    wall = time.perf_counter() - t0
    stop.set()
    print(f"\nworkers={args.workers} threads/worker={args.threads} mem/worker={args.mem}"
          f" order={args.order}\nwall {wall:.1f}s | "
          f"peak RSS {stats.get('peak_gb', 0):.1f} GB | failed: {failed or 'none'}")
```

Do it in this order:

1. Run as-is on month 1: `python work\parallel_runner.py --workers 4 --threads 2 --mem 3GB`.
   Completion order is **not** date order — `imap_unordered` returns results as
   workers finish.
2. Run with `--end 2026-08-08`. Every v2/v3 day prints `FAIL` (strict v1 schema,
   different columns) but the run continues — like A11. That failure is your
   prompt, not a bug.
3. The real work: replace `stage_sql`'s SELECT with your A10 per-era canonical
   staging (era from the day's manifest). Left to you on purpose. Keep A10's
   `WHERE` that drops unparseable timestamps: at small scale staging lands 816
   rows under the manifest sum — 816, not the 803 you logged in A10: you're now
   staging 2026-07-22 too, and its 13 unparseable-timestamp rows join the
   total. Task 4 and Verify expect exactly that. Rerun all 69 days: zero FAILs.

Why one file per day instead of `PARTITION_BY (order_date)` into the shared
lake? Late rows: day D carries corrections for days D−7…D, so two workers would
write the *same* `order_date=` folder and silently overwrite each other's files.

**Accept when:** all 69 small days stage green with `_data_date` on every row,
and your journal explains in two sentences why the `__main__` guard and
top-level `load_day` are required on Windows.

### Task 4 — The committer: one process, one writer (small)

Create `work/commit_stage.py`. It merges the staged files with your A08 strategy
(a): latest `updated_at` wins per order.

```python
"""work/commit_stage.py — A12: the ONLY writer of warehouse.duckdb."""
import sys
from pathlib import Path
import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from lib.labpaths import tmp_dir, warehouse_path

SCALE = "small"
STAGE = tmp_dir(SCALE) / "backfill_stage"

if __name__ == "__main__":
    con = duckdb.connect(str(warehouse_path(SCALE)))     # one writer. Only one.
    con.execute("SET memory_limit='8GB'; SET threads=8; "
                f"SET temp_directory='{tmp_dir(SCALE).as_posix()}';")
    con.execute("CREATE SCHEMA IF NOT EXISTS core;")
    con.execute(f"""
        CREATE OR REPLACE TABLE core.orders_par AS
        SELECT * EXCLUDE (rn) FROM (
            SELECT *, row_number() OVER (PARTITION BY order_id
                                         ORDER BY updated_at DESC) AS rn
            FROM read_parquet('{STAGE.as_posix()}/*.parquet')
        ) WHERE rn = 1;""")
    staged, final = con.execute(f"""
        SELECT (SELECT count(*) FROM read_parquet('{STAGE.as_posix()}/*.parquet')),
               (SELECT count(*) FROM core.orders_par)""").fetchone()
    print(f"staged rows: {staged:,}   core.orders_par: {final:,}")
```

It builds `core.orders_par` (not `core.orders`) so you can compare before you
trust it — swap with the A09 pattern if you like. Lineage rides along for free:
the workers stamped `_data_date`, so it lands in `core.orders_par` too, and the
dedupe keeps the winning row's own delivery date — which is exactly what §7.2
wants that column to mean. Its partner `_run_id` is absent here because this
speed scaffold never registers a run; that ledger row is also where the source
file and the load time live — `core.orders o JOIN ops.etl_runs r ON r.run_id =
o._run_id`, the A07 trace — never on the row itself. Your A11 pipeline, which
does register a run and stamps
both columns, remains the production path — this is a speed lab, not a schema
change. Note the committer runs *after* the pool, alone, so it gets the full
8GB/8-thread budget: parallel jobs have phases, and each phase gets the whole
machine.

**Accept when:** small scale shows staged = **4,116,024** and `core.orders_par`
= **4,053,208** (see Verify yourself). If your `core.orders` is slightly
smaller than `orders_par`, that's your A05 gate quarantining rows the quick
cleaner keeps — explain the gap using your quarantine counts.

### Task 5 — The experiment matrix (small, then full)

Time the *stage phase* under different configs. A proper 3-run-median harness is
A14's job; today, at least note whether a second run differs from the first (the
first pays cold disk cache). On **small**, all 69 days, fill in:

| workers | threads | mem     | wall (s) | peak RSS (GB) |
|---------|---------|---------|----------|---------------|
| 1       | 8       | 8GB     |          |               |
| 2       | 4       | 5GB     |          |               |
| 4       | 2       | 3GB     |          |               |
| 8       | 1       | 1500MB  |          |               |

For reference: on one dev machine, a **month-1-only** run (45 days) staged in
~12 s at 1×8 and ~4–6 s with 4–8 workers — your 69-day totals will land roughly
50% higher. Your numbers will differ anyway — the *ratios* matter. At small scale
per-day work is tiny, so process startup blurs the picture; expect roughly 2–3×.

Then on **full** (flip `SCALE = "full"`), run three configs only: `1×8` (your
reference) plus your two best. Full staging reads ~24 GB of CSV — expect the 1×8
run to take on the order of tens of minutes on the baseline machine. Watch Task
Manager: CPU busy during each day's SQL, dips between days; the pool configs
should flatten those dips.

**Accept when:** both tables are filled, and one sentence chooses your config
and *why* (fastest **within budget** beats fastest overall).

### Task 6 — Break it on purpose (full scale, ONE week only)

Two controlled bad runs, on `--start 2026-06-15 --end 2026-06-21` (seven days
including the 06-19 spike). Before the second: close other apps, save your work,
keep Task Manager visible, be ready with `Ctrl+C`.

1. **Oversubscription** — `--workers 8 --threads 8 --mem 1500MB`: 64 runnable
   threads on 8 cores. Watch CPU pin at 100% while wall-clock lands no better
   than 8×1 — often a bit worse. Cores don't multiply; they just switch between
   threads more, and switching isn't free.
2. **Memory overcommit** — `--workers 8 --threads 1 --mem 12GB`. This imitates
   forgetting `memory_limit` (the default is ~80% of RAM — per process!). Watch
   the Memory graph climb and disk churn; look for spill files in
   `<DATA_ROOT>/tmp`. **Abort with Ctrl+C if memory passes ~90% and the machine
   stops responding.** If the run survives politely, write that down too — one
   week may be too small to hit the limits. The lesson stands: nothing checked
   your total; the *promise* was ~96 GB on a 16 GB machine. A13 does controlled
   near-OOM properly.

**Accept when:** your journal has config, wall-clock, peak RSS, and what Task
Manager showed, for both runs.

### Task 7 — The other way: one process, one big query (small, then full)

A pool is not the only parallelism. DuckDB parallelizes a single query across
many files by itself — hand it the whole v1 file list at once:

```python
"""work/single_query_stage.py — stage all 45 v1 days in ONE query."""
import sys, time
from pathlib import Path
import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from lib.labpaths import raw_orders_dir, tmp_dir
from work.parallel_runner import V1_SCHEMA, daterange    # reuse your own code

SCALE = "small"

if __name__ == "__main__":
    files = [raw_orders_dir(SCALE) / f"orders_{d}.csv"
             for d in daterange("2026-06-01", "2026-07-15")]
    file_list = "[" + ", ".join(f"'{f.as_posix()}'" for f in files) + "]"
    out = tmp_dir(SCALE) / "lake_single_query"
    con = duckdb.connect()
    con.execute("SET threads=8; SET memory_limit='8GB'; "
                f"SET temp_directory='{tmp_dir(SCALE).as_posix()}';")
    # TODO(you): paste the SELECT from stage_sql's `typed` CTE — same cleaners,
    # but its FROM clause now reads ALL the files in one go:
    typed_select = f"""SELECT ...your cleaners from stage_sql...
        FROM read_csv({file_list}, header=true, columns={V1_SCHEMA})"""
    t0 = time.perf_counter()
    con.execute(f"""
        COPY (
            WITH typed AS ({typed_select})
            SELECT *, CAST(order_ts AS DATE) AS order_date FROM typed
        ) TO '{out.as_posix()}' (FORMAT parquet, PARTITION_BY (order_date),
                                 OVERWRITE_OR_IGNORE, ROW_GROUP_SIZE 122880);""")
    print(f"one query, {len(files)} files: {time.perf_counter() - t0:.1f}s")
```

Journal notes while it runs: one process, so `PARTITION_BY` into a shared folder
is *safe* — no other writer exists. Look inside the output: 45 date folders
**plus** `order_date=__HIVE_DEFAULT_PARTITION__`, where rows whose ~0.02%
impossible timestamps became NULL dates land (your real pipeline quarantines
them — A05). One query handles one schema, so one era; all 69 days = three file
lists, three queries (your A10 normalizers again). `_data_date` stops being a
per-day constant here — add `filename=true` to the `read_csv` and slice the date
out of the name: `CAST(filename[-14:-5] AS DATE)` (a regex would work too, but
its `{4}` braces are a landmine inside a Python f-string). Trade-off:
dead-simple code, often excellent speed for pure SQL — but all-or-nothing: no
per-day retry, no per-day `ops.etl_runs` state. In A11's vocabulary, a
restatement. At full scale,
if the big COPY hits a memory error, `SET preserve_insertion_order=false;` lets
DuckDB stream more freely (A14).

Measure on small and full; compare with your pool over the same 45 days. On one
dev machine the single query beat the sequential 1×8 loop but lost to the
8-worker pool at small scale; at full scale the balance can flip. Measure, don't
argue.

**Accept when:** wall-clocks for pool vs single query at both scales are in the
journal, plus two sentences on when you'd pick each.

### Task 8 — Skew: spike days finish last (full)

In any full-scale pool run, the per-day `OK` lines show `2026-06-19` and
`2026-07-11` taking roughly 3× a normal day (compare `bytes` in their
manifests). If a spike day starts near the end, everyone else finishes and the
whole run waits on one worker — a straggler.

Rerun your chosen config with `--order size` (`biggest_first`: manifest `bytes`,
descending). Big days start first; small days pack the gaps. Compare wall-clock
against the date-ordered run: expect a modest but real win (~25% on one dev
machine at small scale).

**Accept when:** both timings are in your journal plus one sentence on why
ordering by size helps.

### Task 9 — Ship it: the fast backfill (full)

Stage all 69 full-scale days with your chosen config and ordering, then run the
committer. Total wall-clock (stage + commit) goes next to the A11 baseline in
your journal. Target: **at least 3× faster**. If you miss it: are you disk-bound
(CPU well under 100% during staging)? Too few workers? Committer dominating
(it's one big query — did it get the full 8GB/8-thread budget)?

Then write one paragraph: **"When NOT to parallelize."** Belongs in it: the job
is already fast enough (engineer-hours cost more than machine-minutes); the work
is disk/network-bound, so more workers just queue on the same bottleneck; tasks
share a writer or depend on each other; one big engine query already
parallelizes better than your hand-rolled pool; the machine is shared, and your
8 workers are someone else's outage.

**Accept when:** ≥3× speedup shown with numbers, and the paragraph exists.

## Definition of Done

- [ ] Budget table filled, including why 8×8 and 8×default are illegal.
- [ ] Lock error demonstrated and pasted in journal, including the read-only case.
- [ ] `parallel_runner.py` stages all 69 small days with zero FAILs (era-aware).
- [ ] Committer is the only process that opens `warehouse.duckdb`; small scale:
      staged = 4,116,024 and `core.orders_par` = 4,053,208, every row stamped
      with `_data_date`.
- [ ] Matrix: 4 configs on small, 3 on full — wall + peak RSS each.
- [ ] Oversubscription and overcommit runs observed and written up.
- [ ] Single-query alternative measured at both scales and compared.
- [ ] `--order size` vs `--order date` timings recorded.
- [ ] Full 69-day backfill ≥3× faster than the A11 baseline (journal math shown).
- [ ] "When NOT to parallelize" paragraph in journal.

## Verify yourself

All 69 small days staged:

```powershell
(Get-ChildItem "$env:ETL_LAB_DATA\small\tmp\backfill_stage\orders_*.parquet").Count  # 69
```

Counts reconcile against the manifests (`read_json_auto` is the A05 trick), and
the committer's result is fully predictable — every late correction and in-file
dup collapses into its original:

```sql
SELECT
  (SELECT count(*)
   FROM read_parquet('<DATA_ROOT>/small/tmp/backfill_stage/*.parquet'))
    AS staged_rows,      -- expect exactly 4,116,024: manifests sum to 4,116,840,
                         -- minus the 816 unparseable-ts rows A10's cleaner drops
  (SELECT sum(rows) - sum(late_rows) - sum(dup_rows)
   FROM read_json_auto('<DATA_ROOT>/small/raw/manifest/orders_*.json'))
    AS expected_unique;  -- 4,054,002; core.orders_par lands 794 lower, at
                         -- 4,053,208 — for 794 orders the dropped row was the only copy
```

If your dedupe lands at **4,054,002** (staged at 4,116,840), your staging kept
the unparseable-timestamp rows — your SELECT lost A10's `WHERE`. Anything
**above** that means your `order_id` canonicalization is leaking: v3 `ORD-…`
corrections aren't matching their v1 originals (revisit A10). Full scale: same
queries — the manifests sum to exactly **82,337,467** (A13 quotes the same
number), so expect roughly 82M staged rows after the same ~0.02%
unparseable-timestamp drop.

The lineage stamp must survive the merge — §7.2 wants `_data_date` on every
committed row, carrying the delivery the *winning* copy came from:

```sql
SELECT count(DISTINCT _data_date) AS delivery_dates,
       count(*) FILTER (_data_date IS NULL) AS unstamped
FROM core.orders_par;                  -- 69 and 0
```

Speedup math for the journal: `A11 baseline ÷ (stage + commit wall-clock) ≥ 3`.

## Common beginner mistakes

- **No `if __name__ == "__main__":` guard.** Every spawned child re-imports your
  file; without the guard each child starts its own pool — `RuntimeError: An
  attempt has been made to start a new process before…bootstrapping phase`, or a
  screen full of Pythons.
- **Workers opening `warehouse.duckdb`.** First one wins; the rest die with the
  Task 2 `IOException`. Workers write Parquet; one committer writes the DB.
- **Forgetting `memory_limit` per worker.** The default is ~80% of RAM *per
  process*. Eight workers on defaults is a ~100 GB promise on a 16 GB machine.
- **Comparing against the wrong baseline.** The honest baseline is 1 worker × 8
  threads — sequential DuckDB already used all cores inside each query.
  Comparing to 1×1 flatters your pool.
- **Workers doing `PARTITION_BY` into the same lake folder.** Late rows put days
  D−7…D in one file, so two workers hit the same `order_date=` folder and
  silently clobber each other's Parquet.
- **Trusting a single timing.** The first run pays cold disk cache; the second
  looks magically faster. Note run order and repeat at least once (A14 builds
  the proper harness).

## Stretch goals

1. **Retry-aware pool.** Swap `multiprocessing.Pool` for
   `concurrent.futures.ProcessPoolExecutor` + `as_completed`, re-submit failed
   dates with your A07 backoff wrapper, and have the committer record per-day
   rows into `ops.etl_runs` so A11's `--only-failed` still works.
2. **Skew-aware scheduling.** Give the two spike days a dedicated solo pass at 8
   threads each, then pool the remaining 67. Measure against biggest-first.
3. **Chunking.** `imap_unordered(..., chunksize=4)` hands each worker 4 dates at
   a time: less dispatch overhead, worse stragglers. Measure chunksize 1 vs 4
   vs 8 on small and explain the shape of the results.
