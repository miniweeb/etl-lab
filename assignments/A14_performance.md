# A14 — Performance tuning

**Topic:** performance · **Est. time:** 4–5h · **Difficulty:** ⭐⭐⭐⭐
**Requires:** A13 done; the full 69-day canonical lake at both scales (A10/A11); raw dim CSVs; roughly 12 GB free on the data drive.

## Why this matters

Every data team owns a job that is correct but slow, and everyone is afraid to
touch it. Today you inherit one. The skill is not "knowing tricks" — it is a
loop: **measure → explain → fix → re-measure**, one change at a time, biggest
cost first. Engineers who guess spend a week making the wrong part 2× faster;
engineers who measure spend an hour making the right part 100× faster — and
after A13 you know what an hour costs when a cloud meter runs.

## Background

**Rank by measured time.** A job spends 70 s in step A and 0.3 s in step B. A
brilliant 100× fix to B saves 0.3 seconds; a lazy 2× fix to A saves 35. You
never optimize *a* step — you optimize the most expensive step. Measure first.

**Where time goes.** Analytical pipelines spend nearly everything in four
places: *scanning* (reading + decoding bytes), *joining*, *aggregating/sorting*,
and — the classic killer — *crossing the SQL↔Python border row by row*. Every
SQL statement carries roughly a millisecond of fixed overhead (parse, plan,
commit). One statement over 60,000 rows pays it once; 60,000 single-row INSERTs
pay it 60,000 times. Engines are *set-based* — they process whole columns at
once; a Python loop over rows throws that away.

**How engines skip work.** The fastest byte is one never read. Your lake gives
DuckDB three levels of skipping:

1. **Partition pruning** (A02): filter on the partition column (`order_date`)
   and whole files are never opened.
2. **Zone maps**: inside a Parquet file, rows live in **row groups** (blocks of
   rows; your A02 `COPY` used the default, 122,880), and each group stores
   every column's min/max. `store_id = 42` skips any group whose range excludes
   42. *Sorted* by `store_id` at write time, ranges are tight and almost every
   group is skipped; unsorted, 42 is everywhere and nothing is. Tiny row groups
   are their own sin: per-group overhead taxes every future scan.
3. **Projection pushdown**: columnar files store columns separately; the reader
   decodes only columns you *name*. `SELECT *` asks for everything — including
   the wide `items` structs — and turns this off by definition.

**Hash joins have sides.** A hash join *builds* an in-memory table from one
side and *probes* it with the other; you want to build on the small side (400
stores), not the big one (a million orders). DuckDB's optimizer re-orders joins
and picks build sides from size estimates — some join folklore is dead. Your
job: don't feed a query joins it doesn't need, and *verify* with `EXPLAIN`.

**Benchmarks lie unless made honest.** From A13: the first run pays the cold
disk cache. So — warm-up, then median of 3, change ONE thing between
measurements, and always prove the output didn't change.

## Setup

- Both scales of the lake on disk, venv active, `journal.md` open. Scripts live
  in `work/` (imports as in A12/A13: `sys.path` insert + `labpaths`); run from
  the repo root. Connection settings as always (SPEC §4): `memory_limit '8GB'`,
  `threads 8`, `temp_directory '<DATA_ROOT>/tmp'`.
- Every script today reads A10's versioned lake through a glob, so the
  timestamp in the dir name doesn't matter:
  `LAKE = (lake_dir(SCALE) / "orders_v=*").as_posix()` — adjust if your A10
  dir is named differently. The glob makes `hive_partitioning=true` surface an
  extra `orders_v` column; harmless for everything today.
- Plans print with box characters; if printing one dies with
  `UnicodeEncodeError`, add `sys.stdout.reconfigure(encoding="utf-8")` up top.
- Optional: the **DuckDB CLI** (separate small download, duckdb.org). Its
  `.timer on` prints `Run Time (s): real …` per query — `real` is wall clock,
  your number. Everything today also works without the CLI.
- Task 6 writes ~8 GB of full-scale experiment files; cleanup is in the DoD.

## Tasks

### Task 1 — Build the stopwatch: `work/bench.py` (small)

Rule of the day: no timing claim without this harness.

```python
"""work/bench.py -- honest timings: one warm-up, then median of 3."""
import statistics, time

def bench(con, label, sql, runs=3, warmup=1):
    for _ in range(warmup):
        con.execute(sql).fetchall()     # pay the cold-cache run (A13) up front
    times = []
    for _ in range(runs):
        t0 = time.perf_counter()
        con.execute(sql).fetchall()     # fetch! or you time only part of the work
        times.append(time.perf_counter() - t0)
    med = statistics.median(times)
    print(f"{label:30s} median {med*1000:9.1f} ms   "
          f"(min {min(times)*1000:.1f} / max {max(times)*1000:.1f})")
    return med
```

Median, not mean: one antivirus hiccup shouldn't move your number. To bench
DDL, write it as `CREATE OR REPLACE TABLE` so run 2 works.

**Accept when:** two bench calls on one query agree closely, and you saw the
warm-up run come in slower at least once.

### Task 2 — Three lenses on one query (small, then full)

Write `work/lenses.py`. The victim: count one day's orders, filtered three
different — all *correct* — ways. Lens 1 is the stopwatch:

```python
BASE = (f"SELECT count(*) FROM read_parquet('{LAKE}/*/*.parquet', "
        f"hive_partitioning=true) WHERE ")
filters = {
    "A strftime(ts) = str": "strftime(order_ts, '%Y-%m-%d') = '2026-06-05'",
    "B cast(ts) = date":    "CAST(order_ts AS DATE) = DATE '2026-06-05'",
    "C order_date = date":  "order_date = DATE '2026-06-05'",
}
for name, f in filters.items():
    bench(con, name, BASE + f)
```

Lens 2 is the plan with real numbers in it — print
`con.execute("EXPLAIN ANALYZE " + BASE + filters[...]).fetchall()[0][1]` for C,
then for A. Lens 3 is the same truth as data:

```python
con.execute("PRAGMA enable_profiling='json';")
con.execute("PRAGMA profiling_output='work/profile.json';")
con.execute(BASE + filters["A strftime(ts) = str"]).fetchall()
con.execute("PRAGMA disable_profiling;")

def ops(node):                          # walk the JSON operator tree
    if "operator_timing" in node:
        yield node["operator_timing"], node["operator_type"], node["operator_cardinality"]
    for child in node.get("children", []):
        yield from ops(child)

prof = json.loads(Path("work/profile.json").read_text())
for tm, op, rows in sorted(ops(prof), reverse=True)[:5]:
    print(f"{tm:8.4f} s   {op:20s} rows out: {rows:,}")
```

One dev machine, small scale: A ≈ 130 ms, B ≈ 20 ms, C ≈ 10 ms. Full scale
(flip `SCALE`): A ≈ 1.9 **s**, B ≈ 40 ms, C ≈ 10 ms — a 200× spread for the
same answer. Why: C prunes on the partition column. B *looks* bad, but the
optimizer rewrites the cast into a range check on `order_ts` and zone maps
rescue it. A hides the column inside a string function no optimizer sees
through — every file opened, every row formatted. In lens 2's scan box, C shows
`File Filters: …` and `Scanning Files: 1/N`; A shows `Total Files Read: N`
with nearly all time in `TABLE_SCAN`. N is your lake's total file count — a
couple hundred (our rebuild: 237), not 69: A10's per-day APPEND loads left
several files per month-2 partition. The `1` is your pruning proof, forever.

**Accept when:** the 3-filters × 2-scales table is in your journal, plus one
sentence: how do you *prove* pruning happened?

### Task 3 — Meet `badpipe.py` (small)

A colleague left; you inherit their nightly job. It is **correct**, and it is
slow. Copy it exactly as `work/badpipe.py`. Read it like a crime scene, but fix
nothing yet.

```python
"""work/badpipe.py -- the nightly store_daily job you just inherited.
It is CORRECT. For a chosen day it produces, under <DATA_ROOT>/tmp/perf/:
  orders_<day>.parquet       day extract for finance (their contract columns:
                             order_id, customer_id, store_id, order_ts, status, order_total)
  store_daily_<day>.parquet  one row per store: orders, revenue
Rule of the game: make it FAST without changing what it produces."""
import sys, time
from pathlib import Path
import duckdb
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from lib.labpaths import lake_dir, raw_dims_dir, tmp_dir

SCALE = "small"                              # Task 5 flips this to "full"
DAY = sys.argv[1] if len(sys.argv) > 1 else "2026-06-05"
LAKE = (lake_dir(SCALE) / "orders_v=*").as_posix()   # A10's versioned lake (see Setup)
DIMS = raw_dims_dir(SCALE)
OUT = tmp_dir(SCALE) / "perf"
OUT.mkdir(parents=True, exist_ok=True)
con = duckdb.connect()
con.execute(f"SET memory_limit='8GB'; SET threads=8; "
            f"SET temp_directory='{tmp_dir(SCALE).as_posix()}';")

t_all = t0 = time.perf_counter()
def step(name):
    global t0
    print(f"  {name:10s} {time.perf_counter() - t0:8.2f} s", flush=True)
    t0 = time.perf_counter()

# step 1: slice the day out of the lake
con.execute(f"""
    CREATE TABLE day_raw AS
    SELECT DISTINCT *
    FROM read_parquet('{LAKE}/*/*.parquet', hive_partitioning=true)
    WHERE strftime(order_ts, '%Y-%m-%d') = '{DAY}'
    ORDER BY order_ts""")
step("slice")

# step 2: "validate every row in Python before we trust it"
cur = con.execute("SELECT * FROM day_raw")
cols = [d[0] for d in cur.description]
rows = cur.fetchall()
i_total = cols.index("order_total")
con.execute("CREATE TABLE day_ok AS SELECT * FROM day_raw LIMIT 0")
marks = ", ".join(["?"] * len(cols))
for r in rows:
    if r[i_total] is not None:               # this is the entire "validation"
        con.execute(f"INSERT INTO day_ok VALUES ({marks})", r)
step("validate")

# step 3: the extract for the finance team
con.execute(f"""
    COPY (SELECT * FROM day_ok)
    TO '{(OUT / f"orders_{DAY}.parquet").as_posix()}'
    (FORMAT parquet, ROW_GROUP_SIZE 5000)""")
step("extract")

# step 4: the store_daily mart
con.execute(f"""
    COPY (
        SELECT o.store_id, s.store_name, s.region,
               count(*) AS orders, sum(o.order_total) AS revenue
        FROM day_ok o
        LEFT JOIN read_csv('{(DIMS / "customers.csv").as_posix()}', header=true) c
               ON o.customer_id = c.customer_id
        JOIN read_csv('{(DIMS / "stores.csv").as_posix()}', header=true) s
          ON o.store_id = s.store_id
        GROUP BY 1, 2, 3 ORDER BY revenue DESC
    ) TO '{(OUT / f"store_daily_{DAY}.parquet").as_posix()}' (FORMAT parquet)""")
step("mart")
print(f"  {'TOTAL':10s} {time.perf_counter() - t_all:8.2f} s")
```

Run at small scale; the per-step timings are your **baseline** — into the
journal. One dev machine: `slice 0.27 · validate 69.86 · extract 0.10 ·
mart 0.13 · TOTAL 70.35 s`. Now list every sin you can find (at least seven)
and, next to each, *which step's measured time it lives in*. Notice: the clever
finds — `SELECT DISTINCT *`, `strftime`, `ORDER BY` — sit inside half a second,
while one boring step is 99% of the runtime. Ranking just told you where to start.

**Accept when:** baseline table + sin list (each with step and cost) are in
the journal.

### Task 4 — Fix it, biggest first (small)

Copy `badpipe.py` to `work/fastpipe.py`, change `OUT` to
`tmp_dir(SCALE) / "perf_fixed"` (keep both outputs to compare), and apply fixes
**one at a time, re-running and recording after each**. That discipline *is*
the skill; resist batching.

**Fix 1 — the loop becomes one statement.** 60k rows ferried into Python, then
60k single INSERTs at ~1 ms overhead each; the "validation" is a WHERE clause
in a costume. Replace all of step 2 with:

```python
con.execute("CREATE TABLE day_ok AS "
            "SELECT * FROM day_raw WHERE order_total IS NOT NULL")
```

Expect the win of the day (one dev machine: 69.86 s → 0.07 s). The core lesson:
**stay set-based; never ferry rows through Python to say something SQL says in
one clause.**

**Fix 2 — filter on the partition column.** In step 1, replace the `strftime`
predicate with `WHERE order_date = DATE '{DAY}'`. Same rows out (check!), and
`EXPLAIN ANALYZE` now shows `Scanning Files: 1/N` — one file opened out of the
whole lake.

**Fix 3 — delete `DISTINCT` and `ORDER BY` — after proving them useless.**
Never delete on faith. Count the day slice plain vs through `SELECT DISTINCT *`
(two subqueries in one SELECT — you have written harder queries since A03).
They match: no two lake rows are byte-identical (even A08's correction copies
differ in `updated_at`), so `DISTINCT` hashed 60k wide rows for nothing; the
`ORDER BY` sorts rows nobody asked to sort. Delete both. If *your* counts
differ, stop and investigate — a data bug outranks any speedup.

**Fix 4 — name your columns.** `day_raw` drags `items` and `meta` — the widest
columns in the lake — through every step, and the extract ships them to
finance, who ignore them. The contract needs six columns; in step 1:

```python
SELECT order_id, customer_id, store_id, order_ts, status, order_total
```

Step 3's `SELECT * FROM day_ok` is now honest. Compare the two extracts'
file sizes with `Get-ChildItem` (one dev machine, full scale: 94 MB → 28 MB —
projection is also a disk-and-download win, not just a scan win).

**Fix 5 — drop the join nobody ordered.** The mart joins `customers` — 60k rows
at small scale, **1.2M at full** — and uses *nothing* from it (someone planned
a per-country mart once). It's a LEFT JOIN on the orders side, so row counts
are preserved: the output is identical without it; the cost is a giant
hash-table *build side*. See it before deleting: `EXPLAIN ANALYZE` the mart and
find the `HASH_JOIN` fed by a customers-sized scan (one dev machine, full
scale: ~205 ms with, ~32 ms without). The optimizer picks build sides and join
order for you — `EXPLAIN` a small-table LEFT JOIN big-lake query sometime and
watch it flip to `Join Type: RIGHT`, building on the 400-row side — but it
cannot know a join is *pointless*. Deleting work is your job. (The mart's
`ORDER BY revenue DESC` stays: 400 rows, and the ranking is the product.)

**Fix 6 — row groups back to sane.** `ROW_GROUP_SIZE 5000` chops even a small
day's extract into 10+ groups (~200 at full scale). Set 122880 or delete the
option; the write barely cares, but *readers* pay for tiny groups every day
after — Task 6 measures how much.

After every fix, prove you broke nothing — both directions, and likewise for
the two extracts comparing the six contract columns (name them on the badpipe
side):

```sql
SELECT count(*) FROM (
  (SELECT * FROM read_parquet('<DATA_ROOT>/small/tmp/perf/store_daily_2026-06-05.parquet'))
  EXCEPT
  (SELECT * FROM read_parquet('<DATA_ROOT>/small/tmp/perf_fixed/store_daily_2026-06-05.parquet'))
);   -- 0 rows differing, both ways, or you optimized the truth away
```

**Accept when:** the journal has one row per fix — step, before s, after s —
and the EXCEPT checks return 0. Small-scale end state on one dev machine:
70.35 s → 0.09 s (~780×). Yours will differ; the shape will not.

### Task 5 — The full-scale showdown (full)

Set `SCALE = "full"` in both scripts. Start the baseline **first**, then go do
Task 6 while it runs — watching time, not waiting time:

```powershell
python work\badpipe.py 2026-06-05
```

Observe while it grinds (Task Manager, or wrap it in A13's resmon): `slice`
takes ~2 s — the strftime scan of ~5 GB of Parquet — then `validate` sits for
roughly **twenty minutes** (one dev machine: TOTAL 1189 s; longer if the
machine is busy): a ~1.2M-row `fetchall` holding ~2 GB of RSS while 1.2M
INSERTs drip through one at a time. Let it finish — you want that number in
writing. Then run `work\fastpipe.py` at full scale: well under a second on that
same machine (~0.3 s). Fill the table:

| scale | badpipe TOTAL | fastpipe TOTAL | speedup |
|-------|--------------|----------------|---------|
| small |              |                |         |
| full  |              |                |         |

Target: **at least 20×**; expect far beyond it (that dev machine: ~780× small,
~4,000× full). Re-run both EXCEPT checks on the full-scale outputs — a speedup
without an equality proof is a rumor.

**Accept when:** table filled, ≥20× at both scales, equality proven, and the
journal names the single fix that bought the most seconds.

### Task 6 — Row groups and zone maps: the storage dials (small, then full)

Two dials set at *write* time decide how much every future reader can *skip*:
row-group size and sort order. Measure both on the six columns you actually
query. Save as `work/rg_lab.py`:

```python
"""work/rg_lab.py -- row-group size x sort order. Writes 4 experiment files."""
# imports + con as usual (and work.bench); SCALE = "small", then "full"
SIX = ("SELECT order_id, customer_id, store_id, order_ts, status, order_total "
       f"FROM read_parquet('{LAKE}/*/*.parquet', hive_partitioning=true)")
EXP = tmp_dir(SCALE) / "rg_exp"
EXP.mkdir(parents=True, exist_ok=True)
variants = {
    "rg5000_unsorted":   (5000,   ""),
    "rg122880_unsorted": (122880, ""),
    "rg122880_sorted":   (122880, "ORDER BY store_id"),
    "rg5000_sorted":     (5000,   "ORDER BY store_id"),
}
for name, (rg, order) in variants.items():
    path = EXP / f"{name}.parquet"
    if path.exists():
        continue
    t0 = time.perf_counter()
    con.execute(f"COPY ({SIX} {order}) TO '{path.as_posix()}' "
                f"(FORMAT parquet, ROW_GROUP_SIZE {rg})")
    print(f"write {name:20s} {time.perf_counter()-t0:6.1f} s")
for name in variants:
    p = (EXP / f"{name}.parquet").as_posix()
    groups = con.execute("SELECT count(DISTINCT row_group_id) "
                         f"FROM parquet_metadata('{p}')").fetchone()[0]
    print(f"--- {name}  ({groups} row groups)")
    bench(con, "  scan-everything agg",
          f"SELECT store_id, sum(order_total) FROM read_parquet('{p}') GROUP BY 1")
    bench(con, "  one-store filter",
          f"SELECT count(*), sum(order_total) FROM read_parquet('{p}') WHERE store_id = 42")
```

Run small first (seconds), then full. Reference shape, one dev machine, full
scale (82M rows, ~1.9 GB per file):

| file | groups | scan-everything | store_id = 42 |
|------|--------|-----------------|---------------|
| rg5000_unsorted   | ~13,400 | ~2.1 s  | ~2.0 s |
| rg122880_unsorted | ~670    | ~0.47 s | ~0.43 s |
| rg122880_sorted   | ~670    | ~0.38 s | **~0.024 s** |
| rg5000_sorted     | ~13,400 | ~1.8 s  | ~0.40 s |

Read it like an engineer: tiny groups tax *every* query ~4×; sorting does
nothing for scan-everything, but with sane groups the one-store filter gets
~18× faster — zone maps skip ~669 of 670 groups. (Also note your *write* times:
the sorted tiny-group file is slowest, minutes at full scale.) See the maps:

```sql
SELECT row_group_id, min(stats_min_value) AS min_store, max(stats_max_value) AS max_store
FROM parquet_metadata('<DATA_ROOT>/tmp/rg_exp/rg122880_sorted.parquet')
WHERE path_in_schema = 'store_id' GROUP BY 1 ORDER BY 1 LIMIT 5;
-- group 0: stores 1..16, group 1: 16..22, ... tight ranges = skippable
```

This works *within your partitioned lake* too. A full-scale day is ~1.2M rows ≈
10 row groups; a small-scale day is ONE group, so within-day sorting cannot
help there — know why. At full scale, rewrite the 06-19 spike partition twice —
`COPY (SELECT * FROM read_parquet('{LAKE}/order_date=2026-06-19/*.parquet'))`
plain vs with `ORDER BY store_id`, both `ROW_GROUP_SIZE 122880` — and bench the
one-store filter on each. One dev machine: ~17 ms → ~6 ms (30 groups, 29
skipped). Same mechanism — and why a partition rewrite job (your A09 swap)
would add `ORDER BY store_id` *if* store-filtered queries are the hot path.
You can only physically sort one way; spend it on your hottest filter.

**Accept when:** both tables (small + full) are in the journal, plus one
sentence: when would a tiny row-group size ever be right?

### Task 7 — Same query, different day: skew (small, then full)

Cost is a function of the *data*, not just the SQL. Take your fixed step-1
slice (as `CREATE OR REPLACE TABLE`, per the Task 1 tip) and bench it for a
normal day (`2026-06-05`) vs the spike day (`2026-06-19`), at both scales.

At **small** scale the two land close together (one dev machine: ~100 vs
~120 ms) despite 3× the rows — fixed overhead (listing a couple hundred lake
files, planning) swamps the work. At **full** scale
the truth comes out: roughly 110 vs 350 ms, the clean 3× the manifests predict.
Two journal lessons: (1) small scale *ranks* problems but cannot *size* them —
confirm at full scale before you promise anything; (2) budget a daily job for
its p95 day, not its average day — A12's straggler problem again, and why "the
nightly load is slow sometimes" tickets track the calendar.

**Accept when:** both ratios plus the two lessons are in the journal, with the
06-05 and 06-19 row counts from the manifests next to them.

### Task 8 — Write your performance checklist

Close the loop. From your journal — not from memory — write a one-page
`work/perf_checklist.md` you would hand the next junior. Required sections:
*before you touch anything* (baseline, equality check ready); *the usual
suspects, in the order I found them* (row-by-row Python, missing partition
filters, `SELECT *`, needless DISTINCT/ORDER BY/joins, bad row groups); *how to
verify* (bench.py, `EXPLAIN ANALYZE`, `Scanning Files:`, profile JSON); *when
to stop* (fast enough is a business number, not an ego number).

**Accept when:** the page exists with at least eight of your own measured
numbers in it.

## Definition of Done

- [ ] `work/bench.py` exists; every timing claimed today is a warm median-of-3.
- [ ] Filter shootout table (3 forms × 2 scales) + pruning proof
      (`Scanning Files: 1/N` — one file scanned) in the journal.
- [ ] `badpipe.py` baseline per-step table at small scale; sins ranked by cost.
- [ ] Six fixes applied one at a time in `fastpipe.py`, each with before/after;
      DISTINCT proven a no-op before deletion.
- [ ] EXCEPT checks return 0 — both artifacts, both directions, both scales.
- [ ] Full-scale showdown ≥20× (expect far more), numbers in the journal.
- [ ] Row-group × sort tables at both scales; zone maps inspected;
      spike-partition sort measured.
- [ ] Skew ratios (small ≈ 1×, full ≈ 3×) recorded with manifest row counts.
- [ ] `work/perf_checklist.md` written from your own measurements.
- [ ] Cleanup: `Remove-Item -Recurse -Force` on `tmp\rg_exp`, `tmp\perf`,
      `tmp\perf_fixed` under both `$env:ETL_LAB_DATA` and its `\small` twin.

## Verify yourself

```sql
-- 1) deterministic anchors (manifests, the A05 trick):
SELECT rows FROM read_json_auto('<DATA_ROOT>/raw/manifest/orders_2026-06-05.json');
-- full: exactly 1,194,322.  small: 59,716.
-- spike 06-19: 3,643,024 (full), 182,151 (small).
-- your order_date=2026-06-05 partition holds roughly that many rows (late
-- corrections move a few in and out; your A03/A05 choices move a few more).

-- 2) pruning proof: EXPLAIN ANALYZE with the order_date filter shows
--    "Scanning Files: 1/N"; the strftime form shows "Total Files Read: N".
--    N = your lake's total file count, a couple hundred after A10's per-day
--    APPEND loads (not 69). The 1 is what matters.

-- 3) row-group sanity: groups ~= rows / ROW_GROUP_SIZE (DuckDB rounds the size
--    up to a multiple of its 2048-row vectors, so 5000 really means 6144):
--    at 122880 expect ~34 (small) / ~670 (full); at 5000 expect ~670 / ~13,400.
```

- Speedup arithmetic: `badpipe TOTAL ÷ fastpipe TOTAL ≥ 20` at both scales.
  Under 20× means a fix didn't land — check the per-step prints; it is almost
  always a still-alive Python loop.
- Both EXCEPT checks 0. Fast **and** right, or it doesn't count.

## Common beginner mistakes

- **Fixing the interesting sin instead of the expensive one.** `strftime` and
  `DISTINCT` are the clever finds — together worth ~0.4 s while the boring loop
  holds 70. Rank by measured time, fix in that order.
- **Comparing a cold run to a warm run.** The first scan pays the disk cache
  (A13). bench.py warms up first — use it for every claim.
- **Changing two things between measurements.** You can't attribute the change;
  if one *regresses*, the pair can cancel out and look like nothing.
- **Making it fast by making it wrong.** Deleting DISTINCT without the count
  proof, or "simplifying" a join that actually filtered rows. The EXCEPT check
  after every fix is non-negotiable.
- **Trusting join folklore instead of EXPLAIN.** DuckDB already builds on the
  small side where it can; verify, then spend your effort deleting joins and
  columns the query never needed.
- **`UnicodeEncodeError` when printing plans on Windows.** Not a DuckDB bug —
  the console is cp1252. `sys.stdout.reconfigure(encoding="utf-8")`.

## Stretch goals

1. **What does the optimizer buy?** `PRAGMA disable_optimizer;`, re-bench Task
   2's filter B and a LEFT JOIN, then `PRAGMA enable_optimizer;`. The ratio is
   work it does free on every query.
2. **Sort tradeoffs are real.** Rebuild `rg122880_sorted` ordered by
   `customer_id`; bench a `store_id` filter and a `customer_id` filter on both
   sorted files. One physical order, two query patterns — write the design note.
3. **Profile your real pipeline.** Wrap your A11 per-day runner's main query
   with the JSON profiler, land `(date, operator_type, operator_timing)` rows
   in an `ops.query_profile` table, and find your top-3 operators for a week.
