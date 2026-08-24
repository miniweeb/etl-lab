# A08 — Append vs upsert vs partition refresh
**Topic:** append / upsert / partition refresh (delete+insert by date) · **Est. time:** 4h · **Difficulty:** ⭐⭐⭐⭐
**Requires:** A07 done; month-1 raw data at both scales; your `warehouse.duckdb`.

## Why this matters

Every pipeline must answer one question: when new data arrives, *how does it enter the table*?
If yesterday's facts never changed, you could just append forever. But this feed — like almost
every real feed — sends **corrections**: today's file quietly contains new versions of orders
from up to a week ago. Pick the wrong load strategy and your warehouse either double-counts
orders or silently shows stale statuses. The three strategies you build today (append-only,
upsert, partition refresh) are *the* three answers the industry has; every tool you will ever
meet, dbt included (A15), is a wrapper around one of them.

## Background

**The corrections phenomenon.** Since A02 you have known that each day's file writes into
*earlier* date partitions. Here is why: about 1.5% of the rows in file D are **late-arriving
corrections** — a new version of an order first seen on some earlier day T (up to 7 days back).
Same `order_id`, advanced `status` (delivered/cancelled/refunded), sometimes a changed total,
and a fresh `updated_at`. The business rule is: **for each `order_id`, the version with the
latest `updated_at` wins**. `updated_at` is our *version authority* — the column that decides
which copy of a row is current. Not file order: you will meet a correction today whose
`updated_at` is *older* than the version you already have. It must lose.

**Strategy A — append-only log.** Never update, never delete: every row from every file goes
into one ever-growing table, and a **dedupe-on-read view** picks the winner per `order_id` with
a window function. Writes are as cheap as writes get, and you keep the full history of every
correction — an audit trail for free. The cost moves to the readers: every query pays the
dedupe. This is how event logs and "bronze" layers work.

**Strategy B — upsert.** *Upsert* = **up**date the row if the key already exists, in**sert** it
if not — one statement, `MERGE INTO`. The table always holds exactly one current row per order.
Reads are cheap and simple; writes now have to look up every incoming key, and history is gone
(the old version is overwritten). Needs storage that can update in place — a database, not a
folder of files. This is the classic warehouse pattern for mutable entities.

**Strategy C — partition refresh** (delete+insert by date). Don't touch individual rows at all:
recompute *whole date partitions*. When file D arrives, delete everything with `order_date`
between D−7 and D, and re-insert those dates fresh from all the data received so far. Blunt,
simple, and beautifully idempotent — and it is the **only** strategy that works when your
storage is immutable files like a Parquet lake (you cannot update a row inside a Parquet file;
you can only rewrite files). The price: you rewrite roughly 8 days of data to load 1.

Why D−7? Because the A06 contract promises corrections arrive at most 7 days late — the
**late-data allowance**. The refresh window must cover every partition where *any* version of
an incoming order can live; the contract is what makes the window sound. This is also why A07's
delete+insert (which you wrote for a single day's rows) gets upgraded today.

All three, run over the same files, must produce **identical current state** — if they don't,
at least one is wrong. Proving that agreement is today's centerpiece. A09 fixes what readers
see *during* a refresh; A11 replays this per-day pipeline over history; A15 shows dbt's
`delete+insert` incremental strategy — literally Strategy C with config instead of code.

## Setup

- Data: month 1, era v1 only, a **10-day window: 2026-06-01 → 2026-06-10**, small scale first.
  Full scale only in Task 8.
- Your real `core.orders` from A05/A07 stays untouched — today builds separate experiment
  tables (`core.orders_log`, `core.orders_upsert`, `core.orders_refresh`) so you can compare.
- One process on the warehouse at a time (DuckDB is single-writer; close stray notebooks).
- Have Task Manager open for Task 8, and `journal.md` open throughout.
- Start `work/a08_load_strategies.py`:

```python
import datetime as dt
import time
from pathlib import Path
import duckdb
from lib.labpaths import data_root, warehouse_path

SCALE = "small"                       # switch to "full" only in Task 8
ROOT = data_root(SCALE)
RAW = ROOT / "raw" / "orders"
MAN = (ROOT / "raw" / "manifest" / "orders_*.json").as_posix()

START = dt.date(2026, 6, 1)
DATES = [START + dt.timedelta(days=i) for i in range(10)]   # 06-01 .. 06-10

con = duckdb.connect(str(warehouse_path(SCALE)))
con.execute(f"SET memory_limit='8GB'; SET threads=8; "
            f"SET temp_directory='{(ROOT / 'tmp').as_posix()}';")
con.execute("CREATE SCHEMA IF NOT EXISTS core")   # exists since A03; harmless if so

TS = "['%Y-%m-%d %H:%M:%S', '%d/%m/%Y %H:%M:%S', '%Y-%m-%dT%H:%M:%S']"

def staged_select(d: dt.date) -> str:
    """One day's file, minimally staged (A03 cleaners), plus the `_data_date` lineage column."""
    f = (RAW / f"orders_{d}.csv").as_posix()
    return f"""
    SELECT
      order_id,
      customer_id,
      store_id,
      try_strptime(order_ts, {TS}) AS order_ts,
      updated_at,
      status,
      payment_method,
      TRY_CAST(NULLIF(
        CASE WHEN order_total LIKE '%,%'
             THEN replace(replace(order_total, '.', ''), ',', '.')
             ELSE replace(TRIM(order_total), '$', '') END,
        'N/A') AS DECIMAL(14,2)) AS order_total,
      COALESCE(CAST(try_strptime(order_ts, {TS}) AS DATE), DATE '{d}') AS order_date,
      DATE '{d}' AS _data_date
    FROM read_csv('{f}',
        header=true,
        columns={{'order_id':'BIGINT','customer_id':'BIGINT','store_id':'INTEGER',
                 'order_ts':'VARCHAR','updated_at':'TIMESTAMP','status':'VARCHAR',
                 'payment_method':'VARCHAR','order_total':'VARCHAR',
                 'items':'VARCHAR','meta':'VARCHAR'}})
    """
```

Three things to notice before moving on. First, there is exactly one lineage column here —
`_data_date` (§7.2), and that is the whole list. It still tells you *which file* a row came
from: the contract's `path_pattern` is one file per business date, so `_data_date` **is**
`orders_<_data_date>.csv`. The file name, the load timestamp, the status and the row count are
facts about the **run**, not about the order; they live on the `ops.etl_runs` row that
`_run_id` points at. These throwaway experiment tables are filled by the bare loop below
instead of A07's ledgered loader, so they carry no `_run_id` either — your real `core.orders`
does. Second,
`order_date` gets a `COALESCE` fallback to the file's date: ~0.02% of rows have impossible
timestamps that clean to NULL, and **a partition key must never be NULL** — a NULL-dated row
can never be matched by `WHERE order_date BETWEEN …`, so Strategy C would silently lose it
forever. Third, we leave `items`/`meta` out of the experiment tables — the strategies behave
identically with more columns, and the JSON strings would just make every rewrite heavier.

## Tasks

### Task 1 — Measure the phenomenon before building anything (small)

Never design a load strategy for data you haven't measured. The manifests already count late
rows — read them with DuckDB:

```python
con.sql(f"""
  SELECT sum(rows) AS rows, sum(late_rows) AS late, sum(dup_rows) AS dups
  FROM read_json_auto('{MAN}')
  WHERE CAST(date AS DATE) BETWEEN DATE '2026-06-01' AND DATE '2026-06-10'
""").show()
```

Small scale says: **577,673 rows, 7,750 late, 284 in-file dups**. None of this is a surprise —
it is the contract keeping its word. The `late_corrections` clause in
`contracts/shopcore_orders_daily.contract.yaml` promises exactly this shape: re-sends up to
7 days late, "LATEST updated_at WINS", dedupe explicitly the warehouse's job. And the 284
in-file dups are 0.05% of the window's rows — comfortably inside the contract's
`within_file_exact_resends: 0.2%` tolerance, so they are expected at-least-once noise, not an
incident. Now prove the overlap is
real: count `order_id`s that appear in *both* the 06-01 and 06-02 files (use `INTERSECT`
between two `read_csv` selects with the explicit schema above). You should get **883** —
compare it with `late_rows` in `orders_2026-06-02.json` (**885**) and write one sentence in
your journal explaining the gap of 2 (hint: can one order be corrected twice in one file?).

**Accept when:** you can state the expected duplicate-id volume for the window from manifests
alone, and your two-file overlap is 883.

### Task 2 — Strategy A: the append-only log (small)

Create an empty log with the staged shape, then load all 10 days — deleting the file's own
rows first, so a rerun of any day cannot double-load it (A07's lesson, applied):

```python
con.execute(f"""
-- grain: one row = one delivered version of an order — a corrected order is here twice
CREATE OR REPLACE TABLE core.orders_log AS {staged_select(DATES[0])} LIMIT 0
""")

t0 = time.perf_counter()
for d in DATES:
    con.execute(f"DELETE FROM core.orders_log WHERE _data_date = DATE '{d}'")
    con.execute(f"INSERT INTO core.orders_log {staged_select(d)}")
print(f"append: {time.perf_counter() - t0:.1f}s")
```

The log must hold **exactly** the manifest sum: 577,673 rows. Nothing dropped, nothing
deduped — an append log is a faithful copy of what arrived. Check there are 0 NULL
`order_date`s, and note the wall-clock (roughly 3 s on the baseline machine).

**Accept when:** `count(*)` = 577,673 = manifest sum, and rerunning one day changes nothing.

### Task 3 — Strategy A continued: dedupe-on-read view (small)

The log has multiple versions of corrected orders. The view picks each order's winner:

```python
con.execute("""
-- grain: one row = one order — its newest version
CREATE OR REPLACE VIEW core.orders_latest AS
SELECT * EXCLUDE (rn) FROM (
  SELECT *,
         row_number() OVER (PARTITION BY order_id
                            ORDER BY updated_at DESC, _data_date ASC) AS rn
  FROM core.orders_log
) WHERE rn = 1
""")
```

Read it aloud: number the versions of each order, newest `updated_at` first, keep row 1.
`EXCLUDE (rn)` drops the helper column from `SELECT *`. The second sort key is a
**tie-breaker**: if two versions ever carried the same `updated_at`, "earliest file wins"
makes the choice deterministic — and, not by accident, it is exactly the choice `MERGE` will
make in Task 4 (its `>` guard refuses an equal timestamp). Not hypothetical: at full scale
this window contains exactly one real tie — order `1520054262`, same `updated_at` in two
files with two different totals. Without the tie-break, two "correct" strategies can legally
disagree by one row, and you would chase that row for an hour.

Notice what the two DDL comments just said (§7.7, the grain rule you have kept since A03): the log
and the view describe the same orders at two different grains — one row = one *delivered version*
there, one row = one *order* here — which is exactly why they are two objects and not one, and why
the same window is 577,673 rows in the first and 569,639 in the second.

Now measure the phenomenon properly across all 10 files:

```sql
SELECT count(*) FROM (
  SELECT order_id FROM core.orders_log
  GROUP BY 1 HAVING count(DISTINCT _data_date) > 1);
```

Expect **7,684** order_ids seen in more than one file — about **1.35%** of the **569,639**
distinct orders, and just under the manifests' 7,750 late rows (some orders were corrected
twice). The view returns 569,639 rows. Time a `count(*)` on the view versus on the log: the
dedupe costs real work on every single read. That is Strategy A's tax.

**Accept when:** view count = 569,639; multi-file percentage computed and journaled.

### Task 4 — Strategy B: upsert with MERGE (small)

Two rules make upserts correct. **Rule 1: one row per key in the source.** The feed has
in-file duplicates (284 in this window), and if the source has two rows for one order_id,
DuckDB's MERGE does *not* error — it silently applies an arbitrary one. So each day gets
deduped in staging first. **Rule 2: guard the update.** Only overwrite when the incoming
version is strictly newer, or a stale correction will clobber a newer row.

```python
con.execute("""
-- grain: one row = one order — its newest version, same grain as core.orders_latest
CREATE OR REPLACE TABLE core.orders_upsert AS SELECT * FROM core.orders_log LIMIT 0
""")

t0 = time.perf_counter()
written = 0
for d in DATES:
    con.execute(f"""
      CREATE OR REPLACE TEMP TABLE stg_day AS
      SELECT * EXCLUDE (rn) FROM (
        SELECT *, row_number() OVER (PARTITION BY order_id
                                     ORDER BY updated_at DESC) AS rn
        FROM ({staged_select(d)})
      ) WHERE rn = 1
    """)
    written += con.execute("""
      MERGE INTO core.orders_upsert t USING stg_day s ON t.order_id = s.order_id
      WHEN MATCHED AND s.updated_at > t.updated_at THEN UPDATE
      WHEN NOT MATCHED THEN INSERT
    """).fetchone()[0]
print(f"upsert: {time.perf_counter() - t0:.1f}s, rows written {written}")
```

(`WHEN MATCHED … THEN UPDATE` with no `SET` list updates all columns by name; same for the
bare `INSERT`.) Expect **575,494** rows written and a final count of **569,639**. Do the
arithmetic in your journal: 577,673 arrived, 284 collapsed as in-file dups — so roughly 1,900
source rows produced *no* write. Those are stale corrections: versions arriving in a later
file with an *older* `updated_at` than what the table already held. The guard refused them.
Yes, this feed really does that; you will meet one face to face in Task 7.

**Accept when:** final count = 569,639 = the view's count; you can explain where the ~1,900
non-writes went; rerunning one day's MERGE reports 0 rows written.

### Task 5 — Strategy C: partition refresh — delete+insert by date (small)

Loading file D recomputes partitions [D−7, D] from the log. Delete the window, re-insert the
winners, atomically (one transaction — a crash between the two statements must not leave a
hole; that was A07):

```python
con.execute("""
-- grain: one row = one order — its newest version as of the last day refreshed
CREATE OR REPLACE TABLE core.orders_refresh AS SELECT * FROM core.orders_log LIMIT 0
""")

t0 = time.perf_counter()
deleted = written = 0
for d in DATES:
    lo = max(START, d - dt.timedelta(days=7))
    con.execute("BEGIN")
    deleted += con.execute(
        "DELETE FROM core.orders_refresh WHERE order_date BETWEEN ? AND ?",
        [lo, d]).fetchone()[0]
    written += con.execute(f"""
      INSERT INTO core.orders_refresh
      SELECT * EXCLUDE (rn) FROM (
        SELECT *, row_number() OVER (PARTITION BY order_id
                                     ORDER BY updated_at DESC, _data_date ASC) AS rn
        FROM core.orders_log
        WHERE _data_date <= DATE '{d}'
      ) WHERE rn = 1 AND order_date BETWEEN DATE '{lo}' AND DATE '{d}'
    """).fetchone()[0]
    con.execute("COMMIT")
print(f"refresh: {time.perf_counter() - t0:.1f}s, deleted {deleted}, written {written}")
```

Two details in that insert repay careful reading. `_data_date <= D` is an **as-of** condition:
your log already holds all 10 files, but on day D the pipeline had only seen files up to D —
the filter replays history honestly (in a live pipeline it holds automatically). And the shape
matters: dedupe over the **whole** as-of log first, *then* keep winners inside the window —
**choose winners globally, place them locally**. The tempting "optimization" of filtering to
the window before deduping is subtly wrong: an order's true winner can live *outside* the
window (an impossible-timestamp correction whose `order_date` fell back to its file date, for
example), and window-local dedupe would then resurrect a stale version. The nasty part: at
small scale both shapes agree perfectly — at full scale the wrong shape ends with 7 duplicated
orders. Develop on small, but never certify on small.

Expect roughly **2.98M rows written and 2.41M deleted** — you received 577k rows and rewrote
five times that. That asymmetry *is* Strategy C: pay in bulk I/O, get simplicity and
file-compatibility back. One caveat to journal: re-running the *newest* day is perfectly
idempotent, but re-running an *older* day D rewinds [D−7, D] to its as-of-D state — corrections
that arrived after D are removed until you replay D+1 onward. A refresh rerun is a mini
backfill: restart from D and roll forward (A11 builds the runner that does exactly this).

**Accept when:** final count = 569,639; delete/write volumes journaled with one sentence on
why they dwarf the arrival volume.

### Task 6 — The reconciliation — and why counts are not enough (small)

Three implementations, one truth. Counts first, then **content**, both directions:

```sql
SELECT
  (SELECT count(*) FROM core.orders_latest)  AS append_view,
  (SELECT count(*) FROM core.orders_upsert)  AS upsert,
  (SELECT count(*) FROM core.orders_refresh) AS refresh,
  (SELECT count(*) FROM (SELECT * FROM core.orders_latest
                         EXCEPT SELECT * FROM core.orders_upsert)) AS av_minus_up
  -- …add the other three diffs yourself: up−av, av−rf, rf−av
;
```

All three counts must be 569,639 and all four diffs 0. Now earn the lesson: copy your Task 5
loop into a scratch table `core.orders_refresh_bad`, but use the naive window `[D, D]` —
delete only day D, insert only `order_date = D`. Run it and reconcile against the view. On the
baseline data the count comes out **569,640 — off by one**, a number nobody would ever notice.
The content diff says **5,824 rows are wrong**: every order whose current version is a
correction is stuck at its stale original. Counting is not reconciliation; always diff
content. Drop the `_bad` table when you're done.

**Accept when:** the three-way reconciliation is all zeros, and your journal states what the
wrong-window run got wrong and why the count barely moved.

### Task 7 — Trace one correction end-to-end (small)

Numbers convince engineers; a single traced row convinces everyone else. Find a
much-corrected order:

```sql
SELECT order_id, count(*) AS versions, count(DISTINCT _data_date) AS files
FROM core.orders_log
GROUP BY 1 HAVING count(DISTINCT _data_date) > 1
ORDER BY versions DESC, order_id LIMIT 3;
```

The top candidate on small scale is `1510000111` (id scheme: day-ordinal 151 = 2026-06-01,
sequence 111) — three versions across three files. Show its full history from the log,
ordered by `updated_at`, then write one query (UNION ALL over the three targets) showing its
final state in each strategy. Look closely at the history: the correction in file 06-02
carries an updated_at *earlier* than the original's — it lost, correctly, in all three
strategies. That is the Task 4 guard doing its job, and file order proving it means nothing.

**Accept when:** history and three-way final state printed; one journal paragraph tells this
order's story in plain business words (ordered, delivered, refunded — when did we learn what?).

### Task 8 — Full scale, and the decision (full)

Flip `SCALE = "full"` and rerun Tasks 2–6 (same 10-day window — 10 files, ~3.5 GB of CSV,
~11.6M rows; the wrong-window experiment is optional here). While it runs, watch Task Manager
and `<DATA_ROOT>/tmp`: the refresh loop rewrites millions of rows per day, and the write
asymmetry becomes something you can *feel* — on the baseline machine expect roughly 20 s for
the append loop and about a minute each for the upsert and refresh loops, with refresh writing
~60M rows to deliver ~11.6M. The dedupe view now costs about a second per read — Strategy A's
tax at scale. Record per-strategy wall-clock and rows written, then fill this table into your
journal:

| | rows written (small / full) | wall-clock (small / full) | read cost | history kept | works on immutable files | fits when… |
|---|---|---|---|---|---|---|
| append+view | 577,673 / 11,553,524 | | dedupe every read | full | yes (append new files) | immutable events, audit needs |
| upsert | 575,494 / 11,509,661 | | none | none | no | mutable entities in a DB |
| refresh | ~2.98M / ~59.6M | | none | none | **yes — the only updater files allow** | file lakes, bounded lateness |

Then decide, in writing: which strategy does *our* pipeline adopt for `core.orders`, and why?
(The lab's answer: keep the append log as the durable source, serve `core.orders` by partition
refresh from it — A07's transaction, now with the correct [D−7, D] window. A09 makes readers
safe during the rewrite; A15 shows dbt shipping this exact strategy as three lines of config.)

**Accept when:** the table is filled with your measured numbers, all three strategies agree at
full scale, and your journal has a defended recommendation.

## Definition of Done

- [ ] Manifest-derived expectation for the window computed (small: 577,673 / 7,750 / 284).
- [ ] `core.orders_log` equals the manifest row sum exactly, at both scales; 0 NULL `order_date`.
- [ ] `core.orders_latest` view exists with a deterministic tie-break; small count = 569,639.
- [ ] Upsert and refresh finish with identical counts, and all four `EXCEPT` diffs are 0 — at
      both scales.
- [ ] Rerun of a single day: append and MERGE change nothing (MERGE reports 0 writes); you can
      explain in one sentence why refresh needs a roll-forward instead.
- [ ] Wrong-window experiment run; journal explains why the count moved by 1 but 5,824 rows
      were wrong.
- [ ] One correction traced end-to-end with identical final state in all three targets.
- [ ] Comparison table + adoption recommendation in `journal.md` with full-scale timings.

## Verify yourself

```sql
-- 1. The window, per manifests (small): rows=577673, late=7750, dups=284
SELECT sum(rows), sum(late_rows), sum(dup_rows)
FROM read_json_auto('<DATA_ROOT>/small/raw/manifest/orders_*.json')
WHERE CAST(date AS DATE) BETWEEN DATE '2026-06-01' AND DATE '2026-06-10';

-- 2. Three-way agreement (small): 569639 | 569639 | 569639, diffs all 0
SELECT (SELECT count(*) FROM core.orders_latest),
       (SELECT count(*) FROM core.orders_upsert),
       (SELECT count(*) FROM core.orders_refresh);

-- 3. The traced order (small) — history in updated_at order:
SELECT _data_date, status, order_total, updated_at
FROM core.orders_log WHERE order_id = 1510000111 ORDER BY updated_at;
--  2026-06-02 | refunded  | 435.92 | 2026-06-02 00:10:35   <- correction that LOST
--  2026-06-01 | delivered |  84.48 | 2026-06-02 20:27:19   <- the original
--  2026-06-06 | refunded  | 230.65 | 2026-06-06 07:53:51   <- current winner
-- Final state in all three strategies: refunded / 230.65 / 2026-06-06 07:53:51
```

Full-scale checkpoints (from the full manifests, so exact): window sums are
**11,553,524 rows / 155,088 late / 5,773 dups**; the three-way agreement count is
**11,392,663**, with **153,678** order_ids (1.35%) seen in more than one file. Small-scale
wall-clocks are seconds (append ~3 s, upsert ~5 s, refresh ~5 s — CSV parsing dominates); at
full scale, roughly 20 s / 1 min / 1 min. If your full-scale log count disagrees with query 1
run against `<DATA_ROOT>/raw/manifest/`, a day failed to load — find it via
`GROUP BY _data_date` before touching anything else.

## Common beginner mistakes

- **Refreshing only `[D, D]`.** Late rows live in earlier partitions. Delete-window ≠
  insert-window ≠ contract allowance is the classic silent-staleness bug — you proved it
  yourself in Task 6, and the count only moved by 1. Its subtler cousin: deduping only the
  rows *inside* the window (Task 5) — winners are chosen globally or not at all.
- **Skipping the source dedupe before MERGE.** Duplicate keys in the source do not error in
  DuckDB — one version wins arbitrarily and the run still reports success.
- **Dropping the `s.updated_at > t.updated_at` guard.** "Matched means update" feels obvious
  and is wrong: on this window an unguarded MERGE ends with ~1,860 silently stale rows
  (last-file-wins instead of latest-version-wins). Only the content diff catches it.
- **A nullable partition key.** Rows with NULL `order_date` are invisible to
  `BETWEEN`-windows: never deleted, never re-inserted. `COALESCE` to the file date (and
  quarantine properly per A05). A07 coalesced to the `updated_at` date instead — also a valid
  non-NULL choice; today we use the file's date so a row's partition is knowable from the file
  alone. Either works; pick one and keep it.
- **DELETE and INSERT outside one transaction.** A crash in between leaves partitions empty,
  and readers mid-refresh see a hole. `BEGIN`/`COMMIT` costs one line (A09 goes further).
- **Reconciling by counts alone.** Two tables with equal counts can disagree on thousands of
  rows. `EXCEPT` in both directions, always.

## Stretch goals

1. **Out-of-order arrival.** Shuffle `DATES` and rerun Tasks 2–5. Which strategies still
   converge to the same final state, and why? (The MERGE guard earns its keep; for refresh,
   think about what its window must cover — then check whether it did.)
2. **Instant SCD2.** From the log alone, build `core.orders_history` with `valid_from` /
   `valid_to` / `is_current` columns using `lead(updated_at) OVER (PARTITION BY order_id
   ORDER BY updated_at)` — the append log makes "slowly changing dimension" history a query,
   not a pipeline.
3. **MERGE growth curve.** Time each day's MERGE separately at full scale and plot the trend
   as the target grows. Extrapolate to a year of data — and note your reasoning for A14,
   where you will learn to verify it with `EXPLAIN ANALYZE`.
4. **Audit the one untested promise.** The contract's `business_rules.status_transitions`
   clause says corrections only ever move an order's status *forward* — and it is the one
   contract block no check has exercised yet. Your append log is the perfect witness: order
   each order's versions by `updated_at` (`lag(status) OVER (PARTITION BY order_id ORDER BY
   updated_at)` over `core.orders_log`), map statuses to their lifecycle rank, and count
   backward moves. Expect 0; if you find any, that's an incident note to shopcore citing
   the clause. A per-day check like A05's cannot see this — only a cross-file view can,
   which is exactly why append logs earn their storage.
