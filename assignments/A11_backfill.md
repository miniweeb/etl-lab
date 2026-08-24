# A11 — Backfill: rebuild history
**Topic:** backfill · **Est. time:** 4–5h · **Difficulty:** ⭐⭐⭐
**Requires:** A10 done; the full 69-day raw history (month 1 + month 2, eras v1–v3) generated at **both** scales; your idempotent per-day pipeline from A07, upgraded in A10 to output the canonical schema; your `work/preflight.py` gate (A07); your `ops.etl_runs` ledger (A07) and `ops.alerts` table (A05).

## Why this matters

Every data team eventually has to reprocess months of history: a bug was found, the schema changed, a new column needs values for old dates. That job is called a backfill, and it is where pipelines go to die — old files hold surprises that yesterday's file never showed you. Today you rebuild all 69 days through one runner, hit a genuinely corrupt file from five weeks ago, and handle it like a professional: diagnose, quarantine, document, resume. You do not start over.

## Background

Three words that get mixed up. Let's pin them down:

- **Incremental load**: process only the newest slice (yesterday's file). Normal daily operation. Cheap.
- **Backfill**: run your normal pipeline over a range of *past* dates, to fill a gap — the pipeline is new, or it was down, or history was never loaded.
- **Restatement**: a backfill that *changes numbers people have already seen*, because logic or source data changed. Technically identical to a backfill; socially very different, because consumers must be warned before their dashboards move.

The most important idea today: **a backfill is not a special script**. It is your normal per-day pipeline, replayed over many dates. If you find yourself writing a separate "backfill version" of your logic, stop — now you have two pipelines that will drift apart. You did the hard work already: A07 made your per-day load idempotent (safe to run any number of times, same result). That is exactly the property that makes replay boring and safe. Today you only add a thin loop around it: a **runner**.

What does a good runner do?

1. **It plans before it acts.** A backfill touches weeks of data; you want to see the plan ("these 69 dates will run") before anything moves. That's a `--dry-run` flag.
2. **It remembers.** Your `ops.etl_runs` table (A07) records every attempt. The runner reads it to decide what still needs doing, so a crash at day 40 never means restarting from day 1.
3. **It continues on error.** The 69 days are independent jobs. One bad day should not kill the other 68 — but the failure must be loud in the end-of-run summary, never swallowed.
4. **It estimates cost.** Rows per second × rows remaining = time remaining. Grown-up teams predict a backfill's runtime (and cloud bill) before launching it.

One subtle trap, straight from your A06 contract: the feed has a **7-day late-data allowance** (you measured this in A08). File D carries corrections for days D−7…D. So if you backfill only a sub-range, say June 10–20, corrections to June 20 live in files up to June 27 — outside your range. A *full-history* replay (what you do today) side-steps this; a *partial* backfill must extend its **file** range 7 days past the **date** range it wants correct. Keep that in your journal; it bites real teams constantly.

Finally, expect today's run to go red on `2026-07-22`. That is deliberate. Somewhere in week 8, shopcore — the e-commerce platform that produces this feed — truncated some lines mid-write during export. Your strict CSV reader — correctly — refuses to guess. You will meet the error, find the root cause (which is *not* what the error message says), route the unreadable lines to quarantine with reasons, write a small postmortem, and resume. And because the contract's structural-quality clause tolerates **zero** malformed lines, part of that postmortem is a note addressed to shopcore: this is a producer incident, not warehouse dirt to mop up quietly. This loop — diagnose, contain, document, resume — is the actual skill.

## Setup

- Both scales today: develop and debug the runner on **small**, then one full-scale run for the record.
- Have open: your repo (`work/`), your journal, Task Manager (Performance tab), and a terminal at the repo root.
- Connection settings stay on the lab convention (the runner sets them):

```sql
SET memory_limit = '8GB'; SET threads = 8;
SET temp_directory = '<DATA_ROOT>/tmp';
```

- Run scripts from the **repo root** with `python -m work.runner ...` so both `lib` and `work` import cleanly.
- Git ritual (§7.4): `git switch -c a11-backfill`, commit after each numbered task (e.g. `git commit -m "A11 task 2: backfill runner"`), merge to `main` when the DoD is green — `guides/git_basics.md` if any of that sounds new.
- Do **not** pre-fix anything for 2026-07-22. Walk into the wall first; that's the lesson.

## Tasks

### Task 1 — Know the target before you fire (small, 10 min)

A backfill starts with a number: how many rows *should* history contain? The manifests know. Read them all with one glob:

```sql
SELECT count(*)          AS files,
       sum(rows)         AS total_rows,
       sum(corrupt_rows) AS corrupt_lines
FROM read_json_auto('<DATA_ROOT>/small/raw/manifest/orders_*.json');
```

At small scale (generator 1.0) this is deterministic: **69 files, 4,116,840 rows, 151 corrupt lines**. Note that `corrupt_lines` is not zero — the manifests have been warning you about 2026-07-22 all along:

```sql
SELECT file, era, rows, corrupt_rows
FROM read_json_auto('<DATA_ROOT>/small/raw/manifest/orders_*.json')
WHERE corrupt_rows > 0;
```

**Accept when:** you have both query results in your journal, plus the same totals for the full scale (roughly 82M rows; your own manifest sum is the truth).

### Task 2 — Build the runner (small, 60–90 min)

Create `work/runner.py`. This is given nearly complete because the CLI pattern is new; wiring in *your* pipeline is your part — and it is the biggest assembly step in the lab, because nothing named `work/pipeline.py` exists yet. Build it now: one module exposing `load_day(con, day, scale) -> int`, stitched from code you already wrote (this stitch-into-a-module move is the §7.4 composable-code ladder doing its job — `guides/composable_code.md` if the pattern feels new). The adapter checklist:

- **Signature.** A07's was `load_day(con, scale, day, chaos=0.0)` — rename the module, reorder the arguments, drop `chaos` (or default it to 0), accept the runner's `run_id` so the §7.2 `_run_id` lineage column gets stamped (see Task 5), and return the day's raw row count so the runner can record it in `ops.etl_runs`.
- **Era dispatch.** Route every date through your A10 `era_of(day)` and per-era schemas, so month-2 files load as canonical rows instead of crashing.
- **Retire the v1 target table.** `core.orders` still has the 13-column v1 shape A07 created — skip this step and day 1 dies with `Binder Error: table orders has 13 columns but 18 values were supplied`. It is derived data (raw + code = truth, as A07 taught), so `DROP TABLE core.orders` once and let your pipeline's `CREATE TABLE IF NOT EXISTS` recreate it in the A10 canonical schema plus the two §7.2 lineage columns (`_data_date`, `_run_id` — that's the whole list; everything else about the load lives on the ledger).
- **Keep the A07 transaction.** Stage → validate → `BEGIN; DELETE day; INSERT; COMMIT` stays intact — the runner's resume logic is only trustworthy because a crashed day leaves nothing behind.

Fifteen lines of scaffold so the assembly has walls to lean on. Nothing in it is new code — each numbered comment names the exact listing its block lifts from, and the checklist above tells you what to adapt while you paste:

```python
"""work/pipeline.py - one day, raw CSV -> canonical core.orders, runner-callable."""
import datetime as dt

from lib.labpaths import manifest_dir, raw_orders_dir
from work.a10_canonical import CANON, era_of, open_day   # A10 Tasks 5-6


def load_day(con, day: dt.date, scale: str, run_id=None) -> int:
    # 1. ensure core.orders — CREATE TABLE IF NOT EXISTS, canonical columns
    #    (A10 Task 4) + the two §7.2 lineage columns (A07 Task 3, ensure_tables)
    # 2. era dispatch: open_day(con, day, era_of(day), raw_orders_dir(scale)),
    #    then TEMP stg_day from CANON (A10 Tasks 5-6 — this replaces the
    #    v1-only step 2 stage in A07 Task 3)
    # 3. validate: count(*) FROM aligned == the manifest's `rows` (A07 Task 3,
    #    step 3 — count aligned, not stg_day: CANON drops unparseable-ts rows)
    # 4. publish: BEGIN; DELETE WHERE _data_date; INSERT stg_day + lineage;
    #    COMMIT (A07 Task 3, step 4 — *minus* its ledger UPDATE: ops.etl_runs
    #    belongs to the runner now)
    ...
    return raw_n   # step 3's raw count — what the runner records in ops.etl_runs
```

And the runner that drives it — `work/runner.py`:

```python
"""Backfill runner: replay the per-day pipeline over a date range."""
from __future__ import annotations

import argparse
import datetime as dt
import time

import duckdb

from lib.labpaths import tmp_dir, warehouse_path
from work.pipeline import load_day        # <- your A07/A10 per-day pipeline
from work.preflight import run_checks     # <- your A07 fail-fast gate (adapt the
                                          #    name if yours differs)

STEP = "day"                              # runner-level step name in ops.etl_runs


def date_range(start: dt.date, end: dt.date) -> list[dt.date]:
    return [start + dt.timedelta(days=i) for i in range((end - start).days + 1)]


def latest_status(con, d: dt.date) -> str | None:
    row = con.execute("""
        SELECT status FROM ops.etl_runs
        WHERE run_date = ? AND step = ?
        ORDER BY started_at DESC LIMIT 1
    """, [d, STEP]).fetchone()
    return row[0] if row else None


def build_plan(con, dates, only_failed: bool, force: bool):
    plan = []                             # (date, "run"|"skip", reason)
    for d in dates:
        st = latest_status(con, d)
        if force:
            plan.append((d, "run", f"--force (last status: {st})"))
        elif only_failed:
            if st == "failed":
                plan.append((d, "run", "last run failed"))
            else:
                plan.append((d, "skip", f"last status is {st}, not failed"))
        elif st == "success":
            plan.append((d, "skip", "already green"))
        else:
            plan.append((d, "run", "never ran" if st is None else f"last status: {st}"))
    return plan


def main() -> None:
    p = argparse.ArgumentParser(description="Backfill a date range, one day at a time.")
    p.add_argument("--scale", choices=["small", "full"], required=True)
    p.add_argument("--start", required=True, type=dt.date.fromisoformat)
    p.add_argument("--end", required=True, type=dt.date.fromisoformat)
    p.add_argument("--only-failed", action="store_true",
                   help="rerun only dates whose last run failed")
    p.add_argument("--force", action="store_true",
                   help="rerun every date, even green ones (restatement)")
    p.add_argument("--dry-run", action="store_true",
                   help="print the plan, touch nothing")
    args = p.parse_args()

    con = duckdb.connect(str(warehouse_path(args.scale)))
    con.execute(f"""SET memory_limit='8GB'; SET threads=8;
                    SET temp_directory='{tmp_dir(args.scale).as_posix()}';""")
    con.execute("""
        CREATE SCHEMA IF NOT EXISTS ops;
        CREATE SEQUENCE IF NOT EXISTS ops.run_id_seq;
        CREATE TABLE IF NOT EXISTS ops.etl_runs (
            run_id BIGINT PRIMARY KEY, run_date DATE, step VARCHAR, status VARCHAR,
            started_at TIMESTAMP, finished_at TIMESTAMP,
            rows BIGINT, source_file VARCHAR, error VARCHAR);
    """)  # same shape and column names as your A07 table — adapt if yours differs

    plan = build_plan(con, date_range(args.start, args.end),
                      args.only_failed, args.force)
    for d, action, reason in plan:
        print(f"{d}  {action:4}  {reason}")
    to_run = [d for d, action, _ in plan if action == "run"]
    print(f"\n{len(to_run)} of {len(plan)} dates selected.")
    if args.dry_run or not to_run:
        return

    t0 = time.time()
    for d in to_run:
        run_id = con.execute("SELECT nextval('ops.run_id_seq')").fetchone()[0]
        con.execute("""INSERT INTO ops.etl_runs           -- source_file: §7.3, the
                       VALUES (?, ?, ?, 'running',        --  file name lives HERE,
                               now(), NULL, NULL, ?, NULL)""",   # not on 1.2M rows
                    [run_id, d, STEP, f"orders_{d}.csv"])
        day_t0 = time.time()
        try:
            if not run_checks(args.scale, d):         # §7.4: every check green,
                raise RuntimeError(                   #  or the day never starts
                    "preflight failed - fix the FAIL line above, then --only-failed")
            n = load_day(con, d, scale=args.scale, run_id=run_id)
            con.execute("""UPDATE ops.etl_runs SET status='success',
                           finished_at=now(), rows=? WHERE run_id = ?""", [n, run_id])
            print(f"{d}  OK    {n:>12,} rows   {time.time() - day_t0:6.1f} s")
        except Exception as e:            # continue-on-error: log it, keep going
            try:
                con.execute("ROLLBACK")   # load_day may have died inside its BEGIN;
            except duckdb.Error:          #  a dead transaction would kill the ledger
                pass                      #  UPDATE too (A07's error-path rule)
            con.execute("""UPDATE ops.etl_runs SET status='failed',
                           finished_at=now(), error=? WHERE run_id = ?""",
                        [str(e)[:500], run_id])
            print(f"{d}  FAIL  {type(e).__name__}: {str(e)[:120]}")

    print(f"\nWall clock: {time.time() - t0:,.1f} s")
    print(con.execute("""
        WITH latest AS (
            SELECT run_date, status, rows,
                   row_number() OVER (PARTITION BY run_date
                                      ORDER BY started_at DESC) AS rn
            FROM ops.etl_runs
            WHERE step = ? AND run_date BETWEEN ? AND ?)
        SELECT status, count(*) AS days, sum(rows) AS rows_loaded
        FROM latest WHERE rn = 1
        GROUP BY status ORDER BY status
    """, [STEP, args.start, args.end]).fetchdf().to_string(index=False))


if __name__ == "__main__":
    main()
```

One new line in the loop deserves its own paragraph: the runner calls your A07 `work/preflight.py` before **every** day, not once at startup — each date brings its own source file and manifest to check (preflight #4), and disks fill and locks appear mid-backfill, not before it. The point is *when* it fails: a permission or disk problem caught by preflight costs nothing at hour 0, while the same problem discovered at row 40 million costs you the evening (§7.4). And because a failed preflight is recorded in `ops.etl_runs` like any other failure, `--only-failed` picks that day up as soon as you fix whatever the FAIL line names.

One column in that `INSERT` is worth a second look, because it is the other half of a decision made back in A03. `ops.etl_runs.source_file` (§7.3) is where the file name is recorded — **once per attempt**, by the runner, at the moment the attempt starts. Your data rows never carry it: §7.2 gives every staging and core row exactly two lineage columns, `_data_date` and `_run_id`, and everything else about the load is one join away (`JOIN ops.etl_runs r ON r.run_id = o._run_id` hands you `r.source_file`, `r.finished_at`, `r.status`, `r.rows`, `r.error`). Today is where that pays: the incident write-up in Task 9 needs the file name, the load time and the row count for one bad day, and it reads them from 69 ledger rows instead of 4 million data rows. Note the runner fills `source_file` on the `INSERT`, not the `UPDATE` — a failed day must still say which file it was trying to read.

Read `build_plan` until the three modes are obvious: default = skip green days (resume), `--only-failed` = retry known failures only, `--force` = rebuild everything (that's a restatement). One state to know before you meet it: kill the runner mid-day (Ctrl-C, power cut) and that day's ledger row stays at `running` — the default resume picks it up (anything not green runs again), but `--only-failed` does not, because it selects exactly the status its name says. Then test the safety catch:

```powershell
python -m work.runner --scale small --start 2026-06-01 --end 2026-08-08 --dry-run
```

Finally, smoke-test the assembly itself — two minutes now versus discovering a bad stitch forty days into Task 3. Call `load_day` directly, one v1 day and one v2 day, in a Python session at the repo root:

```python
import datetime as dt, duckdb
from lib.labpaths import tmp_dir, warehouse_path
from work.pipeline import load_day

con = duckdb.connect(str(warehouse_path("small")))
con.execute(f"SET memory_limit='8GB'; SET threads=8; "
            f"SET temp_directory='{tmp_dir('small').as_posix()}';")
print(load_day(con, dt.date(2026, 6, 2), scale="small"))    # v1 day
print(load_day(con, dt.date(2026, 7, 20), scale="small"))   # v2 day
```

Each number must equal that manifest's `rows` — one equality that proves the whole stitch: per-era `columns=` read, aligner, canonicalizer, atomic publish into the canonical `core.orders`. If 06-02 dies with the 13-versus-18 Binder Error instead, you skipped the checklist's third bullet. And notice what did *not* happen: not one new `ops.etl_runs` row — a direct call stamps `_run_id` NULL and leaves the ledger alone (the runner owns it), which is why Task 3 will still select all 69 dates and simply replace today's two idempotently.

**Accept when:** the smoke test returns **59,024** (2026-06-02) and **59,743** (2026-07-20), each exactly its manifest's `rows`; the dry run prints a 69-line plan and says `69 of 69 dates selected`; and `ops.etl_runs` has no new rows after either check — a dry run that writes anything is a bug, and the ledger is the runner's to write, not `load_day`'s.

### Task 3 — Full-history backfill, first attempt (small, 15 min)

Run it for real:

```powershell
python -m work.runner --scale small --start 2026-06-01 --end 2026-08-08
```

While it runs, watch Task Manager: CPU spikes while each day's SQL executes, then dips between days. The machine is far from saturated — remember this picture; A12 exists because of it. Each day opens with your preflight's PASS lines scrolling past — routine and boring, which is exactly what you want from them (Task 4 shows the payoff). When the runner reaches **2026-07-22 it goes red** — and keeps going. That's the design: one bad day, 68 good ones, and a summary at the end that shows exactly that (`failed 1 / success 68`).

**Accept when:** the summary shows 68 success + 1 failed, and per-day timings and total wall clock are in your journal.

### Task 4 — Diagnose the failure, layer by layer (small, 20 min)

Rule one of incidents: read what actually happened, not what you assume. Rule two: check in order, not in inspiration order. `guides/troubleshooting.md` exists for exactly this moment — a top-down checklist: rule zero, read the error; **layer 1** environment; **layer 2** storage & permissions; **layer 3** the data itself. Open it next to your terminal; 2026-07-22 is about to become its worked example, and the habit you practice here — walk the layers, cross each off with evidence — is worth more than today's answer.

**Rule zero — read the error.** The error is saved where it belongs — in your state table:

```sql
SELECT run_date, error FROM ops.etl_runs
WHERE status = 'failed' ORDER BY started_at DESC LIMIT 1;
```

It says something like *"Error when sniffing file … It was not possible to automatically detect the CSV parsing dialect"*. Copy it into your journal verbatim — then refuse to act on it. Misleading! The dialect (comma, quotes) hasn't changed. Error messages point at the symptom nearest to the code that raised them, not at the root cause; rule zero's job is to collect the words, not to believe them.

**Layers 1 and 2 — environment, storage & permissions.** Before blaming the data, the checklist rules out the environment (right venv? data root set?) and the storage (disk full? permissions? temp on the wrong volume? DB file locked?). Today you cross both off in seconds, with evidence instead of optimism: your runner's preflight ran green for this exact date moments before the crash — data root writable, free space fine, temp on the data volume, warehouse openable, schemas in place. That is the quiet payoff of Task 2's per-day preflight call: mid-incident, it hands you two pre-cleared layers. (On a networked warehouse this is where you would probe DNS, port, TLS, credentials and GRANTs instead — the guide's second column.)

**Layer 3 — the data itself.** Three questions, straight down the checklist. *Does the file exist, at a plausible size?*

```powershell
Get-ChildItem "$env:ETL_LAB_DATA\small\raw\orders\orders_2026-07-2*.csv" | Select-Object Name, Length
```

It's there, ~19 MB — same ballpark as its neighbors, so this is not a failed or truncated-to-nothing delivery. *What does the manifest say about this delivery?* You already know: Task 1's query has been reporting `corrupt_rows: 151` for this file all along — the producer's own paperwork confesses (remember that for the postmortem's detection-gap section). *Is the header intact?*

```powershell
Get-Content "$env:ETL_LAB_DATA\small\raw\orders\orders_2026-07-22.csv" -TotalCount 2
```

A clean 13-column v2 header and a normal first row. Delivered file, plausible size, intact header — but the producer's own manifest admits 151 broken lines. Conclusion: **structural corruption**. Some *lines* are broken; not the file, not your settings, not your code. And for exactly that situation the tool is `store_rejects=true`: parse what is parseable, file the rest as evidence in a reject table. Confirm it in a scratch session (`duckdb.connect()`, in-memory):

```sql
CREATE TABLE probe AS
SELECT * FROM read_csv('<DATA_ROOT>/small/raw/orders/orders_2026-07-22.csv',
    header=true, store_rejects=true,
    columns={'order_id':'BIGINT','customer_id':'BIGINT','store_id':'INTEGER',
             'order_ts':'VARCHAR','updated_at':'TIMESTAMP','status':'VARCHAR',
             'payment_method':'VARCHAR','order_total':'VARCHAR',
             'items':'VARCHAR','meta':'VARCHAR','currency':'VARCHAR',
             'discount_amount':'VARCHAR','channel':'VARCHAR'});   -- era v2

SELECT count(*) FROM probe;                       -- 58,400 at small scale
SELECT count(DISTINCT line) FROM reject_errors;   -- 151 = manifest corrupt_rows
SELECT error_type, count(*) AS records, count(DISTINCT line) AS lines
FROM reject_errors GROUP BY 1 ORDER BY 2 DESC;
```

Two nuances (both verified in this lab): rejects only appear on a **materializing** scan — a bare `count(*)` probe is optimized past parsing and leaves the reject table empty, so use `CREATE TABLE … AS`. And a truncated line yields **one reject record per missing column** — at small scale you'll see ~1,651 records but only 151 distinct `line`s: 150 truncated lines plus exactly 1 unclosed-quote line. Always count `DISTINCT line`.

And with that, rule zero's message finally makes sense: among the 151 is exactly **one** line with an unclosed quote, and that single line poisons DuckDB's dialect sniffer for the whole file — which is why the error talked about dialect detection while the disease was truncated lines. The walk stops at the layer where the evidence converges; today that was layer 3.

**Accept when:** you can say, in one sentence, why the error message mentioned "sniffing" when the root cause was truncated lines; your `DISTINCT line` count equals the manifest's `corrupt_rows`; and your journal shows rule zero and layers 1–3 crossed off in order, each with its one line of evidence.

### Task 5 — Fix, quarantine, resume (small, 30 min)

Now graft the fix into your `load_day`. Three pieces: read tolerantly, save the evidence, and verify you got everything the manifest promised. Bad lines go to quarantine — never to `/dev/null`, and never fixed by hand-editing the raw CSV (raw files are immutable evidence; also, the generator would just recreate them).

```python
from lib.labpaths import manifest_dir, quarantine_dir, raw_orders_dir

def load_day(con, d, scale="small", run_id=None):
    f = raw_orders_dir(scale) / f"orders_{d}.csv"
    rid = ("CAST(NULL AS BIGINT)"                     # ad-hoc runs stay NULL — but
           if run_id is None else int(run_id))        #  typed, so _run_id is BIGINT
                                                      #  either way (§7.2)
    con.execute(f"""
        CREATE OR REPLACE TABLE staging.shopcore_orders_day_raw AS
        SELECT *,
               DATE '{d}' AS _data_date,        -- §7.2 lineage, stamped at ingest:
               {rid}      AS _run_id            --  exactly two columns, no more
        FROM read_csv('{f.as_posix()}', header=true,
                      columns={era_columns(d)},      -- your A10 per-era schemas
                      store_rejects=true)
    """)

    # unreadable lines -> quarantine, with reasons
    n_rejects = con.execute("SELECT count(*) FROM reject_errors").fetchone()[0]
    if n_rejects:
        out = quarantine_dir(scale) / "orders_badlines" / f"file_date={d}"
        out.mkdir(parents=True, exist_ok=True)
        con.execute(f"""
            COPY (SELECT DATE '{d}' AS _data_date, {rid} AS _run_id,
                         now() AS _rejected_at,        -- §7.2: the same two lineage
                         line, column_name,            --  columns quarantine rows get,
                         error_type, error_message,    --  plus the payload-specific
                         csv_line                      --  _rejected_at and the reason
                  FROM reject_errors)
            TO '{(out / 'rejects.parquet').as_posix()}' (FORMAT parquet)
        """)
        con.execute("DELETE FROM reject_errors")   # it accumulates per connection —
                                                   # clear it or day N+1 re-copies day N's rejects
    # reconciliation guard (A05): silent short-loads become loud failures
    mf = manifest_dir(scale) / f"orders_{d}.json"
    expected = con.execute(
        f"SELECT rows FROM read_json_auto('{mf.as_posix()}')").fetchone()[0]
    loaded = con.execute(
        "SELECT count(*) FROM staging.shopcore_orders_day_raw").fetchone()[0]
    if loaded != expected:
        raise RuntimeError(f"{d}: loaded {loaded:,} rows, manifest says {expected:,}")

    # ... the rest of your A07/A10 pipeline: clean -> validate -> atomic write
    #     to warehouse core (carry both _ columns through — §7.2) ...
    return loaded
```

(`era_columns(d)` is not something A10 shipped — it's a one-liner you write from what A10 *did* ship: `{'v1': V1_COLS, 'v2': V2_COLS, 'v3': V3_COLS}[era_of(d)]`.)

Two details worth a pause. The staging table follows the §7.1 shared-namespace rule — `staging.shopcore_orders_day_raw` says *whose* feed this is, because a warehouse full of bare `day_raw` tables is unreadable the day a second source lands. And the §7.2 lineage columns ride along from ingest — **two of them, `_data_date` and `_run_id`, and that is the whole list**, on the quarantine rows exactly as on the core rows.

Which means the quarantine parquet does *not* store the file name — and does not need to. `_run_id` ties every rejected line to the `ops.etl_runs` attempt that met it, so the forensic question "which file was this, read when, by which run, and how did that run end?" is one join away:

```sql
SELECT r.source_file, r.started_at, r.status, r.rows,
       count(DISTINCT q.line) AS bad_lines
FROM read_parquet('<DATA_ROOT>/small/quarantine/orders_badlines/*/*.parquet') q
JOIN ops.etl_runs r ON r.run_id = q._run_id
GROUP BY 1, 2, 3, 4;
-- orders_2026-07-22.csv | <resume timestamp> | success | 58400 | 151
```

That is A07's traceability demo pointed at rejects instead of orders, and it is the same ledger Task 7 turns into incident evidence. Two caveats to keep straight. If you produced these rejects from an ad-hoc `load_day` call, `_run_id` is NULL and the join returns nothing — correct, and informative: no recorded run owns those rows, so rerun the day through the runner. And even without the ledger you are not lost, because in this feed `_data_date` *is* the file name (`orders_<_data_date>.csv`, one file per business date per the contract's `path_pattern`) — the ledger is simply where you read it as a recorded fact instead of reconstructing it.

One scope decision before you rerun anything, because it protects your lake: **`load_day` targets the warehouse core only — leave your A10 lake `COPY … APPEND` out of it.** That `APPEND` is not idempotent, so a 69-day replay through it would silently double the 68 days A10 already wrote, and nothing before A15's exact source count would catch it. Your canonical lake is missing exactly one day — 2026-07-22, the day A10 deferred to you. Repair it once, by hand: after the resume below turns the day green, `COPY` its canonical staging output into your versioned `lake/orders_v=<ts>/` dir with the same one-day `APPEND` your A10 loop used (adjust the dir name to yours). At small scale that appends **58,387** rows — the file's 58,400 minus the 13 unparseable timestamps your cleaner drops — and closes the 69-day lake for good. Verify yourself checks the total.

Then resume — only the red day, not the other 68:

```powershell
python -m work.runner --scale small --start 2026-06-01 --end 2026-08-08 --only-failed
```

**Accept when:** exactly 1 of 69 dates is selected, it goes green loading 58,400 rows (small), the quarantine parquet exists and is queryable, and 07-22's canonical rows are appended to the lake exactly once (count check in Verify yourself).

### Task 6 — The real thing: full scale (full, ~30–90 min wall clock, mostly waiting)

Dry-run first — always, at full scale. Then launch, and go make coffee; sequential is slow, and *measuring how slow* is today's deliverable (A12's job is to beat it by 3× or more).

```powershell
python -m work.runner --scale full --start 2026-06-01 --end 2026-08-08 --dry-run
python -m work.runner --scale full --start 2026-06-01 --end 2026-08-08
```

Observe while it runs, and journal all of it: per-day seconds from the runner output; the two spike days (2026-06-19, 2026-07-11) taking roughly 3× a normal day; 2026-07-22 quarantining roughly 3,000 bad lines this time (exact number: that manifest's `corrupt_rows`); Task Manager showing the busy-burst-then-dip pattern; any spill files appearing in `<DATA_ROOT>/tmp`. On the baseline machine expect very roughly 30–90 minutes total depending on how heavy your pipeline is.

One more step while the coffee is warm: the full-scale lake is missing 2026-07-22 exactly like the small one was. Once that day is green, repeat Task 5's one-day `COPY … APPEND` of its canonical output into your full-scale `orders_v=<ts>/` dir — once.

**Accept when:** the end summary says 69 success, both lakes now hold all 69 days, and your journal records **the total wall clock as the official A12 baseline**.

### Task 7 — Ops wiring: the freshness gate and the run ledger (small, 25 min)

Two pieces of ops plumbing turn today's one-off heroics into a system that watches itself.

**(a) The freshness gate.** The contract's `freshness_sla` is precise: the file for business date D is complete by 02:00 UTC on D+1, the warehouse load must be able to start by 04:00 UTC — a file missing at 04:00 UTC is a *producer incident*. Here's the catch: your runner only notices a missing file if someone happens to run it. A delivery that never arrives triggers no error anywhere, because the job that would have failed never starts. An SLA needs a watchdog that runs on the clock, not on demand. Create `work/freshness_gate.py` — a new file, not an edit to your A05 `work/freshness_check.py`, which keeps its job:

```python
"""Freshness gate: has shopcore delivered every file the contract promises?

contracts/shopcore_orders_daily.contract.yaml, delivery.freshness_sla:
file for business date D is complete by 02:00 UTC on D+1; the warehouse load
must be able to start by 04:00 UTC; missing at 04:00 UTC = producer incident.
"""
from __future__ import annotations

import argparse
import datetime as dt

import duckdb

from lib.labpaths import raw_orders_dir, warehouse_path

FEED_START = dt.date(2026, 6, 1)          # contract effective_from
SLA_HOUR_UTC = 4                          # the 04:00 UTC line in freshness_sla


def due_through(as_of: dt.datetime) -> dt.date:
    """Latest business date whose file must already exist at time `as_of`."""
    lag = 1 if as_of.hour >= SLA_HOUR_UTC else 2   # D's file is due on D+1
    return as_of.date() - dt.timedelta(days=lag)


def main() -> None:
    p = argparse.ArgumentParser(description="Contract freshness gate (§7.3).")
    p.add_argument("--scale", choices=["small", "full"], required=True)
    p.add_argument("--as-of", required=True, type=dt.datetime.fromisoformat,
                   help="simulated UTC clock, e.g. 2026-08-10T04:30")
    args = p.parse_args()

    last_due = due_through(args.as_of)
    expected = [FEED_START + dt.timedelta(days=i)
                for i in range((last_due - FEED_START).days + 1)]
    missing = [d for d in expected
               if not (raw_orders_dir(args.scale) / f"orders_{d}.csv").exists()]

    con = duckdb.connect(str(warehouse_path(args.scale)))
    con.execute("""CREATE SCHEMA IF NOT EXISTS ops;
                   CREATE TABLE IF NOT EXISTS ops.alerts (     -- A05 made this
                       alert_ts TIMESTAMP, run_id BIGINT, severity VARCHAR,
                       source VARCHAR, message VARCHAR)""")
    for d in missing:                     # run_id NULL: no run — that's the point
        con.execute(
            "INSERT INTO ops.alerts VALUES (now(), NULL, 'error', 'freshness', ?)",
            [f"orders_{d}.csv not delivered by 04:00 UTC "
             f"(checked {args.as_of:%Y-%m-%d %H:%M}) - freshness SLA breach; "
             f"producer incident, escalate to #shopcore-oncall"])
        print(f"ALERT freshness  orders_{d}.csv missing - SLA breached")
    if missing:
        raise SystemExit(1)               # non-zero exit = the pager goes off (§7.3)
    print(f"fresh - every file through {last_due} delivered")


if __name__ == "__main__":
    main()
```

Demo it. The feed's last delivery is 2026-08-08, so pretending the clock rolled one day further *is* the simulated missing day — no files harmed:

```powershell
python -m work.freshness_gate --scale small --as-of 2026-08-09T04:30    # all due files exist
python -m work.freshness_gate --scale small --as-of 2026-08-10T04:30    # 08-09 due — and absent
echo $LASTEXITCODE                                                      # 1
```

The first prints `fresh - every file through 2026-08-08 delivered`. The second prints the ALERT line, writes one `ops.alerts` row (severity `error`, source `freshness`) and exits non-zero — console plus exit code simulate paging (§7.3). Note the division of labor with A05: its staleness probe (`freshness_check.py`, `max(_data_date)`) watches the warehouse from the inside; this gate watches shopcore's drop folder from the outside. Different sensors, same `ops.alerts` sink.

**(b) The run ledger is your evidence.** A07 promised that `ops.etl_runs` history is never deleted. Here is the payoff: the postmortem you are about to write (Task 9) needs detection times, resolution times and blast radius — and the ledger has all three on record. Run these three against the small-scale warehouse and save the output (Windows: if you save it by redirecting Python output to a file, set `PYTHONIOENCODING=utf-8` first — query 3's `bar()` draws Unicode block characters that don't fit cp1252, and the redirect dies with `UnicodeEncodeError`):

```sql
-- 1. Attempts per day — the incident fingerprint (who needed a second try?)
SELECT run_date, count(*) AS attempts,
       count(*) FILTER (WHERE status = 'failed') AS failed
FROM ops.etl_runs
WHERE step = 'day'
GROUP BY 1 HAVING count(*) > 1 ORDER BY 1;
-- 2026-07-22 | 2 | 1          (and no other date)

-- 2. The incident timeline — detection to resolution, timestamped
SELECT started_at, finished_at, status, rows, source_file, left(error, 60) AS error
FROM ops.etl_runs
WHERE step = 'day' AND run_date = DATE '2026-07-22'
ORDER BY started_at;
-- a 'failed' attempt carrying the sniffing error, then a 'success' with 58,400 rows;
-- both name orders_2026-07-22.csv, and finished_at is when each attempt ended —
-- the file and the load time the postmortem needs, from the ledger (§7.2/§7.3),
-- never from a column copied onto every row

-- 3. Rows over runs — the blast-radius check (small scale; at full use bar max 4000000)
SELECT run_date, rows, bar(rows, 0, 200000, 40) AS profile
FROM (SELECT run_date, rows,
             row_number() OVER (PARTITION BY run_date
                                ORDER BY started_at DESC) AS rn
      FROM ops.etl_runs
      WHERE step = 'day' AND status = 'success')
WHERE rn = 1 ORDER BY run_date;
```

Read the third one like an incident responder: a weekday plateau around 60k, weekend dips, the two 3× spike bars (06-19, 07-11) — and nothing else unusual. That is containment evidence: the incident touched exactly one day. Attempts prove *when you knew*, the timeline proves *how long the fix took*, the profile proves *how far it spread*.

**Accept when:** the gate is green for `2026-08-09T04:30` and red for `2026-08-10T04:30` with exactly one **new** `freshness` row in `ops.alerts` (message containing `not delivered` — your A05 breach row shares the source) and a non-zero exit; the three ledger queries match the annotated shapes and their outputs are saved for Task 9.

### Task 8 — Cost math and backfill etiquette (both scales, 20 min)

You now have real throughput data in `ops.etl_runs`. Use it:

```sql
SELECT sum(rows) AS rows_done,
       sum(epoch(finished_at - started_at)) AS seconds,
       round(sum(rows) / sum(epoch(finished_at - started_at))) AS rows_per_sec
FROM ops.etl_runs WHERE step = 'day' AND status = 'success';
```

rows/sec × rows remaining = time remaining: that one line of arithmetic is how professionals answer "how long will the backfill take?" *before* launching it (and, in the cloud, "what will it cost?"). Check it: does small-scale rows/sec × your full-scale manifest total predict your actual Task 6 wall clock? Usually it's in the right ballpark but not exact — write down why (bigger files stream differently, spike days skew, per-day overhead is fixed).

Then answer these three prompts in your journal, a short paragraph each:

1. **Chunking:** why might you run a 69-day backfill as 10 weekly chunks instead of one shot? (Think: blast radius of a failure, disk/tmp pressure, a chance to eyeball early results before burning hours.)
2. **Derived marts:** your marts (A04's `category_daily_revenue`, etc.) are built *from* core. After restating core for 69 days, what else must rerun, and in what order?
3. **Restatement comms:** if June's revenue dashboard changes after this rebuild, what do consumers need to hear from you, and *when* — before or after the numbers move? Notice the contract gives you no script here: v1.3.0 has **no restatement clause** (its own `freshness_sla` text admits it). That silence is a gap in the §3.9 sense — your postmortem's follow-ups will flag it.

### Task 9 — The postmortem (15 min)

Every real incident ends with a short, blameless write-up — its job is to make the *next* incident shorter, not to assign guilt. This one has a second audience: the contract's `quality.structural` clause tolerates zero malformed lines — a file with unparseable lines is a **producer incident** — so the write-up includes a note addressed to shopcore's Checkout Platform team (escalation contact is in the contract header), with your reject counts attached, exactly as the clause and `slo_reporting` promise them. Write `work/incidents/2026-07-22_corrupt_export.md` using this skeleton:

```markdown
# Incident: <one line — what broke, where>
**Detected:** <when/how> · **Resolved:** <when> · **Author:** <you> · **Status:** resolved

## Impact        — what was wrong, for how long, which tables/dates affected
## Timeline      — detected → diagnosed → fixed → backfilled; paste Task 7's
                   ledger timeline — timestamps, file name and outcomes from
                   ops.etl_runs, not from memory
## Root cause    — the real cause, not the first error message you saw
## Detection gap — how could we have caught this earlier? (hint: Task 1's manifest
                   query knew about corrupt_rows before you ran anything)
## Producer note — addressed to shopcore (Checkout Platform team): cite
                   quality.structural (malformed lines: zero tolerated), attach the
                   reject counts — 151 lines at small scale, that manifest's
                   corrupt_rows at full. Their incident to fix; ours to contain.
## Resolution    — what changed in the code, one sentence + file/commit
## Action items  — each with an owner; e.g. "alert when manifest corrupt_rows > 0".
                   Include the contract follow-up: if shopcore re-exports a fixed
                   orders_2026-07-22.csv tomorrow, no clause says how that
                   restatement must be announced — v1.3.0 is silent (a gap, §3.9).
                   Propose the amendment via A06's change_management flow.
```

**Accept when:** the root-cause section explains truncated-lines-plus-one-unclosed-quote (not "sniffing failed"); the timeline is pasted from the ledger; the producer note cites the structural clause with your reject counts; and the action items include both a prevention item that would have caught this *before* the backfill ran and the restatement-clause amendment.

## Definition of Done

- [ ] `work/runner.py` exists; `--dry-run`, `--only-failed`, `--force`, and default resume all behave as described, verified at small scale; every executed day opens with a green preflight (§7.4).
- [ ] All 69 days green in `ops.etl_runs` (latest status per date) at **both** scales.
- [ ] 2026-07-22 rejects quarantined as parquet with reasons, counts matching the manifests: 151 bad lines (small), ~3,000 (full, exact per manifest).
- [ ] Reconciliation clean: every day's loaded rows equal its manifest `rows` (query in Verify yourself returns zero mismatches).
- [ ] Lake repaired: 2026-07-22's canonical output appended **once** to your versioned lake dir; small-scale lake total = 4,116,024 rows.
- [ ] Freshness gate demo: `work/freshness_gate.py` green for `--as-of 2026-08-09T04:30`; the simulated missing day (`--as-of 2026-08-10T04:30`) writes exactly one new `ops.alerts` row (severity `error`, source `freshness`, message containing `not delivered`) and exits non-zero.
- [ ] Run-ledger evidence captured: attempts-per-day, incident timeline (with `source_file`), and rows-over-runs outputs saved into the journal/postmortem.
- [ ] Lineage held to §7.2: `core.orders` and the quarantine parquet carry `_data_date` and `_run_id` and no other underscore column; the runner records `source_file` on every `ops.etl_runs` row, and the row→run join recovers file, load time and outcome (queries in Verify yourself).
- [ ] Full-scale sequential wall clock recorded in `journal.md` as the A12 baseline.
- [ ] Postmortem written with root cause, ledger-sourced timeline, the producer note to shopcore (structural clause + reject counts), a prevention action item, and the restatement-gap follow-up.
- [ ] Three etiquette prompts (chunking, marts, comms) answered in the journal.

## Verify yourself

All-green check (expect one row: `success | 69`):

```sql
WITH latest AS (
  SELECT run_date, status,
         row_number() OVER (PARTITION BY run_date ORDER BY started_at DESC) AS rn
  FROM ops.etl_runs WHERE step = 'day')
SELECT status, count(*) FROM latest WHERE rn = 1 GROUP BY 1;
```

Reconciliation vs manifests (expect **zero** rows back):

```sql
WITH latest AS (
  SELECT run_date, rows,
         row_number() OVER (PARTITION BY run_date ORDER BY started_at DESC) AS rn
  FROM ops.etl_runs WHERE step = 'day' AND status = 'success')
SELECT r.run_date, r.rows AS loaded, m.rows AS manifest
FROM latest r
JOIN read_json_auto('<DATA_ROOT>/small/raw/manifest/orders_*.json') m
  ON m.date = r.run_date
WHERE r.rn = 1 AND r.rows != m.rows;
```

Quarantine contents at small scale (deterministic for generator 1.0):

```sql
SELECT error_type, count(*) AS records, count(DISTINCT line) AS lines
FROM read_parquet('<DATA_ROOT>/small/quarantine/orders_badlines/*/*.parquet')
GROUP BY 1 ORDER BY 2 DESC;
-- MISSING COLUMNS  1500 records / 150 lines
-- CAST              150 records / 150 lines   (same 150 truncated lines)
-- UNQUOTED VALUE      1 record  /   1 line
```

At full scale expect the same shape at roughly 20× the volume; the `lines` column must sum exactly to that manifest's `corrupt_rows`. Total loaded rows at small scale: exactly **4,116,840** across 69 days.

Lineage where §7.2 says it belongs — two columns on the row, everything else one join away:

```sql
SELECT column_name FROM duckdb_columns()
WHERE schema_name = 'core' AND table_name = 'orders'
  AND starts_with(column_name, '_');
-- _data_date, _run_id — and nothing else. A _source_file or _loaded_at here means
-- you carried A07's old shape forward instead of dropping and recreating the table.

SELECT o.order_id, o._data_date,
       r.source_file, r.finished_at, r.status, r.rows
FROM core.orders o
JOIN ops.etl_runs r ON r.run_id = o._run_id
WHERE o._data_date = DATE '2026-07-22' LIMIT 1;
-- one row: file name, load time, outcome and row count for the day that broke —
-- recovered from the ledger, which is the point of storing them there once
```

Ledger fingerprint (expect exactly this one row — one failed first try, one clean resume; extra attempts mean you re-ran days, which is fine as long as you can explain them):

```sql
SELECT run_date, count(*) AS attempts,
       count(*) FILTER (WHERE status = 'failed') AS failed
FROM ops.etl_runs WHERE step = 'day'
GROUP BY 1 HAVING count(*) > 1;
-- 2026-07-22 | 2 | 1
```

And the freshness demo left its trace (expect `error | freshness | 1`):

```sql
SELECT severity, source, count(*) AS alerts
FROM ops.alerts
WHERE source = 'freshness' AND message LIKE '%not delivered%'
GROUP BY 1, 2;
```

The `LIKE` filter is not decoration: your A05 Task 9 breach demo left its own `freshness` row in this warehouse (message starting `FRESHNESS BREACH:`), so counting the source alone gives 2.

Lake closed — the 69-day canonical lake A13–A15 build on (adjust `orders_v=` to your A10 dir name):

```sql
SELECT count(*), count(DISTINCT order_date)
FROM read_parquet('<DATA_ROOT>/small/lake/orders_v=*/*/*.parquet',
                  hive_partitioning=true);        -- small: 4,116,024 · 69
```

That is the manifest sum 4,116,840 minus the 816 unparseable-timestamp rows your cleaner drops (13 of them in 07-22). Higher means your replay re-appended days the lake already had — rebuild the versioned dir per A10 Task 7 rather than hand-pruning parquet files. At full scale, the same identity: manifest sum minus your logged dropped rows.

## Common beginner mistakes

- **Trusting the first error message.** "Could not detect the CSV dialect" sounds like a settings problem; the root cause was broken lines. Always confirm with evidence (`store_rejects`, manifests) before "fixing" anything.
- **Probing rejects with a bare `count(*)`.** Non-materializing scans skip parsing, so the reject table stays empty and you conclude the file is fine. Use `CREATE TABLE … AS`.
- **Counting reject records instead of `DISTINCT line`.** 1,651 records ≠ 1,651 bad lines; a truncated line produces one record per missing column. Panic postponed.
- **Not clearing `reject_errors` between days.** It accumulates per connection, so day N+1's quarantine copy silently includes day N's rejects — double-counted evidence.
- **Backfilling a sub-range and forgetting late data.** Correct dates `[A, B]` requires source files up to `B+7` (your A06 contract's late allowance). Full-history replays dodge this; partial ones must not.
- **Hand-editing the corrupt CSV.** Raw data is immutable evidence. Your pipeline must survive the file as-is — that's the whole point.

## Stretch goals

1. **Chunked mode:** add `--chunk-days 7` — run week-sized chunks with a per-chunk summary, and abort the whole backfill if any chunk exceeds a failure threshold (e.g. >1 failed day).
2. **Live ETA:** after each day, print projected time remaining from the running rows/sec and the manifests' remaining row counts. Compare the day-5 prediction with reality.
3. **Restatement drill:** write the actual email to your (imaginary) analytics consumers announcing the June restatement: what changes, when, why, what they should do. 10 lines, no jargon — you are drafting the process the missing restatement clause should have pinned down.
4. **Put the watchdog on the clock.** Task 7's gate has one weakness left: it runs when you remember to run it, and the whole point of a watchdog is that nobody remembers. Hand it to the OS scheduler. Windows (PowerShell; `<python>` = the full path to your venv's `python.exe`, `<repo>` = your repo root):

   ```powershell
   schtasks /create /sc daily /st 04:05 /tn "etl-lab-freshness" /tr 'cmd /c cd /d "<repo>" && <python> -m work.freshness_gate --scale small --as-of 2026-08-10T04:30'
   ```

   The `cmd /c cd /d "<repo>"` is not decoration: the scheduler starts tasks in `System32`, where `python -m work.freshness_gate` imports nothing — and put double quotes around both `<repo>` and `<python>` if the paths contain spaces (a venv inside the repo usually does). And `schtasks` caps the whole `/tr` value at 261 characters — if your paths blow past it, put the command in a small `.cmd` wrapper and point `/tr` at that instead. Two more details: `ETL_LAB_DATA` must be set at the user level (A00's `setx` did that — a scheduled task never reads your open terminal), and the pinned `--as-of` keeps the demo deterministic at one known-missing day; a production gate would drop the flag and read the real clock. macOS/Linux, same idea via `crontab -e`:

   ```
   5 4 * * * cd <repo> && <python> -m work.freshness_gate --scale small --as-of 2026-08-10T04:30
   ```

   Let it fire once for real — wait for 04:05 if you enjoy suspense, or trigger it now with `schtasks /run /tn "etl-lab-freshness"`. Then look at `ops.alerts`: a new `freshness` row whose `alert_ts` is the moment the *scheduler* ran your script — the Verify-yourself count moves from 1 to 2, and that extra row is the first alert this pipeline ever raised without a human at the keyboard. Clean up: `schtasks /delete /tn "etl-lab-freshness" /f` (cron: delete the line). Close with two sentences in your journal on watchdogs running on the clock versus on demand. Here are ours: a gate you run by hand only ever confirms what you already suspected, while the same gate on a schedule finds problems nobody was looking for. That one line — "run my check at 04:05 daily, page on failure" — is the seed of all orchestration: grow it and you get cron pipelines, grow those and you get Airflow.
