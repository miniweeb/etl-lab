# A07 — Failures, retries & idempotency

**Topic:** failure handling, retries, idempotent loads · **Est. time:** 5–6h · **Difficulty:** ⭐⭐⭐
**Requires:** A06 done; month-1 raw data at both scales; your small-scale `warehouse.duckdb`.

## Why this matters

Real pipelines fail all the time: the network drops, a disk fills up, a laptop sleeps
mid-run, someone hits Ctrl+C. Failure itself is not the problem — the problem is what
state it leaves behind, and what happens when you (or a scheduler) run the job again.
Today you make your per-day load *safe to re-run any number of times*. Every serious
orchestrator (Airflow, Dagster, dbt) assumes your tasks have this property; you will
build it by hand so you understand what they assume.

## Background

**Idempotent** (one-line definition): an operation is idempotent if running it once or
running it ten times leaves the system in the same final state. An elevator call
button is idempotent — press it five times, one elevator comes. A "top up my account
by $10" button is not. A crashed or re-run load job can end three ugly ways:

1. **Double data** — the job succeeded, someone ran it again, every row is in twice.
2. **Half data** — the job crashed mid-insert; the table holds part of a day.
3. **Missing data** — the job deleted the old day, crashed before inserting the new one.

**Transactions** remove two of those. A transaction groups statements into one
all-or-nothing unit: after `BEGIN`, nothing is visible until `COMMIT`; if the process
dies first, it is as if nothing ran. DuckDB guarantees this (it is an "ACID" database —
the A, atomicity, is exactly this promise), even when the process is killed outright.
So publishing becomes `BEGIN; DELETE the old copy; INSERT the new copy; COMMIT` — the
§4 delete+insert pattern. Readers see the old day or the new day, never a mix.

**A run ledger** removes the third. A pipeline needs memory of what it has done.
`ops.etl_runs` is a plain table with one row per attempt: date, step,
running/success/failed, row count, error. It answers "did yesterday load?" with a
query instead of a guess — and lets you skip work already done. A11 builds on it.

The ledger also finishes a story A03 started: the `_run_id` lineage column (§7.2)
has sat NULL in every row since then. Today it comes alive — every published row
names the run that wrote it, every run writes its own log file
(`logs/run_<run_id>.log`, §7.3), and a load that gives up for good writes an
`ops.alerts` row. Row → run → log: when a number looks wrong three weeks from now,
you can reconstruct exactly where it came from.

And it collects on A03's other lesson — the *restraint*. A03 gave every row
exactly two warehouse-added columns, `_data_date` and `_run_id`, and told you
that the file name and the load timestamp did **not** belong there because they
are facts about the *run*, not about the order. Today you build the place they do
belong: `ops.etl_runs` carries `source_file` and `finished_at` (§7.3), one row per
attempt, and `_run_id` is the pointer. Task 8 walks the join that gets them back.

One design decision matters more than any code: **pick the unit of work, and make it
replaceable.** Our unit is *one source file* (one day's CSV). Every row already
carries the file's business date — the `_data_date` lineage column from A03 — and
publishing deletes exactly `WHERE _data_date = D` before inserting.
Delete-what-I-would-write, then write: re-run = replace. Note what
we did *not* promise: the same `order_id` still appears in several files (the ~1.5%
late corrections from A02). Making the *content* converge — latest update wins — is
A08's job. Today we only make the *operation* safe.

Finally, **retries**. Transient failures (network blip, briefly locked file) are
worth retrying — with doubling waits, 1s, 2s, 4s, 8s ("exponential backoff"), to
give a struggling system air instead of hammering it. Deterministic ones (corrupt
file, failed validation) just fail again, slower; retry only the transient kind.
One last thing: you have already used an idempotent tool — `datagen/generate.py`
skips existing files unless `--force`.

Two habits ride along today, where they stop being theory: a **preflight** checks
the *environment* before any data moves (Task 4 shows why), and a **git
savepoint** makes an assignment that breaks things on purpose reversible (Task 5).

## Setup

- Venv active, shell at the repo root; your script lives in `work/`.
  **Scale: small for Tasks 1–8, full only in Task 9.**
- Git ritual (§7.4): `git switch -c a07-idempotency`, commit after each numbered
  task, merge to `main` when the DoD is green (`guides/git_basics.md` if any step
  feels fuzzy). Today the ritual earns its keep: Task 5 turns those commits into
  crash insurance.
- Have open: your editor, one PowerShell terminal, and (for Task 9) Task Manager.
- The script sets the usual knobs itself: `memory_limit='8GB'`, `threads=8`,
  `temp_directory='<DATA_ROOT>/tmp'`.
- Heads-up: Task 3 wants a `core.orders` carrying the two §7.2 lineage columns —
  `_data_date` and `_run_id`, and no more. If your
  existing `core.orders` has a different shape, `DROP TABLE core.orders` first. It is
  derived data — rebuildable from `raw/`, and after today you own the tool that
  rebuilds it safely. That mindset (raw + code = truth) is half of this assignment.

## Tasks

### Task 1 — Break it on purpose: the double-run bug (small scale)

Before fixing a bug, meet it. In a Python session, load one day the naive way — then
"accidentally" run the load again:

```python
import duckdb
con = duckdb.connect()   # in-memory scratch; we are breaking things on purpose
RAW = "<DATA_ROOT>/small/raw/orders/orders_2026-06-03.csv"
con.execute(f"CREATE TABLE naive_orders AS SELECT * FROM read_csv('{RAW}')")
print(con.execute("SELECT count(*) FROM naive_orders").fetchone())   # first run
con.execute(f"INSERT INTO naive_orders SELECT * FROM read_csv('{RAW}')")
print(con.execute("SELECT count(*) FROM naive_orders").fetchone())   # "oops"
```

(Auto-detect read is fine here — we only count. The real loader keeps §4's explicit
schema.) You should see 62,707 rows become 125,414. Nothing errored. No warning. The
worst pipeline failures are silent. Also run
`SELECT order_id, count(*) FROM naive_orders GROUP BY 1 HAVING count(*) > 2 LIMIT 5` —
a handful of ids appear **4** times: they were already duplicated inside the file (the
in-file dups from §3.5 of the spec), now doubled again. A08 deals with those.

**Accept when:** you can explain, in one journal sentence, why plain `INSERT` can
never be safe to re-run.

### Task 2 — Build the run ledger: `ops.etl_runs` (small scale)

Open the small warehouse (`duckdb.connect` on
`<DATA_ROOT>/small/warehouse/warehouse.duckdb`) and create:

```sql
CREATE SCHEMA IF NOT EXISTS ops;
CREATE SEQUENCE IF NOT EXISTS ops.run_id_seq;
CREATE TABLE IF NOT EXISTS ops.etl_runs (      -- canonical shape: SPEC §7.3
    run_id      BIGINT PRIMARY KEY,   -- nextval('ops.run_id_seq')
    run_date    DATE,                 -- the data date being loaded
    step        VARCHAR,              -- 'load_day' today; more steps later
    status      VARCHAR,              -- 'running' | 'success' | 'failed'
    started_at  TIMESTAMP,
    finished_at TIMESTAMP,            -- when the load landed: the load timestamp
    rows        BIGINT,               -- rows published (success only)
    source_file VARCHAR,              -- which file this attempt read
    error       VARCHAR               -- error message (failed only)
);
```

Every *attempt* gets its own row — failures are history worth keeping, not shame to
overwrite.

Two of those nine columns deserve a second look, because they are the ones a
junior would have put somewhere else. `source_file` and `finished_at` are exactly
the "where did this come from / when did it land" facts that tempt people into
stamping `_source_file` and `_loaded_at` onto every data row. A03 said no, and
this table is why: the file that produced a delivery and the moment it landed are
properties of the **run**. Store them once here, and 62,707 rows point at them
with a single `_run_id` — instead of carrying 62,707 identical copies of a string
that, if you re-load the day from a corrected file, would all have to be rewritten.
One ledger row changes instead. (Task 8 proves nothing was lost.)

**Accept when:** the table exists and you can say what each column is for without
looking — including which two facts live here *instead of* on the data rows, and
why.

### Task 3 — Refactor: stage → validate → atomic publish (small scale)

Now the real loader, `work/a07_load_day.py`. Shape: stage the file into a temp table,
validate it against the manifest from shopcore, the e-commerce platform that produces
this feed, then publish data *and* ledger update in **one transaction**. Notice three
things. The success `UPDATE` on `ops.etl_runs` sits *inside* `BEGIN…COMMIT`, so the
ledger can never claim success for data that is not there. `DELETE WHERE _data_date = ?`
runs even on the very first load (deletes 0 rows, costs nothing) — first run and
re-run are the *same code path*; fewer paths, fewer bugs. And every attempt allocates
its own `run_id`, then narrates itself to its own `logs/run_<run_id>.log` (§7.3) —
console output for you, the file for the audit trail. One detail to catch in
passing: the `INSERT` that opens the ledger row records `source_file` right
away, *before* anything can go wrong. A failed attempt should still be able to
tell you which file it choked on.

```python
"""work/a07_load_day.py - load one raw day into core.orders, safely rerunnable."""
import argparse, logging, os, time
from datetime import date
from pathlib import Path
import duckdb

REPO_ROOT = Path(__file__).resolve().parents[1]
BASE = Path(os.environ.get("ETL_LAB_DATA", REPO_ROOT / "data"))
log = logging.getLogger("a07")
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")   # console half

def open_run_log(run_id: int) -> logging.FileHandler:
    """§7.3: console for you, logs/run_<run_id>.log for the audit trail."""
    log_dir = REPO_ROOT / "logs"
    log_dir.mkdir(exist_ok=True)
    fh = logging.FileHandler(log_dir / f"run_{run_id}.log", encoding="utf-8")
    fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    log.addHandler(fh)
    return fh

def data_root(scale: str) -> Path:
    return BASE / "small" if scale == "small" else BASE

def connect(scale: str) -> duckdb.DuckDBPyConnection:
    wh = data_root(scale) / "warehouse" / "warehouse.duckdb"
    wh.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(wh))
    con.execute("SET memory_limit='8GB'; SET threads=8;")
    con.execute(f"SET temp_directory='{(data_root(scale) / 'tmp').as_posix()}';")
    return con

def ensure_tables(con: duckdb.DuckDBPyConnection) -> None:
    # paste the three CREATE statements from Task 2 here, then:
    con.execute("""
        CREATE TABLE IF NOT EXISTS ops.alerts (   -- born in A05; IF NOT EXISTS so
            alert_ts TIMESTAMP, run_id BIGINT,    -- the script also runs for anyone
            severity VARCHAR, source VARCHAR,     -- doing assignments out of order
            message VARCHAR)""")                  # (used in Task 7)
    con.execute("""
        -- grain: one row = one delivered version of an order
        -- (unique on order_id, _data_date — corrections re-deliver; A08 folds them)
        CREATE TABLE IF NOT EXISTS core.orders (
            order_id BIGINT, customer_id BIGINT, store_id INTEGER,
            order_ts TIMESTAMP, updated_at TIMESTAMP,
            status VARCHAR, payment_method VARCHAR,
            order_total DECIMAL(14,2), items VARCHAR, meta VARCHAR,
            order_date DATE,
            -- the two §7.2 lineage columns, and no more. _data_date is our
            -- idempotency key: which delivery wrote this row (and, via the
            -- contract's path_pattern, which file). _run_id: NULL since A03 -
            -- real from today, and the join key into ops.etl_runs for
            -- everything else about the load.
            _data_date DATE, _run_id BIGINT
        )""")
    con.execute("CREATE SCHEMA IF NOT EXISTS core")   # move above the CREATE TABLE!

def already_succeeded(con, day: date) -> bool:
    # TODO(you): query ops.etl_runs. Return True if at least one row exists for
    # this run_date with step = 'load_day' AND status = 'success'.
    # Task 8 tests this: a rerun without --force must skip.
    ...

def load_day(con, scale: str, day: date) -> int:
    """One attempt to load one raw file. All-or-nothing."""
    csv_path = data_root(scale) / "raw" / "orders" / f"orders_{day}.csv"
    mf_path = data_root(scale) / "raw" / "manifest" / f"orders_{day}.json"
    if not csv_path.exists():
        raise FileNotFoundError(csv_path)

    run_id = con.execute("SELECT nextval('ops.run_id_seq')").fetchone()[0]
    run_log = open_run_log(run_id)   # every attempt narrates itself to its own file
    con.execute("""
        INSERT INTO ops.etl_runs (run_id, run_date, step, status, started_at,
                                  source_file)
        VALUES (?, ?, 'load_day', 'running', now(), ?)""",
        [run_id, day, csv_path.name])   # §7.3: the file is recorded HERE, once

    try:
        # step 1: what does shopcore's manifest say this file contains?
        expected = con.execute(
            "SELECT rows FROM read_json_auto(?)", [str(mf_path)]).fetchone()[0]

        # step 2: stage - clean + type, 1:1 with raw (swap in your full A03 rules)
        # cleaning fragments from work/cleaners.py (A03) - inlined below so the
        # listing reads alone; your own script should import them instead (§7.4)
        # (a TEMP table is session-private and dies with the connection, so §7.1's
        #  shared-namespace rule does not apply - persistent staging tables keep
        #  the shopcore_ prefix)
        con.execute("""
            CREATE OR REPLACE TEMP TABLE stg_day AS
            SELECT
                order_id, customer_id, store_id,
                try_strptime(order_ts, ['%Y-%m-%d %H:%M:%S', '%d/%m/%Y %H:%M:%S',
                                        '%Y-%m-%dT%H:%M:%S']) AS order_ts,
                updated_at,
                NULLIF(LOWER(TRIM(status)), '') AS status,
                LOWER(TRIM(payment_method)) AS payment_method,
                TRY_CAST(NULLIF(
                  CASE WHEN order_total LIKE '%,%'
                       THEN replace(replace(order_total, '.', ''), ',', '.')
                       ELSE replace(TRIM(order_total), '$', '') END,
                  'N/A') AS DECIMAL(14,2)) AS order_total,
                items, meta
            FROM read_csv(?, header=true,
                columns={'order_id':'BIGINT','customer_id':'BIGINT',
                         'store_id':'INTEGER','order_ts':'VARCHAR',
                         'updated_at':'TIMESTAMP','status':'VARCHAR',
                         'payment_method':'VARCHAR','order_total':'VARCHAR',
                         'items':'VARCHAR','meta':'VARCHAR'})
        """, [str(csv_path)])

        # step 3: validate BEFORE touching core (A05 owns the full check suite)
        staged = con.execute("SELECT count(*) FROM stg_day").fetchone()[0]
        if staged != expected:
            raise ValueError(f"staged {staged} rows, manifest says {expected}"
                             " - refusing to publish")

        # step 4: publish - data AND ledger in one atomic transaction.
        # Lineage stamping (§7.2): two columns, no more. _data_date says which
        # delivery; _run_id ties the row to this attempt's ledger row, which
        # already holds the file name and will hold the finish time.
        con.execute("BEGIN TRANSACTION")
        con.execute("DELETE FROM core.orders WHERE _data_date = ?", [day])
        con.execute("""
            INSERT INTO core.orders
            SELECT *, CAST(COALESCE(order_ts, updated_at) AS DATE) AS order_date,
                   ? AS _data_date, ? AS _run_id
            FROM stg_day""", [day, run_id])
        con.execute("""
            UPDATE ops.etl_runs SET status = 'success', finished_at = now(),
                   rows = ? WHERE run_id = ?""", [staged, run_id])
        con.execute("COMMIT")
        log.info("day %s: %d rows published (run_id=%d)", day, staged, run_id)
        return staged

    except Exception as exc:
        try:
            con.execute("ROLLBACK")          # errors if no txn is open - fine
        except duckdb.Error:
            pass
        con.execute("""
            UPDATE ops.etl_runs SET status = 'failed', finished_at = now(),
                   error = ? WHERE run_id = ?""", [str(exc)[:500], run_id])
        log.error("day %s: attempt failed (run_id=%d): %s", day, run_id, exc)
        raise
    finally:
        log.removeHandler(run_log)           # this run's file is complete
        run_log.close()

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--scale", choices=["small", "full"], required=True)
    ap.add_argument("--date", required=True)
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()
    day = date.fromisoformat(args.date)
    con = connect(args.scale)
    ensure_tables(con)
    if already_succeeded(con, day) and not args.force:
        log.info("day %s already loaded - skipping (use --force to reload)", day)
        return
    load_day(con, args.scale, day)

if __name__ == "__main__":
    main()
```

Fill the two gaps (`already_succeeded`, and yes — one line in `ensure_tables` is
deliberately in the wrong place; put `CREATE SCHEMA` before the table that needs it).
About `COALESCE(order_ts, updated_at)`: ~0.02% of rows had impossible timestamps that
became NULL in cleaning (13 rows in small 2026-06-03); we still need a non-NULL
`order_date`, and `updated_at` is the honest fallback (A05 quarantines them properly).

A §7.4 note, same as A05's: step 2's timestamp and money expressions are the
`work/cleaners.py` fragments from A03, inlined only to keep the listing
self-contained. In your own script, import them instead of re-pasting:
`tests/test_cleaners.py` pins their behavior, and a fragment fixed in one place
is fixed everywhere it is imported.

Run it, run it again with `--force`, and compare an order-independent checksum
(`bit_xor` of row hashes — same rows give the same value regardless of row order):

```powershell
python work\a07_load_day.py --scale small --date 2026-06-03
python work\a07_load_day.py --scale small --date 2026-06-03 --force
```

```sql
SELECT count(*) AS n,
       bit_xor(hash(order_id, updated_at, status, order_total)) AS checksum
FROM core.orders WHERE _data_date = DATE '2026-06-03';
```

**Accept when:** both runs log `62707 rows published`, count + checksum are
identical after the second run, and `logs/` holds one file per run. The checksum
hashes only business columns *on purpose*: `_run_id` **should** differ between the
two runs — and so should the `finished_at` on the ledger row it points at. That is
lineage recording history, not data changing.
Write the checksum in your journal — you will compare against it all day. (`hash()`
values are only comparable within your own DuckDB install — do not compare with a
classmate.)

### Task 4 — Preflight: check the environment before you touch data (small scale)

Your loader now survives crashing *mid-flight*. Next, stop it from taking off into
a storm at all. Think about what actually breaks pipelines at 2 a.m. — rarely the
SQL. The disk is full. The file never arrived. Another process is holding the
database. A config quietly aims temp space at the OS drive. Every one of those is
checkable in under a second, *before* any data moves, and §7.4 turns that
observation into a lab standard: seven checks, one PASS/FAIL line each with a fix
hint, and a **non-zero exit on any FAIL** so a scheduler stops cold. From today,
every runner in this lab calls preflight first — A11's backfill runner will import
it before every single day it loads.

The seven, in the order they run: (1) data root writable, (2) free space on the
data volume, (3) temp dir on the data volume, (4) source CSV + manifest present,
(5) warehouse writable and DB not locked — opening a connection *is* the lock
probe, (6) schemas creatable — `CREATE SCHEMA IF NOT EXISTS` doubles as a free
permission probe, (7) contract parseable — broken YAML must fail here, not
mid-pipeline. This list is where each check's *why* lives (SPEC §7.4 carries the
full standard); the listing below keeps its comments short.

Create `work/preflight.py` — the next rung on the §7.4 ladder, so ladder rules
apply: the docstring states the job and the assumptions.

```python
"""work/preflight.py - the seven SPEC §7.4 environment checks, run before any load.

Fail fast: a permission error at second zero beats one at row 40 million.
One PASS/FAIL line per check, a fix hint on every FAIL, exit code 1 if
anything failed. Pipelines import run_checks(); humans run:

    python work/preflight.py --scale small --date 2026-06-03

Assumes the SPEC §2 layout under ETL_LAB_DATA (else <repo>/data).
"""
import argparse
import os
import shutil
import sys
import uuid
from datetime import date
from pathlib import Path

import duckdb
import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
BASE = Path(os.environ.get("ETL_LAB_DATA", REPO_ROOT / "data"))
CONTRACT = REPO_ROOT / "contracts" / "shopcore_orders_daily.contract.yaml"
MIN_FREE_GB = {"small": 5, "full": 25}   # staging + spill headroom, not a science


def data_root(scale: str) -> Path:
    return BASE / "small" if scale == "small" else BASE


def one_line(exc: Exception) -> str:
    """Errors can be paragraphs; a report line cannot."""
    return " ".join(str(exc).split())[:160]


def same_volume(a: Path, b: Path) -> bool:
    """Windows: drive letters decide ('F:' vs 'C:'). POSIX: device ids do."""
    if a.drive or b.drive:
        return a.drive.lower() == b.drive.lower()
    return a.resolve().stat().st_dev == b.resolve().stat().st_dev


def run_checks(scale: str, day: date, tmp: Path | None = None) -> bool:
    root = data_root(scale)
    tmp = tmp if tmp is not None else root / "tmp"   # the §7.4 temp convention
    wh = root / "warehouse" / "warehouse.duckdb"
    results = []   # rows of (check_no, name, ok, detail, hint_on_fail)

    # 1. data root exists and is writable (a dir can exist and still refuse writes)
    probe = root / f".preflight_probe_{uuid.uuid4().hex}"
    try:
        probe.write_text("probe", encoding="utf-8")
        probe.unlink()
        results.append((1, "data root writable", True, str(root), ""))
    except OSError as exc:
        results.append((1, "data root writable", False, f"{root}: {exc}",
                        "point ETL_LAB_DATA at a folder you own on the data disk"))

    # 2. free space measured on the DATA volume (room on C: helps nothing)
    try:
        free_gb = shutil.disk_usage(root).free / 1e9
        need = MIN_FREE_GB[scale]
        results.append((2, "free space on data volume", free_gb >= need,
                        f"{free_gb:.1f} GB free, need >= {need} GB",
                        "free up the data disk, or point ETL_LAB_DATA at a bigger one"))
    except OSError as exc:
        results.append((2, "free space on data volume", False, str(exc),
                        "data root missing entirely? fix check 1 first"))

    # 3. temp_directory resolves to the data volume (§7.4 temp discipline)
    try:
        on_data = same_volume(tmp, root)
    except OSError:
        on_data = False
    results.append((3, "temp dir on data volume", on_data,
                    str(tmp) if on_data
                    else f"{tmp} is not on the data volume ({root.drive or root})",
                    "spill belongs next to the data: SET temp_directory='<DATA_ROOT>/tmp'"))

    # 4. source CSV for the date exists, is non-empty, and its manifest exists.
    # TODO(you): three ways a delivery is broken - the CSV is missing, the CSV
    # is 0 bytes, the manifest JSON is missing. Append exactly ONE results row
    # (4, "source file + manifest", ok, detail, hint): ok only if none of the
    # three hold. On FAIL the detail must name the exact path that is wrong,
    # and the hint must say what to do about it (typo in --date? generator
    # never run? a 0-byte file is an incident to escalate to shopcore).
    csv_path = root / "raw" / "orders" / f"orders_{day}.csv"
    mf_path = root / "raw" / "manifest" / f"orders_{day}.json"
    ...

    # 5. warehouse dir writable + DB not locked (the connect() is the lock probe)
    con = None
    try:
        wh.parent.mkdir(parents=True, exist_ok=True)
        wprobe = wh.parent / f".preflight_probe_{uuid.uuid4().hex}"
        wprobe.write_text("probe", encoding="utf-8")
        wprobe.unlink()
        con = duckdb.connect(str(wh))
        results.append((5, "warehouse writable, not locked", True, str(wh), ""))
    except (OSError, duckdb.Error) as exc:
        results.append((5, "warehouse writable, not locked", False, one_line(exc),
                        "close the other shell/notebook holding the DB; "
                        "check folder permissions"))

    # 6. schemas exist / are creatable (doubles as a free permission probe)
    if con is None:
        results.append((6, "schemas creatable", False, "skipped: no connection",
                        "fix check 5 first"))
    else:
        try:
            for schema in ("staging", "core", "marts", "ops"):
                con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            results.append((6, "schemas creatable", True,
                            "staging, core, marts, ops", ""))
        except duckdb.Error as exc:
            results.append((6, "schemas creatable", False, one_line(exc),
                            "read-only DB file? on a server: ask for GRANTs on the schema"))
        finally:
            con.close()

    # 7. contract present + parseable (A05/A06 gates read their thresholds from it)
    try:
        parsed = yaml.safe_load(CONTRACT.read_text(encoding="utf-8"))
        if not isinstance(parsed, dict):
            raise ValueError("parsed, but not a YAML mapping")
        results.append((7, "contract parseable", True, CONTRACT.name, ""))
    except (OSError, yaml.YAMLError, ValueError) as exc:
        results.append((7, "contract parseable", False, one_line(exc),
                        "restore it: git restore contracts/<file> (A06 owns the contract)"))

    all_ok = True
    for no, name, ok, detail, hint in results:
        print(f"[{'PASS' if ok else 'FAIL'}] {no}. {name:<30} {detail}")
        if not ok:
            print(f"         fix: {hint}")
            all_ok = False
    return all_ok


def main() -> None:
    ap = argparse.ArgumentParser(description="SPEC §7.4 preflight: seven checks, fail fast")
    ap.add_argument("--scale", choices=["small", "full"], required=True)
    ap.add_argument("--date", required=True, help="YYYY-MM-DD of the file to load")
    ap.add_argument("--tmp", help="temp dir override (default <DATA_ROOT>/tmp) - "
                                  "exists so you can break check 3 on purpose")
    args = ap.parse_args()
    ok = run_checks(args.scale, date.fromisoformat(args.date),
                    Path(args.tmp) if args.tmp else None)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
```

Wire it into the loader — the pipeline runs preflight **first**. Add `sys` to the
loader's import line, then three lines near the top (the `sys.path` insert is the
lab's standard trick for making `work.*` importable when a script is run directly):

```python
REPO_ROOT = Path(__file__).resolve().parents[1]   # already there
sys.path.insert(0, str(REPO_ROOT))                # repo root, so work.* imports resolve
from work import preflight
```

…and in `main()`, right after `day = date.fromisoformat(args.date)`:

```python
    if not preflight.run_checks(args.scale, day):
        sys.exit(2)     # red preflight = hands off the data; non-zero is for schedulers
```

Now the red/green demo — proving a gate can actually close is part of building one:

```powershell
python work\preflight.py --scale small --date 2026-06-03; $LASTEXITCODE
```

```
[PASS] 1. data root writable             <DATA_ROOT>\small
[PASS] 2. free space on data volume      362.3 GB free, need >= 5 GB
[PASS] 3. temp dir on data volume        <DATA_ROOT>\small\tmp
[PASS] 4. source file + manifest         orders_2026-06-03.csv (19,009,154 bytes) + manifest
[PASS] 5. warehouse writable, not locked <DATA_ROOT>\small\warehouse\warehouse.duckdb
[PASS] 6. schemas creatable              staging, core, marts, ops
[PASS] 7. contract parseable             shopcore_orders_daily.contract.yaml
0
```

(The free-space figure is your machine's; the CSV byte count is exact — small-scale
data is deterministic.) Then break check 3 on purpose — aim temp at the OS drive,
precisely the misconfiguration the temp discipline exists to catch:

```powershell
python work\preflight.py --scale small --date 2026-06-03 --tmp C:\Windows\Temp; $LASTEXITCODE
```

```
[FAIL] 3. temp dir on data volume        C:\Windows\Temp is not on the data volume (F:)
         fix: spill belongs next to the data: SET temp_directory='<DATA_ROOT>/tmp'
```

…six PASSes around it, and the exit code is `1` — the part schedulers obey; a red
report that still exits 0 is a smoke alarm with no battery. Try `--date 2026-05-01`
too: your check 4 must go red and name the missing path. And to see check 5 fire,
open the warehouse from a second window — `python -c "import duckdb, time;
con = duckdb.connect(r'<DATA_ROOT>\small\warehouse\warehouse.duckdb'); time.sleep(30)"`
(the `con =` is load-bearing: an unassigned connection is garbage-collected at
once, releasing the lock before you can switch windows) — and run preflight
within 30 seconds: check 5 FAILs with the classic "being used by
another process", and check 6 honestly reports `skipped: no connection`.

> **If the warehouse were a networked database…** the same checklist survives
> with different probes — DNS, port, TLS, credentials, `GRANT`s, pool — walked
> in full, side by side with this lab's column, in `guides/troubleshooting.md` §5.

**Accept when:** the green run prints seven PASS lines and `$LASTEXITCODE` is `0`;
the `--tmp C:\Windows\Temp` run FAILs check 3 with a fix hint and exits `1`; a
bogus `--date` FAILs your check 4 naming the missing path; and a normal
`python work\a07_load_day.py` run now starts with the seven preflight lines.

### Task 5 — Savepoint before the storm: the git rescue kit (small scale)

Tasks 6 and 7 have you deliberately wrecking a working pipeline, with edits made
quickly and under pressure — exactly the setting where a botched edit eats a file
you spent two hours getting right. Right before chaos work, the §7.4
commit-after-each-task habit turns into insurance. Make the savepoint explicit:

```powershell
git add work\a07_load_day.py work\preflight.py
git commit -m "A07 task 5: pre-chaos savepoint"
git log --oneline     # note the savepoint's short sha - the rescue drills need it
```

Now stage the disaster — the *file*, not the data (a rescue drill is pleasantly
free of consequences):

```powershell
Set-Content work\a07_load_day.py "# TODO rewrite everything"   # the 11 p.m. edit
git status --short        #  M work/a07_load_day.py
git diff --stat           # ~167 deletions - the damage, quantified, before any panic
```

**Rescue 1 — the mess is *not* committed.** `git restore <file>` throws away
uncommitted changes to that file and brings back the last committed version:

```powershell
git restore work\a07_load_day.py
git status --short        # clean - the loader is back, byte for byte
```

That rescue only works while the mess is *uncommitted*. For the other two —
the mess is already committed (`git checkout <sha> -- <file>`), and abandon
everything since the savepoint (`git reset --hard`, then `git reflog` to see
that even reset-away commits survive) — run the drills in
`guides/git_basics.md` §7.2–7.4 now, against this file: the transcripts there
are this exact scenario, savepoint commit and all. Close the drill by proving
nothing of value burned:

```powershell
python work\a07_load_day.py --scale small --date 2026-06-03   # already loaded - skipping
```

**Accept when:** the savepoint commit exists; you mangled and recovered the loader
once with `git restore` and once with `git reset --hard`; `git reflog` shows the
whole journey; and the loader still skips-on-rerun afterwards.

### Task 6 — Release the chaos monkey (small scale)

A pipeline that has never crashed mid-flight is untested. Break freely from here —
the Task 5 savepoint has your back. Add a `--chaos P` flag
(`type=float, default=0.0`, passed through to `load_day`) that makes the pipeline fail
on purpose, with probability P, at several points:

```python
import random   # add to the imports

class TransientError(RuntimeError):
    """A failure worth retrying: network blip, locked file, chaos monkey."""

def maybe_fail(point: str, chaos: float) -> None:
    """Chaos monkey: with probability `chaos`, blow up at this point."""
    if random.random() < chaos:
        raise TransientError(f"chaos monkey struck at: {point}")

# call sites in load_day (signature becomes load_day(con, scale, day, chaos=0.0)),
# these four at least, plus any you like:
maybe_fail("after_read_manifest", chaos)   # after step 1
maybe_fail("after_stage", chaos)           # after step 2
maybe_fail("mid_transaction", chaos)       # between DELETE and INSERT  <- the scary one
maybe_fail("before_commit", chaos)         # after INSERT, before COMMIT
```

`mid_transaction` is the money shot: the old day is deleted, the new one not yet
written — without a transaction this crash *loses a day of data*. Now crash it a lot:

```powershell
python work\a07_load_day.py --scale small --date 2026-06-03 --force --chaos 0.6
```

Run it 8–10 times to start — but fair warning: reaching a later point means surviving
every earlier roll first, so the points get exponentially rarer. At chaos 0.6,
`before_commit` fires on only ~4% of runs; seeing it can easily take 20–30 runs. That
is chance, not a bug in your `maybe_fail` wiring. After **every** crash, run the Task 3 checksum query: count still
62,707, checksum unchanged — even when it died between DELETE and INSERT, because
ROLLBACK undid the delete. Watch your ledger fill with evidence:

```sql
SELECT run_id, status, rows, error FROM ops.etl_runs ORDER BY run_id DESC LIMIT 5;
```

**Accept when:** you have seen it die at all four points (keep rerunning until you
have), and the checksum never moved.

### Task 7 — Retry with exponential backoff (small scale)

Crashing cleanly is good; recovering without a human is better. Add:

```python
def with_retry(fn, max_attempts: int = 8, base_delay: float = 1.0,
               max_delay: float = 30.0):
    """Call fn(); on TransientError wait, then try again. Backoff doubles."""
    for attempt in range(1, max_attempts + 1):
        try:
            return fn()
        except TransientError as exc:
            if attempt == max_attempts:
                log.error("attempt %d/%d failed - giving up", attempt, max_attempts)
                raise
            delay = min(base_delay * 2 ** (attempt - 1), max_delay)
            log.warning("attempt %d/%d failed (%s) - retrying in %.1fs",
                        attempt, max_attempts, exc, delay)
            time.sleep(delay)
```

Add `--max-attempts` (default 8) and change the call in `main` to:

```python
    with_retry(lambda: load_day(con, args.scale, day, chaos=args.chaos),
               max_attempts=args.max_attempts)
```

It catches `TransientError` **only**: a `ValueError` from validation means the *data*
is wrong — retrying cannot fix that and must fail immediately. Journal one sentence on
this distinction; it is the most-ignored rule in retry design.

One piece is missing: a load that has given up for good must be *heard*, not
discovered on a dashboard three days later. §7.3's channel for that is `ops.alerts` —
the table your A05 suite created (`ensure_tables` carries a `CREATE TABLE IF NOT
EXISTS` copy, which is why this script runs even out of order). On final failure —
a `TransientError` that exhausted its attempts *or* a `ValueError` that failed fast —
write one `error` row. Retry policy and alert policy are different decisions:

```python
def write_alert(con, day: date, exc: Exception) -> None:
    """Final failure -> one ops.alerts row (severity error). The ledger already
    knows the last failed attempt; the alert points at it."""
    run_id = con.execute("""
        SELECT max(run_id) FROM ops.etl_runs
        WHERE run_date = ? AND step = 'load_day'""", [day]).fetchone()[0]
    con.execute("""
        INSERT INTO ops.alerts VALUES (now(), ?, 'error', 'a07_load_day', ?)""",
        [run_id, f"day {day}: load failed for good: {exc}"[:500]])
    log.error("ALERT (error) written to ops.alerts, run_id=%s - "
              "in production this pages someone", run_id)
```

…and the call in `main` grows a net under it:

```python
    try:
        with_retry(lambda: load_day(con, args.scale, day, chaos=args.chaos),
                   max_attempts=args.max_attempts)
    except Exception as exc:
        write_alert(con, day, exc)
        raise
```

**Accept when:** a chaos run shows `attempt 1/8 failed ... retrying in 1.0s`, `2.0s`,
`4.0s` — doubling delays — and ends in either a success or a clean give-up; and a
forced give-up (`--chaos 1 --max-attempts 3`) leaves exactly one new `ops.alerts` row,
severity `error`, whose `run_id` is the last failed attempt in `ops.etl_runs`.

### Task 8 — The proof (small scale)

Now prove the property this assignment is named after. In order:

1. Clean reference: `--force` run with no chaos. Record count + checksum.
2. Chaos gauntlet: `--force --chaos 0.3 --max-attempts 10`. Attempts will fail and
   retry; let it run to success. Checksum: **identical to step 1**. (Small chance —
   about 1 in 15 — that all 10 attempts fail. Just run it again, and notice that even
   total failure left the table consistent — and paged you via `ops.alerts`. That is
   the property working.)
3. Repeat step 2 twice more. Checksum: still identical. A run that crashed 5 times on
   the way is indistinguishable from a run that succeeded first try.
4. Skip check: run *without* `--force`. It must log `already loaded - skipping` and
   exit in about a second — your `already_succeeded` query reading the ledger.
5. Traceability — **the payoff.** First look at what a `core.orders` row actually
   carries from the warehouse. `DESCRIBE core.orders`: ten source columns, a derived
   `order_date`, and then just `_data_date` and `_run_id`. Two. That is less than
   your instinct wanted — A03 talked you out of `_source_file` and `_loaded_at` and
   promised you would not miss them. Collect on the promise. One join:

   ```sql
   SELECT o.order_id, o._data_date, o._run_id,
          r.source_file, r.finished_at, r.status, r.rows
   FROM core.orders o
   JOIN ops.etl_runs r ON r.run_id = o._run_id
   WHERE o._data_date = DATE '2026-06-03'
   LIMIT 1;   -- any row gives the same answer, and that is the point:
              -- count(DISTINCT _run_id) over the day = 1
   ```

   ```
   ┌────────────┬────────────┬─────────┬───────────────────────┬────────────────────────────┬─────────┬───────┐
   │  order_id  │ _data_date │ _run_id │      source_file      │        finished_at         │ status  │ rows  │
   ├────────────┼────────────┼─────────┼───────────────────────┼────────────────────────────┼─────────┼───────┤
   │ 1530041035 │ 2026-06-03 │       2 │ orders_2026-06-03.csv │ 2026-08-17 00:52:13.272582 │ success │ 62707 │
   └────────────┴────────────┴─────────┴───────────────────────┴────────────────────────────┴─────────┴───────┘
   ```

   (Your `order_id`, `_run_id` and clock differ; `source_file`, `status` and `rows`
   are exact.) Read the right-hand half again: `source_file` is the file this row
   came from, `finished_at` is when it landed, `status` and `rows` are the run's
   verdict and its size. **The first two are precisely what A03 refused to store on
   the row** — and here they are anyway, for any row you care to pick, at the cost
   of one join.

   Say the win out loud, because it is the whole argument: a fact about the *run* is
   stored **once, on the run**, and 62,707 rows point at it with an 8-byte id —
   instead of 62,707 identical copies of a file name and a timestamp sitting in a
   fact table. That is plain normalization, the same rule that keeps a customer's
   address out of every order row. It also pays maintenance: re-load this day from a
   corrected file and the ledger records the new fact in **one row**; the
   copy-it-everywhere design would have to rewrite the same string 62,707 times to
   fix a single fact — 1.2M times at full scale. And in this feed the file name was never
   even new information: the contract's `path_pattern` is one file per business date,
   so `_data_date` already spells `orders_2026-06-03.csv`. The join is what earns
   its keep for the feeds where it doesn't.

   Every one of the 62,707 rows names the *same* run — the last successful attempt.
   Some failed attempts got as far as INSERT, but ROLLBACK erased their rows; only
   the ledger and their log files remember them. Now open `logs/run_<that id>.log`:
   the row's biography in one file. Row → run → log is the audit chain §7.2/§7.3
   promise, and you just walked it.

**Accept when:** four identical checksums in your journal, a ledger
(`SELECT status, count(*) FROM ops.etl_runs GROUP BY 1;`) showing a pile of `failed`
rows and your `success` rows — the honest history of a resilient pipeline — and the
traceability chain walked once: a two-column row → the ledger (file, finish time,
status, rows) → a real log file. Journal one sentence on why those four facts live
on the run and not on the row.

### Task 9 — Full scale (full scale)

Same code, real weight — one full-scale day is ~350 MB of CSV, ~1.2M rows. Load three
days, no chaos:

```powershell
foreach ($d in "2026-06-01","2026-06-02","2026-06-03") {
  python work\a07_load_day.py --scale full --date $d
}
```

Watch Task Manager while it runs: Python + DuckDB should stay around 1–2 GB — DuckDB
streams the CSV; nothing like the pandas cliff from A01. Each day takes roughly 10–60
seconds on the baseline machine (`finished_at - started_at` in `ops.etl_runs` gives each day's exact duration). Then:

- `--force` re-run 2026-06-01: same duration, same checksum (scope the query to
  `_data_date = DATE '2026-06-01'`). Replacing 1.2M rows is cheap for DuckDB.
- Re-run without `--force`: skips instantly.
- Reconcile each day's `rows` in `ops.etl_runs` against
  `SELECT rows FROM read_json_auto('<DATA_ROOT>/raw/manifest/orders_2026-06-01.json')`
  (and the other two dates). They must match exactly.

**Accept when:** three full days green in the ledger, counts equal to manifests, and
one forced re-run proven identical.

## Definition of Done

- [ ] Naive double-run demo executed; journal explains why plain `INSERT` is unsafe.
- [ ] `ops.etl_runs` exists in its canonical §7.3 shape (`source_file` included);
      every attempt today (including failures) is a row in it, naming its file.
- [ ] `work/a07_load_day.py` stages → validates → publishes data + ledger in one
      transaction, keyed by the `_data_date` lineage column; every published row
      stamped with the two §7.2 columns and no more — `_run_id` now real,
      `_source_file`/`_loaded_at` nowhere on the row; both TODO gaps filled.
- [ ] Every attempt wrote its own `logs/run_<run_id>.log` (§7.3).
- [ ] `work/preflight.py` implements the seven §7.4 checks — one PASS/FAIL line and
      a fix hint each, non-zero exit on any FAIL — and the loader runs it before
      touching anything; check 4 (your gap) filled.
- [ ] Preflight red/green demo done: one check broken on purpose (temp dir aimed at
      `C:`), the red run and its exit code seen, then green again.
- [ ] Pre-chaos savepoint committed; the loader mangled and recovered once via
      `git restore` and once via `git reset --hard`; `git reflog` read.
- [ ] Chaos observed at all four points; checksum unchanged after every crash,
      including `mid_transaction`.
- [ ] Retry wrapper shows doubling backoff and retries only `TransientError`; a
      forced give-up wrote one `ops.alerts` row (severity `error`).
- [ ] Four identical checksums (1 clean + 3 chaos runs) recorded at small scale;
      re-run without `--force` skips via the ledger; traceability demo walked
      (row → `_run_id` → ledger → log file), recovering `source_file`,
      `finished_at`, `status` and `rows` from a row that stores none of them.
- [ ] Full scale: 3 days loaded, `rows` matches each manifest, one forced re-run
      proven identical, memory observation noted in journal.
- [ ] Branch merged to `main` only after `python -m pytest tests/ -q` is green
      (§7.4 ritual — nothing new to write today; your A03 cleaner tests must
      still pass).

## Verify yourself

```sql
-- small scale, after any successful load of 2026-06-03: n = 62,707 exactly (the
-- manifest `rows`); the checksum must never change across re-runs or chaos runs
SELECT count(*) AS n,
       bit_xor(hash(order_id, updated_at, status, order_total)) AS checksum
FROM core.orders WHERE _data_date = DATE '2026-06-03';

-- several 'failed' rows (each error naming a chaos point), plus successes;
-- nothing stuck at 'running' once runs have ended
SELECT status, count(*) FROM ops.etl_runs GROUP BY 1;

-- exactly ONE run owns the day: delete+insert replaces wholesale, so the last
-- successful run stamped every row -> expect 1
SELECT count(DISTINCT _run_id) FROM core.orders
WHERE _data_date = DATE '2026-06-03';

-- the §7.2 column budget: core.orders carries exactly TWO warehouse-added
-- columns -> expect precisely (_data_date, DATE) and (_run_id, BIGINT).
-- Any _source_file / _loaded_at here means you stamped run facts onto rows.
SELECT column_name, column_type FROM (DESCRIBE core.orders)
WHERE column_name LIKE '\_%' ESCAPE '\';

-- ...and everything they replaced is one join away, for every row of the day
SELECT count(*) AS rows_traced FROM core.orders o
JOIN ops.etl_runs r ON r.run_id = o._run_id
WHERE o._data_date = DATE '2026-06-03'
  AND r.source_file = 'orders_2026-06-03.csv'
  AND r.finished_at IS NOT NULL;      -- expect 62,707: no row is an orphan

-- after the Task 7 give-up demo: newest alert has severity 'error' and a run_id
-- equal to the last failed run in ops.etl_runs
SELECT severity, run_id, message FROM ops.alerts ORDER BY alert_ts DESC LIMIT 1;

-- torn-write check: per loaded date, count is either 0 (never succeeded) or the
-- full manifest number - anything in between you should now be unable to produce
SELECT _data_date, count(*) FROM core.orders GROUP BY 1 ORDER BY 1;
```

Task 1's naive table: 62,707 → 125,414 after the double run. Full scale: each day's
count must equal its manifest `rows` (roughly 1.2M on weekdays).

```powershell
# preflight: seven PASS lines, then exit code 0
python work\preflight.py --scale small --date 2026-06-03; $LASTEXITCODE

# broken on purpose: check 3 FAILs with a fix hint, then exit code 1 - and the
# same red/green flip must hold for your check 4 with a --date that has no file
python work\preflight.py --scale small --date 2026-06-03 --tmp C:\Windows\Temp; $LASTEXITCODE
```

## Common beginner mistakes

- **Recording success outside the transaction.** Update the ledger inside
  `BEGIN…COMMIT` — otherwise a crash between the two makes the ledger lie, and ledger
  lies become skipped or doubled days later.
- **Deleting by `order_date` instead of `_data_date`.** Remember A02: ~1.5% of a
  file's rows are late rows belonging to *earlier* order dates. Delete by `order_date`
  and every re-run duplicates the late rows. The idempotency key is "what did this
  file write" (`_data_date`), not "what dates does it mention" (`order_date`).
- **Retrying deterministic failures.** A validation error retried 8 times is the same
  error, 8 times slower. Catch `TransientError`, let `ValueError` fly.
- **Bolting `_source_file` / `_loaded_at` back onto `core.orders`.** It feels
  convenient — no join needed. It is denormalized log data in a fact table: the same
  string copied 62,707 times (1.2M at full scale), and a re-load from a corrected
  file leaves every copy stale. Those facts belong to the run. Two lineage columns
  on the row, everything else via `JOIN ops.etl_runs` (Task 8).
- **Swallowing exceptions.** `except Exception: pass` turns a failed load into a
  silently empty day. Record the failure, then re-raise.
- **Forgetting ROLLBACK in the error path.** After a failure inside a transaction, the
  connection still holds the aborted transaction and the ledger `UPDATE` fails too.
  Roll back first (guarded — there may be no transaction open), then record.
- **Testing only the happy re-run.** Run-twice proves half the property. Until you
  have crashed *between* DELETE and INSERT and checked the table, you have not tested
  idempotency.

## Stretch goals

1. **Wire in your gates.** `work/preflight.py` clears the *environment*; your A06
   contract gate and A05 validation suite judge the *data*. Chain them after
   preflight, each recorded as its own `step` in `ops.etl_runs` — the shape of a
   real multi-step pipeline.
2. **Kill it for real.** Exceptions are polite; processes die rudely. Add
   `time.sleep(10)` at the `mid_transaction` point, run a full-scale day, and
   `Stop-Process` the python process from a second terminal during the sleep. Reopen
   the warehouse: the day is intact — DuckDB's atomicity survives process death.
3. **Add jitter.** If 50 workers all fail at once and retry after exactly 1s, they
   stampede the struggling system together ("thundering herd"). Add
   `delay *= random.uniform(0.5, 1.5)` and note why staggered retries are kinder.
