# A09 — Atomic partition swap
**Topic:** partition atomic swap · **Est. time:** 3h · **Difficulty:** ⭐⭐⭐
**Requires:** A08 done; month-1 (era v1) Parquet lake at both scales from A02; `warehouse.duckdb` with a `core` schema from A03+.

## Why this matters

In A08 you refreshed partitions with delete+insert inside a transaction, so nobody could ever see the half-done state. Your Parquet lake has no transactions — it is just files in folders. While you rewrite a partition, anyone reading it at that moment (a dashboard, a teammate, your own next pipeline step) sees whatever files happen to exist right then: partial data, duplicates, or an error. Worst of all, there is no warning that the answer is wrong. And this is not perfectionism: the shopcore orders contract's consumer block declares the data finance-grade and rates double counting a sev-2 — atomicity is a contractual duty. Today you learn to publish rewrites so readers see the old version or the new version — never a mix.

## Background

**Atomic** means all-or-nothing. An atomic rewrite is one where every reader sees either the complete old data or the complete new data. A read that catches the in-between state is called a **torn read**.

Why does an in-place rewrite tear? Because it is two separate steps with a time gap between them:

- **Delete old files, then write new ones.** During the gap, readers see fewer rows, zero rows, or an "IO Error: Cannot open file" if a file vanishes (or is still being written) while DuckDB scans it.
- **Write new files first, then delete the old ones.** During the gap, readers see old and new together — double counts.

Changing the order does not help; the gap itself does the damage. And the gap is as long as your slowest step. Rebuilding one spike-day partition means scanning eight days of raw CSV (late corrections for day D arrive in files D..D+7 — you measured this in A08). At full scale that is a window of tens of seconds, and it happens on every scheduled refresh, forever. Someone *will* query during it.

The fix is one idea, applied at three levels: **never modify what readers are looking at. Build the new version somewhere invisible, then publish it with one cheap, near-instant switch.**

1. **Staging-dir swap (file level).** Write the new partition into a staging folder that readers' globs cannot see. Then `os.rename` it into place. A rename only rewires directory metadata — it takes microseconds no matter how big the folder is, and on a single volume it either fully happens or doesn't.
2. **Table swap (warehouse level).** Build `orders_new` as a real table while readers keep using `orders`, then rename both tables inside one transaction. DuckDB's catalog makes this genuinely atomic.
3. **Versioned dirs + view repoint (lake + catalog level).** Never touch a published folder at all. Each rewrite publishes a whole new version folder (`orders_v=<ts>/`), and a view is re-created to point at the newest one. This is a poor man's **Apache Iceberg / Delta Lake** — the industrial table formats do exactly this, except the "current version" pointer lives in metadata files and only changed data files are rewritten.

Two honest caveats about pattern 1, especially on Windows:

- Replacing a live folder takes **two** renames (live → trash, then staging → live). Two renames are not one atomic step: for a few milliseconds the partition does not exist. Readers must treat "no data for a day I know has data" as *try again*, not as an answer. That mitigation is called **retry-on-read**.
- A rename fails with `PermissionError` while any process holds a file inside that folder open — a reader mid-scan, the Windows search indexer, an antivirus. The fix is unglamorous: retry a few times.

One rule throughout: staging and trash folders must live **on the same drive** as the lake. A cross-drive "rename" silently becomes copy + delete — slow and not remotely atomic.

## Setup

- Two terminals at the repo root (one plays the reader, one the writer), plus Task Manager for the full-scale runs.
- Develop everything at **small** scale first (Tasks 1–6), rerun at **full** scale (Task 7). Month-1 / era-v1 data only.
- Victim partition: the spike day `order_date=2026-06-19`. Big enough to make rewrites slow — exactly when torn reads bite.
- Connection settings: writers use `SET memory_limit='8GB'; SET threads=8;`. The reader script uses `2GB` — counting needs almost nothing, and being a polite memory citizen is a habit A12 will reward.
- This assignment touches only that one partition, plus `.staging`/`.trash` dirs and (Task 9, small scale only) temporary `orders_v=*` folders. Everything else stays untouched.
- Scripts live in `work/`. Run them from the repo root.

## Tasks

### Task 1 — Know your before state (small)

Read the day's manifest and look at the partition folder before touching anything:

```sql
SELECT rows, late_rows, dup_rows
FROM read_json_auto('<DATA_ROOT>/small/raw/manifest/orders_2026-06-19.json');
-- 182151, 2732, 91  (deterministic — the generator is seeded per date)
```

```powershell
Get-ChildItem "$env:ETL_LAB_DATA\small\lake\orders\order_date=2026-06-19"
```

You should see several Parquet files: the big one from loading 06-19 itself, plus small ones appended when days 06-20..06-26 delivered late corrections (the A02 discovery moment). Record the file count and the row count of the partition in your journal.

**Accept when:** manifest numbers match the ones above; before-count noted (roughly 180k rows at small scale — fewer than the manifest's 182,151 because the file's own late rows live in earlier partitions). If the partition is suspiciously tiny instead, see Common beginner mistakes.

### Task 2 — Build the reader (small)

The reader simulates the innocent analyst. It polls one count in a loop and prints only when the answer *changes* — so any line after the first is news.

Create `work/a09_common.py`:

```python
# work/a09_common.py -- shared pieces for A09
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # repo root, for lib
from lib.labpaths import raw_orders_dir

DAY = "2026-06-19"          # month-1 spike day, era v1

V1_COLUMNS = ("{'order_id':'BIGINT','customer_id':'BIGINT','store_id':'INTEGER',"
              "'order_ts':'VARCHAR','updated_at':'TIMESTAMP','status':'VARCHAR',"
              "'payment_method':'VARCHAR','order_total':'VARCHAR',"
              "'items':'VARCHAR','meta':'VARCHAR'}")


def rebuild_sql(scale: str, day: str = DAY) -> str:
    """SELECT that rebuilds one order_date partition from raw.

    Late corrections for day D arrive in files D..D+7 (SPEC 3.4), so scan those
    files, keep day-D rows only, and keep the latest updated_at per order_id --
    the dedupe you built in A08.
    """
    d0 = date.fromisoformat(day)
    files = [raw_orders_dir(scale) / f"orders_{d0 + timedelta(days=k)}.csv"
             for k in range(8)]
    files = [f.as_posix() for f in files if f.exists()]
    return f"""
    WITH src AS (
      SELECT *,
             try_strptime(order_ts,
                  ['%Y-%m-%d %H:%M:%S', '%d/%m/%Y %H:%M:%S',
                   '%Y-%m-%dT%H:%M:%S']) AS order_ts_clean,
             CAST(order_ts_clean AS DATE) AS order_date
      FROM read_csv({files}, header=true, columns={V1_COLUMNS})
      WHERE order_date = DATE '{day}'
    ),
    ranked AS (
      SELECT *, row_number() OVER (PARTITION BY order_id
                                   ORDER BY updated_at DESC) AS rn
      FROM src
    )
    SELECT * EXCLUDE (order_date, rn) FROM ranked WHERE rn = 1
    """
    # EXCLUDE (order_date): the folder name carries the partition value. Files
    # written by PARTITION_BY do not contain that column, so ours must not either.
    # order_ts_clean is the opposite case: the A02 loader wrote it into every
    # lake file, and a glob read wants one schema across all files -- drop it
    # here and every multi-partition read (Task 8, A10) dies with a schema
    # mismatch. A rebuilt file must match the lake's file schema exactly.
```

Create `work/a09_reader.py`:

```python
# work/a09_reader.py -- the innocent analyst: polls one partition's row count
# usage: python work/a09_reader.py <scale> [seconds] [--retry]
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # repo root, for lib
import duckdb
from lib.labpaths import lake_dir
from a09_common import DAY

scale = sys.argv[1]
seconds = float(sys.argv[2]) if len(sys.argv) > 2 and sys.argv[2] != "--retry" else 30

glob = f"{(lake_dir(scale) / 'orders').as_posix()}/*/*.parquet"
con = duckdb.connect()
con.execute("SET memory_limit='2GB';")   # a polite reader: counting needs almost nothing
q = (f"SELECT count(*) FROM read_parquet('{glob}', hive_partitioning=true) "
     f"WHERE order_date = DATE '{DAY}'")


def read_once():
    return con.execute(q).fetchone()[0]


def read_with_retry(attempts=15, wait=0.3):
    """Treat 'no rows' and 'file vanished' as 'a swap is in flight, try again'.
    We KNOW this partition has data, so 0 can only mean mid-swap."""
    last = None
    for _ in range(attempts):
        try:
            n = read_once()
            if n > 0:
                return n
            last = n
        except Exception as e:
            last = e
        time.sleep(wait)
    if isinstance(last, Exception):
        raise last
    return last


print(f"watching order_date={DAY} ({scale}) for {seconds:.0f}s", flush=True)
prev, t_end = object(), time.time() + seconds
while time.time() < t_end:
    ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    try:
        n = read_with_retry() if "--retry" in sys.argv else read_once()
        out = f"count={n}"
    except Exception as e:
        out = f"{type(e).__name__}: {str(e)[:90]}"
    if out != prev:                      # print only when the answer CHANGES
        print(f"{ts}  {out}", flush=True)
        prev = out
    time.sleep(0.1)
```

Run `python work/a09_reader.py small 10` against the untouched lake.

**Accept when:** exactly one `count=...` line prints — a stable lake is a quiet reader.

### Task 3 — Break it: the naive in-place rewrite (small)

Create `work/a09_rewrite_naive.py` — delete the old files, then write the new ones straight into the live folder:

```python
# work/a09_rewrite_naive.py -- partition rewrite done WRONG: in place
# usage: python work/a09_rewrite_naive.py <scale>
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # repo root, for lib
import duckdb
from lib.labpaths import lake_dir
from a09_common import DAY, rebuild_sql

scale = sys.argv[1]
part = lake_dir(scale) / "orders" / f"order_date={DAY}"
con = duckdb.connect()
con.execute("SET memory_limit='8GB'; SET threads=8;")

t0 = time.time()
for f in part.glob("*.parquet"):         # step 1: delete the old files
    f.unlink()
print(f"old files gone at t={time.time() - t0:.1f}s", flush=True)

con.execute(f"COPY ({rebuild_sql(scale)}) "                   # step 2: write the new
            f"TO '{(part / 'data_0.parquet').as_posix()}' (FORMAT parquet)")
print(f"new file ready at t={time.time() - t0:.1f}s  <- that was the torn window",
      flush=True)
```

Terminal 1: `python work/a09_reader.py small 20`. Wait for the first count, then in terminal 2: `python work/a09_rewrite_naive.py small`. Copy the reader's output into your journal. On the baseline machine expect something like:

```
count=180183        <- your before count
count=0             <- partition gone
IOException: IO Error: Cannot open file "...data_0.parquet"   <- half-written file
count=179294        <- new count, roughly 2-3s after the delete
```

The exact sequence varies per run — partial counts, zero, or errors, depending on when each poll lands. Every one of them was served to "the analyst" as a perfectly normal answer.

**Accept when:** your journal shows at least one wrong intermediate answer between the old and new counts.

### Task 4 — Why staging must hide: the leaky tmp dir (small)

Before writing the fix, see the classic mistake: staging the new files *inside* the live tree, e.g. `order_date=2026-06-19.__tmp__`. Simulate it with a copy:

```powershell
$lake = "$env:ETL_LAB_DATA\small\lake\orders"
Copy-Item -Recurse "$lake\order_date=2026-06-19" "$lake\order_date=2026-06-19.__tmp__"
python work\a09_reader.py small 5        # what does the analyst see now?
Remove-Item -Recurse -Force "$lake\order_date=2026-06-19.__tmp__"
```

The count **doubles**. Two things went wrong at once: the readers' glob `*/*.parquet` happily matches the tmp folder, and the filter `order_date = DATE '2026-06-19'` still matches it because DuckDB's DATE cast ignores trailing text — `CAST('2026-06-19.__tmp__' AS DATE)` is `2026-06-19`. Try it. A dot-prefixed folder (`.tmp...`) inside the tree is even worse: DuckDB's glob matches it too, and the mismatched folder name breaks *every* reader with a `Hive partition mismatch` binder error.

Conclusion: staging must live where the glob cannot reach — a sibling of `orders/`, not inside it.

**Accept when:** you saw the doubled count, and can explain it in one sentence.

### Task 5 — The staging-dir swap (small)

Create `work/a09_swap.py`:

```python
# work/a09_swap.py -- partition rewrite done RIGHT: build aside, publish by rename
# usage: python work/a09_swap.py <scale> [--nap 2] [--crash] [--recover]
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # repo root, for lib
import duckdb
from lib.labpaths import lake_dir
from a09_common import DAY, rebuild_sql

scale = sys.argv[1]
lake_root = lake_dir(scale)
live = lake_root / "orders" / f"order_date={DAY}"
staging = lake_root / ".staging" / f"order_date={DAY}"    # OUTSIDE the readers' glob
trash = lake_root / ".trash" / f"order_date={DAY}.{int(time.time())}"


def build_staging():
    if staging.exists():                 # leftover from a dead run: throw it away
        for f in staging.glob("*"):
            f.unlink()
        staging.rmdir()
    staging.mkdir(parents=True)
    con = duckdb.connect()
    con.execute("SET memory_limit='8GB'; SET threads=8;")
    con.execute(f"COPY ({rebuild_sql(scale)}) "
                f"TO '{(staging / 'data_0.parquet').as_posix()}' (FORMAT parquet)")
    print("staging ready (readers never saw a thing)", flush=True)


def rename_with_retry(src: Path, dst: Path, attempts=20, wait=0.25):
    """Windows: a rename fails with PermissionError while ANY process holds a
    file inside src open (a reader mid-scan, the indexer, an antivirus).
    Readers hold files for milliseconds, so a short retry loop wins."""
    for i in range(attempts):
        try:
            os.rename(src, dst)
            return
        except PermissionError:
            if i == attempts - 1:
                raise
            time.sleep(wait)


def swap():
    trash.parent.mkdir(parents=True, exist_ok=True)
    t0 = time.time()
    rename_with_retry(live, trash)            # rename 1: live -> trash
    if "--crash" in sys.argv:
        print("simulated crash between the renames", flush=True)
        os._exit(1)
    if "--nap" in sys.argv:                   # widen the gap on purpose (Task 6)
        time.sleep(float(sys.argv[sys.argv.index("--nap") + 1]))
    rename_with_retry(staging, live)          # rename 2: staging -> live
    print(f"published in {time.time() - t0:.3f}s (old kept in .trash/{trash.name})",
          flush=True)


def recover():
    """Crash-safe because the new data (staging) and the old data (trash) are
    never both gone. Whatever survived decides the fix."""
    if live.exists():
        print("live partition present - nothing to recover")
        return
    if staging.exists() and any(staging.glob("*.parquet")):
        os.rename(staging, live)
        print("finished the swap: staging -> live")
        return
    trashes = sorted((lake_root / ".trash").glob(f"order_date={DAY}.*"))
    if trashes:
        os.rename(trashes[-1], live)
        print("rolled back: newest trash -> live")
        return
    raise RuntimeError("no live, no staging, no trash: rebuild from raw (A11 skill)")


if __name__ == "__main__":
    if "--recover" in sys.argv:
        recover()
    else:
        build_staging()
        swap()
```

Reader in terminal 1 (20s), then `python work/a09_swap.py small` in terminal 2. This time the reader prints exactly one line and stays there. Not two: Task 3 already replaced this partition with the rebuilt content, and the rebuild is deterministic — old and new counts coincide at 179,294, so the print-only-on-change reader has nothing new to say. The win is what is *absent*: no zeros, no IO errors, no partial counts, while the same rebuild that tore Task 3's reads ran start to finish. The slow part happened off-stage; publishing took milliseconds (~0.004s on the baseline machine). Task 6 widens the gap so you can see the swap itself.

**Accept when:** the reader holds one steady count while the swap runs — no zeros, no errors, no partial counts — and the script printed `published in` some milliseconds.

### Task 6 — The gap is real: nap, retry-on-read, crash, recover (small)

The swap is *near*-atomic: between the two renames the partition does not exist. The window is normally ~4 milliseconds — your 0.1s-polling reader will basically never catch it. Prove it exists anyway by widening it:

1. Reader without retry + `python work/a09_swap.py small --nap 2` → the reader prints `count=0` during the nap. A 2-second outage, served as a valid answer.
2. Reader **with** `--retry` + the same `--nap 2` swap → one steady count. The retry loop (15 × 0.3s) simply outlasts the gap. That is retry-on-read: cheap on the reader side, and it converts "wrong answer" into "answer arrives a beat later".
3. Crash safety: run `python work/a09_swap.py small --crash`. The script dies between the renames — the worst possible moment. Confirm the live partition is gone (`Get-ChildItem`), then run `python work/a09_swap.py small --recover` and re-count. Read `recover()` and note the invariant that makes it safe: at every step, staging or trash still holds a complete copy.

**Accept when:** you observed 1–3, and recovery restored a queryable partition with the expected count.

### Task 7 — Now at full scale (full)

Repeat Tasks 3 and 5 with `full` instead of `small`. Before you start, note the full manifest numbers: the spike day has 3,643,024 rows (~1.1 GB of CSV), and the rebuild scans eight full days.

- Naive rewrite: the reader now shows wrong answers for a long stretch — the rebuild took roughly 11s on our machine; expect roughly 10–60s depending on your disk. While it runs, watch Task Manager: python/DuckDB pulling several GB of RAM and all 8 cores during the CSV scan + dedupe. *This* is the torn window your users would live inside every single day.
- Staged swap: the rebuild still takes just as long — but readers don't care, and publishing is still milliseconds. Journal both timings side by side: `rebuild seconds` vs `publish seconds`. That ratio is the whole lesson.

**Accept when:** torn reads observed at full scale with the naive script; a steady, error-free reader through the swap; both timings in your journal.

### Task 8 — Warehouse table swap (small)

Inside `warehouse.duckdb` you do not need rename tricks on files — the catalog gives you real atomicity. Build the replacement as a new table, then rename both tables in one transaction. Run this as a script or in a Python session:

```python
import duckdb
from lib.labpaths import lake_dir, warehouse_path

writer = duckdb.connect(str(warehouse_path("small")))
writer.execute("SET memory_limit='8GB'; SET threads=8;")
reader = duckdb.connect(str(warehouse_path("small")))    # second connection = analyst

glob = (lake_dir("small") / "orders").as_posix() + "/*/*.parquet"
writer.execute(f"""
CREATE OR REPLACE TABLE core.orders_a09 AS
SELECT * FROM read_parquet('{glob}', hive_partitioning=true)
""")
writer.execute("""
CREATE OR REPLACE TABLE core.orders_a09_new AS
SELECT * FROM core.orders_a09 WHERE order_id % 1000 != 0   -- visibly different
""")

writer.execute("BEGIN TRANSACTION")
writer.execute("ALTER TABLE core.orders_a09 RENAME TO orders_a09_old")
writer.execute("ALTER TABLE core.orders_a09_new RENAME TO orders_a09")
print("mid-swap  :", reader.execute("SELECT count(*) FROM core.orders_a09").fetchone())
writer.execute("COMMIT")
print("post-commit:", reader.execute("SELECT count(*) FROM core.orders_a09").fetchone())
writer.execute("DROP TABLE core.orders_a09_old")
```

Both `orders_a09` tables are swap props read straight out of the lake, so neither carries the §7.2 lineage pair: the lake files hold no `_data_date`, and nothing here registers a run to hand out a `_run_id`. The `core.orders` you built in A07 carries both — these two exist only to be renamed at each other and dropped.

Mid-transaction, the reader connection still sees the **old** table, complete and correct; the instant after COMMIT it sees the new one. Both renames became visible together — that is what "atomic" buys you when a catalog manages the names. (Caveat for later: this protects connections to the warehouse. A *separate process* can't even open the file while your writer holds it — the single-writer rule you will meet head-on in A12. And it does nothing for Parquet readers: files are outside the catalog.)

**Accept when:** mid-swap prints the old count, post-commit the new one, and you can say why the file lake can't give you this for free.

### Task 9 — Versioned dirs + view repoint (small only)

The third pattern never modifies a published folder at all. Turn your lake into version 1, point a view at it, publish a version 2, repoint:

```python
import shutil, time
import duckdb
from lib.labpaths import lake_dir, warehouse_path

root = lake_dir("small")
con = duckdb.connect(str(warehouse_path("small")))

v1 = int(time.time())
(root / "orders").rename(root / f"orders_v={v1}")        # instant, same volume
con.execute(f"""
CREATE OR REPLACE VIEW core.lake_orders AS
SELECT * FROM read_parquet('{(root / f"orders_v={v1}").as_posix()}/*/*.parquet',
                           hive_partitioning=true)
""")
# publish version 2: copy v1, then rewrite the partition(s) you need inside it
v2 = v1 + 1
shutil.copytree(root / f"orders_v={v1}", root / f"orders_v={v2}")
# ... rebuild order_date=2026-06-19 inside orders_v=<v2> here (reuse rebuild_sql) ...
con.execute(f"""
CREATE OR REPLACE VIEW core.lake_orders AS
SELECT * FROM read_parquet('{(root / f"orders_v={v2}").as_posix()}/*/*.parquet',
                           hive_partitioning=true)
""")
```

From now on consumers query `core.lake_orders` and never a folder path. `CREATE OR REPLACE VIEW` is a catalog change — atomic, like Task 8. Old versions stay around: free rollback and "what did the data look like yesterday?" for the price of disk.

And that price is the catch: `copytree` duplicated your whole small lake (a few hundred MB, feel how long it takes) — at full scale that would be several GB per publish. Sit with that pain for a second: *this* is why Iceberg and Delta Lake exist. They keep the versioned-pointer idea but track individual files in metadata, so version N+1 reuses every unchanged file and only the rewritten partition costs disk.

**Cleanup (do not skip):** later assignments expect the standard layout. Drop the view, delete `orders_v=<v2>`, and rename `orders_v=<v1>` back to `orders`.

**Accept when:** the view flipped versions atomically; the lake is back at `<DATA_ROOT>/small/lake/orders`.

## Definition of Done

- [ ] Torn-read timeline from the naive rewrite captured in the journal, at small **and** full scale (wrong counts and/or IO errors between old and new).
- [ ] The `.__tmp__` leak reproduced and explained in one sentence.
- [ ] Staged swap keeps the running reader on one steady, error-free count at both scales; publish time ~milliseconds while rebuild time is seconds.
- [ ] `--nap 2` exposes the rename gap; the `--retry` reader rides through it silently.
- [ ] `--crash` then `--recover` leaves a queryable partition with the correct count.
- [ ] Warehouse table swap: reader sees old count mid-transaction, new count after COMMIT.
- [ ] View repointed across two lake versions; standard layout restored afterwards.
- [ ] Journal: rebuild vs publish timings at both scales.

## Verify yourself

```sql
-- manifest (deterministic; small scale)
SELECT rows, late_rows, dup_rows
FROM read_json_auto('<DATA_ROOT>/small/raw/manifest/orders_2026-06-19.json');
-- -> 182151, 2732, 91          (full scale: 3643024, 54645, 1821)
```

```powershell
# rebuilt partition, small scale (exactly, with the rebuild_sql given above):
python work\a09_reader.py small 5
# -> count=179294               (full scale: roughly 3,586,000)
```

- Before the rewrite the partition holds roughly 180k rows (small; A02 measured it exactly: 180,183) — day-19 rows plus appended late-correction files. After: 179,294 — the delta is the in-file duplicates and collapsed late corrections, per A08.
- Naive rewrite reader log: old count → (0 / partial / `IOException`) → new count. Swap reader log: one steady count and nothing else (Task 3 already left the rebuilt 179,294 in place, so there is no flip to see — the absence of garbage is the result).
- `python work/a09_swap.py small --crash` then `--recover` → `finished the swap: staging -> live`, and the count above.
- Task 8 prints the mid-swap (old) count, then a post-commit count that is visibly smaller.

## Common beginner mistakes

- **Staging inside the live tree.** `order_date=D.__tmp__` doubles wildcard readers' counts (DuckDB's DATE cast ignores trailing text), and a dot-prefixed folder breaks every reader with a `Hive partition mismatch`. Staging goes outside the glob, full stop.
- **Trash or staging on a different drive.** The "rename" quietly becomes copy + delete: minutes of non-atomic exposure instead of microseconds. Same volume, always.
- **"Rewriting" by COPYing into the live folder with `OVERWRITE_OR_IGNORE`.** Readers see old + new duplicates while it runs — and it can silently overwrite an existing `data_0.parquet`, eating the late-correction files your A02 loader appended. (If your Task 1 before-count was strangely tiny, your A02 loader already did this to you.)
- **Deleting the old version immediately.** Keep the trashed generation until the new one is verified — it *is* your rollback. A janitor can prune old trash later.
- **No retry on renames.** On Windows the first `PermissionError` (reader mid-scan, indexer, antivirus) will crash a retry-less swap script at the worst moment.
- **Trusting `BEGIN...COMMIT` to protect Parquet readers.** Transactions guard the catalog (tables, views). Files on disk are on their own — that's the whole reason patterns 1 and 3 exist.

## Stretch goals

1. **Pointer file (mini-Iceberg).** Write a `_current.json` next to the partition listing the exact Parquet files that make up the current version. Readers read the pointer, then scan only those files — no globs. Publishing = `os.replace` of one small file, which *is* atomic on Windows: the two-rename gap disappears entirely. Congratulations, you have reinvented the core of a table format's snapshot.
2. **Trash janitor.** A script that deletes `.trash`/old `orders_v=*` entries older than N days but always keeps the most recent one. When would you dare set N=0? (Answer: never.)
3. **Hammer test.** Run the reader at 0.01s intervals while looping 20 swaps. Count how often `rename_with_retry` actually retries — measure how real the Windows file-lock caveat is on your machine.
