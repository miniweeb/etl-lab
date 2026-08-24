# A13 — Resource monitoring & optimization
**Topic:** resource usage · **Est. time:** 4h · **Difficulty:** ⭐⭐⭐⭐
**Requires:** A12 done; the full 69-day canonical lake (A10/A11) and both scales of raw data on disk; ~25 GB free on the data drive.

## Why this matters

In A12 you did capacity math on paper: workers × memory limits ≤ 16 GB. Today you
check the paper against reality, because production data engineers live by one rule:
**you cannot tune what you cannot see.** A pipeline that "feels slow" or "sometimes
crashes" stays broken until someone measures where the RAM, CPU and disk actually
went — and in the cloud, every GB of RAM you over-provision is a line on an invoice.

## Background

**The four resources.** Every run consumes CPU, memory, disk IO (bytes read/written),
and disk space. At any moment exactly one of them is the *bottleneck* — the thing the
run is waiting on. Tuning means finding that one and fixing it, not guessing.

**RSS.** Resident Set Size: the RAM your process physically occupies right now — the
number that matters for "will this fit in 16 GB?". Task Manager shows a close cousin;
`psutil` gives the real thing, scriptable. One subtlety: a pipeline is often a *tree*
of processes (your A12 pool: one parent, N children) — the parent alone misses most.

**Why `memory_limit` exists.** Without a limit, a big sort or join simply grows until
the OS starts *paging* — moving memory to disk behind your back — and the whole machine
turns to glue. DuckDB's `memory_limit` is a contract: "I will never hold more than this
in RAM. If I need more, I will **spill**."

**Spill.** When an operation's working set (say, sorting 82M rows) exceeds the limit,
DuckDB writes intermediate chunks to temp files in `temp_directory` and processes
them piece by piece — *out-of-core* processing. Slower than staying in RAM, but it
finishes, and it never takes the machine down. Today you watch it happen.

**OOM.** "Out of memory" comes in two flavors, and the difference is the whole lesson.
The *clean* flavor: DuckDB decides the limit cannot be honored and raises
`OutOfMemoryException`; your script catches it, logs it, the machine is fine. The
*ugly* flavor: pandas (or any grow-until-it-dies tool) eats RAM until Windows pages,
the mouse stutters, and something crashes — maybe your process, maybe another one.
You met this in A01; today you approach the cliff with a guard rail.

## Setup

- **Two** PowerShell windows at the repo root (venv active in both): one runs
  workloads, one watches. **Task Manager** open on the Details tab.
- Roughly 25 GB free on the data drive (the spill lab needs it). Short on space?
  Everything under `lake/.trash/` (A09/A10's retired dirs, both scales) and
  `tmp/backfill_stage/` (A12) is safe to delete now — `raw/`, the current
  `orders_v=` lake, and `warehouse.duckdb` are all later assignments need.
- The memory limit IS today's experiment — set it per task instead of the usual fixed
  `8GB`, always with `SET temp_directory='<DATA_ROOT>/tmp'` and `SET threads=8`.
- Scales: develop every script on **small** first (seconds per run), then measure on
  **full**. Each task says which scale its numbers refer to.
- Git ritual (§7.4): `git switch -c a13-resource-monitoring`, commit after each
  task, merge to `main` when the DoD is green — `guides/git_basics.md` if stuck.

## Tasks

### Task 1 — Build the monitor: `work/resmon.py` (develop on small)

Save this as `work/resmon.py`. Read it before you run it — the comments marked
*important* are the difference between a monitor that works and one that lies.

```python
"""resmon.py -- run a command, sample its resource usage, write a CSV.

Usage (everything after `--` is the command to run and monitor):
    python work/resmon.py --out monitor.csv -- python my_pipeline.py --date 2026-06-05
"""
import argparse, csv, subprocess, sys, time
from pathlib import Path

import psutil


class ProcessTree:
    """Samples a process and all its children. Important: keep ONE
    psutil.Process object per pid for the whole run -- cpu_percent(interval=None)
    measures "CPU since the last call on this same object", so a fresh
    object would always report 0."""

    def __init__(self, pid: int):
        self.cache: dict[int, psutil.Process] = {pid: psutil.Process(pid)}
        self.root_pid = pid

    def sample(self) -> dict | None:
        root = self.cache.get(self.root_pid)
        if root is None:
            return None
        try:
            for child in root.children(recursive=True):
                self.cache.setdefault(child.pid, child)
        except psutil.NoSuchProcess:
            return None                       # the command already exited
        rss, cpu, read_b, write_b, alive = 0, 0.0, 0, 0, 0
        for pid, p in list(self.cache.items()):
            try:
                with p.oneshot():             # one syscall batch per process
                    rss += p.memory_info().rss
                    cpu += p.cpu_percent(interval=None)
                    io = p.io_counters()
                    read_b += io.read_bytes
                    write_b += io.write_bytes
                    alive += 1
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                del self.cache[pid]           # that process has exited
        return {"rss": rss, "cpu_pct": round(cpu, 1),
                "read_bytes": read_b, "write_bytes": write_b, "nprocs": alive}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="monitor.csv")
    ap.add_argument("cmd", nargs=argparse.REMAINDER)
    args = ap.parse_args()
    cmd = args.cmd[1:] if args.cmd and args.cmd[0] == "--" else args.cmd
    if not cmd:
        ap.error("give a command to run after --")

    child = subprocess.Popen(cmd)
    tree = ProcessTree(child.pid)
    t0 = time.time()
    rows: list[dict] = []
    while child.poll() is None:
        snap = tree.sample()
        if snap is not None:
            snap["t_s"] = round(time.time() - t0, 2)
            rows.append(snap)
        time.sleep(0.5)                       # the sampling interval
    wall = time.time() - t0

    with Path(args.out).open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["t_s", "rss", "cpu_pct",
                                          "read_bytes", "write_bytes", "nprocs"])
        w.writeheader()
        w.writerows(rows)

    if not rows:
        print("resmon: command finished before the first sample")
        return child.returncode
    # important: the first cpu_percent() reading is always 0 -- skip sample one
    cpu_rows = rows[1:] or rows
    print("--- resmon summary ---")
    print(f"wall clock : {wall:8.1f} s")
    print(f"peak RSS   : {max(r['rss'] for r in rows) / 2**30:8.2f} GB")
    print(f"avg CPU    : {sum(r['cpu_pct'] for r in cpu_rows) / len(cpu_rows):8.0f} %"
          "   (100 = one core busy)")
    # max, not last: when a worker exits, its IO counters leave the sum
    print(f"disk read  : {max(r['read_bytes'] for r in rows) / 2**30:8.2f} GB")
    print(f"disk write : {max(r['write_bytes'] for r in rows) / 2**30:8.2f} GB")
    print(f"samples    : {len(rows):8d}   -> {args.out}")
    return child.returncode


if __name__ == "__main__":
    sys.exit(main())
```

Smoke-test it on something harmless (any scale):

```powershell
python work\resmon.py --out work\mon_test.csv -- python -c "import time; x = bytearray(300*2**20); time.sleep(3)"
```

**Accept when:** the summary prints, peak RSS is roughly 0.3 GB (the bytearray), and
`work\mon_test.csv` has one row per half-second.

### Task 2 — Analyze the monitor CSV with DuckDB (small)

You built a monitor that emits CSV. You spend your days analyzing CSVs. Connect the
dots — using your own pipeline's exhaust as data is called *dogfooding*:

```sql
SELECT
  round(max(rss) / pow(2,30), 2)          AS peak_rss_gb,
  round(avg(cpu_pct), 0)                  AS avg_cpu_pct,
  round(max(read_bytes) / pow(2,30), 2)   AS disk_read_gb,
  round(max(t_s), 1)                      AS wall_s
FROM read_csv('work/mon_test.csv', header=true);
```

Then write two queries yourself (practice, not new syntax): peak RSS per 30-second
bucket (`CAST(t_s/30 AS INTEGER)*30`, GROUP BY), and "seconds spent above 4 GB"
(each row is one 0.5 s sample: `count(*) * 0.5` with `WHERE rss > 4*pow(2,30)`).

**Accept when:** you can answer, from SQL alone: what was the peak RSS, and in which
30-second window did it happen?

### Task 3 — Point it at your real pipeline (full scale)

Now wrap the things you actually built (adapt the script names to yours):

```powershell
# one day through your A11 per-day pipeline (repo root; --force reruns the already-green day)
python work\resmon.py --out work\mon_seq.csv -- python -m work.runner --scale full --start 2026-06-05 --end 2026-06-05 --force
# one week through your A12 parallel backfill, with your chosen worker config (flip its SCALE to "full")
python work\resmon.py --out work\mon_par.csv -- python work\parallel_runner.py --start 2026-06-01 --end 2026-06-07 --workers 4 --threads 2 --mem 3GB
```

resmon sees DuckDB because DuckDB works *inside* your Python process; the parallel
workers are picked up as children (the `nprocs` column proves it). In your journal,
answer from the CSVs: (1) peak RSS of the parallel run vs your A12 paper budget — how
close? (2) of the 800% CPU available, how much did each run use? (3) is the sequential
run compute-bound (high CPU) or IO-bound (low CPU while `read_bytes` climbs)?

**Accept when:** both CSVs exist and the three answers are in `journal.md`.

### Task 4 — The spill lab: watch DuckDB trade disk for RAM (full scale)

A full-history sort cannot fit ~82M rows in a small memory budget — perfect. Save as
`work/spill_lab.py`:

```python
"""spill_lab.py -- how does memory_limit change a full-history sort?"""
from pathlib import Path
import threading
import time
import duckdb

from lib.labpaths import lake_dir, tmp_dir

# A10's versioned lake -- adjust "orders_v=*" if your dir name differs. hive_partitioning
# invents an orders_v column from the dir name; harmless for today's queries.
LAKE = (lake_dir("full") / "orders_v=*").as_posix()
TMP = tmp_dir("full")
OUT = (TMP / "sorted_orders.parquet").as_posix()

for limit in ["8GB", "4GB", "1GB"]:
    con = duckdb.connect()                    # fresh connection per run
    con.execute(f"SET memory_limit='{limit}'; SET threads=8;")
    con.execute(f"SET temp_directory='{TMP.as_posix()}'; SET preserve_insertion_order=false;")
    # peak spill, from the engine's own bookkeeping: a second cursor polls
    # duckdb_temporary_files() while the sort runs -- the PowerShell watcher
    # cannot see in-flight sizes (a Windows wart, explained below the listing)
    peak, done = 0, threading.Event()
    def watch():
        global peak
        poller = con.cursor()                 # second cursor on the SAME database
        while not done.is_set():
            rows = poller.execute("SELECT size FROM duckdb_temporary_files()").fetchall()
            peak = max(peak, sum(s for (s,) in rows))
            time.sleep(0.5)
    t = threading.Thread(target=watch)
    t.start()
    t0 = time.time()
    status = "ok"
    try:
        con.execute(f"""
            COPY (SELECT *
                  FROM read_parquet('{LAKE}/*/*.parquet', hive_partitioning=true)
                  ORDER BY customer_id, order_ts)
            TO '{OUT}' (FORMAT parquet)
        """)
    except duckdb.OutOfMemoryException:
        status = "OOM -- limit too small for this workload"
    finally:
        done.set()                            # ALWAYS stop the watcher -- or the script never exits
        t.join()
    print(f"memory_limit={limit:>4}: {time.time()-t0:6.1f} s   "
          f"peak spill {peak / 2**30:5.2f} GB   {status}")
    con.close()
    Path(OUT).unlink(missing_ok=True)         # scratch output, never keep it
```

Run it from the repo root with `python -m work.spill_lab` — the `-m` form, so the
`lib` import resolves (A03's common mistake, A11's rule; every listing today that
imports `lib` runs the same way). Before starting it, put the **watcher** in your
second terminal (Ctrl+C stops it):

```powershell
while ($true) {
  $f = Get-ChildItem "$env:ETL_LAB_DATA\tmp" -File -ErrorAction SilentlyContinue
  $mb = (($f | Measure-Object Length -Sum).Sum) / 1MB
  "{0:HH:mm:ss}  {1} files  {2:N0} MB" -f (Get-Date), $f.Count, $mb
  Start-Sleep 1
}
```

One Windows wart, so you trust the right number: the watcher's **MB column reads
~0 even mid-spill**. NTFS updates a file's size in directory listings lazily —
only when the writing process closes its handle — and DuckDB holds its temp files
open until the query ends. So the watcher proves the *lifecycle* (files appear,
multiply, vanish); for *sizes* the script polls `duckdb_temporary_files()` itself —
the engine's own account of its temp files — and prints the peak per run.

Each full-scale run takes a few minutes on the baseline machine — watching time, not
waiting time. Observe: this sort's working set is roughly twice the 8GB limit, so
even the 8GB run spills a few GB; as the limit drops, spill volume and runtime both
grow — at 1GB, `peak spill` reports many GB, the sort living almost entirely on
disk — and the `duckdb_temp_storage_*.tmp` files vanish the moment the query ends. In Task Manager, the Python process flat-lines *at*
the memory limit instead of growing: the contract holds. (If the 1GB run raises the
clean OOM instead of finishing, that is a valid result too — some operations need a
minimum working set. Write down which happened.) Fill this table in your journal —
your numbers will differ, the *shape* should not:

| memory_limit | wall clock | peak spill in tmp/ | finished? |
|--------------|-----------|--------------------|-----------|
| 8GB          |           |                    |           |
| 4GB          |           |                    |           |
| 1GB          |           |                    |           |

**Accept when:** the table is filled and you can explain, in two sentences, what
spilling is and why the 1GB run is slower but the machine stayed usable.

### Task 4b — Where does spill live? (small scale)

Task 4 proved spill *happens*. Now prove *where* — because in production, where it
lands is the difference between a slow night and a dead server. §7.4's temp
discipline exists for one reason: provisioned machines usually pair a **small OS
disk** with **big attached storage** (a 40 GB C: next to terabytes of data volume
is a normal cloud shape). An engine quietly spilling tens of GB onto the OS drive
fills it — and a full OS drive takes down *everything* on the box: logging stops,
remote desktop breaks, unrelated services crash. Worse, it is a *hidden* failure
point: spill files vanish the moment the query ends (you watched them vanish in
Task 4), so the problem is invisible in every small test run and by the time
someone investigates the crash, the evidence is gone. Your convention block pins
`temp_directory='<DATA_ROOT>/tmp'` precisely so this cannot happen. Today you stop
trusting that line and verify it.

**a) Assert the location, live.** Small scale is enough — this proof needs spill,
not volume. Save as `work/spill_where.py`, run `python -m work.spill_where` from
the repo root (your Task 4 watcher works here too, pointed at
`$env:ETL_LAB_DATA\small\tmp`):

```python
"""spill_where.py -- prove WHERE spill lives: under <DATA_ROOT>/tmp, on the data volume.

Runs a sort that cannot fit a deliberately small memory_limit, asks DuckDB itself
(duckdb_temporary_files()) where its temp files are WHILE the query runs, then
asserts every reported path sits under tmp/ -- preflight check #3, proven live.
"""
import os
import threading
import time
from pathlib import Path

import duckdb

from lib.labpaths import raw_orders_dir, tmp_dir

SCALE = "small"   # the proof needs spill, not scale: small + a small limit spills in seconds
LIMIT = "1GB"     # small enough that June's sort must spill, big enough for 8 threads to start

TMP = tmp_dir(SCALE)
TMP.mkdir(parents=True, exist_ok=True)
OUT = TMP / "spill_where_probe.parquet"

con = duckdb.connect()
con.execute(f"SET memory_limit='{LIMIT}'; SET threads=8;")
con.execute(f"SET temp_directory='{TMP.as_posix()}'; SET preserve_insertion_order=false;")

seen: set[Path] = set()      # every temp-file path DuckDB reports during the run
peak = 0                     # largest total temp size seen in one poll (bytes)
done = threading.Event()

def watch() -> None:
    global peak
    poller = con.cursor()    # second cursor on the SAME database -- sees its temp files
    while not done.is_set():
        rows = poller.execute("SELECT path, size FROM duckdb_temporary_files()").fetchall()
        seen.update(Path(p) for p, _ in rows)
        peak = max(peak, sum(s for _, s in rows))
        time.sleep(0.2)

t = threading.Thread(target=watch)
t.start()
status = "ok"
try:
    con.execute(f"""
        COPY (SELECT * FROM read_csv('{raw_orders_dir(SCALE).as_posix()}/orders_2026-06-*.csv',
                  header=true,
                  columns={{'order_id':'BIGINT','customer_id':'BIGINT','store_id':'INTEGER',
                           'order_ts':'VARCHAR','updated_at':'TIMESTAMP','status':'VARCHAR',
                           'payment_method':'VARCHAR','order_total':'VARCHAR',
                           'items':'VARCHAR','meta':'VARCHAR'}})
              ORDER BY customer_id, order_ts)
        TO '{OUT.as_posix()}' (FORMAT parquet)
    """)
except duckdb.OutOfMemoryException:
    status = f"clean OOM at {LIMIT}"      # too small to even start -- raise LIMIT a notch
finally:
    done.set()               # ALWAYS stop the watcher -- or the script never exits
    t.join()
    con.close()
    OUT.unlink(missing_ok=True)           # scratch output, never keep it

assert seen, f"no spill observed ({status}) -- adjust LIMIT and rerun"
outside = sorted(str(p) for p in seen if not p.is_relative_to(TMP))
assert not outside, f"FAIL: spill OUTSIDE tmp/: {outside}"
os_drive = os.environ.get("SystemDrive", "C:").upper()
verdict = "PASS" if TMP.drive.upper() != os_drive else "WARN -- tmp/ is on the OS drive!"
print(f"query status     : {status}")
print(f"spill files seen : {len(seen)}  (peak {peak / 2**30:.2f} GB on disk at once)")
print(f"all of them under: {TMP}")
print(f"volume           : {TMP.drive}  vs OS drive {os_drive}  -> {verdict}")
```

Two things make this a *proof* rather than a glance. First, the paths come from
DuckDB itself — `duckdb_temporary_files()`, the same poll that gave Task 4 its
`peak spill`, only now we keep the *paths*, not just the sizes — not from us
guessing which directory to watch. Second, the script *asserts*: every reported path under
`tmp/`, and the tmp volume different from the OS drive. That second assertion is
exactly **preflight check #3** from A07 (`work/preflight.py`: "temp_directory
resolves to the data volume") — you are watching, live, the failure that check
exists to prevent. Expected output on the baseline machine (small scale, with
`ETL_LAB_DATA` on a data drive):

```
query status     : ok
spill files seen : 5  (peak 0.27 GB on disk at once)
all of them under: F:\etl_lab\small\tmp
volume           : F:  vs OS drive C:  -> PASS
```

If you get the clean OOM instead: on the baseline machine, below roughly 1 GB the
8-thread CSV reader cannot even start on this data (Task 4's "minimum working
set" note, met again) — raise `LIMIT` one notch and rerun. And note ~0.27 GB of
spill for ~0.5 GB of June CSV: even this small sort lived a third on disk.

**b) Blast-radius arithmetic — how bad would C: have been?** Take the peak spill
you observed in Task 4's full-scale 1GB-limit run (several GB) and ask: if
`temp_directory` pointed at C: instead, how many *concurrent* runs would fill the
OS drive?

```powershell
$freeGB  = [math]::Round((Get-PSDrive C).Free / 1GB, 1)
$spillGB = 6.0   # <-- your Task 4 peak spill (the 1GB-limit run, full scale)
"C: has {0} GB free; at {1} GB spill per run, {2} concurrent runs would fill the OS drive" -f $freeGB, $spillGB, [math]::Floor($freeGB / $spillGB)
```

Example (your numbers will differ): with 90 GB free and 6 GB of spill per run,
14 concurrent runs fill C:. Now recompute for the provisioned-machine shape from
the intro — a 40 GB OS disk with, say, 15 GB free: **2 runs**. Your A12 sweet
spot ran 4 workers at once; if each worker's job spilled like that sort, a
*single* parallel backfill session kills the box. That is the whole §7.4
rationale in one division.

**This is arithmetic only. Never point `temp_directory` at C:\ to "see it for
real"** — filling the OS drive is precisely the incident we are learning to
prevent, and it takes down more than your experiment.

**c) Close the loop in your journal, two sentences:** where does spill live in
this lab and what guarantees it (the convention block *plus* preflight check #3
before every load — a convention is a promise, the check is enforcement); and why
DuckDB's *default* temp location (beside the database file / the working
directory) happens to be safe here but is a trap on a machine where the code — or
the shell's current directory — lives on the OS drive.

**Accept when:** `spill_where.py` prints `-> PASS` on your machine, both
blast-radius numbers (your C: and the 15-GB-free scenario, arithmetic shown) are
in the journal, and the two closing sentences are written.

### Task 5 — The near-OOM cliff, with a guard rail (full scale)

Now the ugly flavor: pandas has no memory limit and no spill — it grows until
something breaks. You will walk toward the cliff with a rope tied on. Save as
`work/oom_probe.py`, run with `python -m work.oom_probe`. **Close heavy apps, save
your work, and never remove the guard.**

```python
"""oom_probe.py -- load full-scale days into pandas until the guard trips."""
import pandas as pd
import psutil

from lib.labpaths import raw_orders_dir

GUARD_RSS_GB = 12        # abort before Windows starts paging us to death
GUARD_AVAIL_GB = 2       # ...or before the SYSTEM runs out, whichever comes first

proc = psutil.Process()
frames = []
for i, path in enumerate(sorted(raw_orders_dir("full").glob("orders_2026-06-*.csv")), 1):
    frames.append(pd.read_csv(path))
    rss = proc.memory_info().rss / 2**30
    avail = psutil.virtual_memory().available / 2**30
    print(f"after {i:2d} day(s): my RSS {rss:5.1f} GB   system available {avail:5.1f} GB")
    if rss > GUARD_RSS_GB or avail < GUARD_AVAIL_GB:
        print(f"GUARD TRIPPED after {i} day(s) -- stopping before Windows stops us.")
        break
```

Watch Task Manager's memory graph climb. On the baseline machine expect roughly
0.5–2 GB of RSS per day (newer pandas stores strings far more compactly — your GB/day
*is* the measurement), so a guard trips somewhere between a handful of days and ~20.
Journal: GB per day, which guard tripped after how many days, and — dividing into 69
days — why "just load it all with pandas" was never an option. Contrast with Task 4:
DuckDB sorted the *whole* 69 days in 1 GB.

**Accept when:** the write-up is in your journal and your machine stayed usable.

### Task 6 — IO volume: CSV vs Parquet (develop small, measure full)

Memory is not the only resource. Ask both formats the same question — June's order
count per store — and count *bytes read* via `io_counters`. Save as `work/io_volume.py`,
run with `python -m work.io_volume`:

```python
"""io_volume.py -- same aggregate, CSV vs Parquet: compare bytes READ."""
import duckdb
import psutil

from lib.labpaths import lake_dir, raw_orders_dir

SCALE = "full"                     # develop with "small" first
proc = psutil.Process()
con = duckdb.connect()
con.execute("SET memory_limit='8GB'; SET threads=8;")

before = proc.io_counters().read_bytes
con.execute(f"""
    SELECT store_id, count(*) AS orders
    FROM read_csv('{(raw_orders_dir(SCALE) / "orders_2026-06-*.csv").as_posix()}',
        header=true,
        columns={{'order_id':'BIGINT','customer_id':'BIGINT','store_id':'INTEGER',
                 'order_ts':'VARCHAR','updated_at':'TIMESTAMP','status':'VARCHAR',
                 'payment_method':'VARCHAR','order_total':'VARCHAR',
                 'items':'VARCHAR','meta':'VARCHAR'}})
    GROUP BY 1
""").fetchall()
csv_gb = (proc.io_counters().read_bytes - before) / 2**30

before = proc.io_counters().read_bytes
# A10's versioned lake -- adjust "orders_v=*" if your dir name differs
con.execute(f"""
    SELECT store_id, count(*) AS orders
    FROM read_parquet('{(lake_dir(SCALE) / "orders_v=*").as_posix()}/*/*.parquet',
                      hive_partitioning=true)
    WHERE order_date BETWEEN DATE '2026-06-01' AND DATE '2026-06-30'
    GROUP BY 1
""").fetchall()
pq_gb = (proc.io_counters().read_bytes - before) / 2**30

print(f"CSV    : read {csv_gb:6.2f} GB")
print(f"Parquet: read {pq_gb:6.2f} GB")
print(f"ratio  : {csv_gb / max(pq_gb, 1e-9):.0f}x less IO with parquet")
```

(Row counts differ slightly — late rows, as you learned in A02/A08; today we compare
bytes, not rows.) At full scale the CSV side must read all of June: ~10 GB. The
Parquet side reads only the `store_id` column of the pruned June partitions — expect
well under 0.1 GB, a 100×+ reduction. Explain *why* in one journal sentence using
three words: columns, compression, partitions.

**Accept when:** both numbers and the sentence are in your journal.

### Task 7 — Right-size the daily load (full scale)

The production question: **"what is the smallest memory_limit that runs the daily
load in under 2× the best time?"** Smallest matters because in the cloud, RAM is the
price tag. Answer with data, not vibes:

1. Copy `spill_lab.py` to `work/rightsize.py`. Replace the sort with the daily load:
   one day's raw CSV (pick `2026-06-05`) → cleaned → partitioned parquet under
   `tmp/rightsize/` — that is, the read → canonicalize → `COPY … PARTITION_BY
   (order_date)` part of your A02/A11 per-day load, writing under `tmp/` instead
   of the lake, warehouse INSERT left out. Reuse that code.
2. Extend the sweep downward: `["8GB","4GB","2GB","1GB","512MB","256MB","128MB","64MB"]`.
3. Print seconds and status per limit, plus a "vs best" ratio. Shape to aim for
   (small-scale dev-run example — your full-scale numbers will differ):

```
memory_limit | seconds | vs best | status
         4GB |     0.4 |    1.3x | ok
       512MB |     0.3 |    1.0x | ok
        64MB |     0.8 |    2.7x | ok
```

Typical shape: flat, flat, then a knee where spilling starts, then possibly a clean
OOM floor. Your answer is the smallest "ok" limit still under 2.0×.

**Accept when:** the full table and the one-line answer are in `journal.md`.

### Task 8 — What your tuning is worth in money (paper exercise)

On your laptop, waste costs time; on a cloud VM the meter runs per hour and bigger
RAM tiers cost more. Use a stand-in price of **$0.40/hour** for an 8-core/16 GB
on-demand VM (or look up a real one). Compute: (1) **speed** — A11 baseline backfill
wall-clock vs A12 parallel; hours saved × $0.40 × 365 runs/year = dollars per year
from A12 alone; (2) **size** — if Task 7 showed the daily load fits in ~4 GB, it fits
a VM tier half the size at roughly half the price; recompute the job's yearly cost.

**Accept when:** both dollar figures are in the journal, arithmetic shown — the
prices are estimates, the habit of doing this arithmetic is the deliverable.

## Definition of Done

- [ ] `work/resmon.py` wraps any command, tracks children, writes a valid CSV,
      prints plausible peak RSS / avg CPU / IO totals.
- [ ] Monitor CSVs analyzed with DuckDB; peak-RSS window identified via SQL.
- [ ] A11 and A12 runs profiled; measured peak RSS vs A12 paper budget in journal.
- [ ] Spill observed live (`duckdb_temp_storage_*.tmp` appearing, then gone; peak
      sizes from the script's `duckdb_temporary_files()` poll) and the 8/4/1 GB
      runtime-vs-memory table filled.
- [ ] Spill *location* proven (§7.4): `work/spill_where.py` prints PASS — every
      engine-reported temp file under `<DATA_ROOT>/tmp`, tmp volume ≠ OS drive —
      and both C:\ blast-radius numbers are in the journal.
- [ ] Near-OOM experiment run *with the guard*, written up; machine stayed usable.
- [ ] CSV-vs-Parquet IO numbers recorded (full scale: ~10 GB vs well under 0.1 GB).
- [ ] Right-sizing table filled; smallest memory_limit under 2× best time named.
- [ ] Both cloud-cost figures computed in the journal.

## Verify yourself

```powershell
# 1) resmon sanity: peak RSS ~0.30 GB (the 300 MB bytearray), ~6-7 samples
python work\resmon.py --out work\mon_test.csv -- python -c "import time; x = bytearray(300*2**20); time.sleep(3)"
```

```sql
-- 2) the workload you sorted in Task 4, straight from the manifests (A05 style)
SELECT count(*) AS files, sum(rows) AS total_rows,
       round(sum(bytes)/pow(2,30), 1) AS csv_gb
FROM read_json_auto('<DATA_ROOT>/raw/manifest/orders_*.json');
-- expect (full scale, generator 1.0): 69 files, 82,337,467 rows, ~24.2 GB
-- June alone (the Task 6 window): 30 files, 36,060,981 rows, ~10.2 GB
```

- During the 1GB spill run the watcher must show several `duckdb_temp_storage_*.tmp`
  files (their MB column near 0 — the Windows wart; the script's `peak spill` must
  report several GB); within seconds of the run ending, `tmp/` must be empty again.
- `python -m work.spill_where` (small scale) must print `query status : ok`, a
  handful of spill files (~5, peak roughly 0.2–0.3 GB), and end with `-> PASS`;
  afterwards `small\tmp\` must be empty again.
- Task 7's table must show a knee. If every limit gives the same time, your workload
  never exceeded the smallest limit — you measured nothing; use full scale.

## Common beginner mistakes

- **Creating a fresh `psutil.Process` every sample.** `cpu_percent(interval=None)`
  compares against the previous call *on the same object* — fresh objects report 0
  forever, and your worker pool looks idle while 8 cores burn.
- **Monitoring only the parent process.** The A12 pool parent sits near 0.2 GB while
  its children hold gigabytes. Always sum the tree.
- **Comparing a cold run against a warm one.** Windows caches file reads: a second
  scan of the same CSV can be much faster with identical `read_bytes`. For fair A/B
  timings run each variant twice and compare same-position runs (A14 formalizes this).
- **Pointing `temp_directory` at C:\ or OneDrive.** Spill files reach many GB, fast;
  a nearly-full or synced drive turns the spill lab into a disk-full incident.
  Task 4b's arithmetic tells you exactly how few runs that takes.
- **Running the near-OOM probe without the guard "just to see".** With RAM full,
  Windows pages and everything crawls — including the editor holding your unsaved
  journal. The guard costs two lines. Keep it.

## Stretch goals

- **Spill curves:** Task 4's watcher thread already polls `duckdb_temporary_files()`
  for a peak — extend it to log `(t_s, total_bytes)` per poll to a CSV at each
  memory_limit, and compare the three spill-over-time curves (with DuckDB, of
  course).
- **Rates, not just totals:** turn resmon's cumulative `read_bytes` into a MB/s
  column with `lag()` over `t_s`; find where the run flips from read- to compute-heavy.
- **Profile the pathological configs:** rerun the A12 oversubscription and
  memory-over-commit experiments under resmon; describe their signatures in the CSV.
- **Resource alerting (§7.3):** give resmon a `--max-rss-gb` flag: when the child
  tree's summed RSS exceeds the budget, set a breach flag, remember the peak, and
  keep monitoring: warn, don't kill. Write the one `ops.alerts` row (severity `warn`,
  source `resmon`, message naming the command and the peak GB) into `warehouse.duckdb`
  — same table A05 created and A07/A11 write to — only *after* the wrapped command
  exits (the child may hold warehouse.duckdb open — A12's single-writer rule; resmon
  must not be the second writer). At most one row per run. Verify with the Task 1
  smoke test at `--max-rss-gb 0.2` against the **small** warehouse — one new `warn`
  row should appear.
