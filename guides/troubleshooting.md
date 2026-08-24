# Troubleshooting — the layered checklist

Something broke. This page is the checklist you walk **top-down** — print it, or keep
it open in a second window while you debug. The order is deliberate: each layer is
cheaper to check than the one below it, and a failure at layer N usually *cannot* be
diagnosed while a layer above it is broken. Wrong venv (layer 1) produces errors that
look like data corruption (layer 3); a full disk (layer 2) produces errors that look
like engine bugs (layer 4). Check cheap things first — not because they're likely,
but because they take ten seconds to rule out and everything below depends on them.

Two notes before the layers:

- **`work/preflight.py` (built in A07) automates layer 2 and layer 3's existence
  checks** — file present, manifest present, plus schemas and a parseable contract.
  The fact it ran at all also vouches for most of layer 1 (this interpreter imports
  duckdb and yaml). So if preflight is green, start at layer 3's *content* checks
  (counts, header, encoding) or layer 6 — layer 4's knobs are still yours to verify.
  This page exists for when preflight itself is red, for machines where it isn't
  set up yet — and to teach you what a preflight for a *networked* warehouse would
  check instead (layer 5).
- A11's diagnosis of the corrupt `2026-07-22` file walks this exact checklist
  top-down as a worked example. Terms you don't recognize are in `GLOSSARY.md`.

**Paste-safe probe pattern.** Several probes below mix PowerShell, Python and SQL
quotes. Rather than fight escaping in one-liners, pipe a here-string into Python —
paste the whole block as-is:

```powershell
@'
import duckdb
print(duckdb.sql("SELECT version()").fetchone())     # ('v1.5.4',)
'@ | python -
```

---

## 0. Before any checklist: two habits that beat all of them

**Habit 1 — read the actual error, bottom-most line first.** A Python traceback
prints the route at the top and the destination at the bottom: the last line is the
error, everything above is just how execution got there. Read the last line, then
the last line of any quoted inner error, *verbatim* — resist summarizing it to
yourself as "the file thing broke". But stay skeptical: errors point at the symptom
nearest the code that raised them, not at the root cause. The lab's own corrupt file
throws *"It was not possible to automatically detect the CSV parsing dialect"* — the
dialect never changed; one unclosed quote poisoned the sniffer (A11 Task 4).

**Habit 2 — reproduce it small.** Never debug inside a 69-day backfill. Extract the
one failing statement, run it against **one file at small scale** in a scratch
in-memory session (`duckdb.connect()`) — the failure loop drops from 40 minutes to
5 seconds, and the small repro *is* what you'll paste when you escalate (§7):

```powershell
@'
import duckdb
duckdb.sql("""SELECT count(*) FROM read_csv(
    'F:/etl_lab/small/raw/orders/orders_2026-07-22.csv', header=true,
    columns={'order_id':'BIGINT','customer_id':'BIGINT','store_id':'INTEGER',
             'order_ts':'VARCHAR','updated_at':'TIMESTAMP','status':'VARCHAR',
             'payment_method':'VARCHAR','order_total':'VARCHAR','items':'VARCHAR',
             'meta':'VARCHAR','currency':'VARCHAR','discount_amount':'VARCHAR',
             'channel':'VARCHAR'})""").fetchone()
'@ | python -
# -> InvalidInputException: Error when sniffing file ... 5 seconds, reproduced.
```

Reproduce the failure **in its exact form**: the same file read with auto-detect
"succeeds" here — returning 58,551 rows where the manifest says 58,400. A repro that
doesn't fail the same way is measuring a different bug.

**Habit 3 (bonus) — ask "what changed since it last worked?"** If the pipeline was
green yesterday, `git log --oneline` shows every suspect. Diff or roll back to the
last green commit before staring at data (`guides/git_basics.md`, taught in A07).

---

## 1. ENVIRONMENT — is this the machine/setup I think it is?

- [ ] **Right Python / venv active?**
      `(Get-Command python).Source` (POSIX: `which python`) — should point into your
      project venv, not `C:\Program Files\Python…`. Cross-check
      `python -c "import sys; print(sys.prefix)"`.
      *Fix:* `.\.venv\Scripts\Activate.ps1` (POSIX: `source .venv/bin/activate`).
- [ ] **Packages importable in THIS interpreter?**
      `python -c "import duckdb, pyarrow, pandas, psutil, yaml; print('ok', duckdb.__version__)"`
      → `ok 1.5.4`. "Works in one terminal, not the other" is almost always two
      Pythons. *Fix:* activate the venv, then `pip install -r requirements.txt`.
- [ ] **Data-root env var set — in THIS shell?**
      `echo $env:ETL_LAB_DATA` (POSIX: `echo $ETL_LAB_DATA`) → `F:\etl_lab`.
      Blank means `lib/labpaths.py` silently falls back to `<repo>/data` — your run
      "works" against the wrong (possibly empty, possibly OneDrive-synced) tree.
      *Fix:* `$env:ETL_LAB_DATA = 'F:\etl_lab'` for this session; A00 shows the
      persistent setting (new terminals only — restart the one you're in).
- [ ] **Right working directory?**
      `Get-Location` → the repo root. `python -m work.runner …` module runs and
      relative paths only resolve from there. One probe answers this line and the
      two above at once:
      ```powershell
      python -m lib.labpaths
      # repo:       C:\...\ETL_SAMPLE   (wherever you cloned it)
      # data root:  F:\etl_lab   (from ETL_LAB_DATA)
      #   small: F:\etl_lab\small
      #   full : F:\etl_lab
      ```
- [ ] **Your own code still sane here?** `python -m pytest tests/ -q` — the cleaner
      tests (`tests/test_cleaners.py`, A03) run in seconds against an in-memory
      DuckDB. Red tests on a "new" machine mean the environment is broken, not the
      data.

## 2. STORAGE & PERMISSIONS — can I physically read and write?

- [ ] **Data root exists?** `Test-Path $env:ETL_LAB_DATA` → `True`.
- [ ] **…and is writable?** Don't trust folder properties — create and delete a
      probe file, exactly like preflight check #1 does (a permission error at hour
      zero beats one at row 40 million):
      ```powershell
      python -c "from pathlib import Path; import os; p = Path(os.environ['ETL_LAB_DATA']) / '_probe.tmp'; p.write_text('x'); p.unlink(); print('writable: yes')"
      ```
- [ ] **Disk free — on the DATA volume?**
      ```powershell
      Get-PSDrive F, C | Select-Object Name,
          @{n='UsedGB';e={[math]::Round($_.Used/1GB,1)}},
          @{n='FreeGB';e={[math]::Round($_.Free/1GB,1)}}
      # Name UsedGB FreeGB          (POSIX: df -h /data)
      # F    114.2  337.4
      # C    409.9   90.1
      ```
      A full-scale run wants tens of GB of headroom on **F:** — C: being full or
      free is irrelevant *unless* the next check is wrong.
- [ ] **`temp_directory` on the data volume — NOT C:?** SPEC §7.4 temp discipline.
      Why it matters: provisioned machines pair a small OS disk with big attached
      storage; an engine spilling 50 GB to C: fills it *silently*, and a full OS
      drive takes down everything on the box. DuckDB's default is `.tmp` next to
      the DB file / current dir. *Fix:* the convention block —
      `SET temp_directory = '<DATA_ROOT>/tmp'` — in every connection; preflight
      check #3 enforces it, A13 shows the spill live. Probe what a session
      actually uses:

```powershell
@'
import duckdb
con = duckdb.connect()          # scratch session: default lands in cwd!
print(con.execute("SELECT current_setting('temp_directory')").fetchone())   # ('.tmp',)
con.execute("SET temp_directory = 'F:/etl_lab/tmp'")
print(con.execute("SELECT current_setting('temp_directory')").fetchone())   # ('F:/etl_lab/tmp',)
'@ | python -
```
- [ ] **Warehouse file locked by another process?** The Windows classic: a notebook
      or second terminal still holds `warehouse.duckdb`. A second `connect` is the
      probe — DuckDB names the culprit, PID included:
      ```powershell
      python -c 'import duckdb; duckdb.connect(r"F:\etl_lab\warehouse\warehouse.duckdb").close(); print("not locked")'
      # locked ->  IOException: IO Error: Cannot open file "...warehouse.duckdb":
      #            The process cannot access the file because it is being used by
      #            another process.
      #            File is already open in
      #            C:\Program Files\Python312\python.exe (PID 36672)
      ```
      Then `Get-Process -Id 36672` tells you *what* it is. *Fix:* close that
      notebook/REPL — or, for deliberate parallelism, the A12 rule: workers write
      parquet, one committer owns the DB.
- [ ] **A sync client interfering?** `$env:ETL_LAB_DATA -match 'OneDrive|Dropbox'`
      → must be `False`. Sync clients lock files mid-write, re-upload every GB you
      touch, and dehydrate "unused" files to placeholders. Code in OneDrive: fine
      (this repo is). Data: never (SPEC §2).

## 3. DATA — is the input what I was promised?

- [ ] **File exists and size > 0?**
      `Get-Item F:\etl_lab\small\raw\orders\orders_2026-06-02.csv | Select-Object Name, Length`
      → `Length 17865094`. A 0-byte file means a dead upstream export — stop here,
      that's a producer conversation, not a parsing puzzle.
- [ ] **Manifest present — and does the row count reconcile?** The producer tells
      you what they sent (`raw/manifest/`, SPEC §3.7); trust nothing until counts
      match:

```powershell
@'
import duckdb
csv = "F:/etl_lab/small/raw/orders/orders_2026-06-02.csv"
man = "F:/etl_lab/small/raw/manifest/orders_2026-06-02.json"
con = duckdb.connect()
got  = con.execute(f"SELECT count(*) FROM read_csv('{csv}', header=true, all_varchar=true)").fetchone()[0]
want = con.execute(f"SELECT rows FROM read_json_auto('{man}')").fetchone()[0]
print(f"csv = {got:,}  manifest = {want:,}  match = {got == want}")
'@ | python -
# csv = 59,024  manifest = 59,024  match = True
```
- [ ] **Header / era as expected?** Read the first line — seconds, and it catches
      schema drift (A10) before any SQL does:
      ```powershell
      Get-Content F:\etl_lab\small\raw\orders\orders_2026-08-01.csv -TotalCount 1
      (Get-Content F:\etl_lab\small\raw\orders\orders_2026-08-01.csv -TotalCount 1).Split(',').Count
      # 10 columns = era v1 · 13 = v2 · 14 (payment_type, loyalty_tier) = v3
      ```
- [ ] **Encoding sane?** First bytes, raw:
      `python -c "print(open(r'F:\etl_lab\small\raw\orders\orders_2026-06-02.csv','rb').read(4))"`
      → `b'"ord'` is healthy UTF-8. `b'\xef\xbb\xbf…'` is a UTF-8 BOM, `b'\xff\xfe…'`
      UTF-16 — both mean some tool re-saved the file; regenerate, don't hand-fix.

## 4. ENGINE — is DuckDB configured for this job?

- [ ] **`memory_limit` vs data size?** Default is ~80% of RAM — fine solo, fatal
      ×8 workers (A12). The lab convention is `SET memory_limit = '8GB'`. Probe
      what the session *actually* has: `SELECT current_setting('memory_limit')` —
      and compare against input size (`(Get-Item <file>).Length / 1GB`), remembering
      an `unnest` of `items` multiplies rows ~2.6×.
- [ ] **Spill dir filling — or failing to?** During a heavy sort/aggregate:
      `(Get-ChildItem $env:ETL_LAB_DATA\tmp | Measure-Object Length -Sum).Sum / 1GB`
      (0 or blank = no spill). Spill growing on **F:** = working as designed
      (slower, alive). "Out of memory" with an *empty* tmp = the temp_directory or
      limit isn't set where you think — back to layer 2's probe.
- [ ] **DuckDB version?** `python -c 'import duckdb; print(duckdb.__version__)'` →
      `1.5.4`. Lab snippets are verified on ≥ 1.4 (SPEC §4); `MERGE INTO` and
      reject-table behavior differ on older versions. Version mismatch between two
      machines usually resolves to layer 1 (two interpreters).

## 5. THE NETWORKED-DB COLUMN — same layers, remote warehouse

This lab is file-based, so "storage" means a disk you own. Against Postgres,
Snowflake or BigQuery the *layer order is identical* — only the probes change:
between you and the data now sit a network, an authentication system and a
permission model, and they fail in exactly that order. Walk it top-down like
layers 1–4; `Test-NetConnection localhost -Port 445` style local probes below show
each output shape harmlessly. Where this lab's preflight automates the storage
layer and the input's existence, a remote-warehouse preflight automates this column.

- [ ] **DNS resolves?** `Resolve-DnsName warehouse.corp.example` (POSIX:
      `dig`/`nslookup`). Local demo: `Resolve-DnsName localhost` → an `A` record,
      `127.0.0.1`. No record → wrong hostname, or you're off the VPN.
      *Ask:* network/platform team — after checking your VPN.
- [ ] **TCP port reachable?** `Test-NetConnection <host> -Port 5432` (POSIX:
      `nc -vz host 5432`). Demo shapes, run against localhost:
      ```powershell
      Test-NetConnection localhost -Port 445    # TcpTestSucceeded : True
      Test-NetConnection localhost -Port 5432   # TcpTestSucceeded : False (+ warning)
      ```
      (445 is Windows file sharing — always listening locally, so it shows you the
      success shape; 5432 shows the failure shape unless you run Postgres.)
      DNS ok + port closed = firewall / security group / DB down.
      *Ask:* platform team; your code cannot fix a closed port.
- [ ] **TLS handshake?** Errors naming "SSL", "certificate", "handshake". Client
      config (`sslmode`, trust store) is yours; an expired server cert is theirs.
      *Ask:* platform/DBA with the exact error line.
- [ ] **Credentials valid?** Port reachable but "authentication failed" — expired
      password, rotated secret, wrong vault key. *Ask:* yourself first (which
      secret did the pipeline actually load?), then the DBA.
- [ ] **Database / schema exists?** "database X does not exist" after auth — wrong
      environment (prod vs staging connection string) more often than a missing DB.
      *Ask:* yourself (connection string), then DBA.
- [ ] **GRANTs on the table?** The classic trap: **"table exists but permission
      denied"** — and some engines report missing SELECT as *"relation does not
      exist"* to avoid leaking names, sending you hunting a table that's right
      there. SELECT, INSERT, and CREATE are separate grants: reading working and
      writing failing is normal, not weird. Generic probe: `SELECT 1 FROM t LIMIT 1`
      vs inserting one probe row (then delete it) — or query
      `information_schema.table_privileges` for your role.
      *Ask:* DBA — grants are policy, not code.
- [ ] **Connection pool exhausted?** Fails only under load, "too many connections" /
      "timeout waiting for connection". *Ask:* your own code first — leaked
      unclosed connections are the usual cause (this lab's single-writer lock in
      layer 2 is the same lesson in miniature); the DBA for limits second.
- [ ] **Timeout vs firewall idle-kill?** Long queries dying at suspiciously round
      times (exactly 30 or 60 min) = an idle timeout between you and the DB, not a
      slow query. *Ask:* platform for the timeout value; fix on your side with
      keepalives or by chunking the work (A11's per-day pattern).

## 6. PIPELINE STATE — what do my own records say?

You built an audit trail in A05/A07 precisely for this moment. Ask it, in order
(all queries against `warehouse.duckdb`):

```sql
-- The saved error of the last failed run (A07's ops.etl_runs):
SELECT run_date, step, source_file, error FROM ops.etl_runs
WHERE status = 'failed' ORDER BY started_at DESC LIMIT 1;
-- ^ source_file lives HERE, on the ledger — one row per attempt — and nowhere
--   else: the delivery's file name is a fact about the run (§7.3), not a column
--   on 1.2M data rows.

-- Attempt history: which dates are flaky vs deterministic failures (A07 §"retry"):
SELECT run_date, count(*) AS attempts,
       sum(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed
FROM ops.etl_runs GROUP BY 1 ORDER BY failed DESC, run_date;

-- Did the pipeline already page you? (A05's ops.alerts):
SELECT alert_ts, severity, source, message
FROM ops.alerts ORDER BY alert_ts DESC LIMIT 5;

-- Freshness in one line (§7.2): how stale is core, really?
SELECT max(_data_date) FROM core.orders;
```

Full context of a failed run lives in its log:
`Get-Content logs\run_42.log -Tail 20` (POSIX: `tail -20 logs/run_42.log`).

And when the question is "where did this **bad row** come from?", the two lineage
columns answer it (§7.2 — a row carries `_data_date` and `_run_id`, and *only*
those two). The row points at its run; the ledger row supplies everything else
about that load — file, load time, status, error:

```sql
SELECT o.order_id, o._data_date, o._run_id,
       r.source_file, r.finished_at, r.status, r.error
FROM core.orders o LEFT JOIN ops.etl_runs r ON r.run_id = o._run_id
WHERE o.order_id = 2020000123;   -- the suspicious row
-- -> 2026-07-21 · run 41 · orders_2026-07-21.csv · 2026-07-22 04:07:44 · success
--    then read logs/run_41.log
```

Three things to know when you run it:

- **Why a join and not a `_source_file` column on the row.** The file name and
  the load timestamp describe the *run*, not the order. Keeping them on the
  ledger means one row per attempt instead of the same string copied onto 1.2M
  rows — and re-loading a date from a corrected file rewrites one ledger row,
  not the whole day. Same reason `r.error` is reachable at all: the row can't
  carry an error it didn't have when it was written.
- **`LEFT JOIN` on purpose.** `_run_id` is NULL for anything loaded before A07
  built the ledger; an inner join would silently drop exactly the oldest rows
  you're least sure about.
- **Even with a NULL `_run_id` the row is not anonymous.** `_data_date` *is* the
  file name in this feed — the contract's `path_pattern` is one file per business
  date, so the delivery is `orders_<_data_date>.csv` and you can go read it. (A
  feed with several files per date genuinely needs the name recorded; it still
  belongs on the ledger row, keyed by run.)

## 7. ESCALATE — but properly

Some walls you should not tunnel through alone: grants (DBA), closed ports
(platform), corrupt deliveries (producer — the contract header names the
escalation contact, and A11's 07-22 postmortem is the template). Asking is not
failure; asking *vaguely* is. The rule — bring exactly three things:

1. **What you tried** — which layers of this checklist you walked, what was green.
2. **The exact error** — bottom-most line, verbatim, plus which run
   (`ops.etl_runs.run_id`) and its log file.
3. **The smallest repro** — the §0 one-file, small-scale snippet that fails the
   same way in seconds, so your helper starts at the wall, not at your 69-day run.

Ten minutes assembling these usually finds the answer before you hit Send — and
when it doesn't, you've handed the next person a solvable problem instead of
"it's broken". That, too, is engineering.
