# Incident: truncated lines in `orders_2026-07-22.csv` blocked one day of the orders backfill

**Detected:** 2026-09-15 10:57 UTC, by the backfill runner on its first pass ·
**Resolved:** 2026-09-15 14:04 UTC ·
**Author:** Data team ·
**Status:** resolved

---

## Impact

One business date, `2026-07-22`, failed to load during the 69-day historical
backfill of `core.orders`. The other 68 dates loaded normally — the runner logs
the failure and continues, so a single bad file does not stall a backfill.

While the day was red, `core.orders` and the canonical lake were both missing it
entirely. No wrong data was published: the load is atomic, so the failed attempt
left nothing behind. This was a gap, not a corruption — a distinction that
matters, because a gap is visible to anyone who counts dates and a corruption is
not.

No downstream consumer was affected, because the backfill was not yet published
when the failure occurred.

Final state: 69/69 dates green at both scales, 4,116,840 rows (small) and
82,337,467 rows (full) in `core.orders`, every date reconciling exactly against
its manifest.

---

## Timeline

Taken from `ops.etl_runs`, not from memory. Small scale:

| started_at | finished_at | status | rows | source_file | error |
|---|---|---|---|---|---|
| 2026-09-15 10:57:10.126 | 2026-09-15 10:57:10.240 | failed | NULL | orders_2026-07-22.csv | `Invalid Input Error: Error when sniffing file "D:/etl_lab/sm…` |
| 2026-09-15 14:04:04.340 | 2026-09-15 14:04:05.253 | success | 58400 | orders_2026-07-22.csv | NULL |

- **10:57:10** — runner reaches the date, `read_csv` fails in 114 ms, ledger row
  goes `failed`, runner continues to 07-23.
- **10:58** — backfill finishes: 68 success, 1 failed.
- **~11:00–13:30** — diagnosis (see Root cause). The layer checklist ruled out
  environment and storage in seconds: preflight had run green for this exact
  date moments before the crash.
- **~13:30–14:00** — `load_day` changed to read tolerantly and quarantine
  rejects.
- **14:04:04 → 14:04:05** — `--only-failed` reruns exactly this one date. Green
  in 0.9 s, 58,400 rows, 151 lines quarantined.

The failed attempt records `rows = NULL`, not `0`. Zero would be a claim; NULL
is the truth — the attempt never got far enough to count anything.

**Detection time was effectively zero; repair time was three hours.** Almost all
of that was diagnosis and writing the fix. The rerun itself took under a second.

---

## Root cause

**Not** what the error message said.

The error reported that DuckDB could not automatically detect the CSV parsing
dialect. The dialect had not changed at all — the header was intact, all 13 v2
columns in the right order, and the first data row parsed cleanly with ordinary
commas and double quotes.

Reading the file with `store_rejects=true` produced the real picture:

| error_type | records | distinct lines |
|---|---|---|
| MISSING COLUMNS | 1,500 | 150 |
| CAST | 150 | 150 |
| UNQUOTED VALUE | 1 | 1 |

- **150 lines were truncated mid-row** during shopcore's export, each cut after
  roughly the third column. 1,500 ÷ 150 = exactly 10 missing columns per line.
  The `CAST` records are a side effect — the surviving fragment of a truncated
  line does not cast to its column's type.
- **1 line carried an unclosed quote.**

That single unclosed quote is what produced the misleading error. The sniffer
reads a sample to infer the dialect; it met a quote that never closes, so no
combination of delimiter and quote character it tried could explain the rest of
the sample. It reported the symptom nearest to where it gave up.

**Root cause: 150 truncated lines plus one unclosed quote in the producer's
export, which together defeated dialect detection and aborted the whole file.**
The lesson is general: an error message points at the symptom closest to the
code that raised it, not at the cause.

---

## Detection gap

**The manifest already said so, before anything ran.**

`raw/manifest/orders_2026-07-22.json` carried `corrupt_rows: 151` — the exact
number the reject table later produced. That value was written when the file was
delivered. It was present throughout A10, present when the lake rebuild skipped
this date, and present when the backfill started. Nobody read it.

So the gap is not a missing signal. The signal existed, was accurate, and was
ignored. Nothing consumed it.

A pre-flight check reading `corrupt_rows` from the manifest would have flagged
this date **before** the backfill started, not 40 dates into it. That check costs
one JSON read per day and is the first action item below.

Secondary gap: the freshness gate built in this cycle watches for *missing*
deliveries. It does not look at delivered-but-damaged files. The two checks are
complementary and both belong in pre-flight.

---

## Producer note

> **To:** shopcore Checkout Platform team — checkout-eng@shopcore.example
> **Escalation:** #shopcore-oncall
> **Re:** malformed lines in `orders_2026-07-22.csv` (both scales)

The daily orders export for business date 2026-07-22 contains malformed lines in
both the small and full exports. Per the data contract
(`shopcore_orders_daily`, `quality.structural`), **zero malformed lines are
tolerated — a file with unparseable lines is a producer incident**. We are
raising it as one, per `slo_reporting.producer_receives`.

Reject counts, matching your own manifest's `corrupt_rows`:

| scale | rows declared | malformed lines | reject records |
|---|---|---|---|
| small | 58,400 | 151 | 1,651 |
| full | 1,168,003 | 3,001 | — |

Breakdown of the malformed lines (small scale):

- **150 lines truncated mid-row**, each cut after approximately the third
  column — consistent with a write that ended before the row was complete.
- **1 line with an unclosed quote**, which additionally defeated CSV dialect
  detection and caused the entire file to be unreadable by a strict parser.

Two observations we think are useful to you:

1. The manifest correctly declared `corrupt_rows: 151`, so the export process
   *knew* the rows were bad and shipped them anyway. If that count is available
   at write time, the export could fail loudly instead.
2. The declared row count (`rows: 58,400`) **excludes** the malformed lines. Our
   reconciliation therefore balances, which is why this was containable. Please
   keep that convention if the export is fixed.

Per-line reject detail — line number, column, error type, and the raw CSV text —
is retained as Parquet at
`quarantine/orders_badlines/file_date=2026-07-22/rejects.parquet` and available
on request.

**Their incident to fix; ours to contain.** We have contained it: the readable
58,400 rows are loaded, the 151 bad lines are quarantined with reasons, and no
downstream consumer saw partial data. We are not asking for a re-export in order
to unblock ourselves. We are reporting it because the contract says a clean file
is what was promised.

---

## Resolution

`work/pipeline.py` — `load_day` now reads with `store_rejects=true`, copies
`reject_errors` to `quarantine/orders_badlines/file_date=<d>/rejects.parquet`
with `_data_date` and `_run_id` lineage, clears the reject table, and verifies
the loaded row count against the manifest before publishing.

The raw CSV was **not** edited. Raw files are immutable evidence, and the
generator would recreate the original anyway. What changed is how we read it.

---

## Action items

| # | Item | Owner |
|---|---|---|
| 1 | **Alert when `manifest.corrupt_rows > 0`.** Add to `work/preflight.py` as an eighth check, reading the manifest for the date being loaded. Warn rather than hard-fail: a file with rejects is now loadable, so the right response is to raise a producer incident, not to block the load. This is the check that would have caught 07-22 before the backfill ran. | Data team |
| 2 | **Weekly reject report to shopcore.** `slo_reporting.producer_receives` already promises weekly quarantine and violation counts per rule. The quarantine Parquet now makes this a query rather than a manual count. | Data team |
| 3 | **Propose a restatement clause — contract amendment.** If shopcore re-exports a fixed `orders_2026-07-22.csv` tomorrow, **no clause says how that restatement must be announced.** Contract v1.3.0 mentions restatements only in passing under `delivery.freshness_sla` ("treat unannounced re-publishes as incidents") and defines no notice period, no channel, and no way for us to distinguish a corrected file from a silent overwrite. Our loader is idempotent, so reloading is safe — but we would not know to reload. Propose via A06's `change_management` flow: a restatement must be announced in `#shopcore-data-changes`, and the manifest must carry a version or revision marker so the difference is detectable without comparing bytes. | Data team → shopcore |
| 4 | **Ask shopcore to fail the export on malformed lines.** See Producer note, observation 1. | shopcore |

Items 1 and 3 are the two that matter. Item 1 closes the detection gap for
*this* failure; item 3 closes a contract gap that has not caused an incident yet
and is the more dangerous of the two, because a silent re-publish would leave us
holding stale data with every ledger row green.