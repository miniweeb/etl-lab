# ETL Lab - from CSV chaos to a governed Parquet lake

A hands-on training course for **complete-beginner data engineers**. You start
with 24 GB of messy daily CSV exports and finish with a partitioned Parquet
lake + DuckDB warehouse + dbt project that is validated, contracted,
idempotent, monitored, and fast - built entirely on your own laptop.

The dataset is synthetic but engineered to hurt in realistic ways: it is too
big for pandas, dirty in a dozen classic ways, arrives late, changes schema
mid-course, and contains one corrupt file. Every "ouch" is a lesson.

## Who this is for

You know basic Python and basic SQL. You have never built a production data
pipeline. After the 16 assignments you will have personally hit - and fixed -
most of the failure modes that fill a data engineer's first year.

## Machine assumptions

Everything is sized for a normal developer laptop:

| | Baseline |
|---|---|
| RAM | **16 GB** |
| CPU | **8 cores** |
| Disk | **~80 GB free** on a fast local drive (SSD) |
| OS | Windows 10/11 (commands in PowerShell; everything also works on macOS/Linux) |
| Software | Python 3.11+, packages from `requirements.txt` |

If you have more RAM/cores: great, but keep the configured limits from the
assignments (`SET memory_limit='8GB'` etc.) so you experience the same
constraints the course is designed around.

## The dataset (generated locally, ~24 GB)

- `orders_YYYY-MM-DD.csv` - one file per day, ~1.2M rows (~350 MB) on weekdays,
  fewer on weekends, **3× on two flash-sale days**. 69 days total across two
  "months" (the second month arrives mid-course with a changed schema).
- ~1.5% of each day's rows are **late corrections** to earlier days.
- Values are ~97% clean; the remaining ~3% covers essentially every classic
  cleansing problem: mixed timestamp formats, epoch-vs-string dates,
  `$`/European-comma/`N/A` amounts, Excel-float IDs, junk enums, mixed
  booleans, duplicate keys, orphan references, JSON with string-typed numbers,
  one structurally corrupt file.
- Dimensions: 1.2M customers, 40k products (with JSON/pipe-list columns), 400 stores.
- Every file has a JSON **manifest** with exact row counts - your ground truth
  for reconciliation.
- The feed comes from **shopcore**, the e-commerce platform, and ships with a
  real data contract in [contracts/](contracts/) - A05 enforces its thresholds
  and A06 teaches you to read it and find its gaps.

Full spec: [SPEC.md](SPEC.md). Generator: [datagen/generate.py](datagen/generate.py)
(deterministic - any file regenerates byte-identical). Forgot a term mid-course?
[GLOSSARY.md](GLOSSARY.md).

## Course map

Do them in order - each builds on the last. The estimates below add up to
roughly 65 hours end to end; a sustainable pace is one assignment every two or
three evenings - about nine weeks.

| # | Assignment | You learn | Est. |
|---|-----------|-----------|------|
| [A00](assignments/A00_setup.md) | Setup & data generation | environment, the lab's rules + git setup | 1–2h |
| [A01](assignments/A01_first_contact.md) | First contact | exploring big CSVs safely, schemas, CSV→Parquet | 3h |
| [A02](assignments/A02_partitioning.md) | Partition the lake | Hive partitioning, partition pruning | 3h |
| [A03](assignments/A03_transformation.md) | Transformation & cleansing | staging→core, type normalization + modeling: grain, fact vs dimension, keys + naming & lineage columns + cleaners module & pytest | 4–6h |
| [A04](assignments/A04_complex_types.md) | Complex data types | JSON→structs/lists, unnest, item-level marts | 3–4h |
| [A05](assignments/A05_validation.md) | Validation & quarantine | DQ checks, contract-driven thresholds, freshness, alerts + pandera/Great Expectations bridge | 4h |
| [A06](assignments/A06_data_contract.md) | Data contracts | read a real contract, find its gaps, pre-flight gates + PII classification & masking + pydantic contract model | 4–5h |
| [A07](assignments/A07_idempotency.md) | Failures, retries, idempotency | run state, transactions, chaos testing + audit trail & logging + preflight & git savepoints | 5–6h |
| [A08](assignments/A08_load_strategies.md) | Load strategies | append vs upsert vs delete+insert by date | 4h |
| [A09](assignments/A09_atomic_swap.md) | Atomic partition swap | publishing without torn reads | 3h |
| [A10](assignments/A10_schema_evolution.md) | Schema evolution | month 2 arrives: new columns, renames, type changes + natural vs surrogate keys | 4h |
| [A11](assignments/A11_backfill.md) | Backfill | date-range runner, resume-on-failure, the corrupt file + freshness alerts, run-ledger audit + scheduled watchdog (stretch) | 4–5h |
| [A12](assignments/A12_parallelization.md) | Parallelization | 69 days on 8 cores, memory budgeting, skew | 4h |
| [A13](assignments/A13_resource_monitoring.md) | Resource monitoring | psutil, memory limits, spill, right-sizing + temp/spill discipline | 4h |
| [A14](assignments/A14_performance.md) | Performance tuning | EXPLAIN ANALYZE, profiling, 20× speedups | 4–5h |
| [A15](assignments/A15_dbt.md) | dbt capstone | sources, incremental models, tests, docs as a data catalog + lineage & impact analysis + modeling design exercise | 6–8h |

## Quickstart

```powershell
git clone <this repo>   # or copy the folder
cd ETL_SAMPLE
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
# then follow A00 - it also sets up git

# pick a data home on a fast local disk with ~80 GB free - NOT OneDrive/Dropbox
setx ETL_LAB_DATA "D:\etl_lab"   # then open a fresh terminal

# generate the small dev dataset (~800 MB, one minute)
python datagen/generate.py --scale small --month 1
python datagen/selftest.py --scale small
```

Then open [A00](assignments/A00_setup.md) and follow it - it walks through the
full-scale generation (~24 GB, 10–20 min) and a guided tour of the data.

## Rules of the lab

1. **Develop on `small`, run on `full`.** Every pipeline is written against the
   1/20-scale copy, then pointed at the real thing. This is how professionals
   avoid 40-minute debug cycles.
2. **Measure everything.** Keep a `journal.md`: wall-clock, peak RAM, row
   counts, before/after for every optimization. Numbers you wrote down beat
   memories every time.
3. **Never trust, always reconcile.** The manifests exist so you can prove your
   load is complete. Use them.
4. **Don't peek ahead.** Several assignments stage surprises (a crash, a schema
   break). Hitting them cold is the point.
5. **Data stays out of the repo** (and out of any synced folder). The repo is
   code and docs; `ETL_LAB_DATA` is data.
6. **Work like an engineer.** One branch per assignment, a commit per task,
   merge to `main` only when the Definition of Done is green
   ([guides/git_basics.md](guides/git_basics.md)). Shared code grows in
   `work/` and gets **imported** by later assignments - never re-pasted
   ([guides/composable_code.md](guides/composable_code.md)) - and
   `python -m pytest tests/ -q` must be green before any merge.

## Guides & references

[guides/git_basics.md](guides/git_basics.md) (branch/commit/merge/undo),
[guides/composable_code.md](guides/composable_code.md) (growing the `work/`
package + pytest), [guides/data_modeling.md](guides/data_modeling.md) (grain,
facts and dimensions, keys, SCD types, and the schools - Kimball, Inmon, Data
Vault, medallion), [guides/data_governance.md](guides/data_governance.md)
(governance, catalog, metadata and the three levels of lineage - a map to where
the lab builds each), [guides/troubleshooting.md](guides/troubleshooting.md) -
**the checklist to open whenever you're stuck** - and
[GLOSSARY.md](GLOSSARY.md) for any term you forgot.

## After the lab

Everything here scales up to a name-brand tool - you have already built the
hard part of each:

- **Orchestrators** (Airflow, Dagster): they schedule, retry, and backfill the
  idempotent per-day tasks you built in A07/A11 - same tasks, a scheduler
  instead of a `for` loop.
- **Object storage** (S3, ADLS, GCS): the Hive lake layout transfers unchanged;
  DuckDB reads `s3://…/order_date=…/` over httpfs exactly like local folders.
- **Table formats** (Apache Iceberg, Delta Lake): the industrial version of
  A09's versioned-dirs-and-pointer publish - snapshots tracked in metadata so
  only changed files are rewritten.
- **Cloud warehouses** (Snowflake, BigQuery): the same SQL and the same
  staging/core/marts layering on elastic hardware - A13's right-sizing
  instincts become a monthly bill, and the modeling vocabulary transfers
  verbatim: the star schema you build in A15 is Kimball's, and
  `staging`/`core`/`marts` is the medallion architecture's bronze/silver/gold
  under other names.
- **Streaming & CDC** (Kafka, Debezium): this lab is batch - files, once a day.
  Continuous event streams and change-data-capture are the next frontier, and
  they reuse every idea here: contracts, idempotency, late data.

## For mentors

[SPEC.md](SPEC.md) documents the dataset contract, exact dirt rates, and each
assignment's binding scope. `datagen/selftest.py` proves a generated dataset
matches it. Assignments have objective Definition-of-Done checklists; most
expected numbers derive from the manifests, so they are checkable without
running the student's code. The signed contract lives in
[contracts/](contracts/), and SPEC.md §3.9 carries the violation-vs-gap answer
key that A05/A06 grading builds on. Governance, catalog, metadata and lineage
are covered explicitly rather than by implication -
[guides/data_governance.md](guides/data_governance.md) maps each concern to the
assignment that builds it, including all three levels of lineage (row-level via
`_data_date`/`_run_id` and the run ledger, table-level via the dbt DAG,
column-level traced by hand in A15).

Natural mentor checkpoints: after A03 (layered model, first `work/` module +
tests), after A07 (idempotency + preflight - the safety rails), after A11 (the
full ops story: runner, alerts, postmortem), and a final walkthrough at A15 -
at each, the student's journal and merged branches are the review material.

**Reviewing a student's repo:** start from `git log --oneline --first-parent
main` - done right it reads as one merge bubble per assignment. Then spot-check
a branch or two for four things: commits come one per task, not one squashed
blob; `python -m pytest tests/ -q` is green when run at the merge commit; later
assignments **import** `work/cleaners.py` and friends instead of re-pasting
them (a quick grep for `from work` versus duplicated cleaner SQL settles it);
and `journal.md` carries the wall-clock and peak-RAM numbers the DoD checklists
demand - missing numbers usually mean the measuring was skipped.
