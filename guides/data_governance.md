# Data governance — a map of what you already built

"Data governance" sounds like a committee, a policy PDF, and a quarterly meeting.
For a working data engineer it is none of those. It is a short list of **questions
people will ask you**, and the **concrete artifacts** that answer them — and you
have been building those artifacts since A03 without the word ever appearing.

So this page is a **map, not a lesson**: everything it names is taught properly in
an assignment, and each row points at where. Read it once around A06 (when the
contract makes the vocabulary click) and again after A15 (when the catalog and the
DAG make the last third real). Unfamiliar terms are in `GLOSSARY.md`.

## 1. Governance is six questions, and each one has a file

| The question someone will ask | The artifact that answers it | If you don't have it |
|---|---|---|
| **Who owns this data?** | the contract's `producer:` / `consumer:` blocks — team, contact, escalation | incidents die in a group chat; nobody is wrong, so nothing is fixed |
| **What did we promise?** | the contract's schema, semantics, tolerances, freshness SLA and `change_management` | every disagreement becomes an argument about memory |
| **What's sensitive here?** | the `data_classification` clause and what it forces (`email_hash`, no raw email) | your warehouse becomes a PII store by accident — exactly what A06 Task 12 catches |
| **Can I trust this number?** | `ops.dq_report`, the quarantine, `ops.alerts`, reconciliation against the manifest | "the dashboard looks wrong" has no evidence trail, only opinions |
| **Who ran it, when, did it work?** | `ops.etl_runs`, `logs/run_<run_id>.log`, and the `_run_id` on every row | you cannot tell a missing load from an empty day |
| **What breaks if this changes?** | lineage — the dbt DAG, and impact analysis run *before* you answer the producer | you agree to a "small" rename and find out downstream, in public |

Notice what is *not* in the right-hand column: no committee, no policy portal, no
approval workflow. Every artifact is a YAML file, a table, or a generated website
— version-controlled and reviewed like code, and "governance" is just the
collective noun for them. An organization with a governance *program* and none of
these artifacts has meetings; one with the artifacts and no program has
governance. The rest of this page walks the four pieces students usually meet as
buzzwords — **coverage**, **metadata**, **catalog**, **lineage**.

## 2. Coverage map — where the lab does each piece

| Governance concern | Where the lab does it |
|---|---|
| Ownership & escalation | `producer:` / `consumer:` blocks in `contracts/shopcore_orders_daily.contract.yaml` (A06 Task 1) |
| Interface promises & change control | the contract itself + semver + `change_management` notice policy (A06 Tasks 1, 10, 11; tested for real in A10 Task 8) |
| Data classification / PII / retention | the `data_classification` clause drafted and then *implemented* as `email_hash` (A06 Task 12; carried into dbt by A15's `stg_shopcore__customers`) |
| Quality standards & enforcement | contract tolerances → the A05 check suite → quarantine → `ops.dq_report` (A05 Tasks 5–7), plus the pre-load gate `work/contract_check.py` (A06 Task 4) |
| Naming & shared-namespace rules | SPEC §7.1, introduced in A03's naming box; carried into dbt model names in A15 |
| Audit trail (who/what/when ran) | `ops.etl_runs`, `ops.alerts`, `logs/run_<run_id>.log` (A07 Tasks 2–3; queried as evidence in A11 Task 7) |
| Incident process | A11 Task 9's postmortem + the producer incident notes from A05/A06 (violation vs gap) |
| Freshness / SLA watching | `work/freshness_check.py` (A05 Task 9) and the on-the-clock `work/freshness_gate.py` (A11 Task 7) |
| Discovery & documentation | `dbt docs generate` (A15 Task 11) — see §4 |
| Access control | **named, not practiced** — see below |

**The honest gap: access control.** Everything above is real in this lab; access
control is not. A file-based warehouse on your own laptop has exactly one
principal — you — so there is nothing to grant, revoke, or audit. In a real shop
it is a first-class concern: who reads the PII columns, who writes to `core`, who
can drop a table, who approves changes to that list. The closest this lab gets is
the *failure mode* — the networked-DB column of `guides/troubleshooting.md` (§5)
teaches what a missing `GRANT` looks like from the outside, including the trap
where a permission error is reported as *"relation does not exist"*, sending you
hunting for a table that is right there.

## 3. Metadata — three kinds, and you write all three

Metadata is data *about* the data. The word causes confusion because people say
it and mean only the first kind:

### 3.1 Technical metadata — the shape

Columns, types, nullability, partitions, file layout, row groups, sizes. Machine
readable, machine generated, and always true — it is derived from the object
itself, so it cannot drift from reality.

```sql
DESCRIBE core.orders;                       -- columns and types (A01 Task 3)
SELECT column_name, data_type, is_nullable  -- the same, queryable
FROM duckdb_columns() WHERE table_name = 'orders';

SELECT row_group_id, row_group_num_rows     -- physical layout (A01 Task 7)
FROM parquet_metadata('<DATA_ROOT>/tmp/orders_2026-06-02.parquet')
WHERE column_id = 0;
```

The Hive directory names in `lake/orders/order_date=…/` are technical metadata too
— that is precisely why partition pruning works (A02) — and so are the producer's
manifests in `raw/manifest/`: row count, bytes, min/max `order_ts`, shipped *with*
the file so you can prove the load is complete (A05 Task 2).

### 3.2 Operational metadata — the behavior

Did it run, when, how long, how many rows, did the checks pass, is it fresh. This
is the kind you had to *build*, and the only kind that goes wrong by simply not
being written. Three tables and three questions:

```sql
-- Is it fresh?  (SPEC §7.2's staleness probe — one query, no file spelunking)
SELECT max(_data_date) FROM core.orders;

-- Did it run?  (A07's ledger — one row per attempt, failures included)
SELECT run_date, step, status, rows, finished_at, error
FROM ops.etl_runs ORDER BY started_at DESC LIMIT 10;

-- Is it good?  (A05's paper trail — one row per date × check, zeros included)
SELECT check_type, check_name, rows_failed, pct_failed, threshold_pct, passed
FROM ops.dq_report WHERE order_date = DATE '2026-06-02' ORDER BY passed, check_name;
```

Plus `ops.alerts` (the pager: severity, source, message) and the per-run log
`logs/run_<run_id>.log`. When someone asks "can I trust yesterday's revenue
number?", those three queries *are* the answer — and A05 insists on writing
zero-failure rows because a check with no record looks exactly like a check that
never ran.

### 3.3 Business metadata — the meaning

What does this column actually *mean*? This is the only kind no tool can derive
for you, and the only kind that answers the question people actually have.

- **Contract comments** (`contracts/shopcore_orders_daily.contract.yaml`): the
  `comment:`, `money:`, `timezone:` and `warehouse_policy:` clauses. `updated_at`
  is the sharpest example in the lab — its *type* is `TIMESTAMP`, which tells you
  nothing; the comment tells you it is the latest-wins dedupe marker, guaranteed
  `>= order_ts`, strictly increasing only for in-file corrections. And its
  deliberate silence about timezone is itself business metadata: a documented
  unknown (A06 Task 3).
- **dbt descriptions** — `description:` on sources (`sources.yml`, A15 Task 2)
  and on models and their key columns (`schema.yml`, A15 Task 10). This is the
  text the catalog renders in §4.
- **`GLOSSARY.md`** — lab-wide definitions of *grain*, *era*, *quarantine*,
  *restatement*, so one word means one thing in two people's heads.

One rule, stated in SPEC §7.6 and carried into A15: **a placeholder description
is worse than none.** `description: the order id` costs a reader ten seconds and
buys them nothing, while making the catalog *look* documented. Write the sentence
you would say out loud to a new teammate, or leave the field empty so the gap
stays visible.

## 4. The catalog — one place to look it up

A data catalog does three jobs:

1. **Discovery** — "is there already a table with store-level daily revenue?"
   (the alternative: asking in chat, and building a fourth duplicate when nobody
   answers).
2. **Trust** — "what does this column mean, who owns it, when did it last load,
   what tests does it pass?" Description, owner, freshness and results in one place.
3. **Impact** — "if this column changes, what downstream breaks?" That is lineage,
   and it is §5.

**A15's `dbt docs generate` is a data catalog.** Say it plainly, because students
usually meet "catalog" as a product name and don't recognize the thing they just
built. Run `dbt docs generate && dbt docs serve` (A15 Task 11) and you get a
searchable site: every model and source, every column with its type and your
`description:`, the tests on each, source freshness, the compiled SQL, and the
DAG. That is a catalog — at the cost of one command and zero infrastructure.

What commercial catalogs — **DataHub**, **OpenMetadata**, **Unity Catalog**,
**Glue Data Catalog** — add on top:

- **Coverage beyond one tool.** dbt only knows what dbt built. A company's data
  also lives in ingestion jobs, notebooks, BI dashboards and hand-made tables; a
  catalog crawls all of them and stitches one graph.
- **Column-level lineage, derived automatically** by parsing the SQL (§5.3).
- **Ownership and classification as enforceable tags** — mark a column `pii` and
  have access policies and masking follow the tag, rather than trusting each
  pipeline to remember (this lab's `email_hash` is the manual version).
- **Usage signals** — who queries this table, how often, which dashboards depend
  on it. That is how you find the tables nobody reads and the ones you must never
  break.

The catch is the same at every scale: a catalog *renders* the metadata you wrote.
Buying one does not create business metadata; it makes the absence searchable.

## 5. Lineage has three levels

"We have lineage" is not a claim until you say *which level*. They answer
different questions and are built by different machinery.

| Level | Granularity | The question it answers | Where the lab builds it |
|---|---|---|---|
| Row-level provenance | one row | where did **this row** come from? | `_data_date` + `_run_id` → `ops.etl_runs` (A03, A07) |
| Table/model-level | one table | what feeds this, and what breaks if the source changes? | the dbt DAG (A15) |
| Column-level | one column | which downstream *numbers* does this column move? | traced by hand in A15 |

### 5.1 Row-level — the two columns and the ledger

Every staging and core row carries exactly two warehouse-added columns (SPEC
§7.2): `_data_date` (the delivery's business date) and `_run_id` (which run wrote
it). Everything else about the load lives on the ledger row that `_run_id` points
at — one row per attempt instead of the same string copied onto 1.2M rows:

```sql
SELECT o.order_id, o._data_date, o._run_id,
       r.source_file, r.finished_at, r.status, r.rows
FROM core.orders o
JOIN ops.etl_runs r ON r.run_id = o._run_id
WHERE o.order_id = 2020000123;      -- the suspicious row
```

From there the trail continues into `logs/run_<run_id>.log` — the A07 Task 8
traceability demo, and the move you make first when a single row looks wrong.

Before A07 exists, `_run_id` is NULL and the join has nothing to hit. You are not
stuck: the contract's `delivery.path_pattern` is `orders_YYYY-MM-DD.csv`, one
file per business date, so `_data_date` **is** the file name. That is exactly why
a `_source_file` column would be storing the same fact twice.

### 5.2 Table/model-level — the DAG

Once A15 is built, `dbt docs` draws the graph: lake → `stg_shopcore__orders` →
`core.fct_orders` → `marts.daily_store_sales`, with tests hanging off each node.
It reads the graph from your `ref()` and `source()` calls, which is why typing a
real table name inside a model silently drops it out of lineage.

The operational use is **impact analysis** — answering "what breaks?" *before*
you reply to the producer. shopcore announces they are dropping a column; you
run:

```powershell
dbt ls --select source:shopcore.orders+     # everything downstream of the source
```

and you have the list of affected models. That is the contract's
`change_management` notice window (30 days for breaking changes) turned into
something you can actually use inside it, instead of finding out in production.

### 5.3 Column-level — the one you still do by hand

dbt does not show it; commercial catalogs derive it by parsing SQL. A15 has you
trace one column end to end yourself, so you know what is being automated:

```
raw orders.order_total
  → stg_shopcore__orders.order_total        (cast, cleaned)
  → core.fct_orders.order_total             (deduped, latest-wins)
  → marts.daily_store_sales.gross_revenue   (sum, grain = date × store × currency)
```

Four hops, and the last is a number finance looks at. That is what column-level
lineage is *for*: "the producer wants to change the money scale — which published
figures move?", and its privacy twin, "we classified `email` as PII — every place
it flowed is now in scope." Table-level lineage says the mart is affected; only
column-level says *which measure*.

## 6. Week one at a new job — eight questions to ask

You will not be handed a contract, a ledger and a catalog. Ask for them — and
treat the answers you *cannot* get as your first backlog:

1. **Is there a contract or spec for this feed?** If not — who is the producer,
   and what do they believe they promised? (`contracts/README.md` has the
   per-column interrogation to run through.)
2. **Who is on call for the source, and where do change announcements land?** A
   channel name is a governance artifact; "ask Priya" is not.
3. **What is the freshness SLA, and what watches it?** A watchdog on the clock,
   or a human noticing a flat dashboard? (A11 Task 7's distinction.)
4. **Where is the run history?** Can you see whether last night's load ran, how
   many rows it wrote, and whether it has failed before?
5. **What is known-dirty versus incident-worthy?** If there is no quality record,
   nobody can tell absorbed producer bugs from new ones.
6. **Which tables hold PII or regulated data, who decided that, and what is the
   retention rule?** Then check whether the pipeline actually implements it —
   A06 Task 12 exists because clauses on paper are not clauses in tables.
7. **What am I allowed to read and write, and who approves more?** The access
   question this lab can only name (§2).
8. **What breaks if I change this model — and how would consumers find out?**
   If the answer is "we'd hear about it", that is not lineage, that is a pager.

Write the answers down. The gaps are not somebody else's problem to raise later —
they are the most valuable pull requests a new engineer can open, because they are
the ones only a newcomer can still see.
