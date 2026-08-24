# A06 — Data contracts
**Topic:** data contract · **Est. time:** 4–5h · **Difficulty:** ⭐⭐⭐
**Requires:** A05 done — your `ops.dq_report` and quarantine output are today's raw
material; month-1 raw data at both scales (from A00). No month-2 data — that arrives in A10.

## Why this matters

In A05 you found the dirt *after* it was already on your disk — by then it is your
problem, whatever the cause. Real teams get burned this way: upstream renames a
column on a Friday, Monday's load silently writes garbage, and the dashboard is
wrong for a week. A **data contract** moves the checkpoint to the boundary between
teams — and this lab ships a real one: the signed agreement between **shopcore,
the e-commerce platform** that produces the orders feed, and you, the warehouse.
Today you learn to read it the way a professional does, hold your A05 findings
against it, hunt the places where it is silent, and build the automated gate that
checks every incoming file *before* a single row is loaded — then close one
silence with your own hands, in your own tables.

## Background

A data contract is the **agreed, versioned promise between a producer and a
consumer**. It is a plain file (YAML here) that both sides review and diff like
code — and that a script can enforce. You have already used one clause of it
without ceremony: A05's thresholds came from the `row_level_tolerances` block of
`contracts/shopcore_orders_daily.contract.yaml`. That file is the real thing:
version 1.3.0, status `agreed`, owned by shopcore's Checkout Platform team, with
an escalation channel for 4am and a change-management policy for everything else.
(1.3.0 — so it has been amended before. Contracts are living documents.)

**How is this different from A05 validation?** Your A05 suite answers "is this
data good enough to load?" from *your* side, after the file arrives, row by row.
The contract answers "is this file the thing we agreed on?" — and because the
promise is written down and versioned, a failure is *accountable*: either
shopcore broke their promise, or the promise must change through a version bump
both sides can see. The gate you build today does not replace A05; it runs
first, as a fast pre-flight check on a sample, and A05 still validates every row
afterwards.

**Violation vs gap — the two failure modes.** A *violation* is dirt the contract
explicitly forbids: a negative `order_total` when the money clause says
`negative_allowed: false`. Violations are quarantined, counted, and **reported
to the producer** — never silently patched, because silent patching hides a
producer bug forever. A *gap* is dirt the contract is *silent* about: a European
decimal comma in a money field, when the money clause never mentioned locale.
You cannot blame shopcore for a promise never made. Gaps get handled explicitly
in your ETL *and* turned into a **proposed amendment** so the silence closes.
Learning to tell these apart is the core skill of this assignment —
`contracts/README.md` calls it hunting the silences before they hunt you.

**Breaking vs non-breaking.** Semver gives change a vocabulary. A *breaking*
change (MAJOR bump: 1.3.0 → 2.0.0) makes existing consumer code wrong: a rename,
a type change, a removed column — the contract's `change_management` block
demands 30 days notice for those. A *non-breaking* change (MINOR: 1.4.0) adds
something existing code tolerates — new columns, new clauses — at 7 days notice;
a PATCH clarifies wording only. The status field has its own lifecycle:
`draft → proposed → agreed → deprecated`. The ritual's point: a breaking change
requires a conversation and a migration plan *before* the file ships, not an
error log after.

## Setup

- Git ritual (§7.4): `git switch -c a06-data-contract`, commit after each
  numbered task (`git commit -m "A06 task 4: contract gate"`), merge to `main`
  when the DoD is green — `guides/git_basics.md` has the full ritual.
- Terminal at the repo root, venv active (`pyyaml` and `pydantic` are both in
  `requirements.txt`). The `contracts/` folder ships with the repo: the signed
  contract plus `contracts/README.md`. **You never edit the signed contract** —
  amendments are drafted separately and proposed (Tasks 11–12).
- `warehouse.duckdb` from A05 on hand (`ops.dq_report`, plus the quarantine
  Parquet under `<DATA_ROOT>/quarantine/orders/`) — with `core.customers` from
  A03 still in it: Task 12 rebuilds that table.
- Scale: develop everything on **small**, run the final gate sweep on **full**.
- No special memory settings today: the gate samples at most 100,000 rows per
  file on throwaway in-memory connections (`duckdb.connect()`); the warehouse is
  not touched until Task 8 (and Task 12's one-table rebuild).

## Tasks

### Task 1 — Read the contract like a professional

Open `contracts/shopcore_orders_daily.contract.yaml` and `contracts/README.md`
side by side. The README's *per-column interrogation* is your checklist: for
every column — what type is it really, can it be null, can it repeat, what
timezone, what currency, and what is *not* written? Work through the contract
clause by clause and journal a one-line answer to each question below, **citing
the clause** that answers it. Two are worked as examples:

1. *What currency is `order_total`, and who applies rounding, how?* — USD,
   single-currency by contract; scale 2, half-up, applied by the **producer**
   (`schema.order_total.money`). The warehouse never re-rounds.
2. *A refund lands. What sign does the amount carry?* — Positive. The "negative"
   lives in `status=refunded`, not in the number (`money.negative_allowed:
   false` + `business_rules.refunds`). A naive `SUM(order_total)` therefore
   *overstates* revenue unless status is considered.
3. What timezone is `order_ts`, who converted it, and who handled DST?
4. Finance wants store-local reporting. Where does timezone conversion happen —
   and why does the contract forbid converting at ingest? (Hint:
   `warehouse_policy` — one raw UTC fact can serve every regional mart; a fact
   converted at ingest serves exactly one.)
5. `customer_id` arrives NULL. Data loss, producer bug, or meaning?
6. Is `order_id` unique? In exactly what scope — per file, per day, per
   real-world order?
7. Which two columns decide which copy of a re-sent order wins?
8. How far back may a file for day D reach? So which lake partitions can
   loading file D touch? (A08 is built on this number.)
9. How do you know a delivery is *complete* — and what two numbers must you
   reconcile on every load?
10. At what time does a missing file stop being your slow morning and become
    shopcore's incident?

**Accept when:** all ten answers are in your journal with clause names, and you
can explain #4 to a rubber duck without opening the file.

### Task 2 — Violation or gap? Classify your A05 findings

Now hold your A05 output against the promise. Summarize your quarantine by
reason (in a DuckDB shell, or via `duckdb.sql(...)`):

```sql
SELECT reject_reason, count(*) AS rows
FROM read_parquet('<DATA_ROOT>/quarantine/orders/order_date=*/*.parquet')
GROUP BY 1 ORDER BY 2 DESC;
```

For **every distinct `reject_reason`** (plus anything notable in
`ops.dq_report`), decide: **violation** (a stated clause is broken — name it),
**gap** (the contract is silent — name the silence), or **tolerated** (the
contract explicitly allows it at this rate). Careful with that last one: the
within-file exact re-sends you met in A03/A05 are *not* a violation —
`duplicates.within_file_exact_resends` tolerates up to 0.2% as the price of
at-least-once delivery.

Then write the two artifacts real teams write:

- `work/incident_note.md` — addressed to `checkout-eng@shopcore.example`
  (producer contact, from the contract): every **violation** with its measured
  count for one day and the clause it breaks. Half a page. A producer must be
  able to act on it without reading your code. This is the weekly report the
  contract's `slo_reporting` block promises them.
- A **gaps list** in your journal — input for Task 3. No blame, no counts
  needed; just "the contract does not say X, and the data does Y".

**Accept when:** every reject_reason carries one of the three labels, the
incident note cites a clause per violation, and nothing in it says "please fix"
about a gap.

### Task 3 — The gap hunt

The contract's author left two `# NOTE` comments in the YAML flagging silences
on purpose — find them; they are your first two entries and your template for
what a "silence" looks like. Then hunt at least **four total** (so two or more
found by you), drawing on everything the data has done to you since A03. For
each, journal three columns:

| silence (what the contract does not say) | how it bites (the concrete dirt or ambiguity) | one-line clause that would close it |
|---|---|---|

A gap is not a bug report. The producer owes you nothing here — that is exactly
what makes gaps dangerous: nobody is accountable until a clause exists. Your
"clause that would close it" column is the seed of Task 11's amendment.

**Accept when:** ≥4 rows, including both seeded `# NOTE`s, each with a concrete
bite you have actually observed (or can demonstrate) in this lab's data.

### Task 4 — The pre-flight gate: `work/contract_check.py`

The contract itself orders this build: `change_management.policy` says consumer
pipelines "must fail loudly (pre-load gate) on undeclared drift rather than load
garbage". First, see why the gate must test against *declared* types, not
guessed ones. Ask DuckDB's sniffer what it thinks one day looks like:

```python
import duckdb, os
from pathlib import Path

f = Path(os.environ["ETL_LAB_DATA"]) / "small" / "raw" / "orders" / "orders_2026-06-01.csv"
con = duckdb.connect()
for c in con.execute("SELECT Columns FROM sniff_csv(?)", [str(f)]).fetchone()[0]:
    print(f"{c['name']:16s} {c['type']}")
```

The sniffer calls `customer_id` a `DOUBLE` — a fraction of a percent of values
look like `"123456.0"` (A03's Excel-float dirt) and the guess widens to fit the
dirt. `order_ts` comes back `VARCHAR`. And `updated_at` comes back `TIMESTAMP
WITH TIME ZONE` — a timezone *nobody promised* (Task 3 material!). Sniffed
types drift with whatever dirt lands in the sample; the contract's `schema`
block is the fixed point to test against.

Now the gate. It checks, in order: file naming → completeness signal (manifest)
→ schema (exact names, exact order) → sampled value probes → late-corrections
window. It stops early when a deeper check would be meaningless (wrong columns
make value checks nonsense). Copy this skeleton into `work/contract_check.py`;
the `TODO` parts are yours — they mirror the given ones.

```python
"""Pre-flight contract gate: check a raw shopcore orders CSV against the
signed contract BEFORE any row is loaded. Exit 0 = all files pass; 1 = at
least one failed."""
import argparse, json, os, re, sys
from datetime import date, timedelta
from pathlib import Path

import duckdb
import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]          # work/ -> repo root
DATA_ROOT = Path(os.environ.get("ETL_LAB_DATA", REPO_ROOT / "data"))
CONTRACT = REPO_ROOT / "contracts" / "shopcore_orders_daily.contract.yaml"

SAMPLE_ROWS = 100_000                  # gate design choice, not a contract clause
# The written form of order_ts is pinned by the contract (schema.order_ts:
# "written as 'YYYY-MM-DD HH:MM:SS', no offset"). Its strptime spelling:
TS_FORMAT = "%Y-%m-%d %H:%M:%S"
# delivery.path_pattern "orders_YYYY-MM-DD.csv" as a regex:
NAMING_RX = re.compile(r"^orders_\d{4}-\d{2}-\d{2}\.csv$")


def pct(s) -> float:
    """'0.5%' -> 0.005 - the contract writes tolerances the way humans read them."""
    return float(str(s).split("%")[0].strip()) / 100.0


def load_contract() -> dict:
    return yaml.safe_load(CONTRACT.read_text(encoding="utf-8"))


def check_file(csv_path: Path, contract: dict) -> list[str]:
    """Return a list of producer-facing problems. Empty list = PASS."""
    problems: list[str] = []
    con = duckdb.connect()                      # in-memory scratch connection

    # ---- 1. delivery: file naming ----------------------------------------
    if not NAMING_RX.match(csv_path.name):
        return [f"file name '{csv_path.name}' does not match delivery.path_pattern "
                f"'{contract['delivery']['path_pattern']}' - wrong file or wrong feed?"]
    file_date = date.fromisoformat(csv_path.stem.split("_")[1])

    # ---- 2. completeness signal: the manifest ----------------------------
    manifest_path = csv_path.parent.parent / "manifest" / (csv_path.stem + ".json")
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        size = csv_path.stat().st_size
        if size != manifest["bytes"]:
            problems.append(f"bytes on disk ({size}) != manifest.bytes "
                            f"({manifest['bytes']}) - truncated or partial delivery? "
                            "(quality.reconciliation)")
    else:
        print(f"  [warn] {csv_path.name}: no manifest found - completeness signal "
              "not verified (ad-hoc file?)")

    # ---- 3. schema: exact names, exact order -----------------------------
    expected = [c["name"] for c in contract["schema"]]
    sniffed = con.execute("SELECT Columns FROM sniff_csv(?)", [str(csv_path)]).fetchone()[0]
    actual = [c["name"] for c in sniffed]
    if actual != expected:
        missing = [c for c in expected if c not in actual]
        extra = [c for c in actual if c not in expected]
        if missing:
            problems.append(f"missing column(s) {missing} - dropped or renamed upstream? "
                            "change_management: that is a MAJOR bump with 30 days notice.")
        if extra:
            problems.append(f"unexpected column(s) {extra} - new export version? "
                            "change_management: additive needs a MINOR bump, 7 days notice.")
        if not missing and not extra:
            problems.append(f"column order changed: expected {expected}, got {actual}")
        return problems     # value checks are meaningless on the wrong columns

    # ---- 4. sampled value probes, everything as text ---------------------
    con.execute("CREATE TEMP TABLE sample AS SELECT * FROM "
                "read_csv(?, header=true, all_varchar=true) LIMIT ?",
                [str(csv_path), SAMPLE_ROWS])
    total = con.execute("SELECT count(*) FROM sample").fetchone()[0]
    tol = {k: pct(v) for k, v in contract["quality"]["row_level_tolerances"].items()}
    enums = {c["name"]: c["allowed_values"]
             for c in contract["schema"] if "allowed_values" in c}
    status_list = ", ".join(f"'{s}'" for s in enums["status"])
    ts_recover = (f"COALESCE(try_strptime(order_ts, ['{TS_FORMAT}', "
                  "'%d/%m/%Y %H:%M:%S']), TRY_CAST(order_ts AS TIMESTAMP))")

    # check -> (column shown in examples, SQL condition for a BAD row,
    #           tolerance, clause cited to the producer)
    checks: dict[str, tuple[str, str, float, str]] = {
        "order_id_not_castable": (
            "order_id", "order_id IS NULL OR TRY_CAST(order_id AS BIGINT) IS NULL",
            0.0, "schema.order_id: BIGINT, not nullable - no tolerance declared"),
        "unparseable_order_ts": (
            "order_ts", f"order_ts IS NULL OR {ts_recover} IS NULL",
            tol["unparseable_order_ts"], "quality.row_level_tolerances"),
        "null_or_invalid_status": (
            "status", f"status IS NULL OR status NOT IN ({status_list})",
            tol["null_or_invalid_status"], "schema.status: allowed_values + case"),
        # TODO "customer_id_not_castable": tolerance 0.0 - but nullable: true!
        #      NULL is guest checkout, not dirt: count only values that are
        #      present AND not castable to BIGINT.
        # TODO "store_id_not_castable": mirror order_id with INTEGER.
        # TODO "unparseable_order_total": tolerance tol["unparseable_order_total"].
        #      Bad row = NULL OR NOT regexp_matches(order_total, '^[0-9]+\.[0-9]{2}$')
        #      - "plain USD, scale 2, no negatives, no symbols" in one regex.
    }
    for name, (col, bad, tolerance, clause) in checks.items():
        n = con.execute(f"SELECT count(*) FROM sample WHERE {bad}").fetchone()[0]
        if n / total > tolerance:
            ex = [r[0] for r in con.execute(
                f"SELECT DISTINCT {col} FROM sample WHERE {bad} LIMIT 3").fetchall()]
            problems.append(f"{name}: {n}/{total} sampled rows ({n / total:.2%}) exceed "
                            f"tolerance {tolerance:.2%} ({clause}). Examples: {ex}")

    # payment_method: enum + case pinned, but row_level_tolerances is SILENT
    # about it. No agreed threshold -> measure and report, do not hard-fail.
    pay_list = ", ".join(f"'{p}'" for p in enums["payment_method"])
    n = con.execute("SELECT count(*) FROM sample WHERE payment_method IS NULL "
                    f"OR payment_method NOT IN ({pay_list})").fetchone()[0]
    if n:
        print(f"  [warn] {csv_path.name}: payment_method {n}/{total} ({n / total:.2%}) "
              "outside the enum - a violation to report weekly (slo_reporting), "
              "but no hard-fail tolerance is declared. Amendment material.")

    # ---- 5. late-corrections window --------------------------------------
    # TODO: SELECT min/max of ts_recover over the sample (min/max skip NULLs).
    # FAIL if the oldest timestamp's date < file_date - window_days
    # (late_corrections.window_days), or the newest is after file_date itself -
    # schema.order_ts.range is [business_date - 7 days, business_date + 1 day),
    # so anything later means shopcore is leaking tomorrow's orders. Cite both
    # clauses in the messages. timedelta helps.
    con.close()
    return problems


def main() -> None:
    ...  # TODO - see below


if __name__ == "__main__":
    main()
```

Two design notes worth reading twice:

- **"Unparseable" is narrower than "wrongly formatted".** Roughly 0.7% of rows
  break the pinned written format (DD/MM and ISO-T forms) — those are
  *violations* you count and report in A05. But the 0.5% hard-fail tolerance is
  for `unparseable_order_ts`: rows no known format recovers (the impossible
  dates, ~0.03%). That is why `ts_recover` tries the recovery formats before
  declaring a row lost. A gate that hard-failed on the strict format would be
  red every single day — and a gate that is always red is a gate nobody reads.
- **What the gate deliberately skips**: `orphan_customer_id`, `orphan_sku_lines`
  and `total_vs_items_mismatch` need the dimension feeds or per-row JSON math —
  too heavy for pre-flight. They stay in your A05 post-load suite. The gate
  covers what one file alone can prove cheaply.

`main()` is yours — you have written CLI scripts since A02. It must: accept
either explicit file paths or `--scale small|full` with `--start`/`--end` (build
`<root>/raw/orders/orders_<date>.csv` per date); print one `[PASS]`/`[FAIL]`
line per file plus its problems (a missing expected file is a FAIL —
`freshness_sla`!); end with `{passed}/{total} files passed contract
{contract} v{version}` (both read from the YAML — the gate never hard-codes
what it enforces) and `sys.exit(1)` if anything failed — that exit code is what
lets Task 8 automate this.

**Accept when:** all TODOs are filled and one small file passes:
`[PASS]` preceded by one `[warn]` line about payment_method (~0.3% — the
contract's silence made visible on every run).

### Task 5 — Validate the contract itself: pydantic

Your gate is now the strictest thing in the pipeline — about the *data*. About
the *contract* it is credulous. `yaml.safe_load` hands back a plain dict, and
every `contract["schema"]`, `contract["quality"]["row_level_tolerances"]` and
`c["allowed_values"]` after it is an unchecked guess about a file that humans
edit. Nothing validates the validator.

Mistakes in a hand-edited YAML document come in two shapes, and this contract is
about to be amended twice (Tasks 11–12), so both are live:

- **A misspelt key.** Someone drafting v1.4.0 types `nullible: true`. YAML is
  delighted — it is a perfectly good mapping key. Nothing reads the document as
  a whole, so the mistake sits there until some line of code happens to touch
  that key: a `KeyError` days later that blames *your script* and never mentions
  the contract — or, wherever you wrote `.get()` or `if "x" in c`, no error at
  all, just a rule that quietly stopped existing. A gate with one rule silently
  switched off is worse than no gate, because you still trust it.
- **A wrong type.** `window_days: seven`. Nothing complains until the
  late-corrections arithmetic runs — at file 30 of 45, with a `TypeError` that
  names `timedelta` and never mentions the contract.

The industry answer is **pydantic**: declare the document's shape as Python
classes, and it either hands back typed objects or raises, naming the exact
field. It is already in `requirements.txt` (`pip install pydantic` if you
slimmed your venv — it is small and pure-python; this lab was verified on 2.13).
You have hand-rolled the checking; now meet the tool that does it declaratively.

Write `work/contract_model.py`. This one is given in full — new concept, and the
value is in reading the *choices*, not typing them:

```python
"""Typed model of the shopcore orders contract.

Job: turn the signed YAML into Python objects whose shape is checked the moment
it is read, so a misspelt key or a wrong type fails HERE, with a field path -
instead of quietly turning a rule off three checks later.

Assumption: blocks the gate actually reads are modelled field by field; blocks
nobody reads are declared as opaque dicts. They still have to be declared -
that is what keeps extra='forbid' meaningful at the top level.
"""
from datetime import date
from pathlib import Path
from typing import Annotated, Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, StringConstraints

REPO_ROOT = Path(__file__).resolve().parents[1]          # work/ -> repo root
CONTRACT = REPO_ROOT / "contracts" / "shopcore_orders_daily.contract.yaml"

# Tolerances are written the way humans read them ('0.5%'). A bare 0.5 is
# ambiguous - half a percent or fifty? - so the shape is pinned, not just the type.
Percent = Annotated[str, StringConstraints(pattern=r"^\d+(\.\d+)?%$")]


class Clause(BaseModel):
    """Base for every modelled block: an unknown key is an error, not a shrug."""
    model_config = ConfigDict(extra="forbid")


class Money(Clause):
    currency: str
    scale: int
    rounding: str
    negative_allowed: bool          # a truthy STRING here would invert the rule


class Column(Clause):
    name: str
    type: str
    nullable: bool                  # required: 'nullible:' cannot pass as this
    allowed_values: list[str] | None = None
    case: str | None = None
    money: Money | None = None
    references: str | None = None
    range: str | None = None
    shape: str | None = None
    timezone: str | None = None
    unique: str | None = None
    warehouse_policy: str | None = None
    business_rule: str | None = None
    comment: str | None = None


class Tolerances(Clause):
    """All six thresholds, by name. Drop one and the gate's lookup would KeyError
    mid-sweep; misspell one and the rule it guards is silently unenforceable."""
    null_or_invalid_status: Percent
    unparseable_order_ts: Percent
    unparseable_order_total: Percent
    orphan_customer_id: Percent
    orphan_sku_lines: Percent
    total_vs_items_mismatch: Percent


class Quality(Clause):
    row_level_tolerances: Tolerances
    structural: dict[str, str]      # declared, not modelled - the gate skips these
    reconciliation: list[str]
    duplicates: dict[str, Any]


class Delivery(Clause):
    path_pattern: str
    method: str
    business_date: str
    format: str
    schedule: str
    freshness_sla: str
    completeness_signal: str
    volume_expectation: str


class LateCorrections(Clause):
    window_days: int                # arithmetic happens on this - it must be an int
    semantics: str


class Contract(Clause):
    contract: str
    version: str
    status: Literal["draft", "proposed", "agreed", "deprecated"]
    effective_from: date
    delivery: Delivery
    late_corrections: LateCorrections
    columns: list[Column] = Field(alias="schema")   # 'schema' is taken on BaseModel
    quality: Quality
    # Declared, not modelled: nothing in the gate reads them.
    producer: dict[str, Any]
    consumer: dict[str, Any]
    change_management: dict[str, Any]
    business_rules: dict[str, Any]
    slo_reporting: dict[str, Any]

    def column(self, name: str) -> Column:
        """Look a column up by contract name (raises if the contract lacks it)."""
        return next(c for c in self.columns if c.name == name)


def load_typed(path: Path = CONTRACT) -> Contract:
    """Parse + validate the contract. Raises pydantic.ValidationError on any
    unknown key, missing key or wrong type, naming the exact field."""
    return Contract.model_validate(yaml.safe_load(path.read_text(encoding="utf-8")))


if __name__ == "__main__":
    c = load_typed()
    print(f"{c.contract} v{c.version} ({c.status}, effective {c.effective_from})")
    print(f"  {len(c.columns)} columns, late window {c.late_corrections.window_days} days")
    print(f"  order_total negative_allowed = {c.column('order_total').money.negative_allowed!r}"
          f" ({type(c.column('order_total').money.negative_allowed).__name__})")
    print(f"  status tolerance = {c.quality.row_level_tolerances.null_or_invalid_status}")
```

Four decisions worth arguing with:

- **`extra="forbid"` is the whole point.** pydantic's default is to *ignore*
  keys it does not recognise. Misspell a **required** key and you would still
  get an error, but pointed at the wrong field — `nullable` is missing, and
  nobody mentions `nullible`. Misspell an **optional** one (`alowed_values:`)
  and the default swallows it in total silence, exactly like `yaml.safe_load`.
  Forbidding extras is what names the mistake that was actually made. Verified
  both ways below — flip the line to `extra="ignore"` and re-run if you want to
  watch it go quiet.
- **Model what the gate reads, not the whole document.** `producer`, `consumer`,
  `change_management`, `business_rules` and `slo_reporting` are prose for humans;
  no check consults them. They are still *declared* as `dict[str, Any]` — because
  with `extra="forbid"` at the top level, an undeclared block is an error, and a
  misspelt block name (`chnage_management:`) should be one.
- **`schema` needs an alias.** `BaseModel.schema` is taken by pydantic itself, so
  the field is `columns` with `alias="schema"`. Validation still reads the YAML
  key `schema`, and — usefully — error paths still *say* `schema`.
- **Types, not just keys.** `negative_allowed: bool` is the sharpest one. In a
  raw dict, a contract that said `negative_allowed: "false"` would hand you the
  **string** `"false"`, which is truthy in Python: `if
  money["negative_allowed"]:` reads it as *negatives are allowed* and the money
  rule inverts. The model returns a real `bool` or refuses.

Run it against the signed contract — it must pass, because a model that rejects
the real document is a bug in the model, not in the contract:

```powershell
python -m work.contract_model
# shopcore_orders_daily v1.3.0 (agreed, effective 2026-06-01)
#   10 columns, late window 7 days
#   order_total negative_allowed = False (bool)
#   status tolerance = 1%
```

Now break it — **on a copy**. The signed contract is never edited, not even for
a demo (that is the whole ethic of this assignment); the corruption lands in
scratch space, like the tampered CSVs of Task 7:

```python
"""Prove the model earns its keep: corrupt a COPY of the contract, watch it die.
Never touches contracts/ - the signed file stays byte-identical."""
import os
from pathlib import Path

import yaml
from pydantic import ValidationError

from work.contract_model import CONTRACT, load_typed

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_ROOT = Path(os.environ.get("ETL_LAB_DATA", REPO_ROOT / "data"))
SCRATCH = DATA_ROOT / "tmp" / "contract_typo"
SCRATCH.mkdir(parents=True, exist_ok=True)
broken = SCRATCH / CONTRACT.name

# --- 1. the real, signed contract: must parse clean --------------------------
c = load_typed()
print(f"[typed] {c.contract} v{c.version} - {len(c.columns)} columns OK")

# --- 2. a copy with the two mistakes that actually happen --------------------
text = CONTRACT.read_text(encoding="utf-8")
text = text.replace("nullable: true              # guest checkout",
                    "nullible: true              # guest checkout")   # misspelt key
text = text.replace("  window_days: 7", "  window_days: seven")       # wrong type
broken.write_text(text, encoding="utf-8")

# --- 3. yaml.safe_load is perfectly happy with it: that IS the problem -------
raw = yaml.safe_load(broken.read_text(encoding="utf-8"))
print(f"[yaml ] loads fine: {len(raw['schema'])} columns, "
      f"customer_id.nullable = {raw['schema'][1].get('nullable', '<<GONE>>')}, "
      f"window_days = {raw['late_corrections']['window_days']!r}")

# --- 4. the model is not ----------------------------------------------------
try:
    load_typed(broken)
    print("[typed] no error - the model is not doing its job")
except ValidationError as e:
    print(f"[typed] {e}")
```

Save as `work/contract_typo_demo.py` and run it (the `-m` form and the
`from work.contract_model import …` line both need an empty `work/__init__.py`
— the same package-style import Task 8 uses):

```powershell
python -m work.contract_typo_demo
# [typed] shopcore_orders_daily v1.3.0 - 10 columns OK
# [yaml ] loads fine: 10 columns, customer_id.nullable = <<GONE>>, window_days = 'seven'
# [typed] 3 validation errors for Contract
# late_corrections.window_days
#   Input should be a valid integer, unable to parse string as an integer [type=int_parsing, input_value='seven', input_type=str]
#     For further information visit https://errors.pydantic.dev/2.13/v/int_parsing
# schema.1.nullable
#   Field required [type=missing, input_value={'name': 'customer_id', '...ported, not dropped.\n'}, input_type=dict]
#     For further information visit https://errors.pydantic.dev/2.13/v/missing
# schema.1.nullible
#   Extra inputs are not permitted [type=extra_forbidden, input_value=True, input_type=bool]
#     For further information visit https://errors.pydantic.dev/2.13/v/extra_forbidden
```

(Those `errors.pydantic.dev/2.13/…` URLs carry your installed pydantic's minor
version — on a newer one they read `2.14`, `2.15`; nothing else in the block
changes.)

Read line 2 twice: `yaml.safe_load` loaded the broken file **without a murmur**,
and reported `customer_id.nullable = <<GONE>>`. That is what your Task 4 gate
would have been running on. Then read the error paths. Not "something is wrong
with the contract" — `schema.1.nullible` says *second column of the schema
block, key `nullible`, not permitted*, which is an address you can hand back to
whoever drafted the amendment. One typo produced two errors (the field that
vanished and the field that appeared), and the wrong type was caught in the same
pass instead of 30 files into a sweep.

Wire it into the gate. `load_contract()` in `work/contract_check.py` grows one
line — the rest of Task 4's code keeps its dicts and does not change:

```python
from work.contract_model import Contract          # new import

def load_contract() -> dict:
    raw = yaml.safe_load(CONTRACT.read_text(encoding="utf-8"))
    Contract.model_validate(raw)      # the gate on the gate: bad contract, no run
    return raw
```

(If you would rather the whole gate spoke attributes — `contract.delivery.`
then a dotted path your editor autocompletes, instead of string keys it cannot
check — return the `Contract` object instead of `raw` and follow the errors
until they stop. `column()` is already there for the per-column lookups. A
twenty-minute refactor and a real upgrade.)

**When NOT to use it.** Right here: one small document, parsed once per run, on
which every other check depends. Wrong, loudly, one layer down: a `Column`-style
model per *data row* means 82 million Python objects for the full feed, each
with its own validation pass — hours of CPU and RAM you do not have, to answer
questions DuckDB answers in one scan over columnar data. Row validation stays in
SQL (A05's check suite) or in a frame-level schema (A05's pandera task, which
validates a whole DataFrame at once, not a row at a time). **pydantic is for
config; SQL and frame schemas are for feeds.** Reach for the wrong one and the
82M-row day is where you find out.

**Accept when:** `python -m work.contract_model` prints the contract's real name
and version, the typo demo shows `yaml.safe_load` accepting the broken copy and
pydantic rejecting it with `schema.1.nullible` in the message, the signed
contract on disk is untouched, and `contract_check.py` now validates the
contract through the model before using it.

### Task 6 — Gate all of month 1 (small)

```powershell
python work\contract_check.py --scale small --start 2026-06-01 --end 2026-07-15
```

Expect 45 `[PASS]` lines (each with its payment_method `[warn]`) and
`45/45 files passed contract shopcore_orders_daily v1.3.0` in roughly 25–30
seconds on the baseline machine — the cost per file is the sample read, not the
file size (remember that for Task 9). If a file FAILs, read the cited clause
before touching anything — and remember the fix is never "edit the signed
contract until green".

**Accept when:** 45/45 PASS at small scale, and the run time is in your journal.

### Task 7 — Break it on purpose: three tampered files (small)

You cannot trust a gate you have never seen fail. Craft three broken copies of
one day file in scratch space — each in its own folder so all three can keep the
real file name (they must get *past* the naming check to test the deeper
layers). Save as `work/make_tampered.py` and run once:

```python
"""Craft three deliberately broken copies of one small day file."""
import csv, os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_ROOT = Path(os.environ.get("ETL_LAB_DATA", REPO_ROOT / "data"))
SRC = DATA_ROOT / "small" / "raw" / "orders" / "orders_2026-06-02.csv"
OUT = DATA_ROOT / "tmp" / "tampered"
N = 5000               # a few thousand rows is plenty - the gate samples anyway

with SRC.open(newline="", encoding="utf-8") as f:
    reader = csv.reader(f)
    header = next(reader)
    data = [next(reader) for _ in range(N)]

def write(folder: str, hdr: list[str], rows: list[list[str]]) -> None:
    p = OUT / folder / SRC.name
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(hdr)
        w.writerows(rows)
    print(f"wrote {p}")

i = header.index("payment_method")     # 1. DROPPED column: it silently vanishes
write("dropped_col", header[:i] + header[i+1:], [r[:i] + r[i+1:] for r in data])

write("added_col", header + ["currency"],      # 2. ADDED column at the end
      [r + ["USD"] for r in data])

j = header.index("order_id")           # 3. TYPE change: BIGINT -> 'ORD-...' string
write("type_change", header,
      [r[:j] + [f"ORD-{int(r[j]):010d}"] + r[j+1:] for r in data])
```

Then point the gate at all three:

```powershell
python work\contract_check.py `
  "$env:ETL_LAB_DATA\tmp\tampered\dropped_col\orders_2026-06-02.csv" `
  "$env:ETL_LAB_DATA\tmp\tampered\added_col\orders_2026-06-02.csv" `
  "$env:ETL_LAB_DATA\tmp\tampered\type_change\orders_2026-06-02.csv"
```

All three must FAIL with messages a *producer* could act on without seeing your
code — which is why the given messages cite `change_management` and the schema
clause instead of stack traces. The dropped and added columns are caught at the
schema layer; the type change slips past it — same names, same order! — and is
caught by the value layer at 100% cast failure, with `'ORD-…'` examples. You
will also see the manifest `[warn]` fire: scratch copies have no completeness
signal, and the gate says so instead of failing ad-hoc checks. Journal: which
layer caught which tamper? Not random tampers, either — a new `currency` column
and string order ids are exactly what shopcore ships as "month 2" in A10.

**Accept when:** `0/3 files passed` and each message names the exact problem.

### Task 8 — Wire the gate into your A05 pipeline (small)

A gate you run by hand when you remember is not a gate. Put it at the very top
of your A05 per-day load script, *before any read of the data itself*:

```python
from work.contract_check import load_contract, check_file   # both live in work/, imported package-style

problems = check_file(csv_path, load_contract())
if problems:
    for msg in problems:
        print(" -", msg)
    raise SystemExit(f"[contract gate] {csv_path.name} FAILED - nothing was loaded.")
print(f"[contract gate] {csv_path.name} PASS")
```

Re-run one small day end-to-end: gate PASS line, then the normal A05 load and
dq_report. Expect `core.orders` to come out of this holding the rerun day
**twice** — plain `INSERT` is not rerun-safe (A05's known limitation, and
exactly the doubled-counts state its beginner-mistakes list warns about). Do
not fix it by hand; A07's Setup has you drop and rebuild this table with a
properly idempotent loader.

Then run the same pipeline against the `type_change` tampered file: give your
`validate_day()` an optional `csv_path` argument (defaulting to the canonical
raw path) so the gate — and only the gate — can be aimed at the tampered copy;
never copy a tampered file over a real path under `raw/`. Confirm the run
aborts *before* the warehouse is touched (row counts in core unchanged — check
`count(*)` before and after).

**Accept when:** clean day loads with a PASS line; tampered day aborts pre-load.

### Task 9 — The full-scale sweep (full)

```powershell
python work\contract_check.py --scale full --start 2026-06-01 --end 2026-07-15
```

Expect 45/45 PASS again — under a minute total on the baseline machine, roughly
a second per file. These files are ~350 MB each, twenty times the small ones,
yet the gate barely slows down, because it samples 100,000 rows and stops.
Watch Task Manager while it runs: memory barely moves. A pre-flight check must
be cheap enough that nobody is ever tempted to skip it — that is a design
requirement, not a nice-to-have.

**Accept when:** 45/45 PASS at full scale; total time in your journal.

### Task 10 — The change catalogue (journal)

Amendments need the semver vocabulary from Background. For each scenario, write
in your journal: breaking or non-breaking, *for whom*, and the bump shopcore
must make (with the notice `change_management` requires). Two are done as
examples:

| # | Upstream change | Breaking? For whom? | Bump |
|---|-----------------|---------------------|------|
| 1 | New optional column appended at the end | Non-breaking for name-based readers; breaking for position-based ones (our exact-order gate flags it for review) | MINOR, 7 days notice |
| 2 | `payment_method` renamed to `payment_type` | Breaking — every query naming the column | MAJOR, 30 days notice |
| 3 | `order_id` becomes `ORD-…` strings | ? | ? |
| 4 | New column inserted in the *middle* | ? | ? |
| 5 | A new `status` value `"on_hold"` starts appearing | ? | ? |
| 6 | Junk-status rate creeps from 0.3% to 0.8% over a month | ? | ? |
| 7 | File naming changes to `orders-YYYYMMDD.csv` | ? | ? |
| 8 | Producer fixes a typo in the contract's description text | ? | ? |

Think hard about #5 and #6, the sneaky ones: #5 breaks any consumer with
exhaustive `CASE status ...` logic even though the schema is untouched; #6
breaks nothing *yet* — it still passes the contract's 1%
`null_or_invalid_status` tolerance — and is exactly the slow drift the
stretch-goal history table would reveal.

**Accept when:** all 8 rows are filled with a defensible one-line reason each.

### Task 11 — Draft amendment v1.4.0

Close the loop: turn gaps into clauses. The signed contract stays untouched —
you draft `work/amendment_v1_4_0.md` with `status: proposed` snippets and a
rollout note, exactly what you would post in `#shopcore-data-changes`. It must
contain:

1. **The locale clause** (given as a start — this converts the euro-comma and
   `$`-prefix *gap* into a *violation* going forward):

```yaml
# proposed for v1.4.0 - status: proposed, effective after both sides sign
  - name: order_total
    money:
      transport_locale:
        decimal_separator: "."       # "1234.56" - never "1.234,56"
        thousands_separator: none
        currency_symbols: none       # never "$79.98"
```

2. **One more clause closing a Task 3 gap of your choice** — `updated_at`
   timezone and meaning, pinned inner types for `items`, a null-representation
   rule for `utm`, a restatement policy, a hard-fail tolerance for
   `payment_method`… Write it in the same YAML style, citing where in the
   contract it would live. One gap is reserved: PII and retention — Task 12
   drafts that clause in full and then makes you *implement* it, so pick a
   different silence here.
3. **The rollout note**, three sentences: the version bump and *why it is
   MINOR* (it adds constraints that match current intent; no consumer code
   breaks; it is more than a wording PATCH), the notice period per
   `change_management`, and the status lifecycle step (`proposed` until
   countersigned — then a data engineer may finally rely on it).

Paste-check that your snippets parse: drop each one into a scratch `.yaml`
file and run it through `yaml.safe_load` (one line of Python — you wrote the
same probe for the contract in A05). A proposed clause with a syntax error is
an unreviewable clause.

**Accept when:** both clauses parse as YAML, and the rollout note names bump,
notice, and status without hedging.

### Task 12 — PII: the clause that cannot stay on paper

One gap from the Task 3 hunt is different in kind from all the others. The
customers feed carries `email` — **PII** (personally identifiable information:
data that points at a real, findable person). Every other silence on your list
risks wrong numbers; this one risks a breach notification. And look at your own
warehouse: `core.customers` has stored raw email addresses since A03. Nobody
decided that — no clause allows it, none forbids it, so the warehouse became a
PII store by accident. Today the silence closes, and unlike the amendment's
other clauses, this one is not done when it parses: it hands *you* cleanup
work in your own tables.

**a) Draft the data-classification clause.** Append it to
`work/amendment_v1_4_0.md` as the amendment's third clause, and paste-check it
through `yaml.safe_load` like the other two:

```yaml
# proposed for v1.4.0 - new top-level block (would sit after business_rules;
# its long-term home is a shopcore_customers contract, which does not exist yet)
data_classification:
  default: internal              # any column not named below
  pii:
    - feed: shopcore_customers   # the dim feed that schema.customer_id references
      column: email
      class: pii_direct_identifier
      retention: >
        The raw value is never persisted in the warehouse. Only the
        irreversible derivative md5(lower(trim(email))) is stored, as
        email_hash, and it lives exactly as long as the customer row does.
      allowed_usage: >
        Joins and duplicate detection via email_hash only. No export, no
        display, no contact lists - any of those needs a new agreement.
```

Two things to notice. *Where it lives:* the honest home — a customers-feed
contract — does not exist yet (stretch goal 2 drafts one), so the block sits
in the only signed agreement between the teams. *What the bump costs:* v1.4.0
stays MINOR — not one byte of any feed changes — yet it hands the consumer a
migration (part b): semver sizes the *promise*, not the work it creates.

**b) Implement it: rebuild `core.customers`.** The clause promises two things
— exposure gone, capability kept — so measure the capability *before* you
destroy the column, or you cannot prove you kept it. In the small warehouse:

```sql
-- BEFORE: how much duplicate-email signal does the raw column carry?
-- (This re-derives, live, the ~0.3% shared-email dirt you met in A03.)
SELECT count(email) - count(DISTINCT email) AS dup_email_rows
FROM core.customers;                            -- small: 180

-- THE REBUILD: same table - but the raw value does not survive it
CREATE OR REPLACE TABLE core.customers AS
SELECT customer_id, name,
       md5(lower(trim(email))) AS email_hash,   -- canonicalize INSIDE the hash
       country, city, signup_ts, is_active,
       _data_date, _run_id                      -- the two lineage columns (§7.2)
FROM core.customers;

-- AFTER, promise half 1 - exposure gone:
DESCRIBE core.customers;              -- email_hash VARCHAR, no email anywhere

-- AFTER, promise half 2 - capability kept: the hash carries the exact
-- same duplicate signal the raw column did.
SELECT count(email_hash) - count(DISTINCT email_hash) AS dup_hash_rows
FROM core.customers;                            -- small: 180 - identical
```

Why `md5(lower(trim(email)))` and not just `md5(email)`? The expression is now
a contract clause — it must not depend on A03's cleaning staying upstream of
it forever, and `md5('John@X.com')` ≠ `md5('john@x.com')`: an un-canonicalized
hash silently loses the very dedupe capability the clause promises. Hash the
canonical form, always, at the hash site. And the signal is still *usable* —
the shared-email pairs A03 could find, you can still find:

```sql
SELECT email_hash, list(customer_id) AS shares_email
FROM core.customers
GROUP BY email_hash HAVING count(*) > 1
LIMIT 3;          -- 3 of the 180 pairs, exactly two customer_ids in each list
```

One last check: run the rebuild a second time. It fails — `Binder Error:
Referenced column "email" not found` — and that error is the clause *working*:
no raw email left to re-hash, or to leak. Repeat part b against the full-scale
warehouse too: 1,200,000 rows, and the dup count is 3,594 before and after.

> **Hashing** is one-way (`md5` here): equality survives — joins and dedupe
> still work — but the value cannot be walked back. **Masking** keeps the
> shape for human eyes; masked values never compare equal. **Tokenization**
> swaps in a token only a locked-down vault can reverse, when the original
> must come back. Rule of thumb: hash for analytics, mask for display,
> tokenize for reversibility — full definitions of all three in the glossary.

This clause has a future, too: in A15, `stg_shopcore__customers` computes the
same `email_hash` straight from the raw feed and never selects the raw column
— the clause enforced by construction in every dbt rebuild.

**Accept when:** the clause parses as YAML inside the amendment;
`DESCRIBE core.customers` shows `email_hash` and no `email`; and the duplicate
count via `email_hash` equals the count you measured on the raw column before
the rebuild (small: 180 = 180).

## Definition of Done

- [ ] Task 1's ten questions answered in your journal, each citing a clause of
      `contracts/shopcore_orders_daily.contract.yaml`.
- [ ] Every A05 `reject_reason` classified violation / gap / tolerated;
      `work/incident_note.md` cites a clause per violation; re-sends ≤0.2% are
      labelled *tolerated*, not violation.
- [ ] Gap list has ≥4 silences including both seeded `# NOTE`s, each with a
      concrete bite and a closing-clause sketch.
- [ ] `work/contract_check.py` reads the shipped contract (name, version,
      tolerances, enums, schema all from the YAML) and runs green: **45/45
      PASS** on month 1 at *small* scale and **45/45 PASS** at *full* scale.
- [ ] `work/contract_model.py` validates the *contract itself* with pydantic
      (`extra="forbid"`): the signed file parses clean, a typo'd **scratch
      copy** fails with a field-precise `schema.1.nullible`, and
      `load_contract()` runs the document through the model before the gate
      uses it. The signed contract on disk is byte-for-byte unchanged.
- [ ] All three tampered files FAIL, each with a producer-facing message naming
      the missing/unexpected column or showing example values of the bad type.
- [ ] Your A05 pipeline refuses to load a tampered file before any data is
      read, and loads a clean day normally.
- [ ] The change catalogue (8 rows) is in your journal.
- [ ] Amendment v1.4.0 drafted: locale clause + one more gap closed, with a
      defensible semver bump and notice period.
- [ ] The data-classification clause is the amendment's third clause and
      parses as YAML: `email` classified as PII, with retention and allowed
      usage stated.
- [ ] `core.customers` carries `email_hash` and **no raw `email` column**
      (`DESCRIBE` proves it), and the hash-based duplicate count equals the
      raw-column count measured before the rebuild (small: 180 = 180).

## Verify yourself

Sanity numbers first (small scale, any day): the *strict* written format for
`order_ts` is broken by roughly 0.7% of rows (0.4% DD/MM + 0.3% ISO-T + a trace
of impossible dates) — violations you reported in Task 2 — but truly
*unparseable* rows sit near 0.03%, comfortably under the 0.5% hard-fail
tolerance. Junk status runs ~0.2–0.3% (tolerance 1%), bad totals ~0.8%
(tolerance 1%), payment_method ~0.3% (warn only — no tolerance declared). That
headroom is the design: normal dirt passes, *change* trips the gate. Sweeps:

```powershell
python work\contract_check.py --scale small --start 2026-06-01 --end 2026-07-15
#   [warn] orders_2026-06-01.csv: payment_method 170/60703 (0.28%) outside the enum - ...
# [PASS] orders_2026-06-01.csv
# ...
# 45/45 files passed contract shopcore_orders_daily v1.3.0     (roughly 25-30 s)
python work\contract_check.py --scale full  --start 2026-06-01 --end 2026-07-15
# ...
# 45/45 files passed contract shopcore_orders_daily v1.3.0     (under a minute)
```

Tampered run: `0/3 files passed`, with a manifest `[warn]` per file and messages
containing `missing column(s) ['payment_method']` (plus the MAJOR/30-days
citation), `unexpected column(s) ['currency']` (MINOR/7-days), and
`order_id_not_castable: 5000/5000 sampled rows (100.00%)` with `'ORD-…'`
examples.

Task 5 — the typed model is a one-command check, and every number in its output
is read out of the contract rather than typed by you:

```powershell
python -m work.contract_model
# shopcore_orders_daily v1.3.0 (agreed, effective 2026-06-01)
#   10 columns, late window 7 days
#   order_total negative_allowed = False (bool)
#   status tolerance = 1%
python -m work.contract_typo_demo
# [yaml ] loads fine: 10 columns, customer_id.nullable = <<GONE>>, ...
# [typed] 3 validation errors for Contract
#         late_corrections.window_days / schema.1.nullable / schema.1.nullible
```

Then prove the signed file survived: `git status` shows `contracts/` unchanged,
and the broken copy is sitting in `<DATA_ROOT>/tmp/contract_typo/` where it
belongs. (`extra="forbid"` is doing the work in error 3: flip that one line to
`extra="ignore"` and the `nullible` error vanishes, leaving only a complaint
about a *different* field — and a misspelt optional key, `alowed_values:`,
goes silent altogether.)

Task 2 classification — your table should match this:

| A05 finding | Classification | Clause / silence |
|---|---|---|
| negative `order_total` | violation | `money.negative_allowed: false` |
| `"N/A"`/empty `order_total` | violation | DECIMAL promised, `nullable: false` |
| junk / padded / wrong-case `status` | violation | `allowed_values` + "lowercase, no padding" |
| `payment_method` casing variants | violation | `allowed_values` + case (report weekly; no hard-fail tolerance — did you notice?) |
| DD/MM + ISO-T `order_ts`, impossible dates | violation | written format pinned in `schema.order_ts` |
| Excel-float `customer_id` `"123456.0"` | violation | BIGINT promised |
| total ≠ items sum beyond ±0.01 | violation | `schema.order_total.business_rule` |
| orphan `customer_id` / SKU | violation | `references` + tolerances |
| within-file exact re-sends (≤0.2%) | **tolerated** | at-least-once delivery |
| European decimal-comma / `$`-prefix totals | **gap** | money clause silent on transport locale |
| string-typed `"qty":"2"` in items | **gap** | items "number" not pinned (seeded `# NOTE`) |
| `utm` null vs `"none"` vs absent key | **gap** | null representation unstated |

Gap hunt — your ≥4 should come from: `updated_at` timezone/meaning (seeded),
items inner types / max array length / optional-key representation (seeded),
money transport locale, `utm` null representation, restatement/re-publish
policy (the `freshness_sla` text itself confesses this one), PII and retention
for the customers feed (the one Task 12 closes for real). Finding something
defensible beyond this list is a feature, not a mistake.

Task 12 — the dims are generated deterministically, so these are exact, not
"roughly". Small warehouse, after the rebuild:

```sql
SELECT count(*) FROM core.customers;                                  -- 60000
SELECT count(email_hash) - count(DISTINCT email_hash)
FROM core.customers;                                                  -- 180
SELECT count(*) FROM (SELECT 1 FROM core.customers
                      GROUP BY email_hash HAVING count(*) > 1);       -- 180 (all pairs)
```

and `DESCRIBE core.customers` lists `email_hash VARCHAR` in the third slot
with no `email` column anywhere. The full-scale warehouse: the same three
queries return 1,200,000 / 3,594 / 3,594 — A03's ~0.3% email-duplication
rate, surviving the removal of the column that carried it.

## Common beginner mistakes

- **Editing the signed contract to make a red gate green.** A contract edit is
  a producer conversation plus a version bump — the amendment path (Tasks 11–12).
  Quietly widening a tolerance is a fig leaf over a broken promise.
- **Classifying every surprise as a violation.** Check that the clause actually
  exists before writing the incident note. Blaming shopcore for a promise never
  made burns credibility you will need for real incidents.
- **Trusting the sniffer as the contract.** Auto-detected types drift with dirt
  (you saw `customer_id` become DOUBLE and `updated_at` grow a timezone). Pin
  names and order exactly; probe types with `TRY_CAST` rates against tolerances.
- **Forgetting NULL in enum checks.** Empty CSV fields arrive as SQL NULL, and
  `status NOT IN (...)` is NULL (not true!) for NULLs — junk silently passes.
- **Running the gate after loading.** Then it is just A05 with extra steps. The
  entire value is refusing *before* bad data enters the warehouse.
- **Scanning whole files in the gate.** Without `LIMIT sample_rows` the
  full-scale sweep reads ~16 GB instead of a few hundred MB. Expensive gates
  get skipped.

## Stretch goals

1. **Contract-check history.** Log every gate result (date, file, check, rate,
   tolerance, pass) to `ops.contract_checks` in `warehouse.duckdb`. A rate
   climbing toward its tolerance over weeks is scenario #6 made visible.
2. **Contract the dimensions.** Draft `contracts/shopcore_customers.contract.yaml`
   (`status: draft`) using the README's per-column interrogation, and extend the
   checker. Harder than it looks: what does the money/timestamp checklist say
   about `signup_ts`, when the *data itself* admits three formats (string
   dates, DD/MM, epoch seconds)? What tolerance is honest?
3. **Smarter sampling.** First-N sampling is fine here because the dirt is
   evenly scattered — but a sorted file would fool it. Try `USING SAMPLE` and
   compare.

---

*Next: A07 makes the pipeline safe to re-run — this gate slots in as its
pre-flight step, and failures start writing `ops.alerts` rows instead of only
console lines. Keep the tripwire armed: in A10 shopcore ships "export v2", and
this gate — plus the 30-days-notice clause it cites — is what stands between
you and silent garbage.*
