# Composable code — grow a library, not a pile of scripts

This guide is for the moment (around A03) when your `work/` folder threatens to become
`clean_day.py`, `clean_day_v2.py`, `clean_day_final.py`, `clean_day_final_REAL.py`. Every
beginner writes one long script per task — it feels faster, and for the first task it is.
This lab deliberately grows something else: a small package of functions that each later
assignment *imports*. Here is why, and how.

## 1. Why: the same cleaner pasted in four scripts is four bugs waiting

Take the money cleaner from A03 — the expression that turns `"$79.98"`, `"93,32"` and
`"N/A"` into a proper `DECIMAL(14,2)`. You need it in the staging build (A03 Task 4). You
need it *again* in the cleaning report (Task 5). A05's validation checks need it. A11's
backfill runs it over 69 files. Paste it four times and you own four copies.

Now the upstream team starts sending `"€79,98"`. You fix the copy you remember. The other
three keep producing NULLs — silently, because `TRY_CAST` never crashes, it just quietly
loses revenue. Worse: months later the copies have *drifted* (someone "improved" one of
them), so staging and the cleaning report now disagree about the same file and you spend an
afternoon discovering they were never the same expression anymore.

One definition, imported everywhere, means one place to fix and one place to test. That is
the entire argument. The lab builds it as a ladder, core → feature:

```
lib/labpaths.py          (given)   where does the data live? every module asks here
    ▲ imported by
work/cleaners.py         (A03)     one cleaning rule per function
    ▲
work/validate_day.py     (A05)     per-day validation gate: flag → report → quarantine
work/contract_check.py   (A06)     schema-vs-contract gate, run before loading
    ▲
work/a07_load_day.py     (A07)     the idempotent per-day load — imports all of the above
work/preflight.py        (A07)     environment checks, called before every load
    ▲
work/pipeline.py         (A11)     A07's loader, stitched into an importable load_day()
work/runner.py           (A11)     date-range CLI: loops the pipeline over days
    ▲
work/parallel_runner.py  (A12)     the same runner, spread over worker processes
```

The rule that keeps the ladder standing: **later work IMPORTS earlier work, never
re-pastes it.** `runner.py` contains no cleaning logic; it calls `pipeline.py`, which calls
the cleaners. When a run fails, `work/preflight.py` and `guides/troubleshooting.md` tell you
*which rung* to look at — that only works because each rung does one job.

## 2. What makes a function composable

Four properties, checkable at a glance:

1. **Does one thing.** Clean one column. Not "clean everything, write parquet, and print".
2. **Parameterized.** Date, scale, and connection come in as arguments — the lab's three
   usual suspects. A hard-coded `'2026-06-02'` means the function has exactly one useful day.
3. **Returns instead of prints.** The caller decides what to do with the result: print it,
   log it, write it to `ops.cleaning_report`, or assert on it in a test. `print()` can do
   only the first.
4. **No hidden globals.** No module-level `con = duckdb.connect(...)` that functions secretly
   share. A function with a hidden connection can't be tested in isolation — and can't run
   in a worker process when A12 parallelizes you.

Here is the refactor, on real lab code. **Before** — one long script, one hard-coded day,
and the money cleaner already pasted twice *within the same file* (staging build + report):

```python
# clean_day.py -- version 1: works, once
import duckdb

con = duckdb.connect()                       # hidden shared state
con.execute("SET TimeZone='UTC';")

con.execute("""
CREATE TABLE stg AS
SELECT order_id,
    TRY_CAST(NULLIF(                          -- money cleaner, paste #1
      CASE WHEN order_total LIKE '%,%'
           THEN replace(replace(order_total, '.', ''), ',', '.')
           ELSE replace(TRIM(order_total), '$', '') END,
      'N/A') AS DECIMAL(14,2)) AS order_total,
    ...
FROM read_csv('F:/etl_lab/small/raw/orders/orders_2026-06-02.csv', ...);  -- hard-coded
""")

raw, bad_total, bad_ts = con.execute("""
SELECT count(*),
       count(*) FILTER (WHERE TRY_CAST(NULLIF(   -- money cleaner, paste #2
         CASE WHEN order_total LIKE '%,%' ... END, 'N/A') AS DECIMAL(14,2)) IS NULL),
       ...
""").fetchone()
print("totals cleaned to NULL:", bad_total)  # prints; nothing downstream can reuse this
```

It runs, and the numbers are even right:

```
raw rows: 59024
totals cleaned to NULL: 117
timestamps cleaned to NULL: 18
```

**After** — the expressions graduate into `work/cleaners.py` as functions that *return the
SQL expression as a string*. DuckDB still does the heavy lifting; Python's only job is to
hand every caller the one blessed definition of the rule:

```python
# work/cleaners.py  (excerpt -- A03 Task 7 builds the real module, six cleaners in all)
def clean_money(col: str = "order_total") -> str:
    """Money string -> DECIMAL(14,2) expression.

    Fixes '$79.98' (symbol), '93,32' / '1.234,56' (European comma form);
    'N/A' and empty become NULL. Negatives SURVIVE on purpose: a negative
    total is a validity problem (A05 quarantines it), not a format problem.
    """
    # thousands-dots must die before the comma becomes a decimal point
    return f"""TRY_CAST(NULLIF(
      CASE WHEN {col} LIKE '%,%'
           THEN replace(replace({col}, '.', ''), ',', '.')
           ELSE replace(TRIM({col}), '$', '') END,
      'N/A') AS DECIMAL(14,2))"""
```

and the script shrinks into a function that takes its three suspects as parameters and
returns its counts:

```python
# any later script
from work.cleaners import clean_money, clean_order_ts

ORDER_COLUMNS = {  # the 10-column v1 read schema, verbatim from A03 Task 2
    'order_id': 'BIGINT', 'customer_id': 'VARCHAR', 'store_id': 'INTEGER',
    'order_ts': 'VARCHAR', 'updated_at': 'TIMESTAMP', 'status': 'VARCHAR',
    'payment_method': 'VARCHAR', 'order_total': 'VARCHAR',
    'items': 'VARCHAR', 'meta': 'VARCHAR'}

def clean_counts(con, root: str, day: str) -> dict:
    """Count what the cleaners turned into NULL for one day's file."""
    raw, bad_total, bad_ts = con.execute(f"""
        SELECT count(*),
               count(*) FILTER (WHERE {clean_money()} IS NULL),
               count(*) FILTER (WHERE {clean_order_ts()} IS NULL)
        FROM read_csv('{root}/raw/orders/orders_{day}.csv',
                      header=true, columns={ORDER_COLUMNS})
    """).fetchone()
    return {"day": day, "raw": raw, "total_null": bad_total, "ts_null": bad_ts}
```

The payoff is immediate — any day, same code, no edits (run against `F:/etl_lab/small`):

```
{'day': '2026-06-02', 'raw': 59024, 'total_null': 117, 'ts_null': 18}
{'day': '2026-06-03', 'raw': 62707, 'total_null': 120, 'ts_null': 13}
```

Same 117/18 as the monolith for 06-02 (they match your A03 `ops.cleaning_report`, as they
must) — but the second line cost nothing, and the returned dict can go straight into a
report table, a log line, or a test assertion. That is what "returns instead of prints" buys.

## 3. Docstrings state the job; comments state constraints

**Docstrings.** Every module and function in `work/` gets one, and it answers two questions:
*what is this for* (one line) and *what does it assume / what does the caller still owe*.
The `clean_money` docstring above is the lab's model: it names the pathologies it fixes,
and it documents the surprising decision (negatives survive) so nobody "fixes" it later.

A bad docstring restates the function name and answers nothing:

```python
def clean_money(col="order_total"):
    """This function cleans the order total column."""   # thanks, we could tell
```

If you could delete the docstring and lose nothing, it was the bad kind.

**Comments.** The lab's rule: *comments state constraints, not narration*. The code already
says what it does; a comment earns its line only by saying what the code *cannot* say — an
ordering requirement, a deliberate non-fix, a trap.

```python
# BAD  (narration -- the next line says exactly this already):
# replace the dots, then replace the comma with a dot
return f"replace(replace({col}, '.', ''), ',', '.')"

# GOOD (constraint -- why THIS order, which a refactor might break):
# thousands-dots must die before the comma becomes a decimal point
```

```python
# GOOD (constraint -- a deliberate non-action someone will itch to "fix"):
# negatives survive: a negative total is A05's validity problem, not a format problem
```

## 4. Tests: the safety net that makes refactoring safe

A shared function is only safe to *change* if something screams when you break it. The
pattern, born in A03 as `tests/test_cleaners.py`, has three ingredients: a **VALUES table**
of hand-picked dirty inputs (one per documented pathology, straight from SPEC §3.5), an
**in-memory DuckDB** (`duckdb.connect()` with no path — no files, no state, instant), and an
**assert of the exact expected outputs**:

```python
# tests/test_cleaners.py  (excerpt -- A03 Task 8 builds the full file)
from decimal import Decimal
import duckdb, pytest
from work.cleaners import clean_money

@pytest.fixture
def con():
    c = duckdb.connect()             # in-memory: no files, no state, instant
    c.execute("SET TimeZone='UTC';")
    return c

def test_money(con):
    got = con.execute(f"""
        SELECT {clean_money('v')} FROM (VALUES
            ('19.99'),      -- already clean: must pass through untouched
            ('$79.98'),     -- currency symbol
            ('93,32'),      -- European decimal comma
            ('1.234,56'),   -- comma AND thousands dot
            ('N/A'),        -- known no-value marker
            (NULL),         -- empty CSV field arrives as NULL
            ('-42.10')      -- negative survives: A05's problem, not ours
        ) t(v)
    """).fetchall()
    assert [r[0] for r in got] == [
        Decimal("19.99"), Decimal("79.98"), Decimal("93.32"),
        Decimal("1234.56"), None, None, Decimal("-42.10")]
```

A `test_order_ts` follows the same pattern (three delivered formats parse; the impossible
`2026-06-31` and plain garbage come back NULL). One test function per cleaner, one VALUES
row per pathology. Run the suite (PowerShell and POSIX alike):

```powershell
python -m pytest tests/ -q
```
```
..                                                                       [100%]
2 passed in 0.09s
```

(That is this excerpt's two tests. The finished A03 module has six cleaners, and its full
suite lands at **`7 passed`** — the exact line A03's Definition of Done keys on.)

Now watch it earn its keep. Suppose during A10 you "simplify" the comma branch to a single
`replace({col}, ',', '.')` — looks equivalent, reads cleaner. Run pytest:

```
F.                                                                       [100%]
...
E       At index 3 diff: None != Decimal('1234.56')
1 failed, 1 passed in 0.26s
```

The test caught it in a quarter of a second: `'1.234,56'` became `'1.234.56'` → NULL. Without
the test, that bug ships, and every four-digit European total silently vanishes from revenue
until someone audits a quarter. This is why the lab's ritual line exists: **`python -m pytest
tests/ -q` must be green before a branch merges** — it joins your Definition of Done from A03
on. First clean value through the test is also the moment to notice the test *is* the VALUES
habit from A03 Task 3b, promoted from "try it once in the REPL" to "checked forever".

## 5. Commit at working seams

Composability has a time dimension too: a history of small, working states. The ritual (set
up in A00, details in `guides/git_basics.md`): branch per assignment, **one commit after each
numbered task**, merge to `main` when the DoD — tests included — is green.

Why not one big commit at the end? Because a commit is a *savepoint*, and git can only take
you back to states you actually saved. A07 makes this concrete: when chaos-testing corrupts
your afternoon, `git reset --hard` back to the last green commit is a 5-second undo — but
only if a green commit from 20 minutes ago exists. One giant "A03 done" commit at the end
gives you exactly one savepoint, at the finish line, where you no longer need it. Small
commits also make `git log --oneline` a readable story and keep every diff small enough to
actually review.

The seam to commit at is a *working* state: task done, `pytest` green, numbers checked. Not
"it's 6pm". Not mid-refactor with two tests red.

## 6. Beyond the lab

Everything here scales up unchanged, only the names get bigger. A production repo calls
`work/` by its industry name `src/` (an installable package with a `pyproject.toml`);
`tests/` keeps its name and grows to hundreds of VALUES-style cases; and the "green before
merge" rule stops relying on your discipline — a CI server (GitHub Actions, GitLab CI) runs
`pytest` on every push and physically blocks the merge button until it passes. The ladder
shape survives too: paths/config at the bottom, pure logic above it, checks above that,
pipelines above that, orchestration (Airflow, Dagster, dbt) on top — each layer importing
the one below, nobody re-pasting anything. Learn the shape on nine small files here and
you will recognize it in every serious data codebase you ever open.
