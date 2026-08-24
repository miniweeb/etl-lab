# A00 — Setup & data generation

**Topic:** environment · **Est. time:** 1–2h · **Difficulty:** ⭐
**Requires:** nothing. This is the start.

## Why this matters

Every data engineering incident report contains a sentence like "the job ran on
the wrong environment / wrong path / wrong Python". Professionals make their
environment boring and reproducible before touching data. You'll also generate
the course dataset here — all ~24 GB of it — and verify it, which is itself
your first taste of a data engineer's core habit: *never assume, always check*.

## Background

This lab simulates a real feed: **shopcore**, the e-commerce platform, exports
one CSV of orders per day. Your laptop plays two roles at once — shopcore's
side (a data generator that writes those CSVs) and *you*, the data engineer who
must turn them into something trustworthy.

Two ideas to internalize now:

**Two scales.** The generator produces the same dataset at two sizes:
`small` (60k rows/day, ~1.2 GB total once both months exist — ~800 MB for
month 1, all you generate today; fits anywhere, runs in seconds) and
`full` (1.2M rows/day, ~24 GB — big enough that careless code visibly
struggles on a 16 GB machine). You will *develop every pipeline on small*
and *run it on full*. Fast feedback while coding, real conditions when it counts.

**Determinism.** The generator is seeded per date: the file for 2026-06-10 is
byte-identical every time anyone generates it. That means expected numbers in
these assignments are exact, regeneration is always safe, and a corrupted
experiment is never a disaster — delete and regenerate.

One warning about disk location: cloud-sync folders (OneDrive, Dropbox,
Documents on many corporate laptops) are poison for data work. A 24 GB dataset
in OneDrive will grind your sync, your disk, and possibly your quota. The lab
reads a `ETL_LAB_DATA` environment variable so data lives on a fast local
disk, away from the repo.

## Setup

What you need before starting:

- Windows 10/11 (macOS/Linux fine too — translate the shell lines), 16 GB RAM, 8 cores
- ~80 GB free on a local SSD
- Python 3.11+ (`python --version`)
- git 2.x (`git --version`) — if missing, install from git-scm.com with default options

## Tasks

### Task 1 — Create the environment

A venv (virtual environment) is a private copy of Python just for this project,
so the lab's package versions can never collide with anything else on your
machine. From the repo root:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python -c "import duckdb, pyarrow, numpy, pandas, psutil, pytest; print('duckdb', duckdb.__version__)"
```

**Accept:** the last line prints a duckdb version ≥ 1.4.

(`pytest` is in that list on purpose — from A03 on, your own code gets its own
tests, and this is the tool that runs them.)

### Task 2 — Put the repo under version control

Over the next 15 assignments you will write real code, and some assignments
(A07 especially) break things *on purpose*. Git is your save-point system: a
**commit** is a snapshot of your code you can always return to, so no
experiment is ever fatal. Code goes in git; data never does — it's huge and
regenerable, and the shipped `.gitignore` file enforces that split for you.

From the repo root:

```powershell
git init -b main
git config user.name "Your Name"
git config user.email "you@example.com"
git config core.editor "notepad"
```

What each line does: `git init -b main` turns this folder into a git
repository whose first branch is named `main` (the modern default name).
The two identity lines label every commit with who made it — git refuses to
commit without them. These `git config` lines apply to this repo only; add
`--global` if you want to set them once for your whole machine.

The `core.editor` line is trap-defusal: if you ever run `git commit` without a
`-m "message"`, git opens an editor and *waits* — with this setting that's
plain Notepad (type your message on the first line, save, close, done) instead
of the terminal editor vim, whose exit sequence (`Esc` then `:q!`) has baffled
generations of juniors. The full story, including the vim rescue, is in
`guides/git_basics.md`.

Now open `.gitignore` in the repo root and read it — it has three groups:

- **data** (`data/`, `*.duckdb`, `tmp/`, `quarantine/`): datasets and
  databases never enter git — they're gigabytes, regenerable, and git handles
  large binaries badly.
- **python** (`__pycache__/`, `*.pyc`, `.venv/`): machine-generated Python
  clutter that every machine rebuilds for itself.
- **dbt & logs** (`**/target/`, `**/dbt_packages/`, `logs/`): tool output,
  recreated on every run (you'll meet these in A07 and A15).

The line-by-line walk lives in `guides/git_basics.md`; for now it's enough to
know *why* each group is excluded.

Make your first commit:

```powershell
git status                          # untracked files — note .venv is NOT listed:
                                    # the .gitignore is already doing its job
git add .
git commit -m "A00: environment up"
git log --oneline
```

**Accept:** `git commit` prints `[main (root-commit) <sha>] A00: environment up`,
and `git log --oneline` shows exactly that one commit.

> **How you'll work from A01 on.** Every assignment follows one git ritual:
> start a branch named `aNN-<topic>` (e.g. `git switch -c a01-first-contact`),
> make one commit after each numbered task (message like
> `A01 task 6: parquet load` — imperative, subject ≤ 72 characters), and merge
> back only when the Definition of Done is green:
> `git switch main` then `git merge --no-ff a01-first-contact`.
> Each assignment's Setup section will remind you in one line. Commands,
> rescue moves and the reasoning live in `guides/git_basics.md`.

### Task 3 — Choose the data home

Pick a folder on a fast local, non-synced drive with ~80 GB free, e.g. `D:\etl_lab`.

This volume will hold more than the dataset: the lab points all **temp and
spill space** at `<DATA_ROOT>\tmp` too — when DuckDB needs more memory than
it's allowed, it spills working data to disk there. Real provisioned machines
usually pair a small OS disk with a big attached data disk, and an engine
quietly spilling tens of GB onto `C:\` fills the OS drive and takes down
everything on the box — a hidden failure point that only shows up under load.
Keeping temp on the data volume removes that failure mode; you'll watch spill
happen for real in A13.

```powershell
setx ETL_LAB_DATA "D:\etl_lab"
```

`setx` affects *future* terminals — close and reopen your terminal (and
re-activate the venv), then verify:

```powershell
python lib/labpaths.py
```

**Accept:** it prints your chosen root with "(from ETL_LAB_DATA)" — not the
`<repo>/data` fallback.

### Task 4 — Generate the small dataset

```powershell
python datagen/generate.py --scale small --month 1
```

Watch the output: one line per day with row counts and sizes. Note two days
are much bigger than the rest (2026-06-19 and 2026-07-11 — flash sales; they
will matter a lot later) and weekends are smaller.

**Accept:** 45 files under `<DATA_ROOT>\small\raw\orders\`, ~800 MB total.

> Note `--month 1`. There is a month 2. Do not generate it, do not read about
> it. It arrives when the course says it arrives (A10).

### Task 5 — Generate the full dataset

This is the real thing: ~16 GB of CSV for month 1 (month 2 adds ~8 GB later).
It takes roughly 10–20 minutes and peaks at
several GB of RAM with the default 4 workers. Close the RAM-hungry apps,
start it, and go make coffee — waiting for data is an authentic part of the job:

```powershell
python datagen/generate.py --scale full --month 1
```

While it runs, open **Task Manager → Performance** and watch CPU and disk.
This is your first resource observation — by A13 this will be a reflex.

**Accept:** 45 files under `<DATA_ROOT>\raw\orders\`, ~16 GB total, plus
`customers.csv` (~110 MB), `products.csv`, `stores.csv` under `raw\dims\`.

### Task 6 — Verify — never assume

```powershell
python datagen/selftest.py --scale small
python datagen/selftest.py --scale full
```

**Accept:** every check PASS on both scales. If anything fails: delete the
offending file and regenerate (determinism makes this safe), then re-run.

### Task 7 — Look at the data (without opening Excel)

Excel would try to load 1.2M rows into memory and truncate at 1,048,576 rows —
never inspect big CSVs with spreadsheet apps. Use a streaming peek instead:

```powershell
Get-Content "$env:ETL_LAB_DATA\raw\orders\orders_2026-06-02.csv" -TotalCount 5
```

Read the five lines. Really read them. Answer for yourself:
- What columns do you see?
- The `items` column contains JSON *inside* CSV — how is it escaped?
- Open the matching manifest:
  `Get-Content "$env:ETL_LAB_DATA\raw\manifest\orders_2026-06-02.json"` —
  what does it promise about the file?
- Skim `contracts/shopcore_orders_daily.contract.yaml` in the repo — the
  promises this file makes — you will hold shopcore to them from A05 on.

### Task 8 — Start your journal

Create `journal.md` in the repo root:

```markdown
# Lab journal
## A00 — <today's date>
- full generation wall-clock: ___ min, workers: 4
- total raw size: ___ GB (orders) + ___ MB (dims)
- one thing that surprised me: ___
```

You will add timings and observations to this file in every assignment. In
A12–A14 your own journal numbers become the baseline you optimize against.

## Definition of Done

- [ ] venv active, `requirements.txt` installed (incl. pytest), duckdb ≥ 1.4
- [ ] git repo initialized on branch `main`; identity and `core.editor` configured;
      first commit `A00: environment up` exists
- [ ] `ETL_LAB_DATA` set to a local non-synced drive; `python lib/labpaths.py` confirms it
- [ ] small + full month-1 data generated (45 files each) with dims
- [ ] `selftest.py` fully green on both scales
- [ ] you can state from memory: the 10 columns of a month-1 orders file
- [ ] `journal.md` created with your first entries

## Verify yourself

```powershell
# the repo is a git repo on main with your first commit
git log --oneline      # -> 5a1ce3e A00: environment up   (your sha will differ)
git branch             # -> * main
```

```powershell
# file count must be 45 at each scale
(Get-ChildItem "$env:ETL_LAB_DATA\raw\orders\*.csv").Count
(Get-ChildItem "$env:ETL_LAB_DATA\small\raw\orders\*.csv").Count
```

```powershell
# manifests promise exact row counts — pick any day and remember this trick
Get-Content "$env:ETL_LAB_DATA\raw\manifest\orders_2026-06-10.json"
```

## Common beginner mistakes

- Putting `ETL_LAB_DATA` inside OneDrive/Documents anyway ("it's just this
  once"). Your sync client will spend the whole course fighting you.
- Running `setx` and staying in the same terminal — the variable isn't there.
  New terminal.
- Skipping the `git config` identity lines — your first commit stops with
  "Please tell me who you are". Set them, then commit again.
- Double-clicking a 350 MB CSV. Windows will try to open it in Excel or
  Notepad; both end badly. Peek with `Get-Content -TotalCount`.
- Skipping the selftest. Half of the later assignments quote exact numbers
  that assume a verified dataset.
- Generating month 2 "to be ready". You'll rob yourself of A10's main lesson.

## Stretch goals

- Read `datagen/profiles.py` — just the constants at the top. This file is
  shopcore's side of the "data contract" you'll work with in A06. Its constants
  are the ground truth that `contracts/shopcore_orders_daily.contract.yaml`
  writes down as promises.
- Time `generate.py --scale small --month 1 --force` with `--workers 1` vs
  `--workers 4` and note the speedup — a preview of A12.
