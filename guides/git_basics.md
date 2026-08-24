# Git basics — savepoints for a lab that breaks things on purpose

> Referenced by **A00** (setup), **A07** (rollback), and **A15** (remote
> stretch goal). The ritual it teaches is defined in `SPEC.md` §7.4. Every
> transcript below was actually run with git 2.47 on Windows (PowerShell);
> output is trimmed and paths shortened.

## 1. Why version control, before any commands

From A07 on, this lab injects failures **on purpose** — a `--chaos 0.3` flag
that blows up your pipeline at random points, corrupt files, schema changes that
land mid-month. You will also break things by accident: paste over a working
function, "simplify" a cleaner and discover an hour later that nothing parses.

Git gives you *savepoints*. Each commit is a full snapshot of your code that you
can go back to, compare against, or pull a single file out of. Once a commit
exists, it is very hard to truly lose (section 7.4 proves this). The rule of
thumb for the whole course:

- **Code goes in git.** Everything in `work/`, `tests/`, your `journal.md`.
- **Data never does.** It's 24 GB, regenerable from `datagen/generate.py`, and
  lives under your `ETL_LAB_DATA` root anyway (`F:\etl_lab` on the transcript
  machine). The shipped `.gitignore` (section 3) enforces this split so you
  can't get it wrong by accident.

Commit *small and often* — after each numbered task, not once per evening. A
savepoint you didn't make is the only one that can't rescue you.

## 2. One-time setup

Run this once, in the repo root (A00 walks the same lines):

```powershell
git init -b main
git config user.name "Your Name"
git config user.email "you@example.com"
git config core.editor "notepad"
```

`git init -b main` turns the folder into a repository with `main` as the branch
name. If you typed plain `git init` instead, git 2.47 creates `master` and
prints a wall of hints:

```text
hint: Using 'master' as the name for the initial branch. This default branch name
hint: is subject to change. ...
```

Not an error — fix it with `git branch -m main`, and make `main` the default for
every future repo with `git config --global init.defaultBranch main`.

The two identity lines matter because git **refuses to commit** without them:

```text
PS> git commit -m "A00: environment up"
Author identity unknown

*** Please tell me who you are.
```

Without `--global` these settings apply to this repo only; add `--global` to
make them your machine-wide defaults.

### The commit-message screen (read this before it happens to you)

Every commit needs a message. When you run `git commit -m "..."` you provide it
inline. When you run **bare `git commit`** — easy to do by accident — git opens
your configured editor, and *waits* for you to write the message there. The
commit only happens when you save the file and close the editor. That's why we
set `core.editor "notepad"`: the screen that appears is just Notepad showing a
template like this (everything starting `#` is ignored):

```text
# Please enter the commit message for your changes. Lines starting
# with '#' will be ignored, and an empty message aborts the commit.
#
# On branch main
# Changes to be committed:
#	new file:   journal.md
#
```

What to do: **type your message on the first line, save (Ctrl+S), close the
window.** The commit completes the moment Notepad exits. Close without typing
anything and git safely cancels: `Aborting commit due to empty commit message.`

> **Vim rescue box.** One day you'll be on a machine without your config — a
> server, a colleague's laptop — and git will open its built-in default editor,
> **vim**: a full-terminal screen with `~` down the left edge where typing seems
> to do nothing. Don't fight it, just exit:
> - save the message and finish the commit: press `Esc`, type `:wq`, press Enter
> - abandon the commit entirely: press `Esc`, type `:q!`, press Enter
>
> (`w` = write, `q` = quit, `!` = "I mean it". These two lines are all you need.)

## 3. The shipped `.gitignore`, line by line

The repo ships with a `.gitignore` — a list of paths git pretends not to see.
Open it; here is every entry and why it's there:

| entry | why it's ignored |
|---|---|
| `data/` | raw/lake data never enters git: gigabytes, regenerable, and git is miserable with huge binaries. (Your real data lives under your `ETL_LAB_DATA` root; this entry is a belt-and-braces guard in case any lands in the repo.) |
| `*.duckdb`, `*.duckdb.wal` | `warehouse.duckdb` is *built* by your pipeline — rebuild it, don't commit it. The `.wal` is DuckDB's write-ahead log, a transient sidecar file. |
| `tmp/` | spill/scratch space (temp discipline, SPEC §7.4). Temp files are by definition disposable. |
| `quarantine/` | rejected rows written by A05's validation split — data, produced from data. |
| `__pycache__/`, `*.pyc` | Python's compiled bytecode cache; regenerated on every run. |
| `.pytest_cache/` | pytest's own cache (last-failed results and the like); regenerated on every test run — same story as `__pycache__`. |
| `.venv/`, `venv/` | your virtual environment: thousands of installed files, recreated from `requirements.txt` in one command. |
| `**/target/`, `**/dbt_packages/` | dbt's build output and downloaded packages (A15) — same story, generated. |
| `logs/` | per-run log files from A07's runner — output of runs, not source. |

One principle covers all of it: **commit what you write, never what gets
generated.** If a file can be rebuilt by running something, git is the wrong
place for it. Your `work/` scripts and `tests/` are the opposite — they're the
whole point, so they are *not* ignored. To ask why a file is being ignored:

```text
PS> git check-ignore -v warehouse.duckdb
.gitignore:3:*.duckdb	warehouse.duckdb
```

## 4. The daily verbs: status, diff, add, commit, log

The loop you'll run dozens of times a day. Scene: mid-A02, you've edited the
loader and started a journal. (Transcript is on `main` for brevity; from A01 on
you'd be on an assignment branch — section 6.)

**`git status` — "what state am I in?"** Run it constantly; it's free.

```text
PS> git status
On branch main
Changes not staged for commit:
  (use "git restore <file>..." to discard changes in working directory)
	modified:   work/build_lake.py

Untracked files:
	journal.md
```

Two kinds of change: *modified* (tracked file edited) and *untracked* (never
seen by git). Ignored files show up in neither.

**`git diff` — "what exactly did I change?"** Read this before every commit;
it's how you catch the debug line you forgot to delete.

```text
PS> git diff
--- a/work/build_lake.py
+++ b/work/build_lake.py
@@ -5,4 +5,5 @@ def load_day(day: date) -> int:
     con = duckdb.connect()
     con.execute("SET memory_limit='8GB'")
+    con.execute("SET threads=8")
     # switch to APPEND mode comes in task 3
```

Lines with `+` are added, `-` removed. After staging, plain `git diff` goes
quiet — use `git diff --staged` to see what's about to be committed.

**`git add` — choose what goes in the snapshot.** Prefer naming files
(`git add work/build_lake.py`): it forces you to look at what you're committing.
`git add -A` stages *everything* (as does `git add .` from the repo root) —
fine for a tidy tree, risky when scratch files are lying around.

```text
PS> git add work/build_lake.py
PS> git status
Changes to be committed:
	modified:   work/build_lake.py
Untracked files:
	journal.md
```

Staged and unstaged coexist on purpose: commit the loader now, the journal
separately.

**`git commit` — take the savepoint.** (Bare `git commit`, no `-m`, does the
same but collects the message via the editor flow from section 2.)

```text
PS> git commit -m "A02 task 2: pin memory_limit and threads in loader"
[main 39da365] A02 task 2: pin memory_limit and threads in loader
 1 file changed, 1 insertion(+)
```

**`git log --oneline` — the savepoint list.** One line per commit, newest
first; the hex prefix is the commit's SHA (its name — you'll use these in
section 7):

```text
PS> git log --oneline
ab96e16 A02 task 6: journal timings for month-1 loop
39da365 A02 task 2: pin memory_limit and threads in loader
a20e9a7 A00: environment up
```

## 5. Writing commit messages

A commit message is a note to a future, more stressed version of you scanning
`git log --oneline` for "the last commit where things worked". House rules:

- **Imperative subject** ("add", "fix", "restore" — not "added", "adding"),
  **≤ 72 characters**, no trailing period.
- **Lab format: `ANN task K: what`** — assignment, task number, then what the
  commit does. Examples in the course's format:

```text
A00: environment up
A02 task 2: per-day loader skeleton
A03 task 4: staging cleaners for order_total
A05 task 6: quarantine writer for contract rejects
A07 task 5: pre-chaos savepoint
```

Say *what*, not *how* ("staging cleaners for order_total", not "edited
cleaners.py"). The diff already shows the how.

## 6. The assignment branch ritual

From A01 on, every assignment follows one loop. Why a branch: `main` only ever
holds *finished, green* work, so "roll back to something that works" always has
a target — and if an assignment goes sideways, the mess is contained.

```text
PS> git switch -c a03-transformation
Switched to a new branch 'a03-transformation'
```

`switch -c` creates the branch and moves you onto it (name pattern:
`aNN-topic`). Now work the assignment, **one commit per numbered task** — e.g.
task 7 builds `work/cleaners.py`, task 8 adds `tests/test_cleaners.py`:

```text
PS> git commit -m "A03 task 7: cleaners graduate to work/cleaners.py"
[a03-transformation 8ab172c] A03 task 7: cleaners graduate to work/cleaners.py
 1 file changed, 13 insertions(+)
```

When the Definition of Done is green — including `python -m pytest tests/ -q`
green, per SPEC §7.4 (the module-and-test pattern lives in
`guides/composable_code.md`) — merge back:

```text
PS> git switch main
Switched to branch 'main'
PS> git merge --no-ff a03-transformation
Merge made by the 'ort' strategy.
 tests/test_cleaners.py | 14 ++++++++++++++
 work/cleaners.py       | 13 +++++++++++++
 2 files changed, 27 insertions(+)
```

Notepad opens first with a pre-filled message (`Merge branch
'a03-transformation'`) — save, close, done. `--no-ff` ("no fast-forward")
forces a real merge commit, so history shows the assignment as one bubble:

```text
PS> git log --oneline --graph
*   ebef8cf Merge branch 'a03-transformation'
|\
| * 386ed6f A03 task 8: pytest cleaner tests over VALUES table
| * 8ab172c A03 task 7: cleaners graduate to work/cleaners.py
|/
* ab96e16 A02 task 6: journal timings for month-1 loop
```

The branch has done its job; delete the label (the commits stay):

```text
PS> git branch -d a03-transformation
Deleted branch a03-transformation (was 386ed6f).
```

`-d` only deletes *merged* branches; on unmerged work it refuses with
`error: the branch '...' is not fully merged`.

## 7. Rescue: when things go wrong

A07's chaos work is where you'll use these for real. Pick by symptom:

| symptom | tool |
|---|---|
| uncommitted edits I regret | `git restore <file>` |
| one file was better in an old commit | `git checkout <sha> -- <file>` |
| the whole branch is hosed, roll back to green | `git reset --hard <sha>` |
| I reset too far / "lost" commits | `git reflog` |

**7.1 `git restore <file>` — undo uncommitted edits.** You pasted junk over
`work/cleaners.py` but never committed it:

```text
PS> git restore work/cleaners.py
PS> git status
nothing to commit, working tree clean
```

The file is back to the last committed version. Careful: your uncommitted edits
are gone for good — that's the *point*, but read `git diff` first if unsure.

**7.2 `git checkout <sha> -- <file>` — recover one file from an old commit.**
A07 task 5's drill stages exactly this: you overwrite `work/a07_load_day.py`
with `# TODO rewrite everything` and — the twist — *commit* it, so 7.1's rescue
is useless: the last committed version *is* the junk. Find the last commit that
touched the file, then pull just that file from it:

```text
PS> git log --oneline -- work/a07_load_day.py
43fadea A07: experiment that went nowhere
c954830 A07 task 5: pre-chaos savepoint
PS> git checkout c954830 -- work/a07_load_day.py
```

The old version lands in your tree already staged; rerun the loader (it skips —
still green) and commit it: `A07: bring the loader back from the savepoint`.
Only the named file moves — everything else stays put. (The modern
near-equivalent, `git restore --source c954830 work/a07_load_day.py`, leaves
the change unstaged.)

**7.3 `git reset --hard <sha>` — roll the branch back to green.** Two chaos
experiments went nowhere; you want the branch exactly as it was at the last
good commit:

```text
PS> git log --oneline -3
8f2b62e A07 task 6: chaos experiment, attempt 2
9059d3f A07 task 6: chaos experiment, attempt 1
c954830 A07 task 5: pre-chaos savepoint
PS> git reset --hard c954830
HEAD is now at c954830 A07 task 5: pre-chaos savepoint
```

> **Warning — this one has teeth.** `reset --hard` rewrites your working tree:
> all **uncommitted** changes are destroyed with no undo, and the branch forgets
> the commits after `<sha>`. Before running it: `git status` (commit or restore
> anything pending), then triple-check the SHA. And **never reset commits you
> have already pushed/shared** — teammates' history would break; on shared work
> use `git revert <sha>`, which undoes by adding a new commit instead.

**7.4 `git reflog` — the safety net.** The reset-away commits are *not* gone.
`reflog` is git's private diary of everywhere HEAD has been:

```text
PS> git reflog -6
c954830 HEAD@{0}: reset: moving to c954830
8f2b62e HEAD@{1}: commit: A07 task 6: chaos experiment, attempt 2
9059d3f HEAD@{2}: commit: A07 task 6: chaos experiment, attempt 1
c954830 HEAD@{3}: reset: moving to c954830
a5e9ee8 HEAD@{4}: commit: A07: bring the loader back from the savepoint
43fadea HEAD@{5}: commit: A07: experiment that went nowhere
PS> git reset --hard 8f2b62e
HEAD is now at 8f2b62e A07 task 6: chaos experiment, attempt 2
```

Grab the SHA from the reflog line and reset to it — the "lost" work is back.
(The `HEAD@{1}` names also work as targets, but **quote them in PowerShell**:
`git reset --hard "HEAD@{1}"`. Unquoted `@{...}` is PowerShell hashtable
syntax, so git sees garbage; bare SHAs sidestep this, and bash needs no
quotes.) Moral: a committed change is almost impossible to lose; an
uncommitted one is one `reset --hard` from gone. Commit early, commit often.

## 8. Working with a remote (push, pull, PR)

Everything so far lives in one `.git` folder on one disk. A **remote** is a
second copy of the repository that commits travel to and from — an off-machine
backup, the bridge to a second computer, the way teams share code. The lab
never requires one, but the moves are identical whether the remote is GitHub
or a plain folder on your disk — so you can learn all of them offline, with a
folder.

### 8.1 A bare repo is all a "server" is

Your repo is working files plus history in `.git`. A **bare** repo is the
history *only* — no working files, nobody edits in it; it exists to be pushed
to and pulled from. That is essentially what GitHub is: a bare repo on someone
else's machine, with a website on top. Make your own stand-in next to your
repo (any local folder outside the repo works — though, like everything in
this lab, not inside OneDrive/Dropbox: two sync systems fighting over one
folder ends badly):

```text
PS> git init --bare -b main ..\origin.git
Initialized empty Git repository in C:/Users/you/Proj/origin.git/
```

Note `-b main` *again*. A plain `git init --bare` points the new repo's HEAD
at `master`; push only `main` into it and a later clone checks out nothing,
warning `remote HEAD refers to nonexistent ref` — section 2's habit paying
for itself. Now introduce your repo to it:

```text
PS> git remote add origin ..\origin.git
PS> git remote -v
origin  ..\origin.git (fetch)
origin  ..\origin.git (push)
```

`origin` is not magic, just the conventional nickname for "the main remote".
On a real host the only thing that changes in this whole section is the
address: `git remote add origin https://github.com/you/etl-lab.git`.

### 8.2 `git push` — send commits out

```text
PS> git push -u origin main
branch 'main' set up to track 'origin/main'.
To ..\origin.git
 * [new branch]      main -> main
```

`-u` (`--set-upstream`) records the pairing "local `main` ↔ `origin/main`".
Do it once, on the *first* push of each branch; from then on plain `git push`
and `git pull` know where to go. An assignment branch is the same move:

```text
PS> git switch -c a15-dbt
PS> git commit -m "A15 task 1: dbt project scaffold and profile"
PS> git push -u origin a15-dbt
branch 'a15-dbt' set up to track 'origin/a15-dbt'.
To ..\origin.git
 * [new branch]      a15-dbt -> a15-dbt
```

After the next task's commit, plain `git push` suffices:

```text
PS> git commit -m "A15 task 2: shopcore source declarations"
PS> git push
To ..\origin.git
   d5d53b8..7424e92  a15-dbt -> a15-dbt
```

Read that last line as "origin's `a15-dbt` advanced from `d5d53b8` to
`7424e92`". One rule keeps push simple: it only *adds* history. If the remote
has commits you don't, push refuses (`! [rejected] ... non-fast-forward`) —
the fix is always pull first, sort out the merge locally, push again. This is
also why section 7.3 forbids `reset --hard` on pushed commits: push can't
take history back that others may already have.

### 8.3 `git fetch` / `git pull` — bring commits back

For pull to have anything to do, commits must reach the remote from
*somewhere else*. One `git clone` gives you that somewhere: a full copy with
`origin` already wired up, standing in for your laptop — or a teammate. Run
it from the *parent* folder (never inside your repo — you'd nest one repo in
another):

```text
PS> cd ..
PS> git clone .\origin.git laptop
Cloning into 'laptop'...
done.
```

(In the clone, set `user.name`/`user.email` again — section 2's config is
per-repo here, and this copy is playing a different machine.) On the
"laptop", make a commit and `git push` it. Then back in your real repo, on
`main` (`git switch main`):

```text
PS> git fetch
From ..\origin
   a306577..178dd12  main       -> origin/main
PS> git status
On branch main
Your branch is behind 'origin/main' by 1 commit, and can be fast-forwarded.
```

`fetch` downloads the news and moves your *remote-tracking* pointer
`origin/main` — and touches nothing else. Your files don't change; fetch is
always safe. `pull` is fetch **plus** merging your branch up to date:

```text
PS> git pull
Updating a306577..178dd12
Fast-forward
 journal.md | 1 +
 1 file changed, 1 insertion(+)
```

Habit pair for life with a remote: **pull before you branch, push after you
merge.** Solo on one machine, pull will mostly report `Already up to date` —
it costs a second, and starts saving you the day machine number two appears.

### 8.4 On GitHub/GitLab this becomes a pull request

On a hosted remote, the ritual grows exactly one step. You already do:
branch, commit per task, push the branch. Instead of merging locally, you
then click **open a pull request** — "please merge `a15-dbt` into `main`".
The site shows the branch's commits and one combined diff; a teammate reads
the diff, comments, approves; the merge button performs the same `--no-ff`
merge as section 6. (GitLab calls it a *merge request* — same thing.)

Solo, you can still do the reviewer's half — and should. Before merging, read
your own branch the way a reviewer would:

```text
PS> git log --oneline main..a15-dbt
7424e92 A15 task 2: shopcore source declarations
d5d53b8 A15 task 1: dbt project scaffold and profile
PS> git diff --stat main...a15-dbt
 dbt_lab/dbt_project.yml | 1 +
 dbt_lab/sources.yml     | 1 +
 2 files changed, 2 insertions(+)
```

`main..a15-dbt` (two dots) lists the commits the merge would bring;
`main...a15-dbt` (three dots) diffs against the point where the branch left
`main` — exactly what the PR page shows. Drop `--stat` for the line-by-line
read; the debug print you forgot to delete has nowhere to hide. Then merge as
in section 6, push `main`, and retire the branch on both sides:

```text
PS> git merge --no-ff a15-dbt
Merge made by the 'ort' strategy.
 2 files changed, 2 insertions(+)
PS> git push
To ..\origin.git
   178dd12..25ddf86  main -> main
PS> git branch -d a15-dbt
Deleted branch a15-dbt (was 7424e92).
PS> git push origin -d a15-dbt
To ..\origin.git
 - [deleted]         a15-dbt
```

Step back and look at what you just ran: branch → commit per task → read the
diff → merge on green. That *is* the team PR workflow, minus one reviewer —
the ritual you have practiced since A00 was the real thing all along. Add a
remote and a colleague and the only new muscle is push/pull, and you now have
that one too.

## 9. Cheatsheet

| command | does |
|---|---|
| `git init -b main` | new repo, branch named `main` |
| `git config user.name "..."` / `user.email "..."` | identity on every commit (required) |
| `git config core.editor "notepad"` | bare `git commit` opens Notepad, not vim |
| `git status` | what's modified / staged / untracked |
| `git diff` / `git diff --staged` | unstaged / staged changes, line by line |
| `git add <file>` / `git add -A` | stage one file / everything |
| `git commit -m "ANN task K: what"` | savepoint with inline message |
| `git log --oneline` (`--graph`) | history, one line per commit |
| `git switch -c aNN-topic` / `git switch main` | new assignment branch / back to main |
| `git merge --no-ff aNN-topic` | fold green assignment into main, keep the bubble |
| `git branch -d aNN-topic` | delete merged branch (refuses if unmerged) |
| `git init --bare -b main <path>\origin.git` | stand-in "server" repo: history only, no working files |
| `git remote add origin <path-or-url>` / `git remote -v` | register / list the remote nicknamed `origin` |
| `git push -u origin <branch>` | first push of a branch; `-u` pairs it with the remote |
| `git push` / `git pull` | send new commits / fetch + merge, on the paired branch |
| `git fetch` | download remote news; moves `origin/*` only, never your files |
| `git clone <path-or-url>` | full copy of a remote repo, `origin` pre-wired |
| `git push origin -d aNN-topic` | delete the merged branch on the remote too |
| `git log main..br` / `git diff main...br` | the PR view: commits / diff a merge would bring in |
| `git restore <file>` | discard uncommitted edits to a file |
| `git checkout <sha> -- <file>` | fetch one file from an old commit |
| `git reset --hard <sha>` | branch back to `<sha>`; **destroys uncommitted work** |
| `git reflog` | where HEAD has been — finds "lost" commits |
| vim trap | `Esc :wq` Enter = save & continue · `Esc :q!` Enter = bail out |
