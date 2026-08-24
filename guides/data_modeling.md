# Data modeling — the shape of the target

Most of this lab is about *moving* data: partitions, retries, backfills, parallelism. Modeling
is the other half — deciding what the data should look like when it arrives, and defending that
shape to the next person who has to query it.

This page is the **reference**, not a new assignment. Everything here is either practiced
somewhere in the lab (the section says where) or explicitly **named as not practiced**, so the
word is not foreign the first time an interviewer uses it. Read §1–§2 around A03, §3 after A10
(it owns keys, and states the result A10 asks you to measure), §4–§7 after A15's design exercise,
and §8 whenever you are about to create a table. A few rows and examples name outcomes from
assignments you have not reached — those name the assignment in parentheses; skim past them rather
than reading ahead. Lab words live in `GLOSSARY.md`; the binding rules are SPEC §7.7.

## 1. Grain first

**One row = one \_\_\_\_.** One sentence, for every table you create, written as a comment on
the DDL and as the first line of the dbt `description:`. A table whose grain you cannot say in
one sentence is not a table you designed — it is one you accumulated. Which keys are unique,
which measures are safe to sum, whether a join is correct: all of it follows from the grain,
so the sentence comes first and the DDL second.

| Table | One row = |
|---|---|
| `staging.shopcore_orders` | one order, as of the latest correction **in this file** |
| `core.orders` (A05, A07) | one **delivered version** of an order — unique on (`order_id`, `_data_date`), never on `order_id` alone |
| `core.orders_enriched` | one order, with its customer and store attributes attached |
| `core.order_items` | one **line item** of one delivered order **version** |
| `core.orders_log` (A08) | one **delivered version** of an order |
| `core.orders_latest` (A08, view) | one order — its newest version |
| `core.daily_store_sales` | one store × one `order_date` |
| `marts.daily_store_sales` (A15) | one store × one `order_date` × one **currency** |
| `marts.category_daily_revenue` | one `order_date` × category × currency |
| `core.customers` / `core.products` / `core.stores` | one customer / sku / store, **as of the last load** |

Two rows are worth staring at. `core.orders_log` and `core.orders_latest` describe the same
subject at *different* grains — which is exactly why A08 makes them two objects. And
`marts.daily_store_sales` carries `currency` in its grain where `core.daily_store_sales` does
not: A10 introduced VND rows at ×25,000, so one `gross_revenue` summed across currencies became a
lie. **Grain is a decision, and decisions get revisited when the data changes.**

Read the word **version** in the `core.orders` and `core.order_items` rows literally. Neither
table dedupes: A05 and A07 load every row the file delivers, and A04 unnests that same un-deduped
day table. On small `2026-06-02` that is 31 orders shipped twice, each contributing a second line
set numbered from 1 — 149,190 item rows over 149,104 distinct (`order_id`, `line_no`) pairs. The
one-row-per-order grain arrives with A08's dedupe, and with the A15 facts built downstream of it;
until then, writing "one row = one order" on these two tables would be writing a wish.

### The three classic grain errors

**(a) Mixed grain — two things in one table.** The tempting version of A04 is to widen
`core.orders` with `sku_1, qty_1, sku_2, qty_2, …`. Then half your columns mean "per order" and
half "per line", every aggregate needs a footnote, and a seventh item has nowhere to go. A04 does
the other thing: `unnest` produces a **second table at a different grain**. Two grains, two tables.

**(b) Silently changing grain.** The dangerous one, because nothing errors. `unnest`, a
one-to-many join, a window function returning every row instead of one — each multiplies your row
count while the query still runs. A04's **conservation check** is the habit that catches it:
after any grain change, prove the new row count a second way.

```sql
SELECT (SELECT count(*) FROM core.order_items)                          AS item_rows,
       (SELECT sum(json_array_length(items)) FROM shopcore_orders_day)  AS expected;
-- 149,190 = 149,190 on small 2026-06-02
```

**(c) Fan-out — the one that costs money.** Join order grain to item grain and every order's
measures repeat once per line. Verified on small `2026-06-02`, tables exactly as A03 and A04
build them:

```sql
SELECT round(sum(order_total), 2) FROM core.orders_enriched;
-- 7520558.16    <- the truth, at order grain

SELECT round(sum(o.order_total), 2)
FROM core.orders_enriched o JOIN core.order_items i USING (order_id);
-- 25319217.13   <- 3.37x too big. No error. No warning.
```

The inflation (3.37×) is **larger** than the average items per order (2.53): bigger orders carry
more lines, so the join weights expensive orders more heavily — you cannot even undo the damage
by dividing by the average. Two habits prevent it. **Sum each measure at its own grain**, then
join the results — revenue from `core.orders_enriched`, units from `core.order_items`, joined on
`(order_date, store_id)`. And **after any join, `count(*)` must equal the count of the side you
meant to preserve**: here it is 149,190 against 58,993.

## 2. Facts and dimensions

A **fact** records events at a stated grain and carries **measures** — numbers you sum. A
**dimension** describes the entities those events reference and carries **attributes** — values
you filter and group by. Two tests tell you which you hold: *does a row record something that
happened, or something that is?* (events → fact, entities → dimension), and *would summing this
column mean anything?* (`order_total`, `qty` → measure; `country`, `category` → attribute).

| Kind | Table | Measures / attributes |
|---|---|---|
| **fact** | `core.orders` (one delivered version) · `fct_orders` (one order) | `order_total`; `discount_amount` on `fct_orders` — and on `core.orders` only from A10/A11 on |
| **fact** | `core.order_items` · `fct_order_items` | `qty`, `unit_price` |
| **fact** | `core.daily_store_sales` · `marts.daily_store_sales` | `orders`, `customers`, `gross_revenue`, `cancelled_orders` |
| **dim** | `core.customers` · `dim_customers` | `country`, `city`, `signup_ts`, `is_active` |
| **dim** | `core.products` · `dim_products` | `category`, `subcategory`, `unit_price`, `tags` (+ `brand` / `color`, parsed out of `attrs` in A15) |
| **dim** | `core.stores` · `dim_stores` | `store_name`, `region`, `country`, `opened_date` |

The second test needs judgement, and the lab has the column that proves it:
**`dim_products.unit_price` is an attribute** (today's list price — summing it means nothing) while
**`fct_order_items.unit_price` is a measure** (what that order was actually charged). Same name,
different kind of thing; A15 makes both descriptions say so, because no reader can infer it from
the SQL.

### Additive, semi-additive, non-additive

**Additive** — sums correctly across every dimension. Verified: roll the 723 rows of
`core.daily_store_sales` back up and you recover the order-grain figures exactly — 58,993 orders,
7,520,558.16 revenue. That is what additive buys: the rollup is a legitimate stand-in for the fact.

**Semi-additive** — sums across some dimensions but **never across time**. Balances, headcounts,
inventory levels: Monday's bank balance plus Tuesday's is not a number, and the correct roll-up
over time is *last* or *average*, not *sum*. **This lab has no semi-additive measure** — every
measure it builds is a flow, not a level. The nearest would-be: a daily snapshot of "orders
currently sitting in `paid` status per store" — sum across stores, never across days.

**Non-additive** — ratios and rates; you cannot average an average.
`core.daily_store_sales.avg_order_value` is the lab's own trap, on small `2026-06-02`:

```sql
SELECT round(avg(avg_order_value), 2) FROM core.daily_store_sales;
-- 124.94   <- wrong: every store weighted equally, whatever its volume

SELECT round(sum(gross_revenue) / sum(orders - orders_missing_total), 2)
FROM core.daily_store_sales;
-- 127.74   <- right, and exactly the AOV over core.orders_enriched
```

The rule: **store the numerator and the denominator; divide at read time.** Notice *why* the
second query can be written at all — the rollup carries `orders_missing_total`, so the honest
denominator (orders that actually contributed money) is reconstructible. A03 puts that column
there so an aggregate carries its uncertainty; the modeling payoff is a re-derivable ratio.

**Distinct counts are non-additive too.** Summing `core.daily_store_sales.customers` across its
723 rows gives 58,721, where the true distinct customer count across the table is **29,479** —
because 15,802 customers ordered at more than one store. *Across the table*, not "that day": those
723 rows cover two `order_date`s plus NULL, exactly as §1 warned. Recompute a distinct count from the fact; never
sum it from a rollup.

### Star, snowflake, and bridges

**Star schema**: the fact in the middle, dimensions one join away. Read A15's `daily_store_sales`
model — fact, one `left join` to `dim_stores`, `group by`. That is the whole shape, and
`fct_orders` + `fct_order_items` + `dim_customers` / `dim_products` / `dim_stores` is a textbook one.

**Snowflake schema**: the dimensions themselves normalized further — here that *would* mean
`dim_products` → a `dim_subcategory` → a `dim_category`, and `dim_stores` → a `dim_region`. Benefit:
a category name spelled once. Cost: two more joins in every query an analyst writes. This lab
chooses **star** — a product dim of 2,000 rows (40,000 at full scale) is not a storage problem, and
every extra join is a query somebody gets wrong. Snowflake earns its place when a dimension is
genuinely huge, or when a sub-entity has attributes worth managing on its own or shared by several
dimensions.

**Bridge tables** resolve a many-to-many: keys only, no measures. The lab's real many-to-many is
**product ↔ tag** — `dim_products.tags` is a `VARCHAR[]` (`"wireless|audio|sale"` split on `|`), a
product has several tags and a tag has many products. The lab stores it as a list column queried
with `unnest`, DuckDB's answer to the problem a row-store solves with a bridge table
`product_tags(sku, tag)`; the list is cheaper because nothing else joins on a tag.
`core.order_items` *looks* like a bridge — it does resolve orders ↔ skus, both many — but it
carries `qty` and `unit_price`, and a table with measures at its own grain is a fact. That is the
test: **a bridge has keys only.**

## 3. Keys

*Read this section after A10. A10 owns the lesson (SPEC §7.7 rule 2), and the numbers below are
the ones it asks you to measure yourself — meeting them here first costs you the discovery.*

| Kind | Where it comes from | The trade |
|---|---|---|
| **natural** | the source (`order_id` as shopcore assigns it) | free and meaningful — and entirely outside your control |
| **canonical** | you own the *format*, the source owns the meaning (A10's `order_id` VARCHAR + `order_id_num` BIGINT) | cheap and still debuggable against the source; survives format changes, not meaning changes |
| **surrogate** | you own all of it — a sequence, or a hash of the natural key | fully decoupled from the producer, at the cost of a key lookup on every load and one more hop when tracing a row back to the source |

**A10 is the worked example, and the sharpest modeling moment in the course.** shopcore ships
`order_id` as `BIGINT` for two months, then as `'ORD-0000000000'` — a natural key that changed type
underneath a running pipeline. A10 proves the damage by joining the v3 file's late corrections
against v1/v2 history two ways:

```sql
... ON v.order_id = CAST(h.order_id_num AS VARCHAR);                      -- 0 matched
... ON CAST(replace(v.order_id,'ORD-','') AS BIGINT) = h.order_id_num;    -- 705 matched
```

Zero matches raises no error. It means 705 corrections would have been upserted as brand-new
orders — silent double counting of real revenue. That is the bill for using a producer's key raw,
and it is why `order_id_num` exists. Which of the three you pick comes down to one question:
**how much do you trust the producer's key?** A10 is your evidence.

**Composite keys.** More than one column identifies the row. `fct_order_items` is keyed by
(`order_id`, `line_no`) — neither is unique alone, and A15 can put a `unique` test on the pair
because that fact is unnested from the *deduped* `fct_orders`. A04's `core.order_items` looks like
it has the same key and does not: it unnests the un-deduped day table, so on small `2026-06-02`
86 (`order_id`, `line_no`) pairs appear twice, and the table carries no version column to complete
the key with. Same shape, one dedupe apart, two different keys — which is precisely what those two
tables' grain sentences in §1 already told you. The contract's `latest_wins_key: [order_id, updated_at]` is a third composite,
identifying a *version* of an order, which is precisely the grain of `core.orders_log`. When you
cannot name a single unique column, that is usually information about your grain, not a missing id.

**Degenerate dimensions.** `order_id` sits on `fct_orders` as a column, but there is no
`dim_orders` — every attribute an order has is already on the fact. A key with no dimension behind
it is a **degenerate dimension**, and keeping it is not laziness: it is how a support ticket ("the
customer says order ORD-0000012345 is wrong") becomes a `WHERE` clause. Drop it and the fact is
un-auditable. Same split for `store_id`, where the dimension does exist: the fact keeps the key,
the dimension keeps the attributes.

## 4. Normalize or denormalize

One trade, stated once. **Denormalize** = copy a value into more rows; buys fewer joins and simpler,
faster queries, costs storage plus a standing obligation to rebuild whenever the source of the copy
changes. **Normalize** = store each fact once and join to reach it; buys one place to update and no
drift, costs a join on every read. There is no default — the lab asks this question twice and lands
on **opposite sides**, which is the point, because the reasoning is identical.

**Denormalized: `core.orders_enriched`.** A03 copies `country` from `core.customers` and
`store_name` / `region` from `core.stores` onto every order row, because those columns appear in
nearly every question anyone asks of orders, and re-joining 1.2M rows to a 1.2M-row dimension each
time is a real cost. The bill is stated, not hidden: when a customer changes country, every order
row that copied it is stale until rebuild — acceptable precisely because the table *is* rebuilt
every delivery.

**Normalized: the lineage columns (§7.2).** The same question answered the other way. `_source_file`
and `_loaded_at` are facts about the *run*, not about the order, so the row carries exactly two
columns and one `JOIN ops.etl_runs ON run_id = _run_id` recovers the rest. A03's lineage box makes
that argument and A03's stretch 4 measures it; this guide does not re-run either.

Same question, opposite answers, same tie-breaker: **how often does the copied value change relative
to how often you read it?** A customer's country changes rarely and is read constantly → copy it. A
run's metadata changes on every attempt and is read during incidents → join for it. Two riders on
that rule, and they are what actually decide it in production. First, storage is the weak argument
either way — 69 distinct file names dictionary-encode down to almost nothing in Parquet, so the
modeling reason is the real reason. Second, **a denormalized column is only as safe as its rebuild**.
`core.orders_enriched` may copy `country` because it is `CREATE OR REPLACE`d every delivery, so the
copy is never more than one load stale. Copy the same value into a table you *append* to and you
have signed up to update history by hand, forever — nobody notices a stale copy, because the query
still returns a number. Ask what rebuilds the copy *before* you make it, not after.

## 5. History — slowly changing dimensions

`core.customers` answers "where does this customer live?" It cannot answer "where did they live
when they placed that order?" — the load overwrote it. Whether you can answer the second question
is a design decision with three standard answers.

| Type | What it does | The trade |
|---|---|---|
| **Type 1** | overwrite; keep only the current value | simplest and smallest — history is gone permanently |
| **Type 2** | a new row per change, with `valid_from` / `valid_to` / `is_current` | full history and correct point-in-time joins — the dim grows, and every query must filter |
| **Type 3** | add a `previous_country` column beside `country` | one step of history, very cheap — only one step, only for columns chosen in advance |

**This lab is Type 1 by default**: `CREATE OR REPLACE TABLE core.customers` overwrites, and
`dim_customers` is a plain `select *`. Legitimate for a lab — and also the choice that quietly makes
historical reporting impossible, so it should be a decision you made rather than defaulted into.
Where the lab builds **Type 2**:

- **A08, stretch 2** — `core.orders_history` with `valid_from` / `valid_to` / `is_current`, from the
  append log via `lead(updated_at) OVER (PARTITION BY order_id ORDER BY updated_at)`. Because A08
  kept *every delivered version* in `core.orders_log`, Type 2 history is a **query**, not a
  pipeline. An append-only log is what SCD2 is made of.
- **A15, stretch 1** — `dbt snapshot` on `dim_customers` (`strategy: check`,
  `check_cols: ['country','is_active']`): the same thing formalized, with dbt maintaining the
  `valid_from` / `valid_to` bookkeeping.

**Choosing.** Type 2 when someone will ask a point-in-time question ("revenue by the region the
store was in *then*"), or when an auditor will ask it for you. Type 1 when nobody will, or when the
dimension is a lookup nobody reports on historically. Type 3 when exactly one comparison matters
("this quarter's loyalty tier vs. last") and Type 2's cost is not worth paying — and that cost lands
on the **fact** side. Once a dimension has versions, joining `fct_orders` to `dim_customers` on
`customer_id` alone is ambiguous: it matches every version. A correct point-in-time join needs the
order's timestamp between `valid_from` and `valid_to`, or the fact must store the surrogate key of
the version current when the event happened. That is why Type 1 is not a shameful answer.

## 6. Fact types

| Type | One row = | The lab's |
|---|---|---|
| **Transaction** | one event | `core.orders` (one delivered version of an order) · `fct_orders` (one order, latest correction wins); `core.order_items` · `fct_order_items` (one order line) |
| **Periodic snapshot / aggregate** | one entity, one period | `core.daily_store_sales` · `marts.daily_store_sales`; `marts.category_daily_revenue` — all three are the *aggregate* half, see below |
| **Accumulating snapshot** | one process instance, one column per milestone | **named, not built** — see below |

**Transaction facts** are insert-only in principle, which is why they are the majority and the
easiest to get right; every idempotency pattern in A07/A08 assumes one. **Periodic snapshots** exist
for two reasons: scanning 82M transaction rows for every dashboard is wasteful, and some questions
have no event behind them — "how many stores took zero orders on Tuesday?" has no row at all in the
transaction fact, while a snapshot whose grain is *generated* can carry the zero.

Which is why SPEC §7.7 gives the type a double-barrelled name, and why the lab's rollups sit on the
**aggregate** half of it. `core.daily_store_sales` and `marts.daily_store_sales` are `GROUP BY`s over
the fact: one row per store-day **that had activity**, and a store that took no orders produces no
row at all. On small `2026-06-02` the rollup carries 399 rows for that `order_date` against 400
stores in `core.stores` (and 306 for the previous date, off that day's late corrections) — ask
it "how many stores took zero orders" and it answers 0, wrongly, with no error. A *true* periodic
snapshot builds its grain from the dimension instead — `dim_stores` cross-joined to a date spine,
`LEFT JOIN` the fact, `coalesce` the measures to 0 — so the zero rows exist because you generated
them, not because an event happened. The lab does not build that scaffold. Building it is a good
stretch goal, and the 399-of-400 is the check that tells you it worked.

**Accumulating snapshots** are the type this lab names but does not build, and the difference
matters: their rows are **updated in place, repeatedly**, as an instance walks a process. Here it
would be one row per order with `created_ts`, `paid_ts`, `shipped_ts`, `delivered_ts` — the
contract's lifecycle (`created -> paid -> shipped -> delivered`; `cancelled` only from created/paid;
`refunded` only from paid/shipped/delivered) — and the interesting measures are the *gaps*: hours
from paid to shipped, shipped to delivered. It stays unbuilt for two honest reasons, and the first
belongs to the feed: shopcore ships only the *current* `status` as of `updated_at`, never a timestamp
per milestone, so every milestone would have to be reconstructed from the correction history in
`core.orders_log` (A08's stretch 4 walks exactly that) — and only ~1.5% of orders are ever delivered
twice, so nearly every milestone column would land NULL. **A fact type is only as good as the events
you actually receive.** The second reason is ours: a fact updated in place fights the
delete+insert-by-date idempotency the rest of the lab is built on. That tension is the lesson.

## 7. The schools, briefly

Four names you will hear. You do not need to pick a side — you need to recognize which one someone
is arguing from.

| School | The shape | Built for |
|---|---|---|
| **Kimball** | dimensional, **bottom-up**: one business process at a time, each with its star, dimensions conformed so marts can join | analysts querying it directly; fastest to first value. **This lab is Kimball-shaped** — one star, not a bus; see below |
| **Inmon** | normalized, **top-down**: one enterprise warehouse in third normal form first, dimensional marts *derived* from it | reconciling many source systems into one agreed truth; slower to first value |
| **Data Vault** | **hubs** (business keys) + **links** (relationships) + **satellites** (attributes, versioned by load time); insert-only, everything stamped with source and load date | auditability and absorbing schema drift without rewriting history — at the cost of many joins between you and a readable answer, so it is nearly always a raw/integration layer with marts on top |

**Which of those did you build?** Be precise, because an interviewer will be. **Neither Inmon nor
Data Vault appears anywhere in this lab** — hub, link and satellite are recognition vocabulary and
nothing more. Kimball is the only one whose *shape* you built, and even there it is one star (A15's
`fct_orders` + `fct_order_items` + three dims), not the conformed-dimension bus Kimball actually
prescribes: no business-process rollout, no conformed-dimension matrix, no bus architecture. SPEC
§7.7 files all three under *named, not practiced* for that reason. "Kimball-shaped — I built one
star, not a bus" is the sentence that is both true and worth saying.

**Medallion** — **bronze / silver / gold** — is not a rival to those three: it names the *layers*,
not the modeling technique. And this lab already is one:

| Medallion | This lab | What happens there |
|---|---|---|
| **bronze** | `lake/orders/…` Parquet + `staging.*` | as delivered — typed and aligned, but not judged |
| **silver** | `core.*` — `core.orders`, `core.customers`, `fct_orders`, `dim_*` | cleaned, conformed, deduped; one agreed definition per entity |
| **gold** | `marts.*` — `daily_store_sales`, `category_daily_revenue` | purpose-built for consumption |

Say it plainly: **this lab's `staging` / `core` / `marts` is bronze / silver / gold under different
names** (§7.1 picked the names; §7.7 makes the mapping explicit). Asked whether you have worked with
a medallion architecture, the answer is yes — you built one.

And one honest line to carry into any interview: **almost no real shop is purely any of these.** The
common shape is an append-only raw layer with Data Vault instincts, a conformed layer that is
Kimball in spirit, dbt-built marts, and medallion names painted over the whole thing. Anyone calling
their warehouse a pure Inmon or pure Kimball implementation is describing the diagram, not the tables.

## 8. The checklist — run this before you `CREATE TABLE`

Eight questions. If you cannot answer one, you are not ready to write the DDL.

1. **What is one row?** Say it out loud before any SQL, then put it on the DDL as a comment and in
   the dbt `description:` as its first line. If the sentence needs an "and", you have two tables.
2. **Fact or dimension?** Events with measures, or entities with attributes? A table honestly both
   is two tables.
3. **What makes a row unique, and who controls that key?** Natural, canonical, or surrogate (§3)?
   If the producer owns it, what happens the day they change it — see A10's 0-versus-705.
4. **Which columns are measures, and is each additive, semi-additive, or non-additive?** Ratios get
   stored as numerator *and* denominator; distinct counts do not belong in a rollup at all.
5. **Which dimensions does it reference, and what happens when one changes?** Type 1 or Type 2
   (§5)? Choosing Type 1 is fine; not noticing that you chose it is not.
6. **What did I copy in, and what rebuilds it?** Every denormalized column is a standing refresh
   obligation. Name the trigger, or do not copy the column.
7. **How does it refresh, and is that idempotent?** Full rebuild, append, upsert, or delete+insert
   by date (A08's three strategies)? And which date range does one delivery touch — file D changes
   partitions D−7…D.
8. **What is the check that proves it right?** A conservation count after a grain change (A04),
   reconciliation against the manifest (A05), `not_null` / `unique` / `relationships` tests (A15).
   A table with no check is a table you are trusting on vibes.

A ninth, unofficial: **could someone else understand this table from its name and description
alone?** If not, no naming convention or catalog tooling will save it —
see `guides/data_governance.md` §3.3.
