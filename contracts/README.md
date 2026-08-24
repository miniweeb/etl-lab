# Data contracts — anatomy, and the questions behind every line

A data contract is the **agreed, versioned promise** between the team that
produces a dataset and the team that consumes it. Schemas describe what data
*is*; contracts pin down what both sides may *rely on* — and who is at fault
when reality disagrees.

[`shopcore_orders_daily.contract.yaml`](shopcore_orders_daily.contract.yaml)
is the live example this lab loads against (A05/A06). Study it clause by
clause. It is deliberately *realistic*, which means it is good **and still has
holes** — finding them is an A06 exercise.

## Anatomy of a contract

| Block | The question it answers | If it's missing... |
|---|---|---|
| identity & version | which agreement is this, and did it change? | you can't tell a breaking change from a bug |
| producer / consumer / escalation | who do I page at 4am? | incidents die in a group chat |
| change management | how much notice before a rename? | A10 happens to you unannounced |
| delivery & freshness SLA | when is "late"? what signals "complete"? | you alert on vibes |
| business-date semantics | what does the date in the filename *mean*? | off-by-one-day reports, timezone fights |
| per-column schema + semantics | see below | every ETL bug ever |
| quality tolerances | how dirty is "normal" vs incident-worthy? | you silently absorb producer bugs |
| business rules | what combinations are impossible? | garbage that types can't catch |
| duplicate / latest-wins policy | which copy of a row is true? | double counting |

## The per-column interrogation

For **every** column, a data engineer asks — and the contract must answer:

**Money columns**
- Currency? Fixed or per-row? Scale (decimals)? Rounding rule, and who applies it?
- Are negatives allowed — and what do they *mean* (refund? correction? bug)?
- Locale of transport: decimal separator, thousands separators, currency symbols?
- Is the total derivable from parts (`total = Σ qty×price`)? Within what tolerance?

**Timestamp columns**
- Timezone: UTC? store-local? server-local? Is there an offset in the value?
- Already converted by the producer, or raw? Who handles DST?
- Event time vs processing time vs export time — which one is this?
- Warehouse policy: keep raw (recommended: store UTC, convert in marts) or convert at ingest?
- Valid range relative to the file's business date?

**Keys and identity**
- Nullable? What does null *mean* (guest checkout ≠ missing data)?
- Unique — per file, per day, or per real-world entity? Reused ever?
- Referential: which feed is the parent, and what do we do with orphans?

**Enums and strings**
- Closed value list? Casing/padding guarantees? Encoding?
- What is the null representation — SQL NULL, empty string, `"none"`, absent key?

**Nested/JSON columns**
- Exact inner shape and types? Are numbers guaranteed numeric, or may they
  arrive string-encoded? Optional keys: absent vs null?

## Violation vs gap — the two failure modes

- **Violation**: the contract states a rule and the data breaks it
  (e.g. a negative `order_total` when `negative_allowed: false`).
  → Quarantine, count, and **report to the producer**. Never silently patch.
- **Gap**: the contract is *silent* and reality surprises you
  (e.g. European decimal commas — the money clause never mentioned locale).
  → You cannot blame the producer for a promise never made. **Propose a
  contract amendment**, then handle the data explicitly in the meantime.

Learning to tell these apart — and to *hunt the silences* before they hunt
you — is the single most valuable skill this folder teaches.

## What "good" looks like when you ask for a new feed

Before writing any ETL for a new source, get answers (in writing, in a
contract) to at least: business-date semantics, timezone of every timestamp,
money locale/currency/sign rules, null representations, uniqueness scope,
late-data window, completeness signal (manifest/control table), freshness SLA,
change-notice policy, and an escalation contact. If a producer cannot answer
one of these, that answer is "undefined" — plan for it to go wrong.
