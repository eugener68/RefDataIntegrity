# Surrogate Key Repair via Key-Map Tables

**Restoring fact–dimension referential integrity after an uncoordinated dimension reload**

Companion documentation for `surrogate_key_repair_notebook.py`

> **Operators:** use **[RUNBOOK.md](RUNBOOK.md)** for step-by-step instructions. This document is the technical deep dive.

## 0. Choosing the Right Notebook

This repository provides two complementary methods for restoring referential integrity
after a dimension reload with regenerated surrogate keys. **Pick one target end-state**
— you generally should not run both notebooks on the same dimension.

| | **This doc — Key-Map Repair** | **[SQL Server SK Alignment](sqlserver_sk_alignment_method.md)** |
|---|---|---|
| Notebook | `surrogate_key_repair_notebook.py` | `sqlserver_sk_alignment_notebook.py` |
| Target SK on dims & facts | **Reloaded Databricks SKs** | **SQL Server SK values** |
| Fixes dimensions? | No — facts only | Yes — rebuilds `dim_*_fixed` |
| Fact repair mechanism | Key-map: `old_sk → new_sk` | SQL Server fact join on business keys (through D-1), then NK / key-map for today |
| Best when | All facts still carry old SKs, or you are standardizing on Databricks keys | SQL Server is golden through D-1; facts mix SS initial load + Databricks incrementals |
| Extra parameters | — | Legacy fact table, business keys, cutoff date |

**Use this repair notebook when:**

- The reloaded Databricks dimension SKs are the long-term standard.
- Facts need to be re-keyed from SQL Server SKs to Databricks SKs via natural key.
- You do not need to change the dimension table itself.

**Use the [alignment notebook](sqlserver_sk_alignment_method.md) when:**

- SQL Server SK values must remain canonical in Databricks.
- Fact tables contain **mixed cohorts** (historical SS SKs + incremental Databricks SKs).
- SQL Server facts are available through D-1 and join on a stable business-key grain.

Both notebooks share the same safety model: dry-run default, snapshot + key-map for audit,
rebuild into `*_fixed` tables, manual swap. Key-map tables from either run are worth keeping.

---

## 1. The Problem This Solves

During a SQL Server → Databricks data warehouse migration, dimension tables were
re-loaded from source **without preserving their original surrogate keys (SKs)**.
Identity-style SKs were regenerated from scratch, while the fact tables still carry
the *old* SK values. The result:

- Fact foreign keys point at the **wrong** dimension rows (old SK 42 = "Acme Corp"
  in the legacy dim, but SK 42 = "Zenith Ltd" in the reloaded dim), or
- Fact foreign keys point at **non-existent** rows (orphans), and
- Because both old and new SKs are dense integer sequences starting near 1, the
  corruption is **silent** — joins still "work", they just return wrong answers.

### Why this is recoverable

Surrogate keys are arbitrary, but **natural keys (business keys) are not**. As long as:

1. the legacy dimensions (with their original SKs *and* natural keys) are still
   accessible — in your case via **Lakehouse Federation** to the original SQL Server, and
2. the reloaded Databricks dimensions still carry the same natural key columns,

then every old SK can be translated to its correct new SK through the natural key:

```
fact.old_sk ──► legacy dim ──► natural_key ──► new dim ──► new_sk
```

The **key-map table** materializes exactly this translation, one row per mapping,
with an explicit status for every case that *couldn't* be mapped. Facts are then
re-keyed through the map in a single, auditable, reversible pass.

---

## 2. Method Overview

```
┌─────────────────────┐     ┌──────────────────────┐
│ SQL Server (legacy) │     │ Databricks (current) │
│ via Foreign Catalog │     │                      │
│                     │     │                      │
│  DimCustomer        │     │  gold.dim_customer   │
│  CustomerSK (old)   │     │  customer_sk (new)   │
│  CustomerCode  ◄────┼──┬──┼─►customer_code       │
└─────────┬───────────┘  │  └──────────┬───────────┘
          │              │             │
          ▼         natural key        ▼
   ┌──────────────┐   matching   ┌─────────────┐
   │  SNAPSHOT    │──────────────│   KEY-MAP   │
   │ staging.     │              │ keymap.     │
   │ legacy_dim_* │              │ *_keymap    │
   └──────────────┘              └──────┬──────┘
                                        │ old_sk → new_sk
                                        ▼
                              ┌───────────────────┐
                              │ fact_*_fixed      │
                              │ (rebuilt, audited,│
                              │  then swapped in) │
                              └───────────────────┘
```

The pipeline has seven stages, each a section in the notebook:

| Stage | What happens | Writes data? |
|---|---|---|
| 1. Snapshot | Legacy dim materialized from foreign catalog into `staging` | Yes (staging only) |
| 2. Pre-checks | Collation/case duplicates, natural-key uniqueness on both sides | No |
| 3. Key-map build | `natural_key → old_sk → new_sk` with match status | Yes (keymap only) |
| 4. Audit | Status distribution + per-fact impact report — **hard stop, read it** | No |
| 5. Fact rebuild | Each fact rebuilt into a `*_fixed` copy with corrected SKs | Only if `dry_run=false` |
| 6. Validation | Orphan checks + optional measure reconciliation vs. legacy | No |
| 7. Swap | Manual `ALTER TABLE ... RENAME` — deliberately not automated | Manual |

### Design principles baked into the method

- **Never repair in place.** Old and new SK ranges overlap (both start near 1). An
  in-place `UPDATE`/`MERGE` can re-map a row that was already re-mapped. Rebuilding
  into a `*_fixed` table maps every row exactly once and leaves the original
  untouched as a rollback path.
- **Snapshot, don't federate live.** The snapshot freezes the reference point (the
  source might still be changing), avoids dragging the full table over the wire on
  every iteration, and serves as durable audit evidence of what you mapped against.
- **Normalize natural keys explicitly.** SQL Server's default collation is
  case-insensitive and pads/ignores trailing spaces; Spark compares strings exactly.
  All NK comparisons use `upper(trim(cast(col as string)))`, composite keys joined
  with `'||'`, NULL components replaced by a `~NULL~` sentinel.
- **Every unmappable row is visible, never silently dropped.** Orphans get an
  explicit status in the map and land on the unknown member (`-1` by default) in
  the repaired fact.
- **Dry run by default.** The first run builds snapshots, maps, and audits, but
  cannot touch a fact table.

---

## 3. Prerequisites

Before running the notebook, confirm all of the following:

1. **Lakehouse Federation (foreign catalog)** to the legacy SQL Server is set up
   and you can `SELECT` from the legacy dimension tables, e.g.
   `SELECT * FROM sqlserver_dw.dbo.DimCustomer LIMIT 10;`
2. **The legacy dims still reflect the state the facts were keyed against.** If
   the SQL Server dims have changed since the facts were loaded, the mapping may be
   partially wrong — check `ORPHAN_OLD` counts carefully in stage 4.
3. **Natural key columns exist on both sides** and identify the same business
   entity. You must know which column(s) they are for every dimension.
4. **You know which facts were also reloaded.** Facts reloaded *after* the dims
   already carry new SKs and **must not** be re-keyed. The stage-4 audit surfaces
   these as `NOT_IN_LEGACY_DIM`, but you should know your list up front.
5. **Permissions:** `CREATE SCHEMA` / `CREATE TABLE` on the staging and keymap
   schemas, `SELECT` on the foreign catalog, and (for the non-dry run)
   `CREATE TABLE` rights in the schema holding the facts.
6. **An unknown member row exists** in every dimension (SK = `-1` by convention),
   or you accept that unmapped fact rows will carry a dangling `-1` until you add one.
7. **Compute:** any UC-enabled cluster or SQL warehouse attached to the notebook.
   Serverless works. Size for your largest fact table (stage 5 rewrites it once).

---

## 4. Installing the Notebook

1. Download `surrogate_key_repair_notebook.py`.
2. In Databricks: **Workspace → (your folder) → Import → File**, select the `.py` file.
3. It imports as a notebook (the file uses Databricks notebook source format —
   `# COMMAND ----------` cell separators and `# MAGIC %md` markdown cells).
4. Attach a cluster. On first run of the widget cell, 19 parameter widgets appear
   at the top of the notebook.

---

## 5. Parameter Reference

All parameters are notebook **widgets** — editable text boxes/dropdowns at the top
of the notebook UI. They can also be supplied programmatically when running the
notebook from a **Job/Workflow** (see §7), which is the recommended way to process
many dimensions.

> **Golden rule for the two NK lists:** `legacy_nk_cols` and `new_nk_cols` are
> matched **positionally**. Same number of columns, same business meaning at each
> position, even if the column names differ between systems.

### 5.1 Source (legacy) side

| # | Widget | Example | Description |
|---|---|---|---|
| 0 | `foreign_catalog` | `sqlserver_dw` | Name of the Lakehouse Federation catalog that points at the legacy SQL Server. Find it in **Catalog Explorer**; it's whatever you named the foreign catalog when creating the connection. |
| 1 | `legacy_schema` | `dbo` | Schema containing the legacy dimension inside that catalog. |
| 2 | `legacy_dim_table` | `DimCustomer` | Legacy dimension table name, exactly as it appears in the foreign catalog. Also used (lower-cased) to name the snapshot `staging.legacy_dimcustomer` and the map `keymap.dimcustomer_keymap`. |
| 3 | `legacy_sk_col` | `CustomerSK` | The surrogate key column in the **legacy** dim — the values your facts currently hold. |
| 4 | `legacy_nk_cols` | `CustomerCode` | Natural/business key column(s) in the legacy dim, **comma-separated** for composite keys, e.g. `ProductCode,SourceSystem`. Order matters (matched positionally with `new_nk_cols`). |

**How to choose `legacy_nk_cols`:** the natural key is the column (or set of
columns) that uniquely identifies the business entity *independently of the
warehouse* — customer number, product code + source system, account ID, etc.
Tests you can run to verify a candidate:

```sql
-- must return 0 rows for an SCD1 dim:
SELECT CustomerCode, COUNT(*) FROM sqlserver_dw.dbo.DimCustomer
GROUP BY CustomerCode HAVING COUNT(*) > 1;

-- for SCD2, uniqueness is (natural key + version), so instead check:
SELECT CustomerCode, ValidFrom, COUNT(*) FROM sqlserver_dw.dbo.DimCustomer
GROUP BY CustomerCode, ValidFrom HAVING COUNT(*) > 1;
```

If no combination of source columns is unique, stop — that dimension cannot be
repaired by this method and needs a data-modeling decision first.

### 5.2 Target (current) side

| # | Widget | Example | Description |
|---|---|---|---|
| 5 | `new_catalog_schema` | `main.gold` | `catalog.schema` of the reloaded dimension in Databricks (two-part, no table name). |
| 6 | `new_dim_table` | `dim_customer` | The reloaded dimension table name. |
| 7 | `new_sk_col` | `customer_sk` | Surrogate key column in the **current** dim — the values facts *should* hold after repair. |
| 8 | `new_nk_cols` | `customer_code` | Natural key column(s) in the current dim, comma-separated, **same count and order** as `legacy_nk_cols`. |

### 5.3 SCD configuration

| # | Widget | Example | Description |
|---|---|---|---|
| 9 | `scd_type` | `1` or `2` | Dropdown. Type 1 = dim keeps only current values (one row per natural key). Type 2 = dim keeps history (multiple rows per natural key with validity windows). Determines both the matching logic and the uniqueness rules in pre-checks. |
| 10 | `legacy_valid_from` | `ValidFrom` | **SCD2 only.** Validity-start column in the legacy dim. Leave blank for SCD1. |
| 11 | `legacy_valid_to` | `ValidTo` | **SCD2 only.** Validity-end column in the legacy dim. `NULL` is treated as open-ended (9999-12-31). |
| 12 | `new_valid_from` | `valid_from` | **SCD2 only.** Validity-start column in the current dim. |
| 13 | `new_valid_to` | `valid_to` | **SCD2 only.** Validity-end column in the current dim. |

**Not sure which type your dim is?** Run:
```sql
SELECT customer_code, COUNT(*) AS versions
FROM main.gold.dim_customer GROUP BY customer_code
ORDER BY versions DESC LIMIT 5;
```
Multiple rows per natural key + validity-date columns ⇒ SCD2. One row per key ⇒
treat as SCD1 (even if columns like `valid_from` exist but every row is current).

### 5.4 Facts to repair

| # | Widget | Example | Description |
|---|---|---|---|
| 14 | `fact_tables` | `main.gold.fact_sales:customer_sk,main.gold.fact_returns:customer_sk` | Comma-separated list of `fully.qualified.table:fk_column` pairs — every fact that references this dimension, with the name of its FK column. |
| 15 | `fact_event_date_cols` | `order_date,return_date` | **SCD2 only.** One event-date column **per fact table, in the same order** as `fact_tables`. Used to pick the correct dimension version when one legacy row overlaps multiple new validity windows. Leave blank for SCD1. |

**Format details for `fact_tables`:**
- Each entry is `table:fk_column` separated by a single colon.
- Tables should be fully qualified (`catalog.schema.table`).
- A fact referencing the dim through **two roles** (e.g. `ship_to_customer_sk`
  and `bill_to_customer_sk`) is listed **twice**:
  `main.gold.fact_sales:ship_to_customer_sk,main.gold.fact_sales:bill_to_customer_sk`.
  Note: each entry rebuilds the fact independently — for multi-role facts, run the
  first role, swap, then run the second role against the swapped table (or extend
  the notebook to handle both FKs in one rebuild).

**Choosing the event-date column (SCD2):** it must answer "*as of when should this
fact see the dimension?*" — usually the transaction/order/event date, **not** the
load timestamp. If facts can carry event dates outside every dimension validity
window (very early or late-arriving facts), those rows land on the unknown member;
review them in stage 5 output.

### 5.5 Infrastructure & behavior

| # | Widget | Example | Description |
|---|---|---|---|
| 16 | `staging_schema` | `main.staging` | Where legacy snapshots are written (`legacy_<dim>` tables). Created if missing. |
| 17 | `keymap_schema` | `main.keymap` | Where key-map tables are written (`<dim>_keymap`). Created if missing. **Keep these tables permanently** — they are your lineage/audit record. |
| 18 | `unknown_member_sk` | `-1` | SK assigned to fact rows that cannot be mapped. Must match (or you must create) the unknown-member row in your dimensions. |
| 19 | `dry_run` | `true` / `false` | Dropdown, defaults to `true`. When `true`: snapshot, pre-checks, key-map, and audit all run, but **no `*_fixed` fact table is created**. Set to `false` only after the stage-4 audit looks right. |

---

## 6. Step-by-Step Run Procedure

Repeat this procedure **once per dimension**, starting with the smallest one to
build confidence.

### Step 1 — Configure widgets

Fill in all widgets per §5. Then run the parameter cell. It prints a
**configuration summary** — read it; this catches most typos. The cell *fails
deliberately* if:
- `legacy_nk_cols` and `new_nk_cols` have different column counts,
- `scd_type=2` but any validity column is blank,
- `scd_type=2` and the count of `fact_event_date_cols` ≠ count of `fact_tables`,
- any `fact_tables` entry is missing its `:fk_column` part.

### Step 2 — Snapshot (run section 1)

Materializes `staging.legacy_<dim>` from the foreign catalog with normalized
natural keys and (for SCD2) validity columns, then tags it with `snapshot_source`
and `snapshot_at` table properties. Verify the printed row count roughly matches
your expectation for that dimension.

### Step 3 — Pre-checks (run section 2) — *do not skip*

Two audits, both about natural-key trustworthiness:

- **2a — legacy side duplicates.** For SCD1, any natural key mapping to multiple
  legacy SKs means SQL Server's case-insensitive collation was merging values
  (`'ABC01'` vs `'abc01'`) that may now be separate rows in Databricks. These keys
  need a human dedup decision **before** the map can be trusted for them.
- **2b — current side duplicates.** Multiple current SKs per natural key on an
  SCD1 dim almost always means the reload double-inserted rows. **Fix the
  dimension first**, then re-run from Step 2.

✅ on both checks ⇒ proceed. ⚠️ anywhere ⇒ resolve, re-run.

### Step 4 — Build the key-map (run section 3)

Creates `keymap.<dim>_keymap`. One row per legacy dimension row (SCD2) or per
natural key (SCD1), with:

| Column | Meaning |
|---|---|
| `natural_key` | Normalized (possibly composite) business key |
| `old_sk` | SK in the legacy dim — what facts currently hold |
| `new_sk` | SK in the reloaded dim — what facts should hold |
| `valid_from`, `valid_to` | SCD2: validity window of the matched *new* row (NULL for SCD1) |
| `map_status` | `MATCHED` / `ORPHAN_OLD` / `ORPHAN_NEW` / `AMBIGUOUS` |
| `created_at` | Build timestamp |

Status semantics and what to do about each:

| `map_status` | Meaning | Action |
|---|---|---|
| `MATCHED` | Clean 1:1 translation | None — these drive the repair |
| `ORPHAN_OLD` | Member existed in legacy dim, missing after reload | Facts using it go to unknown member. A large count ⇒ the reload **dropped members** — fix the dim load, re-run from Step 2 |
| `ORPHAN_NEW` | Member only exists in the new dim | No fact impact; informational (reload added members or split case-variants) |
| `AMBIGUOUS` | SCD2: one legacy row overlaps several new validity windows (reload re-cut version boundaries) | Normal for SCD2 reloads; resolved per-fact-row by event date in Step 6 |

### Step 5 — Audit (run section 4) — *hard stop*

Two reports:

1. **Status distribution** of the key-map. Healthy ≈ overwhelmingly `MATCHED`.
2. **Per-fact impact**: for every configured fact, how many rows fall in each
   status — including the extra bucket `NOT_IN_LEGACY_DIM` = fact rows whose FK
   value never existed in the legacy dim. The two usual causes:
   - **The fact was itself reloaded after the dims** ⇒ it already carries new SKs
     and **must be removed from `fact_tables`** before the repair run, or you will
     corrupt it.
   - The FK was already broken pre-migration ⇒ those rows will (correctly) go to
     the unknown member.

Do not proceed until every number in this section is explained.

### Step 6 — Repair the facts (set `dry_run=false`, run section 5)

For each configured fact, creates `<fact>_fixed` containing every original column,
with the FK column replaced via the map:

- **SCD1:** `LEFT JOIN` on `old_sk` over `MATCHED` rows only.
- **SCD2:** join additionally constrained by
  `event_date >= valid_from AND event_date < valid_to`, over `MATCHED` and
  `AMBIGUOUS` rows — the event date is the arbiter that resolves ambiguity.
- Anything unmatched ⇒ `unknown_member_sk`.

The notebook enforces a **row-count invariant**: `*_fixed` must have exactly the
source row count. `❌ ROW COUNT CHANGED` means the SCD2 join fanned out because the
*reloaded* dim has overlapping validity windows for the same natural key — that's
a dimension data-quality bug; fix it and re-run from Step 2. **Never swap a fact
that failed this check.**

Also printed: rows sent to the unknown member, per fact. Compare to the Step-5
audit — they should reconcile.

### Step 7 — Validate (run section 6)

1. **Referential integrity:** `LEFT ANTI JOIN` from each `*_fixed` to the current
   dim must return 0 orphans (unknown member excluded).
2. **Measure reconciliation (recommended):** the commented template compares
   per-natural-key measure totals between the legacy fact (via federation) and the
   repaired fact. This is the only check that catches the worst failure mode —
   re-keying that **shifted** value between members (totals right, attribution
   wrong). Copy the template, set your measure column(s), investigate any
   non-trivial `diff`.

### Step 8 — Swap (manual, deliberate)

```sql
ALTER TABLE main.gold.fact_sales       RENAME TO main.gold.fact_sales_broken;
ALTER TABLE main.gold.fact_sales_fixed RENAME TO main.gold.fact_sales;
```

Keep `*_broken` until business sign-off, then drop. Rollback at any point before
that is the reverse pair of RENAMEs.

### Step 9 — Repeat per dimension

Re-run from Step 1 with the next dimension's parameters. Order rarely matters
(each dim/FK pair is independent), but if a fact references several broken dims,
either repair them in successive passes (swap between passes) or extend stage 5 to
re-key multiple FKs in one rebuild.

---

## 7. Running at Scale (Jobs / Workflows)

With many dimensions, drive the notebook from a Databricks Job with one task per
dimension, passing widgets as task parameters:

```json
{
  "foreign_catalog": "sqlserver_dw",
  "legacy_schema": "dbo",
  "legacy_dim_table": "DimProduct",
  "legacy_sk_col": "ProductSK",
  "legacy_nk_cols": "ProductCode,SourceSystem",
  "new_catalog_schema": "main.gold",
  "new_dim_table": "dim_product",
  "new_sk_col": "product_sk",
  "new_nk_cols": "product_code,source_system",
  "scd_type": "1",
  "fact_tables": "main.gold.fact_sales:product_sk,main.gold.fact_inventory:product_sk",
  "staging_schema": "main.staging",
  "keymap_schema": "main.keymap",
  "unknown_member_sk": "-1",
  "dry_run": "true"
}
```

Recommended pattern: a first job run with `dry_run=true` across **all** dimensions,
a human review of every stage-4 audit, then a second run with `dry_run=false` for
the dimensions that passed. Keep the swap manual regardless.

---

## 8. Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Parameter cell raises `must have the same number of columns` | NK lists out of sync | Make `legacy_nk_cols` and `new_nk_cols` same length/order |
| Snapshot is empty / errors | Foreign catalog name, schema, or table wrong; federation connection broken | `SELECT * FROM <foreign_catalog>.<schema>.<table> LIMIT 1` to isolate |
| Huge `ORPHAN_OLD` count | Reload dropped members, or NK columns mismatched (comparing the wrong columns) | Spot-check a few orphan natural keys in both dims manually |
| Huge `ORPHAN_NEW` count | Reload added members (often fine), or case-variant splitting | Cross-check with pre-check 2a output |
| Many `AMBIGUOUS` (SCD2) | Reload re-cut version boundaries | Expected; resolved by event date. Only worry if step 6 then sends many rows to unknown member |
| `NOT_IN_LEGACY_DIM` rows in audit | That fact was reloaded after the dims (already has new SKs!) or pre-existing corruption | Remove already-reloaded facts from `fact_tables` before repairing |
| `❌ ROW COUNT CHANGED` in stage 5 | Overlapping validity windows in the **reloaded** dim | Fix dim validity windows, re-run from snapshot |
| Many rows on unknown member after repair | Event dates outside all validity windows (early/late-arriving facts), or genuine orphans | Inspect a sample; consider widening the earliest `valid_from` or accepting unknown member |
| Measure reconciliation shows shifted totals | Wrong NK chosen (not actually unique per entity), or case-merge dedup decided incorrectly | Re-verify NK uniqueness tests in §5.1 |

---

## 9. After the Repair: Preventing Recurrence

1. **Keep the key-map tables forever.** They document exactly what moved and why.
2. **Make surrogate key generation deterministic**, so reloads become idempotent:
   ```sql
   SELECT xxhash64(upper(trim(customer_code))) AS customer_sk, ...
   ```
   Same natural key ⇒ same SK, on every reload, forever. (Collision risk of
   xxhash64 over realistic dimension sizes is negligible; if policy forbids
   hash keys, keep sequential SKs but make every load **look up the key-map
   first** and only mint new SKs for genuinely new natural keys.)
3. **Add post-load RI checks to the pipeline** — the stage-6 anti-join orphan
   check is three lines and would have caught this on day one:
   ```sql
   SELECT COUNT(*) FROM gold.fact_sales f
   LEFT ANTI JOIN gold.dim_customer d ON f.customer_sk = d.customer_sk;
   ```
4. **Enforce load order in orchestration:** dimensions before facts, always, with
   the fact load failing hard if its dimension lookups miss.

---

## 10. See Also

- **[SQL Server SK Alignment (D-1 Golden Source)](sqlserver_sk_alignment_method.md)** —
  align dims and facts to SQL Server SKs using federated SQL Server facts through D-1.
- **`sqlserver_sk_alignment_notebook.py`** — companion notebook for that method.

---

*Document generated 2026-06-10 to accompany `surrogate_key_repair_notebook.py`.*
