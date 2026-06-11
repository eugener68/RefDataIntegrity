# SQL Server SK Alignment (D-1 Golden Source)

**Aligning Databricks dimensions and facts to SQL Server surrogate keys without wiping the lakehouse**

Companion documentation for `sqlserver_sk_alignment_notebook.py`

> **Operators:** use **[RUNBOOK.md](RUNBOOK.md)** for step-by-step instructions. This document is the technical deep dive.

## 0. Choosing the Right Notebook

This repository provides two complementary methods for restoring referential integrity
after a dimension reload with regenerated surrogate keys. **Pick one target end-state**
— you generally should not run both notebooks on the same dimension.

| | **[Key-Map Repair](surrogate_key_repair_method.md)** | **This doc — SQL Server SK Alignment** |
|---|---|---|
| Notebook | `surrogate_key_repair_notebook.py` | `sqlserver_sk_alignment_notebook.py` |
| Target SK on dims & facts | **Reloaded Databricks SKs** | **SQL Server SK values** |
| Fixes dimensions? | No — facts only | Yes — rebuilds `dim_*_fixed` |
| Fact repair mechanism | Key-map: `old_sk → new_sk` | SQL Server fact join on business keys (through D-1), then NK / key-map for today |
| Best when | All facts still carry old SKs, or you are standardizing on Databricks keys | SQL Server is golden through D-1; facts mix SS initial load + Databricks incrementals |
| Extra parameters | — | Legacy fact table, business keys, cutoff date |

**Use this alignment notebook when:**

- SQL Server SK values must remain the canonical standard in Databricks.
- Fact tables contain **mixed cohorts** (historical SS SKs + incremental Databricks SKs).
- SQL Server dims and facts are current through D-1 and join on business keys.

**Use the [repair notebook](surrogate_key_repair_method.md) when:**

- The reloaded Databricks dimension SKs are the long-term standard.
- Facts only need re-keying from SQL Server SK → Databricks SK via natural key.
- You do not need to rebuild the dimension table.

Both notebooks share the same safety model: dry-run default, snapshot + key-map for audit,
rebuild into `*_fixed` tables, manual swap. Key-map tables from either run are worth keeping.

---

## 1. The Problem This Solves

During a SQL Server → Databricks migration, dimension tables were re-loaded **without
preserving their original surrogate keys (SKs)**. Identity-style SKs were regenerated
from scratch. Facts now carry **two different SK namespaces in the same table**:

| Cohort | How the fact row was loaded | What the FK column holds |
|---|---|---|
| **Initial / historical load** | Bulk copy from SQL Server | SQL Server SK (correct entity, wrong dim after reload) |
| **Incremental loads** | Databricks ETL from external sources | Databricks SK (looked up against the reloaded dim) |

Because both namespaces use dense integer sequences starting near 1, the integer `42`
is **ambiguous** — it might mean "Acme Corp" in SQL Server or "Zenith Ltd" in the
reloaded Databricks dim, depending on which cohort the row belongs to. Joins still
"work"; they return silently wrong answers.

### Why this method is different from the repair notebook

See **§0** for the full comparison. In short: the
[repair notebook](surrogate_key_repair_method.md) fixes facts to match **reloaded
Databricks SKs** via a key-map (`old_sk → new_sk`). **This notebook** is for when
**SQL Server SK values** are canonical and SQL Server remains a living golden source
through D-1 — copying FKs from federated SQL Server facts on business keys for rows
through the cutoff, and using natural-key / key-map fallback for today-only data.

### Why this is recoverable without wiping the system

Surrogate keys are arbitrary; **natural keys and business keys are not**. As long as:

1. SQL Server dims and facts are accessible via **Lakehouse Federation** and current
   through D-1, and
2. Databricks dims still carry the same natural key columns, and
3. Each fact can be joined to its SQL Server counterpart on a stable **business-key
   grain** (order id + line id, transaction id, etc.),

then integrity can be restored by **rebuilding** dims and facts into `*_fixed` tables
and swapping — the same safe, reversible pattern as the repair notebook. Nothing
requires dropping the catalog or reloading the entire lakehouse.

---

## 2. Method Overview

```
┌────────────────────────────── SQL Server (golden, through D-1) ──────────────────────────────┐
│  DimCustomer                          FactSales                                              │
│  CustomerSK, CustomerCode             OrderID, LineID, CustomerSK, ...                       │
└────────────┬────────────────────────────────────┬────────────────────────────────────────────┘
             │ federation snapshot               │ business-key join (event_date ≤ D-1)
             ▼                                   ▼
┌──────────────────────────── Databricks ────────────────────────────────────────────────────────┐
│                                                                                              │
│  staging.legacy_dimcustomer ──NK──► keymap.dimcustomer_keymap                                │
│         │                                    │                                               │
│         └──────────────► gold.dim_customer_fixed  (SK = SQL Server SK)                       │
│                                    ▲                                                         │
│  gold.fact_sales ──BK join─────────┴──► gold.fact_sales_fixed                              │
│       │                              (FK = SQL Server SK through cutoff;                     │
│       └── event_date > D-1 ──NK/keymap──►  NK dim lookup or new_sk → old_sk)                 │
└──────────────────────────────────────────────────────────────────────────────────────────────┘
```

The pipeline has nine stages, each a section in the notebook:

| Stage | What happens | Writes data? |
|---|---|---|
| 1. Snapshot | SQL Server dim materialized into `staging` | Yes (staging only) |
| 2. Pre-checks | NK uniqueness on SQL Server snapshot and current dim | No |
| 3. Key-map build | `natural_key → sql_server_sk → databricks_sk` with status | Yes (keymap only) |
| 4. Audit key-map | Status distribution — **hard stop, read it** | No |
| 5. Dim rebuild | Current dim rebuilt into `dim_*_fixed` with SQL Server SKs | Only if `dry_run=false` |
| 6. Fact match audit | Per-fact SQL Server join match rates through cutoff | No (read-only) |
| 7. Fact rebuild | Each fact rebuilt into `*_fixed` with SQL Server FKs | Only if `dry_run=false` |
| 8. Validation | Orphan checks against `dim_*_fixed` | No |
| 9. Swap | Manual `ALTER TABLE ... RENAME` — deliberately not automated | Manual |

### Fact FK resolution logic (one column)

```
IF event_date ≤ cutoff (default D-1):
    FK = SQL Server fact FK          -- business-key join (authoritative)
         OR dim lookup by fact NK    -- fallback if SS row missing
         OR key-map new_sk → old_sk  -- fallback if fact has Databricks SK only
         OR unknown_member_sk
ELSE (today-only in Databricks):
    FK = dim lookup by fact NK
         OR key-map new_sk → old_sk
         OR unknown_member_sk
```

### Design principles baked into the method

- **Never repair in place.** Old SQL Server SKs and new Databricks SKs overlap in the
  same integer range. In-place `UPDATE`/`MERGE` can corrupt rows mid-pass. Rebuilding
  into `*_fixed` tables maps every row exactly once and leaves originals as rollback.
- **SQL Server fact join for D-1 data.** Business keys resolve the mixed-cohort
  problem without load-batch metadata — the federated SQL Server fact row is the
  authoritative source of FK values for matching transactions.
- **Snapshot the dimension, federate the fact.** The dim snapshot freezes the NK→SK
  reference for audit and iteration. Facts join live to SQL Server through federation
  (current through D-1); for very large facts, consider snapshotting legacy facts
  separately if performance requires it.
- **Normalize natural keys and business keys explicitly.** SQL Server collation is
  case-insensitive; Spark compares strings exactly. All NK/BK comparisons use
  `upper(trim(cast(col as string)))`, composite keys joined with `'||'`, NULL
  components replaced by a `~NULL~` sentinel.
- **Fix dimensions before facts.** Facts rebuilt in stage 7 join against `dim_*_fixed`
  for D+0 NK lookups. Swap the dimension before or together with facts.
- **Dry run by default.** First run builds snapshots, key-map, and match-rate audits,
  but creates no `*_fixed` tables.

### When to use which notebook

See **§0** for the full comparison. Quick cases:

| Situation | Use |
|---|---|
| Target end-state is **reloaded Databricks SKs** | [Repair notebook](surrogate_key_repair_method.md) |
| Target end-state is **SQL Server SK values** | **This notebook** |
| SQL Server is current through D-1 with matching fact grain | **This notebook** (preferred) |
| No SQL Server fact table / no business keys | [Repair notebook](surrogate_key_repair_method.md), or define BKs first |
| All facts still carry only SQL Server SKs, no incrementals | **This notebook** (dim fix may suffice; facts may need no changes) |

---

## 3. Prerequisites

Before running the notebook, confirm all of the following:

1. **Lakehouse Federation** to SQL Server is working:
   ```sql
   SELECT * FROM sqlserver_dw.dbo.DimCustomer LIMIT 10;
   SELECT * FROM sqlserver_dw.dbo.FactSales LIMIT 10;
   ```
2. **SQL Server data is current through D-1** for the tables you are aligning. Rows
   with `event_date ≤ D-1` should exist in SQL Server at the same business grain as
   Databricks. If SQL Server lags further behind, widen `cutoff_date` explicitly or
   fix the lag first.
3. **Business keys are defined** for every fact — the column(s) that uniquely identify
   a fact row at the same grain in both systems (e.g. `OrderID + LineID`). Verify:
   ```sql
   -- must return 0 rows on both sides at the fact grain:
   SELECT OrderID, LineID, COUNT(*)
   FROM sqlserver_dw.dbo.FactSales
   GROUP BY OrderID, LineID HAVING COUNT(*) > 1;
   ```
4. **Natural key columns** on both dims identify the same business entity. See §5.1
   for uniqueness tests.
5. **Event-date column** on each fact reflects business time (order date, transaction
   date) — used for the D-1 cutoff and SCD2 version resolution. Not the load timestamp.
6. **Permissions:** `CREATE SCHEMA` / `CREATE TABLE` on staging and keymap schemas,
   `SELECT` on the foreign catalog, and (for non-dry run) `CREATE TABLE` in schemas
   holding dims and facts.
7. **Unknown member row** in every dimension (SK = `-1` by convention), or accept
   unmapped rows landing on `-1`.
8. **Compute:** UC-enabled cluster or SQL warehouse. Size for the largest fact rebuild.

---

## 4. Installing the Notebook

1. Download `sqlserver_sk_alignment_notebook.py`.
2. In Databricks: **Workspace → (your folder) → Import → File**, select the `.py` file.
3. It imports as a notebook (`# COMMAND ----------` cell separators and `# MAGIC %md`
   markdown cells).
4. Attach a cluster. On first run of the widget cell, **28 parameter widgets** appear
   at the top of the notebook.

---

## 5. Parameter Reference

All parameters are notebook **widgets**. They can be supplied from a **Job/Workflow**
when processing many dimensions (see §7).

> **Positional matching rules:**
> - `legacy_nk_cols` ↔ `new_nk_cols` — same count, same business meaning per position.
> - `fact_tables`, `legacy_fact_tables`, `legacy_fact_fk_cols`, `legacy_fact_bk_cols`,
>   `new_fact_bk_cols`, `fact_event_date_cols`, `fact_nk_cols` — **same index = same
>   fact**. Index 0 across all lists refers to the first fact, index 1 to the second, etc.
> - Within one fact, business keys are separated by **`;`**. Facts are separated by **`,`**.

### 5.1 SQL Server dimension (source of SK values)

| # | Widget | Example | Description |
|---|---|---|---|
| 0 | `foreign_catalog` | `sqlserver_dw` | Lakehouse Federation catalog pointing at SQL Server. |
| 1 | `legacy_schema` | `dbo` | Schema of the SQL Server dimension. |
| 2 | `legacy_dim_table` | `DimCustomer` | SQL Server dimension table name. Used (lower-cased) to name `staging.legacy_dimcustomer` and `keymap.dimcustomer_keymap`. |
| 3 | `legacy_sk_col` | `CustomerSK` | SK column in the SQL Server dim — the values that will become canonical in Databricks. |
| 4 | `legacy_nk_cols` | `CustomerCode` | Natural/business key column(s) in the SQL Server dim, comma-separated for composite keys. |

**How to choose `legacy_nk_cols`:** the natural key uniquely identifies the business
entity independently of the warehouse — customer number, product code + source system,
etc.

```sql
-- must return 0 rows for an SCD1 dim:
SELECT CustomerCode, COUNT(*) FROM sqlserver_dw.dbo.DimCustomer
GROUP BY CustomerCode HAVING COUNT(*) > 1;

-- for SCD2, uniqueness is (natural key + version):
SELECT CustomerCode, ValidFrom, COUNT(*) FROM sqlserver_dw.dbo.DimCustomer
GROUP BY CustomerCode, ValidFrom HAVING COUNT(*) > 1;
```

If no column combination is unique, stop — resolve the data model before aligning SKs.

### 5.2 Current Databricks dimension

| # | Widget | Example | Description |
|---|---|---|---|
| 5 | `new_catalog_schema` | `main.gold` | `catalog.schema` of the current dim (two-part, no table name). |
| 6 | `new_dim_table` | `dim_customer` | Current Databricks dimension table. |
| 7 | `new_sk_col` | `customer_sk` | SK column to replace with SQL Server values. |
| 8 | `new_nk_cols` | `customer_code` | NK column(s) in the current dim — same count and order as `legacy_nk_cols`. |

### 5.3 SCD configuration

| # | Widget | Example | Description |
|---|---|---|---|
| 9 | `scd_type` | `1` or `2` | SCD1 = one row per NK. SCD2 = history with validity windows. |
| 10 | `legacy_valid_from` | `ValidFrom` | **SCD2 only.** Validity start in SQL Server dim. |
| 11 | `legacy_valid_to` | `ValidTo` | **SCD2 only.** Validity end in SQL Server dim. `NULL` = open-ended. |
| 12 | `new_valid_from` | `valid_from` | **SCD2 only.** Validity start in current dim. |
| 13 | `new_valid_to` | `valid_to` | **SCD2 only.** Validity end in current dim. |

**Not sure which type?**
```sql
SELECT customer_code, COUNT(*) AS versions
FROM main.gold.dim_customer GROUP BY customer_code
ORDER BY versions DESC LIMIT 5;
```
Multiple rows per NK with validity columns ⇒ SCD2. One row per key ⇒ SCD1.

### 5.4 Facts and SQL Server fact join

| # | Widget | Example | Description |
|---|---|---|---|
| 14 | `fact_tables` | `main.gold.fact_sales:customer_sk` | Comma-separated `catalog.schema.table:fk_column` pairs. |
| 15 | `legacy_fact_tables` | `FactSales` | SQL Server fact table name per entry, comma-separated. |
| 16 | `legacy_fact_fk_cols` | `CustomerSK` | FK column in the SQL Server fact (the column copied through cutoff). |
| 17 | `legacy_fact_bk_cols` | `OrderID;LineID` | Business keys on the SQL Server fact. **`;` between keys, `,` between facts.** |
| 18 | `new_fact_bk_cols` | `order_id;line_id` | Business keys on the Databricks fact — same count and order as legacy BKs for that fact. |
| 19 | `fact_event_date_cols` | `order_date` | Event-date column per fact — drives cutoff split and SCD2. **Required for every fact.** |
| 20 | `fact_nk_cols` | `customer_code` | NK column(s) on the fact for D+0 dim lookup. Blank = key-map fallback only. Use `;` within fact, `,` between facts. |

**Business key format examples:**

| Facts configured | `legacy_fact_bk_cols` | `new_fact_bk_cols` |
|---|---|---|
| One fact, two keys | `OrderID;LineID` | `order_id;line_id` |
| Two facts, one key each | `OrderID,ReturnID` | `order_id,return_id` |
| Two facts, mixed keys | `OrderID;LineID,ReturnID` | `order_id;line_id,return_id` |

**Choosing business keys:** use the grain at which the fact is uniquely identified in
*both* systems — typically the same primary key the SQL Server ETL uses. Do not use
surrogate keys as business keys.

**Choosing `fact_nk_cols`:** if today's incremental rows carry the customer code (or
product code) from the external source, list those columns so D+0 rows can resolve SK
via the fixed dim. If the fact only has the wrong Databricks SK and no NK, leave blank
and the notebook falls back to the reverse key-map (`new_sk → sql_server_sk`).

**Multi-role facts** (e.g. `ship_to_customer_sk` and `bill_to_customer_sk`): list the
same fact table twice in all positional lists with different FK columns. Run and swap
one role at a time, or extend the notebook to re-key both FKs in one pass.

### 5.5 Cutoff, orphan policy, infrastructure

| # | Widget | Example | Description |
|---|---|---|---|
| 21 | `cutoff_mode` | `d_minus_1` | `d_minus_1` = SQL Server fact join for rows with `event_date ≤ yesterday`. `explicit` = use `cutoff_date` instead. |
| 22 | `cutoff_date` | `2026-06-10` | Inclusive through this date when `cutoff_mode=explicit`. Leave blank for `d_minus_1`. |
| 23 | `orphan_new_dim_sk_policy` | `keep_current` | How to SK members that exist only in Databricks (`ORPHAN_NEW`). See §5.6. |
| 24 | `staging_schema` | `main.staging` | Legacy dim snapshots (`legacy_<dim>`). Created if missing. |
| 25 | `keymap_schema` | `main.keymap` | Key-map tables (`<dim>_keymap`). **Keep permanently** for lineage. |
| 26 | `unknown_member_sk` | `-1` | SK for unmapped fact rows. |
| 27 | `dry_run` | `true` / `false` | Default `true`. When true: no `dim_*_fixed` or `fact_*_fixed` tables created. |

### 5.6 ORPHAN_NEW dimension policy

`ORPHAN_NEW` = natural key exists in Databricks but not in the SQL Server snapshot
(today's new customer, member added only in Databricks, case-variant split, etc.).

| Policy | Behavior | When to use |
|---|---|---|
| `keep_current` | Retain the current Databricks SK for that row | Members genuinely new since SQL Server D-1 cutoff; most common default |
| `allocate_after_max` | Assign `MAX(sql_server_sk) + row_number` | Need all SKs in a single contiguous namespace above SQL Server max |

Members with `keep_current` SKs above the SQL Server max are expected — validation
allows them. Document them for downstream consumers.

---

## 6. Step-by-Step Run Procedure

Repeat **once per dimension**, starting with the smallest. **Fix and swap the
dimension before facts** that depend on it for D+0 lookups.

### Step 1 — Configure widgets

Fill in all widgets per §5. Run the parameter cell and read the **configuration
summary**. The cell fails deliberately if:

- NK lists have different column counts,
- SCD2 is selected but validity columns are blank,
- any positional fact list has a different length than `fact_tables`,
- legacy and new business-key counts differ for the same fact,
- `cutoff_mode=explicit` but `cutoff_date` is empty.

### Step 2 — Snapshot (run section 1)

Materializes `staging.legacy_<dim>` from the federated SQL Server dim with normalized
natural keys and (for SCD2) validity columns. Verify row count and `max sql_server_sk`
match expectations.

### Step 3 — Pre-checks (run section 2) — *do not skip*

- **SCD1 legacy duplicates:** one NK mapping to multiple SQL Server SKs ⇒ collation
  merge issue or bad source data — resolve before proceeding.
- **SCD1 current duplicates:** multiple Databricks SKs per NK ⇒ botched reload — dedup
  the dim first.

✅ on both ⇒ proceed. ⚠️ anywhere ⇒ resolve, re-run from Step 2.

### Step 4 — Build key-map (run section 3)

Creates `keymap.<dim>_keymap`:

| Column | Meaning |
|---|---|
| `natural_key` | Normalized business key |
| `old_sk` | SQL Server SK (target/canonical value) |
| `new_sk` | Current Databricks SK (pre-alignment value) |
| `valid_from`, `valid_to` | SCD2: validity window of matched Databricks row |
| `map_status` | `MATCHED` / `ORPHAN_OLD` / `ORPHAN_NEW` / `AMBIGUOUS` |
| `created_at` | Build timestamp |

| `map_status` | Meaning | Action |
|---|---|---|
| `MATCHED` | NK exists on both sides | Dim gets SQL Server SK; key-map available for D+0 fallback |
| `ORPHAN_OLD` | In SQL Server, missing in Databricks | Reload dropped members — fix dim load, re-run |
| `ORPHAN_NEW` | In Databricks only | Apply `orphan_new_dim_sk_policy`; expected for post-D-1 members |
| `AMBIGUOUS` | SCD2: one SQL Server row overlaps several Databricks windows | Resolved per fact row by event date in stage 7 |

### Step 5 — Audit key-map (run section 4) — *hard stop*

Read the status distribution. Investigate any large `ORPHAN_OLD` count before
rebuilding. Note `ORPHAN_NEW` count and confirm your SK policy is appropriate.

Do not proceed until every status count is explained.

### Step 6 — Fact match audit (run section 6) — *works in dry-run*

For each fact, review the bucket distribution:

| Bucket | Meaning |
|---|---|
| `MATCHED_SS_FACT` | `event_date ≤ cutoff` and business-key join to SQL Server fact succeeded |
| `MISSING_SS_FACT` | Through cutoff but no SQL Server match — investigate grain, filters, lag |
| `AFTER_CUTOFF` | Today-only rows — will use NK / key-map in stage 7 |

Healthy runs show `MATCHED_SS_FACT` ≈ total rows through cutoff. A large
`MISSING_SS_FACT` count means wrong business keys, grain mismatch, or SQL Server
not as current as assumed — **do not set `dry_run=false` until explained**.

### Step 7 — Rebuild dimension (set `dry_run=false`, run section 5)

Creates `<dim>_fixed` with SQL Server SKs:

- **SCD1 `MATCHED`:** `customer_sk = sql_server_sk` from key-map.
- **SCD1 `ORPHAN_NEW`:** per policy (keep current or allocate after max).
- **SCD2:** match on NK + overlapping validity windows; assign SQL Server SK per version.

Row-count invariant: fixed dim must equal source dim row count. `❌ ROW COUNT CHANGED`
⇒ fix SCD2 overlap logic or source data, re-run from Step 2.

Printed: count of rows whose SK changed (matched by natural key).

### Step 8 — Rebuild facts (run section 7)

For each fact, creates `<fact>_fixed`:

- **`event_date ≤ cutoff`:** FK from SQL Server fact; fallbacks: fact NK → fixed dim,
  then key-map `new_sk → old_sk`, then unknown member.
- **`event_date > cutoff`:** fact NK → fixed dim, then key-map, then unknown member.

Row-count invariant must hold. Compare `through cutoff` vs `SS fact join matched`
counts to the Step 6 audit — they should reconcile.

### Step 9 — Validate (run section 8)

1. **Dim SK sanity:** SKs in the SQL Server range should exist in the snapshot (except
   `ORPHAN_NEW` with `keep_current` or `allocate_after_max`).
2. **Referential integrity:** `LEFT ANTI JOIN` from each `fact_*_fixed` to `dim_*_fixed`
   must return 0 orphans (unknown member excluded).

**Recommended additional check** — measure reconciliation through cutoff:

```sql
-- Per-day totals: Databricks fixed vs SQL Server federated fact
SELECT f.event_date, SUM(f.sales_amount) AS databricks_amt
FROM main.gold.fact_sales_fixed f
WHERE f.event_date <= date_sub(current_date(), 1)
GROUP BY 1
-- compare to same aggregation on sqlserver_dw.dbo.FactSales
```

Non-trivial diffs ⇒ wrong business keys or FK column — investigate before swap.

### Step 10 — Swap (manual, deliberate)

Swap **dimension first**, then facts:

```sql
-- Dimension
ALTER TABLE main.gold.dim_customer       RENAME TO main.gold.dim_customer_broken;
ALTER TABLE main.gold.dim_customer_fixed RENAME TO main.gold.dim_customer;

-- Facts
ALTER TABLE main.gold.fact_sales         RENAME TO main.gold.fact_sales_broken;
ALTER TABLE main.gold.fact_sales_fixed   RENAME TO main.gold.fact_sales;
```

Keep `*_broken` until business sign-off. Rollback is the reverse pair of RENAMEs.

### Step 11 — Repeat per dimension

Re-run from Step 1 for the next dimension. If one fact references multiple broken dims,
repair all referenced dims (swap each) before the final fact pass, or repair facts once
all dims are aligned.

---

## 7. Running at Scale (Jobs / Workflows)

Drive the notebook from a Databricks Job with one task per dimension:

```json
{
  "foreign_catalog": "sqlserver_dw",
  "legacy_schema": "dbo",
  "legacy_dim_table": "DimCustomer",
  "legacy_sk_col": "CustomerSK",
  "legacy_nk_cols": "CustomerCode",
  "new_catalog_schema": "main.gold",
  "new_dim_table": "dim_customer",
  "new_sk_col": "customer_sk",
  "new_nk_cols": "customer_code",
  "scd_type": "1",
  "fact_tables": "main.gold.fact_sales:customer_sk",
  "legacy_fact_tables": "FactSales",
  "legacy_fact_fk_cols": "CustomerSK",
  "legacy_fact_bk_cols": "OrderID;LineID",
  "new_fact_bk_cols": "order_id;line_id",
  "fact_event_date_cols": "order_date",
  "fact_nk_cols": "customer_code",
  "cutoff_mode": "d_minus_1",
  "orphan_new_dim_sk_policy": "keep_current",
  "staging_schema": "main.staging",
  "keymap_schema": "main.keymap",
  "unknown_member_sk": "-1",
  "dry_run": "true"
}
```

Recommended pattern:

1. **Job 1:** `dry_run=true` across all dimensions — review key-map audits and fact
   match rates (sections 4 and 6).
2. **Human review** of every `MISSING_SS_FACT` and `ORPHAN_OLD` count.
3. **Job 2:** `dry_run=false` for dimensions that passed.
4. **Manual swap** per table after validation — never automate RENAME in the job.

---

## 8. Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `legacy_nk_cols` / `new_nk_cols` column count error | Lists out of sync | Same length and order |
| Positional list length error | Fact widget lists mismatched | Ensure same number of comma-separated entries in all fact lists |
| Snapshot empty / federation error | Wrong catalog, schema, or broken connection | `SELECT * FROM <fqn> LIMIT 1` |
| Large `ORPHAN_OLD` | Reload dropped dim members or wrong NK columns | Spot-check orphan keys in both systems; fix dim load |
| Large `ORPHAN_NEW` | Members added in Databricks after D-1 | Expected if `keep_current`; confirm policy |
| Large `MISSING_SS_FACT` in stage 6 | Wrong business keys, different grain, or SQL Server lag | Verify BK uniqueness; compare row counts by day; adjust cutoff |
| `MATCHED_SS_FACT` low but facts look correct | Event-date column wrong (load date vs business date) | Use transaction/order date for cutoff |
| Many `AMBIGUOUS` (SCD2) | Reload re-cut version boundaries | Expected; check stage 7 unknown-member count |
| `❌ ROW COUNT CHANGED` on fact | Join fan-out — overlapping SCD2 windows or bad BK join | Fix dim validity or BK grain |
| `❌ ROW COUNT CHANGED` on dim | SCD2 match produced duplicates | Fix validity windows; re-run snapshot |
| Many unknown-member rows after rebuild | Missing SS fact match + no fact NK + no key-map hit | Add `fact_nk_cols`; inspect sample rows |
| Historical rows wrong after swap | BK join matches wrong SS row | Verify composite keys; check for type/format differences |
| D+0 rows wrong | No fact NK and key-map miss | Add NK to incremental pipeline; ensure dim swapped first |
| Measure reconciliation diffs | Wrong FK column or BK grain | Re-verify `legacy_fact_fk_cols` and BK lists |

---

## 9. After Alignment: Preventing Recurrence

1. **Keep key-map tables permanently** — they document the Databricks→SQL Server SK
   translation and support auditing incremental rows loaded during the migration window.

2. **Dim loads must preserve SQL Server SKs** — on every reload, copy SK from the
   federated SQL Server dim by natural key. Never regenerate identity SKs:
   ```sql
   SELECT s.CustomerSK AS customer_sk, d.*
   FROM staging.dim_customer_source d
   JOIN sqlserver_dw.dbo.DimCustomer s ON normalized_nk(d) = normalized_nk(s)
   ```

3. **Incremental fact loads: lookup by natural key**, not by reusing arbitrary SK
   integers from external files:
   ```sql
   SELECT coalesce(dim.customer_sk, -1) AS customer_sk, ...
   FROM staging.ext_sales s
   JOIN gold.dim_customer dim ON upper(trim(s.customer_code)) = upper(trim(dim.customer_code))
   ```

4. **Post-load RI checks** after every fact and dim load:
   ```sql
   SELECT COUNT(*) FROM gold.fact_sales f
   LEFT ANTI JOIN gold.dim_customer d ON f.customer_sk = d.customer_sk
   WHERE f.customer_sk <> -1;
   ```

5. **Daily reconciliation through D-1** while SQL Server remains parallel golden
   source — compare row counts and measure totals by day until cutover is complete.

6. **Enforce load order:** dimensions before facts; fail the fact load on unresolved NK.

---

## 10. Quick Reference: End-State by Row Type

| Row type | `event_date` | Dim SK source | Fact FK source |
|---|---|---|---|
| Historical SS load | ≤ D-1 | SQL Server dim via NK | Already SS SK, or copied from SS fact via BK |
| Databricks incremental | ≤ D-1 | SQL Server dim via NK | **SQL Server fact via BK** (authoritative) |
| Today-only incremental | > D-1 (cutoff) | SQL Server dim via NK (+ ORPHAN_NEW policy) | Fact NK → fixed dim, or key-map fallback |

---

## 11. See Also

- **[Surrogate Key Repair via Key-Map Tables](surrogate_key_repair_method.md)** —
  re-key facts to reloaded Databricks SKs without changing dimensions.
- **`surrogate_key_repair_notebook.py`** — companion notebook for that method.

---

*Document generated 2026-06-11 to accompany `sqlserver_sk_alignment_notebook.py`.*
