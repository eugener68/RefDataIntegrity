# Surrogate Key Integrity — Team Runbook

**Audience:** Data engineering / DW operations team running SK repair in Databricks  
**Repository:** [RefDataIntegrity](https://github.com/eugener68/RefDataIntegrity)

This is the **single operational runbook** for restoring fact–dimension referential integrity after dimension reloads with regenerated surrogate keys during SQL Server → Databricks migration.

---

## Table of contents

1. [What you are fixing](#1-what-you-are-fixing)
2. [Choose the right notebook](#2-choose-the-right-notebook)
3. [Prerequisites](#3-prerequisites)
4. [Data model conventions](#4-data-model-conventions)
5. [Pre-flight checklist (every run)](#5-pre-flight-checklist-every-run)
6. [Parameters reference](#6-parameters-reference)
7. [Procedure A — Key-map repair (Databricks SKs)](#7-procedure-a--key-map-repair-databricks-sks)
8. [Procedure B — SQL Server SK alignment](#8-procedure-b--sql-server-sk-alignment)
9. [Procedure C — SCD2 hub (multi-FK)](#9-procedure-c--scd2-hub-multi-fk)
10. [Procedure D — Hub FK to SCD2 lookup dim](#10-procedure-d--hub-fk-to-scd2-lookup-dim)
11. [Swap and rollback](#11-swap-and-rollback)
12. [Validation and sign-off](#12-validation-and-sign-off)
13. [Troubleshooting](#13-troubleshooting)
14. [Test environment and automated tests](#14-test-environment-and-automated-tests)
15. [Related documents](#15-related-documents)

---

## 1. What you are fixing

After an uncoordinated dimension reload:

- Facts and SCD2 hubs still hold **old** surrogate keys (from SQL Server initial load).
- Reloaded dimensions have **new** surrogate keys for the same business entities.
- Because both SK sequences start near 1, joins **still succeed** but return **wrong** dimension rows (silent corruption).

**Recovery does not require natural keys on facts.** Natural keys are configured on **dimension tables** only. Facts/hubs are re-keyed via:

```
fact.old_sk  →  key-map.old_sk  →  key-map.new_sk  →  write to fact
```

The key-map is built by matching **legacy dim ↔ current dim** on natural key (+ validity dates for SCD2).

---

## 2. Choose the right notebook

| Target end-state | Notebook | When to use |
|---|---|---|
| **Reloaded Databricks SKs** | `surrogate_key_repair_notebook.py` | Long-term standard is Databricks keys; facts need old→new re-keying |
| **SQL Server SK values** | `sqlserver_sk_alignment_notebook.py` | SS is golden through D-1; mixed fact cohorts (SS bulk + Databricks incremental) |

**Do not run both notebooks on the same dimension.**

Deep-dive docs (same content, notebook-specific detail):

- [surrogate_key_repair_method.md](surrogate_key_repair_method.md)
- [sqlserver_sk_alignment_method.md](sqlserver_sk_alignment_method.md)

---

## 3. Prerequisites

### Infrastructure

- [ ] Unity Catalog–enabled Databricks workspace
- [ ] Lakehouse Federation catalog to SQL Server (production) **or** synthetic test catalogs (practice)
- [ ] Schemas for staging and key-map output (e.g. `recon_tgt.staging`, `recon_tgt.keymap`)
- [ ] Notebooks imported to workspace (**Workspace → Import → File**)

### Data readiness

- [ ] Legacy dimensions accessible (federated SQL Server or `recon_src.sales` in tests)
- [ ] Current (broken) dimensions in Databricks gold/silver
- [ ] **Natural key columns identified** for each dimension (see §6)
- [ ] List of **all fact and hub tables** referencing each dimension
- [ ] For alignment: SQL Server fact tables + **business keys** on fact grain (not dim NK)

### Team readiness

- [ ] Operator has read access to legacy catalog and write access to staging/keymap/gold
- [ ] Change window agreed; rollback plan understood (§11)
- [ ] Dry-run reviewed by second person before `dry_run=false`

---

## 4. Data model conventions

### SCD2 standard (all SCD2 tables in this program)

| Column | Values |
|---|---|
| `effectiveStartDate` | Validity start |
| `effectiveEndDate` | Open-ended current → `2999-12-31` |
| `recordStatus` | `C` = current, `H` = historic |

**Repair matching uses dates, not `recordStatus`.**

### Denormalized facts / SCD2 hubs

- Facts and hubs may contain **only FK columns** (no dim natural keys) — supported.
- Hub tables (e.g. `account_details_scd2`) are treated as **tables with FKs to re-key**, same as facts.
- One repair run = **one dimension**; list every fact/hub table that references it in `fact_tables`.

---

## 5. Pre-flight checklist (every run)

Run **before** each dimension repair:

```sql
-- 1. NK uniqueness on legacy dim (SCD1 example)
SELECT customer_id, COUNT(*) FROM <legacy_catalog>.<schema>.customer_dim
GROUP BY customer_id HAVING COUNT(*) > 1;

-- 2. NK uniqueness on current dim
SELECT customer_id, COUNT(*) FROM <tgt_catalog>.gold.customer_dim
GROUP BY customer_id HAVING COUNT(*) > 1;

-- 3. Sample silent corruption (same SK, different entity)
SELECT 'legacy' src, customer_key, customer_id FROM <legacy>.customer_dim WHERE customer_key = 1
UNION ALL
SELECT 'broken' src, customer_key, customer_id FROM <tgt>.gold.customer_dim WHERE customer_key = 1;
```

- [ ] NK uniqueness checks return **0 rows** (SCD1) or expected version counts (SCD2)
- [ ] You know which facts were **reloaded after dims** — exclude them from `fact_tables`
- [ ] Widgets filled for **this dimension only**
- [ ] `dry_run=true` for first execution

---

## 6. Parameters reference

### Natural keys — user-configured, positional pairing

| Widget | Legacy (SQL Server / `recon_src`) | Current (Databricks / `recon_tgt.gold`) |
|---|---|---|
| `legacy_nk_cols` | e.g. `customer_id` | — |
| `new_nk_cols` | — | e.g. `customer_id` |

Composite example: `product_code,source_system` on **both** sides, same order.

### SCD2 validity widgets

| Widget | Example |
|---|---|
| `legacy_valid_from` / `legacy_valid_to` | `effectiveStartDate` / `effectiveEndDate` |
| `new_valid_from` / `new_valid_to` | `effectiveStartDate` / `effectiveEndDate` |
| `scd_type` | `2` |

### Facts and hubs

| Widget | Example |
|---|---|
| `fact_tables` | `recon_tgt.gold.transaction_fact:customer_key,recon_tgt.gold.return_fact:customer_key` |
| `fact_event_date_cols` | **SCD2 only:** one date column per table, same order — e.g. `transaction_date,return_date` |

For hub → SCD2 lookup: `fact_tables=…account_details_scd2:account_type_key`, `fact_event_date_cols=effectiveStartDate`.

### Safety

| Widget | Default | Meaning |
|---|---|---|
| `dry_run` | `true` | Build snapshot + key-map + audit only; no `*_fixed` tables |
| `unknown_member_sk` | `-1` | FK value when mapping fails |

Full widget list: see notebook header cells or [surrogate_key_repair_method.md §5](surrogate_key_repair_method.md).

---

## 7. Procedure A — Key-map repair (Databricks SKs)

**One complete pass per dimension.** Repeat for each dimension that facts/hubs reference.

### Step A1 — Import and open notebook

Open `surrogate_key_repair_notebook.py` in Databricks.

### Step A2 — Set widgets (example: `customer_dim`, two facts)

| Widget | Value |
|---|---|
| `foreign_catalog` | `recon_src` |
| `legacy_schema` | `sales` |
| `legacy_dim_table` | `customer_dim` |
| `legacy_sk_col` | `customer_key` |
| `legacy_nk_cols` | `customer_id` |
| `new_catalog_schema` | `recon_tgt.gold` |
| `new_dim_table` | `customer_dim` |
| `new_sk_col` | `customer_key` |
| `new_nk_cols` | `customer_id` |
| `scd_type` | `1` |
| `fact_tables` | `recon_tgt.gold.transaction_fact:customer_key,recon_tgt.gold.return_fact:customer_key` |
| `staging_schema` | `recon_tgt.staging` |
| `keymap_schema` | `recon_tgt.keymap` |
| `dry_run` | `true` |

Leave SCD2 validity widgets **blank** for SCD1.

### Step A3 — Run all cells (dry run)

The notebook executes:

| Stage | Output | Your action |
|---|---|---|
| 1. Snapshot | `recon_tgt.staging.legacy_<dim>` | Confirm row count ≈ legacy dim |
| 2. Pre-checks | Duplicate NK report | Stop if SCD1 NK maps to multiple SKs |
| 3. Key-map | `recon_tgt.keymap.<dim>_keymap` | **Read carefully** |
| 4. Audit | Status distribution + per-fact impact | **Hard stop — do not continue if unexplained** |
| 5. Repair | Skipped (dry run) | — |
| 6. Validation | Skipped | — |

### Step A4 — Interpret key-map audit

| `map_status` | Meaning | Action |
|---|---|---|
| `MATCHED` | NK exists both sides | Will re-key |
| `ORPHAN_OLD` | In legacy, missing in reload | Facts → unknown member; fix dim if unexpected |
| `ORPHAN_NEW` | New dim only | Informational |
| `AMBIGUOUS` | SCD2 overlap | Resolved by fact event date at repair time |

Also check per-fact **`NOT_IN_LEGACY_DIM`**: fact SK never existed in legacy dim → fact was likely **already reloaded**; remove from `fact_tables`.

### Step A5 — Execute repair

- [ ] Set `dry_run=false`
- [ ] Re-run from **Stage 3** (or full notebook)
- [ ] Confirm each fact: `✅` row count unchanged
- [ ] Confirm orphan count on `*_fixed` vs current dim = **0** (excluding `-1`)

### Step A6 — Swap (manual)

See §11. One swap per `*_fixed` table.

### Step A7 — Repeat

Re-run from Step A2 for the next dimension (`product_dim`, `customer_scd2`, hub lookup dims, etc.).

**Order:** Any order per dimension; if a hub has 7 FKs to 7 dims, run 7 passes (swap hub between passes).

---

## 8. Procedure B — SQL Server SK alignment

Use when **SQL Server SK values** must be the canonical standard.

### Additional widgets (beyond repair)

| Widget | Example |
|---|---|
| `legacy_fact_tables` | `transaction_fact,return_fact` |
| `legacy_fact_fk_cols` | `customer_key,customer_key` |
| `legacy_fact_bk_cols` | `transaction_id,return_id` |
| `new_fact_bk_cols` | `transaction_id,return_id` |
| `fact_event_date_cols` | `transaction_date,return_date` |
| `fact_nk_cols` | Optional for D+0; blank = key-map fallback |
| `cutoff_mode` | `d_minus_1` |

### Steps

1. `dry_run=true` — review fact match audit (`MATCHED_SS_FACT`, `MISSING_SS_FACT`, `AFTER_CUTOFF`)
2. Review dim key-map (same as Procedure A)
3. `dry_run=false` — rebuilds **`dim_*_fixed`** and **`fact_*_fixed`**
4. Validate RI + row counts
5. Swap dim first, then facts (§11)

**Facts do not need dim natural keys.** Through D-1, FKs come from SQL Server fact join on **business keys** (`transaction_id`, `detail_sk`, etc.).

---

## 9. Procedure C — SCD2 hub (multi-FK)

**Pattern:** One SCD2 hub (`account_details_scd2`) with many FKs to **separate** dimensions.

### Rules

- **One repair run = one dimension = one FK column** on the hub (for SCD1 lookup dims).
- List the hub in `fact_tables` with the **single FK** being repaired.
- After each pass: **swap** `account_details_scd2_fixed` → `account_details_scd2` before the next dim.

### Example pass — `market_dim` (SCD1)

```
legacy_dim_table = market_dim
legacy_sk_col    = market_key
legacy_nk_cols   = market_code
scd_type         = 1
fact_tables      = recon_tgt.gold.account_details_scd2:market_key
```

Full 7-pass matrix: [databricks/sk_integrity_test_guide.md § Multi-FK SCD2 hub](databricks/sk_integrity_test_guide.md).

Also include any **regular facts** referencing the same dim in the same run (Procedure A multi-fact pattern).

---

## 10. Procedure D — Hub FK to SCD2 lookup dim

When a hub FK points to an **SCD2 dimension** (e.g. `account_type_scd2`), not SCD1:

```
legacy_dim_table     = account_type_scd2
legacy_sk_col        = account_type_key
legacy_nk_cols       = account_type_code
legacy_valid_from    = effectiveStartDate
legacy_valid_to      = effectiveEndDate
new_valid_from       = effectiveStartDate
new_valid_to         = effectiveEndDate
scd_type             = 2
fact_tables          = recon_tgt.gold.account_details_scd2:account_type_key
fact_event_date_cols = effectiveStartDate
```

**All hub rows** (historic and current) re-key in **one pass** — each row uses its own `effectiveStartDate` for version resolution.

You can add other facts referencing `account_type_scd2` in the same run; provide one event-date column per table.

---

## 11. Swap and rollback

### Swap (after validation passes)

```sql
-- Fact example
ALTER TABLE recon_tgt.gold.transaction_fact       RENAME TO recon_tgt.gold.transaction_fact_broken;
ALTER TABLE recon_tgt.gold.transaction_fact_fixed RENAME TO recon_tgt.gold.transaction_fact;

-- Hub example (after each hub FK pass)
ALTER TABLE recon_tgt.gold.account_details_scd2       RENAME TO recon_tgt.gold.account_details_scd2_broken;
ALTER TABLE recon_tgt.gold.account_details_scd2_fixed RENAME TO recon_tgt.gold.account_details_scd2;
```

### Rollback (before dropping `_broken`)

Reverse the two `RENAME` statements.

### Retention

- Keep `*_broken` until business sign-off
- **Keep key-map tables permanently** — audit lineage

---

## 12. Validation and sign-off

### Required checks

| Check | Pass criterion |
|---|---|
| Row-count invariant | `count(source) = count(*_fixed)` |
| Referential integrity | Zero orphans on `*_fixed` → current dim (exclude `-1`) |
| Key-map audit | `ORPHAN_OLD` explained and accepted |
| Silent corruption spot-check | Join sample rows — correct dim attributes after swap |

### Recommended

- Measure reconciliation (template in notebook §6 comments) — per-NK totals legacy vs fixed
- Document each dimension: operator, date, key-map table name, orphan counts

### Sign-off template

```
Dimension: _______________  Date: __________  Operator: __________
Key-map:   _______________keymap
Facts fixed: ________________________________
Orphans to -1: _______  Accepted by: __________
Swap completed: [ ] Yes  Rollback tested: [ ] Yes
```

---

## 13. Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Huge `ORPHAN_OLD` | Reload dropped members or wrong NK columns | Verify NK widgets; fix dim reload |
| High `NOT_IN_LEGACY_DIM` | Fact already has new SKs | Remove fact from `fact_tables` |
| `❌ ROW COUNT CHANGED` | SCD2 overlapping windows on current dim | Fix dim validity; re-run |
| Many rows → `-1` after SCD2 repair | Event date outside validity windows | Check `fact_event_date_cols`; inspect AMBIGUOUS map |
| Wrong customer after repair | Repaired wrong dim or swapped wrong table | Rollback; verify key-map sample |
| Hub partially fixed | Only one FK pass done | Complete remaining passes; swap between each |

---

## 14. Test environment and automated tests

### Build synthetic data (practice before production)

```
00_setup_trial.py  →  02_setup_bridge_test.py  →  10_setup_sk_integrity_test_env.py (recreate=true)
  →  11_run_sk_integrity_tests.py
```

**On an existing recon workspace** (after `01`/`04`): run only **`10`** then **`11`**.  
Notebook `10` overwrites `recon_src.sales.account_dim` and patches `transaction_fact.account_key`; it does **not** touch `recon_tgt.silver`.

**Do not run `03`** for SK tests — that notebook is recon-only (ambiguous column aliases).

| Catalog.schema | Role |
|---|---|
| `recon_src.sales` | Golden source (SQL Server stand-in) |
| `recon_tgt.gold` | Broken SK state |
| `recon_tgt.staging` / `recon_tgt.keymap` | Repair outputs |

### Documents

| File | Purpose |
|---|---|
| [databricks/SK_INTEGRITY_TEST_CASES.md](databricks/SK_INTEGRITY_TEST_CASES.md) | Detailed test case catalog |
| [databricks/sk_integrity_test_guide.md](databricks/sk_integrity_test_guide.md) | Widget presets per scenario |
| [databricks/11_run_sk_integrity_tests.py](databricks/11_run_sk_integrity_tests.py) | Automated setup validation |

---

## 15. Related documents

| Document | Content |
|---|---|
| [surrogate_key_repair_method.md](surrogate_key_repair_method.md) | Key-map repair deep dive |
| [sqlserver_sk_alignment_method.md](sqlserver_sk_alignment_method.md) | Alignment deep dive |
| [databricks/SK_INTEGRITY_TEST_CASES.md](databricks/SK_INTEGRITY_TEST_CASES.md) | Test cases |
| [README.md](README.md) | Repository overview |
