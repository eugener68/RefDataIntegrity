# SK Integrity Test Guide

**Test environment for surrogate-key repair notebooks — built on Recon catalogs**

Companion to `10_setup_sk_integrity_test_env.py`. **Operators:** see [RUNBOOK.md](../RUNBOOK.md). **Test cases:** [SK_INTEGRITY_TEST_CASES.md](SK_INTEGRITY_TEST_CASES.md).

---

## Overview

This setup **reuses the Recon Generator catalogs** already in your workspace. It does
**not** replace `recon_tgt.silver` (hash-mismatch recon tests continue to work).

| Catalog.schema | Role |
|---|---|
| `recon_src.sales` | **Golden source** (SQL Server / federation stand-in) |
| `recon_tgt.silver` | Unchanged — Recon Generator hash/count tests |
| `recon_tgt.gold` | **Broken SK state** — reloaded dims + mixed-cohort facts |
| `recon_tgt.staging` / `recon_tgt.keymap` | Repair notebook working tables |

### Prerequisites

Run these notebooks **first** (in order):

| Notebook | Provides |
|---|---|
| `00_setup_trial.py` | `customer_dim`, `transaction_fact`, `customer_scd2` |
| `02_setup_bridge_test.py` | `product_dim` |
| `10_setup_sk_integrity_test_env.py` | Broken gold, `account_dim`, `account_key` on facts, hub, all SK scenarios |

**SK path:** `00` → `02` → `10` → `11` — **no `03` required.**

**Existing recon DB** (after `01`/`04`): run **`10`** + **`11`** only.

**Recon + SK:** `00` → `01` → `02` → `04` → `10` → `11` — skip `03` unless testing recon table aliases.

---

## Data model

### SCD2 column standard (all SCD2 tables)

| Column | Values | Notes |
|---|---|---|
| `effectiveStartDate` | date | Validity start |
| `effectiveEndDate` | date | Open-ended current row → `2999-12-31` |
| `recordStatus` | `C` / `H` | **C** = current, **H** = historic |

Repair notebook widgets map these to `legacy_valid_from` / `legacy_valid_to` (and the `new_*` pair).

### Golden — `recon_src.sales` (existing + 2 new facts)

| Table | Rows (typ.) | Notes |
|---|---|---|
| `customer_dim` | 1,000 | NK = `customer_id`, SK = `customer_key` |
| `product_dim` | 50 | NK = `product_code` (+ `source_system='ERP'` added at setup) |
| `customer_scd2` | 1,000 | SCD2: `effectiveStartDate`, `effectiveEndDate`, `recordStatus` |
| `account_dim` | 121 | SK fixture (120 + unknown); NK = `account_id` (`AID0001`…) — **written by `10_setup`** |
| `transaction_fact` | 1,000 | FKs: `customer_key`, `product_key`, **`account_key`** |
| `return_fact` | **new** ~200 | FKs: `customer_key`, `product_key` |
| `scd2_activity_fact` | **new** ~400 | FK: `surrogate_key` → SCD2 customer dim |
| **`account_details_scd2`** | **new** ~240 | **SCD2 hub** — 7 FKs → 7 separate lookup dims |
| `subscriber_dim`, `market_dim`, … | **new** | SCD1 lookup dims (see below) |

**Lookup dimensions:** SCD1 — `subscriber_dim`, `market_dim`, `autopay_dim`, `billing_product_dim`, `payment_method_dim`, `account_dim`. **SCD2 lookup on hub:** `account_type_scd2`. Standalone SCD1 `account_type_dim` also exists for isolated tests.

Facts and dims are **aligned on SK** in `recon_src` (correct golden state).

### Multi-FK SCD2 hub — `account_details_scd2` (your real-world pattern)

This is **not** role-playing (two FKs → same dim). It is one **SCD2 entity table** with
many FK columns, each pointing to a **different** reference dimension — like
`accountDetails` with `keyAccount`, `keySubscriber`, `keyMarket`, `keyAutoPay`, etc.

```
account_details_scd2 (SCD2 hub)          Reference dims
─────────────────────────────            ────────────────────────
detail_sk                                account_dim          ← account_key (SCD1)
account_number (NK)                      subscriber_dim       ← subscriber_key (SCD1)
effectiveStartDate / effectiveEndDate    account_type_scd2    ← account_type_key (SCD2)
recordStatus                             market_dim           ← market_key (SCD1)
current_balance, credit_score            autopay_dim          ← autopay_key (SCD1)
                                         billing_product_dim  ← billing_product_key (SCD1)
                                         payment_method_dim   ← payment_method_key (SCD1)
```

Reports/cubes join the hub to each dim on its FK to denormalize natural columns.

### How repair notebooks cover this

**One notebook run = one dimension = one FK column on the hub.**

The repair notebook re-keys a **single FK** per pass (`fact_tables` = `…account_details_scd2:market_key`).
It does **not** re-key all FKs at once — that matches how you fix production: reload/fix
`market_dim`, re-key `market_key` on every table that references it (including the SCD2 hub).

**Workflow for the full hub:**

1. Run repair for `market_dim` → `fact_tables = recon_tgt.gold.account_details_scd2:market_key` (+ any other facts referencing market)
2. Validate, **swap** `account_details_scd2_fixed` → `account_details_scd2`
3. Run repair for `subscriber_dim` → `…account_details_scd2:subscriber_key`
4. Swap again. Repeat for each lookup dim (7 passes for 7 FKs).

Each pass leaves **other FK columns unchanged** — only the column for the dimension being repaired is updated.

**SCD2 note:** Lookup dims are SCD1 → repair uses `scd_type=1`, no event-date column needed on the hub for those FKs. If an FK pointed to another **SCD2** dim, set `scd_type=2` and use the hub’s business date (`effectiveStartDate` or a transaction date) as `fact_event_date_cols`.

### Broken — `recon_tgt.gold` (created by setup)

| Table | Mirrors | Corruption |
|---|---|---|
| `customer_dim` | `recon_src.sales.customer_dim` | Regenerated SKs; 30 ORPHAN_OLD, 15 ORPHAN_NEW |
| `product_dim` | `product_dim` | Regenerated SKs (reverse-sorted NK) |
| `customer_scd2` | `customer_scd2` | Regenerated SKs; validity +15 days → `AMBIGUOUS` |
| `account_dim` | `account_dim` | Regenerated SKs (from §1b fixture) |
| `transaction_fact` | `transaction_fact` | Mixed `load_batch` cohorts |
| `return_fact` | `return_fact` | Mixed cohorts |
| `scd2_activity_fact` | `scd2_activity_fact` | All `INITIAL_SS` |
| **`account_details_scd2`** | `account_details_scd2` | Mixed cohorts on **all 7 FK columns** |
| `subscriber_dim`, `market_dim`, … | lookup dims | Regenerated SKs each |

### Fact cohorts (`load_batch` on broken facts)

| Value | FK source | Simulates |
|---|---|---|
| `INITIAL_SS` | Golden `recon_src` SK | Bulk SQL Server migration load |
| `DB_INCREMENTAL` | Broken dim lookup SK | Databricks incremental from external sources |
| `INITIAL_SS_ORPHAN` | SS SK for dropped dim members | Broken RI |

~5% of broken-fact rows get `transaction_date` / `return_date` = **today** (D+0 alignment tests). Golden `recon_src` dates are **not** modified.

---

## Built-in scenarios

| Scenario | How to test | Expected signal |
|---|---|---|
| Silent wrong join | Join `recon_tgt.gold.transaction_fact` → `customer_dim` on SK | Wrong customer name, join succeeds |
| Repair SCD1 customer | Repair notebook on `customer_dim` | `ORPHAN_OLD` ~30, `ORPHAN_NEW` ~15 |
| Repair two facts | `transaction_fact` + `return_fact` | Per-fact audit in stage 4 |
| Repair product (composite NK) | `product_code,source_system` | Key-map on product |
| Repair SCD2 | `customer_scd2` + `scd2_activity_fact` | `AMBIGUOUS` in key-map |
| **Multi-FK SCD2 hub** | Repair matrix § below — one dim per pass | Orphans on each hub FK pre-repair |
| SQL Server alignment | Alignment notebook + SS fact join | High `MATCHED_SS_FACT` |
| Orphans pre-repair | Anti-join fact/hub → dim | Count > 0 |
| Post-repair RI | Validation stage | Orphans = 0 |

---

## Widget presets — Key-map repair

### Test 1: `customer_dim` → two facts (SCD1)

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
| `unknown_member_sk` | `-1` |
| `dry_run` | `true` |

### Test 2: `product_dim` (composite NK)

| Widget | Value |
|---|---|
| `legacy_dim_table` | `product_dim` |
| `legacy_sk_col` | `product_key` |
| `legacy_nk_cols` | `product_code,source_system` |
| `new_dim_table` | `product_dim` |
| `new_sk_col` | `product_key` |
| `new_nk_cols` | `product_code,source_system` |
| `fact_tables` | `recon_tgt.gold.transaction_fact:product_key,recon_tgt.gold.return_fact:product_key` |

### Test 3: `customer_scd2` (SCD2)

| Widget | Value |
|---|---|
| `legacy_dim_table` | `customer_scd2` |
| `legacy_sk_col` | `surrogate_key` |
| `legacy_nk_cols` | `customer_id` |
| `legacy_valid_from` | `effectiveStartDate` |
| `legacy_valid_to` | `effectiveEndDate` |
| `new_dim_table` | `customer_scd2` |
| `new_sk_col` | `surrogate_key` |
| `new_nk_cols` | `customer_id` |
| `new_valid_from` | `effectiveStartDate` |
| `new_valid_to` | `effectiveEndDate` |
| `scd_type` | `2` |
| `fact_tables` | `recon_tgt.gold.scd2_activity_fact:surrogate_key` |
| `fact_event_date_cols` | `event_date` |

### Test 4: `account_dim` → fact + hub (third FK — TC-REPAIR-SCD1-004)

Provided by `10_setup` §1b. NK = `account_id` (string codes `AID0001`…, not notebook `01`/`03` formats).

| Widget | Value |
|---|---|
| `legacy_dim_table` | `account_dim` |
| `legacy_sk_col` | `account_key` |
| `legacy_nk_cols` | `account_id` |
| `new_dim_table` | `account_dim` |
| `new_sk_col` | `account_key` |
| `new_nk_cols` | `account_id` |
| `scd_type` | `1` |
| `fact_tables` | `recon_tgt.gold.transaction_fact:account_key,recon_tgt.gold.account_details_scd2:account_key` |

---

## Multi-FK SCD2 hub — repair matrix (`account_details_scd2`)

Run **once per row**. Swap `account_details_scd2` after each successful pass before the next dim.

| Pass | `legacy_dim_table` | `legacy_sk_col` / `new_sk_col` | `legacy_nk_cols` / `new_nk_cols` | `fact_tables` (FK to re-key) |
|---|---|---|---|---|
| 1 | `account_dim` | `account_key` | `account_id` | `recon_tgt.gold.account_details_scd2:account_key` |
| 2 | `subscriber_dim` | `subscriber_key` | `subscriber_id` | `recon_tgt.gold.account_details_scd2:subscriber_key` |
| 3 | `account_type_scd2` | `account_type_key` | `account_type_code` | `…account_details_scd2:account_type_key` |
| 4 | `market_dim` | `market_key` | `market_code` | `…account_details_scd2:market_key` |
| 5 | `autopay_dim` | `autopay_key` | `autopay_code` | `…account_details_scd2:autopay_key` |
| 6 | `billing_product_dim` | `billing_product_key` | `billing_product_code` | `…account_details_scd2:billing_product_key` |
| 7 | `payment_method_dim` | `payment_method_key` | `payment_method_code` | `…account_details_scd2:payment_method_key` |

Shared widgets for every pass: `foreign_catalog=recon_src`, `legacy_schema=sales`, `new_catalog_schema=recon_tgt.gold`, `staging_schema=recon_tgt.staging`, `keymap_schema=recon_tgt.keymap`, `dry_run=true` first.

**Pass 3 (`account_type_scd2`) — SCD2 lookup:** set `scd_type=2`, validity widgets = `effectiveStartDate` / `effectiveEndDate`, `fact_event_date_cols=effectiveStartDate`. All other hub passes use `scd_type=1`.

**Alignment notebook:** use `recon_src.sales.account_details_scd2` as the legacy fact; business key = `detail_sk`; set `fact_event_date_cols=effectiveStartDate` if you need date-based cutoff on the hub rows.

---

## Widget presets — SQL Server SK alignment

### Test 5: Align `customer_dim` + `transaction_fact`

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
| `legacy_fact_tables` | `transaction_fact,return_fact` |
| `legacy_fact_fk_cols` | `customer_key,customer_key` |
| `legacy_fact_bk_cols` | `transaction_id,return_id` |
| `new_fact_bk_cols` | `transaction_id,return_id` |
| `fact_event_date_cols` | `transaction_date,return_date` |
| `fact_nk_cols` | `customer_id,customer_id` |
| `cutoff_mode` | `d_minus_1` |
| `staging_schema` | `recon_tgt.staging` |
| `keymap_schema` | `recon_tgt.keymap` |
| `dry_run` | `true` |

### Test 6: Align `product_dim` + both facts

Same as Test 5 but product widgets; `legacy_fact_fk_cols` = `product_key,product_key`;
`fact_nk_cols` = `product_code;source_system,product_code;source_system`.

---

## Recommended sequence

```
00 → 02 → 10 (recreate=true) → 11 (phase=setup)
  → repair/alignment notebook (dry_run=true) → dry_run=false → swap
  → 11 (phase=post_repair)
```

Re-run `10` with `recreate=true` to reset the broken baseline between test cycles.

---

## Related docs

- [Surrogate Key Repair method](../surrogate_key_repair_method.md)
- [SQL Server SK Alignment method](../sqlserver_sk_alignment_method.md)
- Recon setup: `00_setup_trial.py` … `04_setup_resolver_stress_test.py`
