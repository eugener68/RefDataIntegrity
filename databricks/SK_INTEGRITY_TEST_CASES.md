# SK Integrity — Test Case Catalog

**Companion to:** `10_setup_sk_integrity_test_env.py`, `11_run_sk_integrity_tests.py`, [RUNBOOK.md](../RUNBOOK.md)

This document defines **automated and manual test cases** for the surrogate-key repair suite. Each case maps to synthetic data in `recon_src.sales` (golden) and `recon_tgt.gold` (broken).

---

## How to run

| Step | Action |
|---|---|
| 1 | Run **`10_setup_sk_integrity_test_env.py`** (`bootstrap=auto`, `recreate=true`) |
| 2 | Run `11_run_sk_integrity_tests.py` — validates setup (Phase 1) |
| 3 | Run repair/alignment notebooks per case (Phase 2 — manual or job) |
| 4 | Re-run `11_run_sk_integrity_tests.py` with `phase=post_repair` after swaps (Phase 3) |

Scenario registry table: `recon_tgt.gold.sk_test_scenario_manifest`

---

## Test case summary

| ID | Category | Pattern | Synthetic data |
|---|---|---|---|
| TC-SETUP-001 | Setup | Environment exists | All catalogs/schemas |
| TC-DATA-001 | Data quality | Silent corruption | `customer_dim` src vs tgt SK=1 |
| TC-DATA-002 | Data quality | Mixed cohorts | `load_batch` on facts |
| TC-DATA-003 | Data quality | Pre-repair orphans | Anti-join facts/hub → dims |
| TC-REPAIR-SCD1-001 | Repair | SCD1, single fact | `customer_dim` → `transaction_fact` |
| TC-REPAIR-SCD1-002 | Repair | SCD1, multiple facts | `customer_dim` → two facts |
| TC-REPAIR-SCD1-003 | Repair | SCD1 composite NK | `product_dim` → two facts |
| TC-REPAIR-SCD1-004 | Repair | SCD1 third FK | `account_dim` → `transaction_fact.account_key` |
| TC-REPAIR-SCD2-001 | Repair | SCD2 current facts | `customer_scd2` → activity (CURRENT) |
| TC-REPAIR-SCD2-002 | Repair | SCD2 historic facts | activity (HISTORIC cohort) |
| TC-REPAIR-SCD2-003 | Repair | SCD2 AMBIGUOUS | `customer_scd2` +15d validity shift |
| TC-REPAIR-HUB-001 | Repair | Hub → SCD1 FK | `market_dim` → hub `market_key` |
| TC-REPAIR-HUB-002 | Repair | Hub → SCD2 lookup | `account_type_scd2` → hub FK |
| TC-REPAIR-HUB-003 | Repair | Hub 7-pass matrix | All hub FK columns |
| TC-REPAIR-MULTI-001 | Repair | Multi-table same dim | Two facts one key-map run |
| TC-ALIGN-001 | Alignment | Customer D-1 | SS fact join on `transaction_id` |
| TC-ALIGN-002 | Alignment | Product composite | SS fact join + composite NK |
| TC-ORPHAN-001 | Repair | ORPHAN audit | ~30 ORPHAN_OLD, ~15 ORPHAN_NEW |
| TC-POST-001 | Post-repair | Zero orphans | `*_fixed` anti-join dim |
| TC-POST-002 | Post-repair | Row-count invariant | source = fixed counts |

---

## Detailed test cases

### TC-SETUP-001 — Environment prerequisites

**Objective:** Confirm synthetic environment is ready.

**Preconditions:** Notebooks 00, 02, 10 executed.

**Automated assertions:** Tables exist; row counts > 0; scenario manifest present.

**Expected:** All assertions pass.

---

### TC-DATA-001 — Silent corruption

**Objective:** Prove broken state mimics production silent wrong-join.

**Data:** `customer_key = 1` maps to different `customer_id` in src vs tgt.

**Manual check:**
```sql
SELECT s.customer_id src_id, t.customer_id tgt_id
FROM recon_src.sales.customer_dim s
JOIN recon_tgt.gold.customer_dim t ON s.customer_key = t.customer_key
WHERE s.customer_key = 1;
```

**Expected:** Same SK, different `customer_id`.

---

### TC-DATA-002 — Mixed fact cohorts

**Data:** `load_batch` ∈ `{INITIAL_SS, DB_INCREMENTAL, INITIAL_SS_ORPHAN}` on `transaction_fact`.

**Expected:** All three values present.

---

### TC-DATA-003 — Pre-repair orphans

**Expected:** Anti-join fact/hub → dim counts > 0 before repair.

---

### TC-REPAIR-SCD1-001 — SCD1 single fact

**Widgets:**
```
fact_tables = recon_tgt.gold.transaction_fact:customer_key
(scd_type=1, customer_dim widgets — see sk_integrity_test_guide Test 1)
```

**Expected post-repair:** Zero orphans; row count unchanged.

---

### TC-REPAIR-SCD1-002 — SCD1 multiple facts

**Widgets:**
```
fact_tables = recon_tgt.gold.transaction_fact:customer_key,recon_tgt.gold.return_fact:customer_key
```

**Expected:** Both `*_fixed` tables pass post checks in one run.

---

### TC-REPAIR-SCD1-004 — Third FK on transaction_fact

**Provided by:** `10_setup` §1b (replaces notebook `03` for SK tests).

**Data:**
- `recon_src.sales.account_dim` — 121 rows (120 + unknown), NK = `account_id` (`AID0001`…)
- `transaction_fact.account_key` always populated on golden and broken copies

**Widgets:** See sk_integrity_test_guide Test 4.

**Expected post-repair:** Zero orphans on `transaction_fact.account_key` and hub `account_key`.

---

### TC-REPAIR-SCD1-003 — Composite natural key

**Widgets:** `legacy_nk_cols=product_code,source_system` (both sides).

---

### TC-REPAIR-SCD2-001 — SCD2 current activity facts

**Widgets:** `customer_scd2`, `scd_type=2`, `fact_event_date_cols=event_date`.

**Data:** `activity_cohort='CURRENT'`.

---

### TC-REPAIR-SCD2-002 — SCD2 historic activity facts

**Data:** `activity_cohort='HISTORIC'`; `event_date` within historic validity window.

**Expected:** Historic facts map to historic dim version, not current.

---

### TC-REPAIR-HUB-002 — Hub SCD2 lookup FK

**Widgets:**
```
legacy_dim_table=account_type_scd2
scd_type=2
fact_tables=recon_tgt.gold.account_details_scd2:account_type_key
fact_event_date_cols=effectiveStartDate
```

**Expected:** Hub re-keyed without dim NK on hub rows.

---

### TC-REPAIR-HUB-003 — Full hub matrix

Seven passes — see [sk_integrity_test_guide.md](sk_integrity_test_guide.md). Pass 3 uses SCD2 widgets for `account_type_scd2`.

---

### TC-ALIGN-001 — SQL Server alignment

See sk_integrity_test_guide Test 5. Expect high `MATCHED_SS_FACT` through D-1.

---

### TC-ORPHAN-001 — ORPHAN audit

Dry-run on `customer_dim`. Expect ORPHAN_OLD ≈ 30, ORPHAN_NEW ≈ 15.

---

### TC-POST-001 / TC-POST-002

Run `11_run_sk_integrity_tests.py` with `phase=post_repair` after creating `*_fixed` tables.

---

## Regression gate

- [ ] `10_setup` completes
- [ ] `11` Phase 1 all pass
- [ ] TC-REPAIR-SCD1-002 dry-run audit reviewed
- [ ] TC-REPAIR-HUB-002 dry-run builds SCD2 key-map
