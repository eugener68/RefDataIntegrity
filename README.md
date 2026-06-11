# RefDataIntegrity

Databricks notebooks and runbooks for restoring **fact–dimension referential integrity** after dimension tables were reloaded with regenerated surrogate keys during a SQL Server → Databricks migration.

SQL Server remains accessible as the golden source via **Lakehouse Federation**.

## Start here — team runbook

**[RUNBOOK.md](RUNBOOK.md)** — unified step-by-step instructions for operators (both notebooks, hub patterns, swap/rollback, troubleshooting).

## Two approaches

| Approach | Notebook | Documentation | Use when |
|---|---|---|---|
| **Key-map repair** | [`surrogate_key_repair_notebook.py`](surrogate_key_repair_notebook.py) | [`surrogate_key_repair_method.md`](surrogate_key_repair_method.md) | Target end-state is **reloaded Databricks SKs**; re-key facts via natural key |
| **SQL Server SK alignment** | [`sqlserver_sk_alignment_notebook.py`](sqlserver_sk_alignment_notebook.py) | [`sqlserver_sk_alignment_method.md`](sqlserver_sk_alignment_method.md) | Target end-state is **SQL Server SK values**; SQL Server is current through D-1; mixed fact cohorts |

See **§0 — Choosing the Right Notebook** in either method doc for the full comparison.

## Safety model (both notebooks)

- **Dry run by default** — audits and key-maps first, no `*_fixed` tables until you opt in
- **Rebuild, never in-place** — overlapping SK ranges make `UPDATE`/`MERGE` dangerous
- **Manual swap** — `ALTER TABLE ... RENAME` is deliberately left to the operator
- **Keep key-map tables** — permanent lineage and audit evidence

## Quick start

1. Import notebooks into Databricks (**Workspace → Import → File**).
2. Read **[RUNBOOK.md](RUNBOOK.md)** for step-by-step procedure.
3. Practice on synthetic data: run `databricks/10_setup_sk_integrity_test_env.py`, then `11_run_sk_integrity_tests.py`.
4. Run repair with `dry_run=true`, review audits, then `dry_run=false` and swap after validation.

## Repository contents

```
RUNBOOK.md                            # Unified team runbook (start here)
surrogate_key_repair_notebook.py      # Databricks notebook — Databricks SK repair
surrogate_key_repair_method.md        # Repair deep dive
sqlserver_sk_alignment_notebook.py    # Databricks notebook — SQL Server SK alignment
sqlserver_sk_alignment_method.md      # Alignment deep dive
databricks/
  10_setup_sk_integrity_test_env.py   # Synthetic broken + golden data (all scenarios)
  11_run_sk_integrity_tests.py       # Automated test runner
  SK_INTEGRITY_TEST_CASES.md          # Test case catalog
  sk_integrity_test_guide.md          # Widget presets per scenario
```

## Prerequisites

- Unity Catalog–enabled Databricks workspace
- Lakehouse Federation foreign catalog to SQL Server (or use the synthetic test environment below)
- Natural keys identified for each dimension (and business keys for SQL Server fact joins in the alignment path)

## Test environment (synthetic)

Built on the **Recon Generator catalogs** (`recon_src` / `recon_tgt`) already in your workspace:

| Notebook | Purpose |
|---|---|
| `databricks/00_setup_trial.py` … `04_setup_resolver_stress_test.py` | Recon Generator base data (`recon_src.sales`, `recon_tgt.silver`) |
| [`databricks/10_setup_sk_integrity_test_env.py`](databricks/10_setup_sk_integrity_test_env.py) | All scenarios: multi-fact, SCD2, hub, SCD2 lookup |
| [`databricks/11_run_sk_integrity_tests.py`](databricks/11_run_sk_integrity_tests.py) | Automated setup/post-repair tests |
| [`databricks/SK_INTEGRITY_TEST_CASES.md`](databricks/SK_INTEGRITY_TEST_CASES.md) | Detailed test case definitions |
| [`databricks/sk_integrity_test_guide.md`](databricks/sk_integrity_test_guide.md) | Widget presets |
| [`RUNBOOK.md`](RUNBOOK.md) | Operator runbook |

**Scenarios in synthetic data:** SCD1 single/multi-fact, composite NK, SCD2 facts (current + historic), SCD2 hub multi-FK, hub → SCD2 lookup (`account_type_scd2`), mixed cohorts, orphans, silent corruption.

**Golden source:** `recon_src.sales`. **Broken state:** `recon_tgt.gold` — does **not** touch `recon_tgt.silver`.
