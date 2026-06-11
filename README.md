# RefDataIntegrity

Databricks notebooks and runbooks for restoring **fact–dimension referential integrity** after dimension tables were reloaded with regenerated surrogate keys during a SQL Server → Databricks migration.

SQL Server remains accessible as the golden source via **Lakehouse Federation**.

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

1. Import the chosen `.py` notebook into Databricks (**Workspace → Import → File**).
2. Read the companion `.md` runbook for parameters and step-by-step procedure.
3. Run with `dry_run=true`, review audits, then set `dry_run=false` and swap after validation.

## Repository contents

```
surrogate_key_repair_notebook.py      # Databricks notebook — Databricks SK repair
surrogate_key_repair_method.md        # Runbook
sqlserver_sk_alignment_notebook.py  # Databricks notebook — SQL Server SK alignment
sqlserver_sk_alignment_method.md      # Runbook
```

## Prerequisites

- Unity Catalog–enabled Databricks workspace
- Lakehouse Federation foreign catalog to SQL Server
- Natural keys identified for each dimension (and business keys for SQL Server fact joins in the alignment path)
