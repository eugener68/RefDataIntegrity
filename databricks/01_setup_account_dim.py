# Databricks notebook source
# TITLE: Recon Generator — Account Dim Setup
#
# Adds account_dim tables to the existing recon_src / recon_tgt catalogs
# (created by 00_setup_trial.py).  Run AFTER that notebook.
#
#   recon_src.sales.account_dim                  (1 000 rows — source)
#
#   recon_tgt.silver.account_dim                 (hash-mismatch:  30 rows differ)
#   recon_tgt.silver.account_dim_count_mismatch  (count-mismatch: 950 rows — 50 missing)
#   recon_tgt.silver.account_dim_match           (match:          identical to source)
#
# ─── Prerequisites ────────────────────────────────────────────────────────────
#  • 00_setup_trial.py must have been run first (catalogs + schemas must exist)
#  • DBR 13+, Unity Catalog enabled
# ──────────────────────────────────────────────────────────────────────────────

# COMMAND ----------

# ── Step 1: Data generators ───────────────────────────────────────────────────

from datetime import date, timedelta
import numpy as np
import pandas as pd

SEED               = 42
N_DIM              = 1_000
HASH_MISMATCH_ROWS = 30    # rows that differ in the hash-mismatch target
COUNT_DROP         = 50    # rows missing from the count-mismatch target

_ACCOUNT_TYPES    = ["Checking", "Savings", "Credit", "Investment", "Mortgage"]
_ACCOUNT_STATUSES = ["Active", "Active", "Active", "Suspended", "Closed"]
_CURRENCIES       = ["USD", "USD", "USD", "EUR", "GBP", "CAD"]
_BRANCHES         = [f"BR{str(i).zfill(3)}" for i in range(1, 21)]

_rng = np.random.default_rng(SEED)


def _rand_dates(start: str, end: str, n: int):
    s, e = date.fromisoformat(start), date.fromisoformat(end)
    return [s + timedelta(days=int(d)) for d in _rng.integers(0, (e - s).days, n)]


def _account_dim_src() -> pd.DataFrame:
    n = N_DIM
    statuses   = [_ACCOUNT_STATUSES[i % len(_ACCOUNT_STATUSES)] for i in range(n)]
    credit_lim = np.where(
        _rng.integers(0, 5, n) == 0, 0.0,
        np.round(_rng.uniform(1_000, 50_000, n), 2),
    )
    return pd.DataFrame({
        "account_key":     np.arange(1, n + 1, dtype="int64"),
        "account_id":      [f"ACC{str(i).zfill(6)}" for i in range(1, n + 1)],
        "account_name":    [f"Account Holder {i}" for i in range(1, n + 1)],
        "account_type":    [_ACCOUNT_TYPES[i % len(_ACCOUNT_TYPES)] for i in range(n)],
        "account_status":  statuses,
        "customer_id":     _rng.integers(1, 501, n).astype("int32"),
        "open_date":       _rand_dates("2018-01-01", "2024-12-31", n),
        "credit_limit":    np.round(credit_lim, 2),
        "current_balance": np.round(_rng.uniform(-500, 25_000, n), 2),
        "currency_code":   [_CURRENCIES[i % len(_CURRENCIES)] for i in range(n)],
        "branch_code":     [_BRANCHES[i % len(_BRANCHES)] for i in range(n)],
        "load_date":       [date(2025, 1, 1)] * n,
    })


def _account_dim_tgt_hash_mismatch(src: pd.DataFrame) -> pd.DataFrame:
    tgt = src.copy()
    tgt.loc[50:50 + HASH_MISMATCH_ROWS - 1, "current_balance"] = -9999.99
    tgt.loc[50:50 + HASH_MISMATCH_ROWS - 1, "account_status"]  = "Unknown"
    return tgt


def _write(df: pd.DataFrame, full_table: str) -> None:
    spark.createDataFrame(df).write.format("delta").mode("overwrite").saveAsTable(full_table)
    count = spark.table(full_table).count()
    print(f"  {full_table:60s}  {count:>6,} rows")


print("Generators defined.")

# COMMAND ----------

# ── Step 2: Generate source data ──────────────────────────────────────────────

src = _account_dim_src()
print(f"Generated {len(src):,} source rows.")

# COMMAND ----------

# ── Step 3: Write source table ────────────────────────────────────────────────

print("Writing source table…")
_write(src, "recon_src.sales.account_dim")

# COMMAND ----------

# ── Step 4: Write target tables ───────────────────────────────────────────────
# Three scenarios so you can test each recon outcome independently.

print("Writing target tables…")

# Hash-mismatch: same row count, 30 rows have wrong current_balance + account_status
_write(_account_dim_tgt_hash_mismatch(src), "recon_tgt.silver.account_dim")

# Count-mismatch: 50 rows missing → count gate fires
_write(src.iloc[:N_DIM - COUNT_DROP], "recon_tgt.silver.account_dim_count_mismatch")

# Match: identical to source → recon should report all green
_write(src, "recon_tgt.silver.account_dim_match")

# COMMAND ----------

# ── Step 5: Verify ────────────────────────────────────────────────────────────

rows = []
for full_table in [
    "recon_src.sales.account_dim",
    "recon_tgt.silver.account_dim",
    "recon_tgt.silver.account_dim_count_mismatch",
    "recon_tgt.silver.account_dim_match",
]:
    cnt = spark.table(full_table).count()
    catalog, schema, table = full_table.split(".")
    rows.append((catalog, schema, table, cnt))

display(spark.createDataFrame(rows, ["catalog", "schema", "table_name", "num_rows"]))

# COMMAND ----------

# ── Step 6: Recon Generator UI settings ──────────────────────────────────────
#
# Use the values below in the Recon Generator UI to test each scenario.
#
# ┌─────────────────────────┬──────────────────────────────────────────────────────────────────┐
# │ Setting                 │ Value                                                            │
# ├─────────────────────────┼──────────────────────────────────────────────────────────────────┤
# │ Source catalog          │ recon_src                                                        │
# │ Source schema           │ sales                                                            │
# │ Source table            │ account_dim                                                      │
# │ Target catalog          │ recon_tgt                                                        │
# │ Target schema           │ silver                                                           │
# │ Target table            │ account_dim              ← hash-mismatch (30 rows differ)        │
# │                         │ account_dim_count_mismatch ← count-mismatch (950 vs 1 000 rows) │
# │                         │ account_dim_match          ← match (all green)                  │
# │ Business key column(s)  │ account_id                                                       │
# └─────────────────────────┴──────────────────────────────────────────────────────────────────┘
#
# Expected results:
#   account_dim              → passes count gate, fails hash gate (30 mismatched rows)
#   account_dim_count_mismatch → COUNT_MISMATCH | src=1,000 | tgt=950 | diff=50
#   account_dim_match          → all green

print("Setup complete. See comment above for Recon Generator UI settings.")
