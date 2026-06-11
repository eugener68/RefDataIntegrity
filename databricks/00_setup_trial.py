# Databricks notebook source
# TITLE: Recon Generator — Trial Setup
#
# Creates two Unity Catalog catalogs with synthetic tables that mirror the
# three table types supported by the Recon Generator:
#
#   recon_src.sales.customer_dim        (dimension, 1 000 rows)
#   recon_src.sales.transaction_fact    (fact,      1 000 rows)
#   recon_src.sales.customer_scd2       (type-2 SCD, 1 000 rows)
#
#   recon_tgt.silver.customer_dim       (hash-mismatch: 30 rows differ)
#   recon_tgt.silver.transaction_fact   (hash-mismatch: 30 rows differ)
#   recon_tgt.silver.customer_scd2      (hash-mismatch: 30 current rows differ)
#
# ─── Prerequisites ────────────────────────────────────────────────────────────
#  • Run on a cluster with DBR 13+ (Unity Catalog enabled)
#  • Your user must be metastore admin OR have CREATE CATALOG privilege
#  • Optional if you use 10_setup_sk_integrity_test_env.py (bootstrap=auto) for SK + recon base
#  • No external packages needed — only pandas + pyspark (both pre-installed)
# ──────────────────────────────────────────────────────────────────────────────

# COMMAND ----------

# ── Step 1: Create catalogs and schemas ───────────────────────────────────────

spark.sql("CREATE CATALOG IF NOT EXISTS recon_src COMMENT 'Simulates source catalog (Lakehouse Federation stand-in)'")
spark.sql("CREATE CATALOG IF NOT EXISTS recon_tgt COMMENT 'Target Unity Catalog (native Delta)'")

spark.sql("CREATE SCHEMA IF NOT EXISTS recon_src.sales   COMMENT 'Source sales schema'")
spark.sql("CREATE SCHEMA IF NOT EXISTS recon_tgt.silver  COMMENT 'Target silver schema'")

print("Catalogs and schemas ready.")

# COMMAND ----------

# ── Step 2: Synthetic data generators (mirrors tests/synthetic/_generate.py) ──

from datetime import date, datetime, timedelta
import numpy as np
import pandas as pd

SEED             = 42
N_DIM            = 1_000
N_FACT           = 1_000
N_SCD2_CURRENT   = 300
N_SCD2_HIST      = 700
HASH_MISMATCH    = 30   # rows that differ in the target (hash-mismatch scenario)
COUNT_DROP       = 50   # rows missing from target (count-mismatch scenario)

_CITIES   = ["New York","Los Angeles","Chicago","Houston","Phoenix",
             "Philadelphia","San Antonio","San Diego","Dallas","San Jose"]
_STATES   = ["NY","CA","IL","TX","AZ","PA","TX","CA","TX","CA"]
_SEGMENTS = ["Bronze","Silver","Gold","Platinum"]

_rng = np.random.default_rng(SEED)


def _rand_dates(start: str, end: str, n: int):
    s, e = date.fromisoformat(start), date.fromisoformat(end)
    return [s + timedelta(days=int(d)) for d in _rng.integers(0, (e - s).days, n)]


def _rand_timestamps(n: int):
    base = datetime(2025, 1, 1)
    offsets = [timedelta(seconds=int(s)) for s in _rng.integers(0, 86_400 * 365, n)]
    return pd.to_datetime([base + o for o in offsets]).astype("datetime64[us]")


# ── customer_dim ──────────────────────────────────────────────────────────────

def _dim_src() -> pd.DataFrame:
    n = N_DIM
    ci = _rng.integers(0, len(_CITIES), n)
    return pd.DataFrame({
        "customer_key":  np.arange(1, n + 1, dtype="int64"),
        "customer_id":   np.arange(1, n + 1, dtype="int32"),
        "customer_name": [f"Customer {i}" for i in range(1, n + 1)],
        "email":         [f"customer{i}@example.com" for i in range(1, n + 1)],
        "city":          [_CITIES[i] for i in ci],
        "state_code":    [_STATES[i]  for i in ci],
        "segment":       [_SEGMENTS[i % len(_SEGMENTS)] for i in range(n)],
        "created_date":  _rand_dates("2020-01-01", "2024-12-31", n),
        "is_active":     [True] * n,
    })


def _dim_tgt_hash_mismatch(src: pd.DataFrame) -> pd.DataFrame:
    tgt = src.copy()
    tgt.loc[100:100 + HASH_MISMATCH - 1, "city"]    = "Boston"
    tgt.loc[100:100 + HASH_MISMATCH - 1, "segment"] = "Unknown"
    return tgt


# ── transaction_fact ──────────────────────────────────────────────────────────

def _fact_src() -> pd.DataFrame:
    n = N_FACT
    qty  = _rng.integers(1, 21, n).astype("int32")
    unit = np.round(_rng.uniform(5.0, 500.0, n), 2)
    disc = np.round(_rng.uniform(0.0, 50.0, n), 2)
    return pd.DataFrame({
        "transaction_id":     np.arange(1, n + 1, dtype="int64"),
        "customer_key":       _rng.integers(1, 201, n).astype("int64"),
        "product_key":        _rng.integers(1, 51, n).astype("int32"),
        "transaction_date":   _rand_dates("2024-01-01", "2024-12-31", n),
        "transaction_amount": np.round(unit * qty - disc, 2),
        "quantity":           qty,
        "unit_price":         unit,
        "discount_amount":    disc,
        "net_amount":         np.round(unit * qty - disc, 2),
        "load_ts":            _rand_timestamps(n),
    })


def _fact_tgt_hash_mismatch(src: pd.DataFrame) -> pd.DataFrame:
    tgt = src.copy()
    tgt.loc[200:200 + HASH_MISMATCH - 1, "transaction_amount"] = 0.01
    tgt.loc[200:200 + HASH_MISMATCH - 1, "net_amount"]         = 0.01
    return tgt


# ── customer_scd2 ─────────────────────────────────────────────────────────────

def _scd2_src() -> pd.DataFrame:
    nc, nh = N_SCD2_CURRENT, N_SCD2_HIST

    ci_c = _rng.integers(0, len(_CITIES), nc)
    current = pd.DataFrame({
        "surrogate_key":  np.arange(1, nc + 1, dtype="int64"),
        "customer_id":    np.arange(1, nc + 1, dtype="int32"),
        "customer_name":  [f"Customer {i}" for i in range(1, nc + 1)],
        "email":          [f"customer{i}@example.com" for i in range(1, nc + 1)],
        "city":           [_CITIES[i] for i in ci_c],
        "state_code":     [_STATES[i]  for i in ci_c],
        "segment":        [_SEGMENTS[i % len(_SEGMENTS)] for i in range(nc)],
        "recordStatus":       ["C"] * nc,
        "effectiveStartDate": [date(2024, 1, 1)] * nc,
        "effectiveEndDate":   [date(2999, 12, 31)] * nc,
        "load_ts":        _rand_timestamps(nc),
    })

    ci_h = _rng.integers(0, len(_CITIES), nh)
    cids = _rng.integers(1, nc + 1, nh).astype("int32")
    historical = pd.DataFrame({
        "surrogate_key":  np.arange(nc + 1, nc + nh + 1, dtype="int64"),
        "customer_id":    cids,
        "customer_name":  [f"Customer {i}" for i in cids],
        "email":          [f"customer{i}.old@example.com" for i in cids],
        "city":           [_CITIES[i] for i in ci_h],
        "state_code":     [_STATES[i]  for i in ci_h],
        "segment":        [_SEGMENTS[i % len(_SEGMENTS)] for i in range(nh)],
        "recordStatus":       ["H"] * nh,
        "effectiveStartDate": [date(2022, 1, 1)] * nh,
        "effectiveEndDate":   [date(2023, 12, 31)] * nh,
        "load_ts":        _rand_timestamps(nh),
    })

    return pd.concat([current, historical], ignore_index=True)


def _scd2_tgt_hash_mismatch(src: pd.DataFrame) -> pd.DataFrame:
    tgt = src.copy()
    curr_idx = tgt.index[tgt["recordStatus"] == "C"][:HASH_MISMATCH]
    tgt.loc[curr_idx, "city"]    = "Boston"
    tgt.loc[curr_idx, "segment"] = "Unknown"
    return tgt


print("Data generators defined.")

# COMMAND ----------

# ── Step 3: Write source tables ───────────────────────────────────────────────
# These are the "source of truth" — both catalogs start identical
# except for the intentional mismatches we inject into the target below.

dim_src  = _dim_src()
fact_src = _fact_src()
scd2_src = _scd2_src()

def _write(df: pd.DataFrame, full_table: str, overwrite: bool = True) -> None:
    mode = "overwrite" if overwrite else "errorIfExists"
    spark.createDataFrame(df).write.format("delta").mode(mode).saveAsTable(full_table)
    count = spark.table(full_table).count()
    print(f"  {full_table:50s}  {count:>6,} rows")

print("Writing source tables…")
_write(dim_src,  "recon_src.sales.customer_dim")
_write(fact_src, "recon_src.sales.transaction_fact")
_write(scd2_src, "recon_src.sales.customer_scd2")

# COMMAND ----------

# ── Step 4: Write target tables (hash-mismatch scenario) ──────────────────────
# Default scenario: same row count, but 30 rows differ in attribute values.
# The recon script should pass the count gate and fail the hash gate.

print("Writing target tables (hash-mismatch scenario)…")
_write(_dim_tgt_hash_mismatch(dim_src),   "recon_tgt.silver.customer_dim")
_write(_fact_tgt_hash_mismatch(fact_src), "recon_tgt.silver.transaction_fact")
_write(_scd2_tgt_hash_mismatch(scd2_src), "recon_tgt.silver.customer_scd2")

# COMMAND ----------

# ── Step 5 (optional): Create count-mismatch and match variants ───────────────
# Uncomment to create extra tables for testing the other two scenarios.
# Swap these into the Recon Generator UI table name to test each case.

# COUNT-MISMATCH targets (50 fewer rows → count gate fires)
# _write(dim_src.iloc[:N_DIM  - COUNT_DROP],  "recon_tgt.silver.customer_dim_count_mismatch")
# _write(fact_src.iloc[:N_FACT - COUNT_DROP], "recon_tgt.silver.transaction_fact_count_mismatch")
# current_only = scd2_src[scd2_src["recordStatus"] == "C"]
# hist_only    = scd2_src[scd2_src["recordStatus"] == "H"]
# _write(pd.concat([current_only.iloc[:N_SCD2_CURRENT - COUNT_DROP], hist_only], ignore_index=True),
#        "recon_tgt.silver.customer_scd2_count_mismatch")

# MATCH targets (identical to source → recon should report all green)
# _write(dim_src,  "recon_tgt.silver.customer_dim_match")
# _write(fact_src, "recon_tgt.silver.transaction_fact_match")
# _write(scd2_src, "recon_tgt.silver.customer_scd2_match")

print("Done — optional tables commented out above.")

# COMMAND ----------

# ── Step 6: Verify ────────────────────────────────────────────────────────────

from pyspark.sql import functions as F

rows = []
for full_table in [
    "recon_src.sales.customer_dim",
    "recon_src.sales.transaction_fact",
    "recon_src.sales.customer_scd2",
    "recon_tgt.silver.customer_dim",
    "recon_tgt.silver.transaction_fact",
    "recon_tgt.silver.customer_scd2",
]:
    cnt = spark.table(full_table).count()
    catalog, schema, table = full_table.split(".")
    rows.append((catalog, schema, table, cnt))

display(spark.createDataFrame(rows, ["catalog", "schema", "table_name", "num_rows"]))

# COMMAND ----------

# ── Step 7: Print connection info for Recon Generator UI ─────────────────────
# Copy these values into the Recon Generator connection panels.

import subprocess, re

workspace_url = spark.conf.get("spark.databricks.workspaceUrl", "")
# HTTP path is visible under:  Compute → SQL Warehouses → <your warehouse> → Connection Details
print(f"""
╔══════════════════════════════════════════════════════════════════════════╗
║           Recon Generator — Connection Details                          ║
╠══════════════════════════════════════════════════════════════════════════╣
║  Workspace URL  : {workspace_url:<54s}║
║  System         : Databricks                                            ║
║  HTTP Path      : (SQL Warehouse → Connection Details → HTTP path)      ║
║  Auth           : Personal Access Token                                 ║
║  Token          : (User Settings → Developer → Access Tokens → New)    ║
╠══════════════════════════════════════════════════════════════════════════╣
║  SOURCE catalog : recon_src      schema: sales                          ║
║  TARGET catalog : recon_tgt      schema: silver                         ║
╚══════════════════════════════════════════════════════════════════════════╝
""")
