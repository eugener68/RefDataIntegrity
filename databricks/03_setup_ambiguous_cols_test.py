# Databricks notebook source
# TITLE: Recon Generator — Ambiguous Column Test Setup
#
# Creates lookup tables with column names that deliberately clash with each
# other and with transaction_fact, to test the new "Table alias" feature in
# the Lookup Table panel.
#
# Ambiguity matrix
# ─────────────────────────────────────────────────────────────────────
#  column       transaction_fact  customer_dim  product_dim  account_dim
#  load_ts            ✓               ✓             ✓            ✓    ← same name in all 4
#  is_active          –               ✓             ✓            ✓    ← clash across 3 lookups
#  record_type        –               ✓             ✓            ✓    ← clash across 3 lookups
# ─────────────────────────────────────────────────────────────────────
#
# Tables created / overwritten
# ─────────────────────────────────────────────────────────────────────
#  recon_src.sales.transaction_fact   — adds account_key surrogate
#  recon_tgt.silver.transaction_fact  — same + 30 rows with wrong amounts
#
#  recon_src.sales.customer_dim       — adds load_ts, is_active, record_type
#  recon_tgt.silver.customer_dim      — 15 rows differ in city + load_ts
#
#  recon_src.sales.product_dim        — adds load_ts, is_active, record_type
#  recon_tgt.silver.product_dim       — 10 rows differ in category + record_type
#
#  recon_src.sales.account_dim        — new; account_key, load_ts, is_active, record_type
#  recon_tgt.silver.account_dim       — 5 rows differ in account_type
#
# ─── Prerequisites ────────────────────────────────────────────────────────────
#  • 00_setup_trial.py must have run first (catalogs + schemas exist)
#  • DBR 13+, Unity Catalog enabled
# ──────────────────────────────────────────────────────────────────────────────

# COMMAND ----------

# ── Step 1: Shared constants and helpers ──────────────────────────────────────

from datetime import date, datetime, timedelta
import numpy as np
import pandas as pd

SEED          = 42
N_FACT        = 1_000
N_DIM         = 1_000
N_PRODUCTS    = 50
N_ACCOUNTS    = 20      # account_key range [1, 20] in transaction_fact

_rng = np.random.default_rng(SEED)

_CITIES   = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
             "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose"]
_STATES   = ["NY", "CA", "IL", "TX", "AZ", "PA", "TX", "CA", "TX", "CA"]
_SEGMENTS = ["Bronze", "Silver", "Gold", "Platinum"]
_CATEGORIES = ["Electronics", "Clothing", "Food & Beverage", "Home & Garden", "Sports"]
_BRANDS     = ["AlphaBrand", "BetaCo", "GammaTech", "DeltaWorks", "EpsilonGoods"]
_ACCT_TYPES = ["Corporate", "Retail", "Government", "Non-Profit"]


def _rand_dates(start: str, end: str, n: int):
    s, e = date.fromisoformat(start), date.fromisoformat(end)
    return [s + timedelta(days=int(d)) for d in _rng.integers(0, (e - s).days, n)]


def _rand_timestamps(n: int):
    base = datetime(2025, 1, 1)
    offsets = [timedelta(seconds=int(s)) for s in _rng.integers(0, 86_400 * 365, n)]
    return pd.to_datetime([base + o for o in offsets]).astype("datetime64[us]")


def _write(df: pd.DataFrame, full_table: str) -> None:
    (spark.createDataFrame(df).write
         .format("delta")
         .mode("overwrite")
         .option("overwriteSchema", "true")   # replace schema if table already exists
         .saveAsTable(full_table))
    cnt = spark.table(full_table).count()
    print(f"  {full_table:58s}  {cnt:>6,} rows")


print("Constants and helpers ready.")

# COMMAND ----------

# ── Step 2: transaction_fact — add account_key ────────────────────────────────
#
# account_key is a new surrogate that points to account_dim.
# All other columns are identical to 00_setup_trial.py.

def _fact_src() -> pd.DataFrame:
    n = N_FACT
    qty  = _rng.integers(1, 21, n).astype("int32")
    unit = np.round(_rng.uniform(5.0, 500.0, n), 2)
    disc = np.round(_rng.uniform(0.0, 50.0, n), 2)
    return pd.DataFrame({
        "transaction_id":     np.arange(1, n + 1, dtype="int64"),
        "customer_key":       _rng.integers(1, N_DIM + 1, n).astype("int64"),
        "product_key":        _rng.integers(1, N_PRODUCTS + 1, n).astype("int32"),
        "account_key":        _rng.integers(1, N_ACCOUNTS + 1, n).astype("int32"),   # NEW
        "transaction_date":   _rand_dates("2024-01-01", "2024-12-31", n),
        "transaction_amount": np.round(unit * qty - disc, 2),
        "quantity":           qty,
        "unit_price":         unit,
        "discount_amount":    disc,
        "net_amount":         np.round(unit * qty - disc, 2),
        "load_ts":            _rand_timestamps(n),
    })


fact_src = _fact_src()
fact_tgt = fact_src.copy()
fact_tgt.loc[200:229, "transaction_amount"] = 0.01   # 30 rows differ
fact_tgt.loc[200:229, "net_amount"]         = 0.01

print("Writing transaction_fact…")
_write(fact_src, "recon_src.sales.transaction_fact")
_write(fact_tgt, "recon_tgt.silver.transaction_fact")

# COMMAND ----------

# ── Step 3: customer_dim — add load_ts, is_active, record_type ───────────────
#
# load_ts    clashes with transaction_fact.load_ts
# is_active  clashes with product_dim.is_active and account_dim.is_active
# record_type clashes with product_dim.record_type and account_dim.record_type

def _customer_src() -> pd.DataFrame:
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
        # ── ambiguous columns (same names appear in other tables) ──────────
        "load_ts":       _rand_timestamps(n),       # clashes with transaction_fact.load_ts
        "is_active":     [True] * n,                # clashes with product_dim, account_dim
        "record_type":   ["A"] * n,                 # clashes with product_dim, account_dim
    })


cust_src = _customer_src()
cust_tgt = cust_src.copy()
# 15 rows differ: wrong city + shifted load_ts
cust_tgt.loc[100:114, "city"]    = "Boston"
cust_tgt.loc[100:114, "load_ts"] = pd.Timestamp("2099-01-01")   # obviously wrong

print("Writing customer_dim…")
_write(cust_src, "recon_src.sales.customer_dim")
_write(cust_tgt, "recon_tgt.silver.customer_dim")

# COMMAND ----------

# ── Step 4: product_dim — add load_ts, is_active, record_type ────────────────
#
# Same column names as customer_dim → these clash with each other when both
# lookups are joined to transaction_fact at the same time.

def _product_src() -> pd.DataFrame:
    n = N_PRODUCTS
    cats   = [_CATEGORIES[i % len(_CATEGORIES)] for i in range(n)]
    brands = [_BRANDS[i % len(_BRANDS)] for i in range(n)]
    return pd.DataFrame({
        "product_key":  np.arange(1, n + 1, dtype="int32"),
        "product_code": [f"PRD-{str(i).zfill(3)}" for i in range(1, n + 1)],
        "product_name": [f"Product {chr(65 + (i % 26))}{i}" for i in range(1, n + 1)],
        "category":     cats,
        "brand":        brands,
        "unit_cost":    np.round(_rng.uniform(5.0, 250.0, n), 2),
        # ── ambiguous columns ──────────────────────────────────────────────
        "load_ts":      _rand_timestamps(n),        # clashes with transaction_fact + customer_dim
        "is_active":    [True] * n,                 # clashes with customer_dim + account_dim
        "record_type":  ["A"] * n,                  # clashes with customer_dim + account_dim
    })


prod_src = _product_src()
prod_tgt = prod_src.copy()
# 10 rows differ: wrong category + wrong record_type on target
prod_tgt.loc[10:19, "category"]    = "Uncategorised"
prod_tgt.loc[10:19, "record_type"] = "D"            # "deleted" — wrong on target

print("Writing product_dim…")
_write(prod_src, "recon_src.sales.product_dim")
_write(prod_tgt, "recon_tgt.silver.product_dim")

# COMMAND ----------

# ── Step 5: account_dim — new table ──────────────────────────────────────────
#
# account_key range [1, 20] matches the account_key added to transaction_fact.
# Contains the same ambiguous column names as customer_dim and product_dim.

def _account_src() -> pd.DataFrame:
    n = N_ACCOUNTS
    return pd.DataFrame({
        "account_key":   np.arange(1, n + 1, dtype="int32"),
        "account_id":    np.arange(1001, 1001 + n, dtype="int32"),
        "account_name":  [f"Account {chr(65 + i)}" for i in range(n)],
        "account_type":  [_ACCT_TYPES[i % len(_ACCT_TYPES)] for i in range(n)],
        "region":        [["East", "West", "Central", "South"][i % 4] for i in range(n)],
        # ── ambiguous columns ──────────────────────────────────────────────
        "load_ts":       _rand_timestamps(n),       # clashes with all other tables
        "is_active":     [True] * n,                # clashes with customer_dim + product_dim
        "record_type":   ["A"] * n,                 # clashes with customer_dim + product_dim
    })


acct_src = _account_src()
acct_tgt = acct_src.copy()
# 5 rows differ: wrong account_type on target
acct_tgt.loc[0:4, "account_type"] = "Unknown"

print("Writing account_dim…")
_write(acct_src, "recon_src.sales.account_dim")
_write(acct_tgt, "recon_tgt.silver.account_dim")

# COMMAND ----------

# ── Step 6: Verify all tables ─────────────────────────────────────────────────

rows = []
for full_table in [
    "recon_src.sales.transaction_fact",
    "recon_src.sales.customer_dim",
    "recon_src.sales.product_dim",
    "recon_src.sales.account_dim",
    "recon_tgt.silver.transaction_fact",
    "recon_tgt.silver.customer_dim",
    "recon_tgt.silver.product_dim",
    "recon_tgt.silver.account_dim",
]:
    cnt = spark.table(full_table).count()
    catalog, schema, table = full_table.split(".")
    rows.append((catalog, schema, table, cnt))

display(spark.createDataFrame(rows, ["catalog", "schema", "table_name", "num_rows"]))

# COMMAND ----------

# ── Step 7: Test instructions ─────────────────────────────────────────────────

print("""
╔══════════════════════════════════════════════════════════════════════════════╗
║      Recon Generator — Ambiguous Column Test Instructions                  ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                            ║
║  MAIN TABLE:      transaction_fact                                         ║
║  SRC catalog/schema: recon_src / sales                                     ║
║  TGT catalog/schema: recon_tgt / silver                                    ║
║                                                                            ║
║  Ambiguous columns (same name in multiple tables):                         ║
║    load_ts     — transaction_fact + all three lookup tables                ║
║    is_active   — customer_dim + product_dim + account_dim                  ║
║    record_type — customer_dim + product_dim + account_dim                  ║
║                                                                            ║
╠═ Resolver 1 — customer_dim ═════════════════════════════════════════════════╣
║  Surrogate key (main table) : customer_key                                 ║
║  Lookup join key            : customer_key                                 ║
║  Natural cols               : customer_id, customer_name, load_ts,         ║
║                               is_active, record_type                       ║
║  Table alias                : cd                                           ║
║  Expected mismatches        : 15 rows — city + load_ts differ              ║
║                                                                            ║
╠═ Resolver 2 — product_dim ══════════════════════════════════════════════════╣
║  Surrogate key (main table) : product_key                                  ║
║  Lookup join key            : product_key                                  ║
║  Natural cols               : product_code, product_name, category,        ║
║                               load_ts, is_active, record_type              ║
║  Table alias                : pd                                           ║
║  Expected mismatches        : 10 rows — category + record_type differ      ║
║                                                                            ║
╠═ Resolver 3 — account_dim ══════════════════════════════════════════════════╣
║  Surrogate key (main table) : account_key                                  ║
║  Lookup join key            : account_key                                  ║
║  Natural cols               : account_id, account_name, account_type,      ║
║                               load_ts, is_active, record_type              ║
║  Table alias                : ad                                           ║
║  Expected mismatches        : 5 rows — account_type differs                ║
║                                                                            ║
╠═ Without table aliases (control test) ══════════════════════════════════════╣
║  Add resolvers WITHOUT aliases first — the generated PySpark should fail  ║
║  or produce AnalysisException about ambiguous column references.           ║
║  Then add aliases cd / pd / ad — the generated code should run cleanly.   ║
║                                                                            ║
╠═ Column selection for transaction_fact hash ════════════════════════════════╣
║  Include : transaction_id, transaction_date, transaction_amount,           ║
║            quantity, net_amount + natural cols from all resolvers          ║
║  Exclude (surrogates): customer_key, product_key, account_key              ║
║                                                                            ║
╚══════════════════════════════════════════════════════════════════════════════╝
""")
