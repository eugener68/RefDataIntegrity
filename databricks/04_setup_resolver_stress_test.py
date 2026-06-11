# Databricks notebook source
# TITLE: Recon Generator — Resolver Stress Test Setup
#
# Extends transaction_fact with 7 new surrogate FK columns and creates a
# matching lookup dimension for each one, giving a total of 10 resolver JOINs
# you can wire up in the Recon Generator UI to stress-test the sql_full
# drilldown emitter.
#
# New FK columns added to transaction_fact
# ────────────────────────────────────────────────────────────────────────────
#  channel_key      → sales_channel_dim      (10 rows)
#  ship_address_key → address_dim           (100 rows)
#  payment_key      → payment_dim            (20 rows)
#  order_key        → order_dim             (200 rows)
#  promo_key        → promotion_dim          (30 rows)
#  store_key        → store_dim              (25 rows)
#  care_key         → care_interaction_dim   (50 rows)
#
# Existing lookup tables (from earlier notebooks) — already usable as resolvers
# ────────────────────────────────────────────────────────────────────────────
#  customer_key     → customer_dim          (1 000 rows)
#  product_key      → product_dim            (50 rows)
#  account_key      → account_dim            (20 rows)
#
# Mismatch summary (target vs source)
# ────────────────────────────────────────────────────────────────────────────
#  transaction_fact        : 30 rows with wrong transaction_amount / net_amount
#  sales_channel_dim       :  2 rows with wrong channel_type
#  address_dim             : 10 rows with wrong city / state_code
#  payment_dim             :  3 rows with wrong card_brand
#  order_dim               : 20 rows with wrong order_status
#  promotion_dim           :  5 rows with wrong discount_pct
#  store_dim               :  4 rows with wrong store_region
#  care_interaction_dim    :  8 rows with wrong resolution_status
#
# ─── Prerequisites ────────────────────────────────────────────────────────────
#  • 00_setup_trial.py and 03_setup_ambiguous_cols_test.py must have run
#    (catalogs, schemas, and existing lookup tables must exist)
#  • DBR 13+, Unity Catalog enabled
# ──────────────────────────────────────────────────────────────────────────────

# COMMAND ----------

# ── Step 1: Shared constants and helpers ──────────────────────────────────────

from datetime import date, datetime, timedelta
import numpy as np
import pandas as pd

SEED          = 42
N_FACT        = 1_000
N_DIM         = 1_000       # customer_dim cardinality
N_PRODUCTS    = 50          # product_dim cardinality
N_ACCOUNTS    = 20          # account_dim cardinality

# new lookup cardinalities
N_CHANNELS    = 10
N_ADDRESSES   = 100
N_PAYMENTS    = 20
N_ORDERS      = 200
N_PROMOS      = 30
N_STORES      = 25
N_CARE        = 50

_rng = np.random.default_rng(SEED)


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
         .option("overwriteSchema", "true")
         .saveAsTable(full_table))
    cnt = spark.table(full_table).count()
    print(f"  {full_table:60s}  {cnt:>6,} rows")


print("Constants and helpers ready.")

# COMMAND ----------

# ── Step 2: Update transaction_fact — add 7 new FK columns ───────────────────
#
# Regenerates the full fact table (same seed as 03_setup_ambiguous_cols_test.py)
# with 7 additional surrogate FK columns.  Mismatch rows are identical to the
# previous version (rows 200-229: wrong transaction_amount + net_amount).

def _fact_src() -> pd.DataFrame:
    n = N_FACT
    qty  = _rng.integers(1, 21, n).astype("int32")
    unit = np.round(_rng.uniform(5.0, 500.0, n), 2)
    disc = np.round(_rng.uniform(0.0, 50.0, n), 2)
    return pd.DataFrame({
        "transaction_id":     np.arange(1, n + 1, dtype="int64"),
        # ── existing FK columns ────────────────────────────────────────────
        "customer_key":       _rng.integers(1, N_DIM + 1,      n).astype("int64"),
        "product_key":        _rng.integers(1, N_PRODUCTS + 1, n).astype("int32"),
        "account_key":        _rng.integers(1, N_ACCOUNTS + 1, n).astype("int32"),
        # ── new FK columns ─────────────────────────────────────────────────
        "channel_key":        _rng.integers(1, N_CHANNELS + 1,  n).astype("int32"),
        "ship_address_key":   _rng.integers(1, N_ADDRESSES + 1, n).astype("int32"),
        "payment_key":        _rng.integers(1, N_PAYMENTS + 1,  n).astype("int32"),
        "order_key":          _rng.integers(1, N_ORDERS + 1,    n).astype("int32"),
        "promo_key":          _rng.integers(1, N_PROMOS + 1,    n).astype("int32"),
        "store_key":          _rng.integers(1, N_STORES + 1,    n).astype("int32"),
        "care_key":           _rng.integers(1, N_CARE + 1,      n).astype("int32"),
        # ── measure columns ────────────────────────────────────────────────
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
fact_tgt.loc[200:229, "transaction_amount"] = 0.01   # 30 rows differ (same as before)
fact_tgt.loc[200:229, "net_amount"]         = 0.01

print("Writing transaction_fact…")
_write(fact_src, "recon_src.sales.transaction_fact")
_write(fact_tgt, "recon_tgt.silver.transaction_fact")

# COMMAND ----------

# ── Step 3: sales_channel_dim ─────────────────────────────────────────────────
#
# Represents the sales channel through which a transaction was made.
# 10 rows — channel_key [1..10].
# Mismatch: 2 rows have wrong channel_type in target.

_CHANNEL_TYPES = ["Online", "In-Store", "Phone", "Partner", "Mobile",
                  "Wholesale", "Marketplace", "Direct", "Reseller", "Kiosk"]
_CHANNEL_REGIONS = ["North", "South", "East", "West", "Central"]


def _channel_src() -> pd.DataFrame:
    n = N_CHANNELS
    return pd.DataFrame({
        "channel_key":    np.arange(1, n + 1, dtype="int32"),
        "channel_code":   [f"CH{str(i).zfill(2)}" for i in range(1, n + 1)],
        "channel_name":   [_CHANNEL_TYPES[i] for i in range(n)],
        "channel_type":   [_CHANNEL_TYPES[i] for i in range(n)],
        "channel_region": [_CHANNEL_REGIONS[i % len(_CHANNEL_REGIONS)] for i in range(n)],
        "is_active":      [True] * n,
        "load_ts":        _rand_timestamps(n),
    })


ch_src = _channel_src()
ch_tgt = ch_src.copy()
ch_tgt.loc[3:4, "channel_type"] = "Unknown"    # 2 rows differ

print("Writing sales_channel_dim…")
_write(ch_src, "recon_src.sales.sales_channel_dim")
_write(ch_tgt, "recon_tgt.silver.sales_channel_dim")

# COMMAND ----------

# ── Step 4: address_dim ───────────────────────────────────────────────────────
#
# Shipping address for each transaction.
# 100 rows — ship_address_key [1..100].
# Mismatch: 10 rows have wrong city + state_code in target.

_CITIES    = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
              "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose",
              "Austin", "Jacksonville", "Fort Worth", "Columbus", "Charlotte",
              "Indianapolis", "San Francisco", "Seattle", "Denver", "Nashville"]
_STATES    = ["NY", "CA", "IL", "TX", "AZ", "PA", "TX", "CA", "TX", "CA",
              "TX", "FL", "TX", "OH", "NC", "IN", "CA", "WA", "CO", "TN"]
_COUNTRIES = ["US"]
_ADDR_TYPES = ["Residential", "Commercial", "P.O. Box", "Warehouse"]


def _address_src() -> pd.DataFrame:
    n = N_ADDRESSES
    ci = _rng.integers(0, len(_CITIES), n)
    return pd.DataFrame({
        "address_key":   np.arange(1, n + 1, dtype="int32"),
        "address_line1": [f"{_rng.integers(100, 9999)!s} Main St" for _ in range(n)],
        "address_line2": [None if _rng.random() < 0.7 else f"Suite {_rng.integers(1, 99)!s}" for _ in range(n)],
        "city":          [_CITIES[i] for i in ci],
        "state_code":    [_STATES[i]  for i in ci],
        "zip_code":      [f"{_rng.integers(10000, 99999)!s}" for _ in range(n)],
        "country_code":  ["US"] * n,
        "address_type":  [_ADDR_TYPES[i % len(_ADDR_TYPES)] for i in range(n)],
        "is_verified":   [True] * n,
        "load_ts":       _rand_timestamps(n),
    })


addr_src = _address_src()
addr_tgt = addr_src.copy()
addr_tgt.loc[20:29, "city"]       = "Unknown City"    # 10 rows differ
addr_tgt.loc[20:29, "state_code"] = "XX"

print("Writing address_dim…")
_write(addr_src, "recon_src.sales.address_dim")
_write(addr_tgt, "recon_tgt.silver.address_dim")

# COMMAND ----------

# ── Step 5: payment_dim ───────────────────────────────────────────────────────
#
# Payment method used for a transaction.
# 20 rows — payment_key [1..20].
# Mismatch: 3 rows have wrong card_brand in target.

_PAY_METHODS = ["Credit Card", "Debit Card", "PayPal", "Bank Transfer",
                "Crypto", "Gift Card", "Buy Now Pay Later", "Cash", "Check", "Wire"]
_PAY_TYPES   = ["Card", "Card", "Digital Wallet", "Bank", "Digital Wallet",
                "Prepaid", "BNPL", "Cash", "Check", "Bank"]
_CARD_BRANDS = ["Visa", "Mastercard", "Amex", "Discover", "UnionPay",
                "JCB", "N/A", "N/A", "N/A", "N/A"]


def _payment_src() -> pd.DataFrame:
    n = N_PAYMENTS
    methods = [_PAY_METHODS[i % len(_PAY_METHODS)] for i in range(n)]
    types   = [_PAY_TYPES[i % len(_PAY_TYPES)] for i in range(n)]
    brands  = [_CARD_BRANDS[i % len(_CARD_BRANDS)] for i in range(n)]
    return pd.DataFrame({
        "payment_key":      np.arange(1, n + 1, dtype="int32"),
        "payment_code":     [f"PAY{str(i).zfill(3)}" for i in range(1, n + 1)],
        "payment_method":   methods,
        "payment_type":     types,
        "card_brand":       brands,
        "is_recurring":     [i % 5 == 0 for i in range(n)],
        "requires_auth":    [True] * n,
        "load_ts":          _rand_timestamps(n),
    })


pay_src = _payment_src()
pay_tgt = pay_src.copy()
pay_tgt.loc[5:7, "card_brand"] = "Unknown"    # 3 rows differ

print("Writing payment_dim…")
_write(pay_src, "recon_src.sales.payment_dim")
_write(pay_tgt, "recon_tgt.silver.payment_dim")

# COMMAND ----------

# ── Step 6: order_dim ─────────────────────────────────────────────────────────
#
# The parent order that a transaction belongs to.
# 200 rows — order_key [1..200].
# Mismatch: 20 rows have wrong order_status in target.

_ORDER_STATUSES = ["New", "Processing", "Shipped", "Delivered", "Returned",
                   "Cancelled", "On Hold"]
_ORDER_SOURCES  = ["Web", "Mobile", "Phone", "In-Store", "Partner API"]
_PRIORITIES     = ["Low", "Normal", "High", "Urgent"]


def _order_src() -> pd.DataFrame:
    n = N_ORDERS
    return pd.DataFrame({
        "order_key":      np.arange(1, n + 1, dtype="int32"),
        "order_number":   [f"ORD-{str(i).zfill(6)}" for i in range(1, n + 1)],
        "order_status":   [_ORDER_STATUSES[i % len(_ORDER_STATUSES)] for i in range(n)],
        "order_source":   [_ORDER_SOURCES[i % len(_ORDER_SOURCES)] for i in range(n)],
        "priority":       [_PRIORITIES[i % len(_PRIORITIES)] for i in range(n)],
        "order_date":     _rand_dates("2024-01-01", "2024-12-31", n),
        "ship_by_date":   _rand_dates("2024-01-05", "2025-01-05", n),
        "is_flagged":     [False] * n,
        "load_ts":        _rand_timestamps(n),
    })


ord_src = _order_src()
ord_tgt = ord_src.copy()
ord_tgt.loc[50:69, "order_status"] = "Unknown"    # 20 rows differ

print("Writing order_dim…")
_write(ord_src, "recon_src.sales.order_dim")
_write(ord_tgt, "recon_tgt.silver.order_dim")

# COMMAND ----------

# ── Step 7: promotion_dim ─────────────────────────────────────────────────────
#
# Promotion or discount applied to a transaction.
# 30 rows — promo_key [1..30].
# Mismatch: 5 rows have wrong discount_pct in target.

_PROMO_TYPES = ["Percentage Off", "Fixed Amount Off", "BOGO", "Free Shipping",
                "Bundle Deal", "Loyalty Points"]
_PROMO_CHANNELS = ["Email", "SMS", "In-App", "Website", "Partner", "Direct Mail"]


def _promo_src() -> pd.DataFrame:
    n = N_PROMOS
    return pd.DataFrame({
        "promo_key":      np.arange(1, n + 1, dtype="int32"),
        "promo_code":     [f"PROMO{str(i).zfill(3)}" for i in range(1, n + 1)],
        "promo_name":     [f"Promotion {i}" for i in range(1, n + 1)],
        "promo_type":     [_PROMO_TYPES[i % len(_PROMO_TYPES)] for i in range(n)],
        "promo_channel":  [_PROMO_CHANNELS[i % len(_PROMO_CHANNELS)] for i in range(n)],
        "discount_pct":   np.round(_rng.uniform(5.0, 50.0, n), 2),
        "min_order_value": np.round(_rng.uniform(0.0, 200.0, n), 2),
        "is_stackable":   [i % 3 == 0 for i in range(n)],
        "start_date":     _rand_dates("2024-01-01", "2024-06-30", n),
        "end_date":       _rand_dates("2024-07-01", "2024-12-31", n),
        "load_ts":        _rand_timestamps(n),
    })


promo_src = _promo_src()
promo_tgt = promo_src.copy()
promo_tgt.loc[10:14, "discount_pct"] = 0.0    # 5 rows differ (zeroed out)

print("Writing promotion_dim…")
_write(promo_src, "recon_src.sales.promotion_dim")
_write(promo_tgt, "recon_tgt.silver.promotion_dim")

# COMMAND ----------

# ── Step 8: store_dim ─────────────────────────────────────────────────────────
#
# Physical or virtual store where the transaction originated.
# 25 rows — store_key [1..25].
# Mismatch: 4 rows have wrong store_region in target.

_STORE_TYPES   = ["Flagship", "Outlet", "Pop-Up", "Online", "Franchise"]
_STORE_REGIONS = ["Northeast", "Southeast", "Midwest", "Southwest", "West"]
_STORE_FORMATS = ["Large", "Medium", "Small", "Virtual"]


def _store_src() -> pd.DataFrame:
    n = N_STORES
    return pd.DataFrame({
        "store_key":     np.arange(1, n + 1, dtype="int32"),
        "store_code":    [f"STR{str(i).zfill(3)}" for i in range(1, n + 1)],
        "store_name":    [f"Store {i}" for i in range(1, n + 1)],
        "store_type":    [_STORE_TYPES[i % len(_STORE_TYPES)] for i in range(n)],
        "store_region":  [_STORE_REGIONS[i % len(_STORE_REGIONS)] for i in range(n)],
        "store_format":  [_STORE_FORMATS[i % len(_STORE_FORMATS)] for i in range(n)],
        "open_date":     _rand_dates("2010-01-01", "2023-12-31", n),
        "is_active":     [True] * n,
        "load_ts":       _rand_timestamps(n),
    })


store_src = _store_src()
store_tgt = store_src.copy()
store_tgt.loc[8:11, "store_region"] = "Unknown"    # 4 rows differ

print("Writing store_dim…")
_write(store_src, "recon_src.sales.store_dim")
_write(store_tgt, "recon_tgt.silver.store_dim")

# COMMAND ----------

# ── Step 9: care_interaction_dim ──────────────────────────────────────────────
#
# Customer care case linked to a transaction (e.g. a return or complaint).
# 50 rows — care_key [1..50].
# Mismatch: 8 rows have wrong resolution_status in target.

_CASE_TYPES      = ["Return", "Complaint", "Inquiry", "Feedback", "Escalation"]
_CASE_CHANNELS   = ["Phone", "Email", "Chat", "In-Store", "Social Media"]
_RESOLUTION_STTS = ["Open", "In Progress", "Resolved", "Closed", "Escalated"]


def _care_src() -> pd.DataFrame:
    n = N_CARE
    return pd.DataFrame({
        "care_key":           np.arange(1, n + 1, dtype="int32"),
        "case_number":        [f"CASE-{str(i).zfill(5)}" for i in range(1, n + 1)],
        "case_type":          [_CASE_TYPES[i % len(_CASE_TYPES)] for i in range(n)],
        "case_channel":       [_CASE_CHANNELS[i % len(_CASE_CHANNELS)] for i in range(n)],
        "resolution_status":  [_RESOLUTION_STTS[i % len(_RESOLUTION_STTS)] for i in range(n)],
        "satisfaction_score": _rng.integers(1, 6, n).astype("int32"),   # 1-5 CSAT
        "first_contact_res":  [i % 2 == 0 for i in range(n)],           # FCR flag
        "open_date":          _rand_dates("2024-01-01", "2024-12-31", n),
        "close_date":         _rand_dates("2024-01-05", "2025-01-05", n),
        "load_ts":            _rand_timestamps(n),
    })


care_src = _care_src()
care_tgt = care_src.copy()
care_tgt.loc[15:22, "resolution_status"] = "Unknown"    # 8 rows differ

print("Writing care_interaction_dim…")
_write(care_src, "recon_src.sales.care_interaction_dim")
_write(care_tgt, "recon_tgt.silver.care_interaction_dim")

# COMMAND ----------

# ── Step 10: Verify all tables ────────────────────────────────────────────────

rows = []
for full_table in [
    # updated fact
    "recon_src.sales.transaction_fact",
    "recon_tgt.silver.transaction_fact",
    # existing lookups (unchanged)
    "recon_src.sales.customer_dim",
    "recon_tgt.silver.customer_dim",
    "recon_src.sales.product_dim",
    "recon_tgt.silver.product_dim",
    "recon_src.sales.account_dim",
    "recon_tgt.silver.account_dim",
    # new lookups
    "recon_src.sales.sales_channel_dim",
    "recon_tgt.silver.sales_channel_dim",
    "recon_src.sales.address_dim",
    "recon_tgt.silver.address_dim",
    "recon_src.sales.payment_dim",
    "recon_tgt.silver.payment_dim",
    "recon_src.sales.order_dim",
    "recon_tgt.silver.order_dim",
    "recon_src.sales.promotion_dim",
    "recon_tgt.silver.promotion_dim",
    "recon_src.sales.store_dim",
    "recon_tgt.silver.store_dim",
    "recon_src.sales.care_interaction_dim",
    "recon_tgt.silver.care_interaction_dim",
]:
    cnt = spark.table(full_table).count()
    catalog, schema, table = full_table.split(".")
    rows.append((catalog, schema, table, cnt))

display(spark.createDataFrame(rows, ["catalog", "schema", "table_name", "num_rows"]))

# COMMAND ----------

# ── Step 11: Confirm new FK columns in transaction_fact ───────────────────────

new_fks = ["channel_key", "ship_address_key", "payment_key",
           "order_key", "promo_key", "store_key", "care_key"]

tf = spark.table("recon_src.sales.transaction_fact")
actual_cols = set(tf.columns)
missing = [c for c in new_fks if c not in actual_cols]

if missing:
    raise AssertionError(f"Missing FK columns in transaction_fact: {missing}")
else:
    print("All new FK columns present in transaction_fact ✓")
    for fk in new_fks:
        stats = tf.selectExpr(f"min({fk})", f"max({fk})").collect()[0]
        print(f"  {fk:20s}  range [{stats[0]}, {stats[1]}]")

# COMMAND ----------

# ── Step 12: Test instructions ────────────────────────────────────────────────

print("""
╔══════════════════════════════════════════════════════════════════════════════╗
║       Recon Generator — Resolver Stress Test Instructions                  ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                            ║
║  GOAL: configure transaction_fact with all 10 resolver JOINs in the UI     ║
║  and generate a notebook using drilldown_mode = sql_full.                  ║
║                                                                            ║
║  SOURCE table : recon_src.sales.transaction_fact                           ║
║  TARGET table : recon_tgt.silver.transaction_fact                          ║
║  Business key : transaction_id                                             ║
║  Partition col: transaction_date  (optional but recommended)               ║
║                                                                            ║
║  Add 10 Lookup Table (resolver) entries:                                   ║
║  ─────────────────────────────────────────────────────────────────────     ║
║  #   Join key (fact)    Lookup table (src)            Lookup key           ║
║  1   customer_key       recon_src.sales.customer_dim      customer_key     ║
║  2   product_key        recon_src.sales.product_dim       product_key      ║
║  3   account_key        recon_src.sales.account_dim       account_key      ║
║  4   channel_key        recon_src.sales.sales_channel_dim channel_key      ║
║  5   ship_address_key   recon_src.sales.address_dim       address_key      ║
║  6   payment_key        recon_src.sales.payment_dim       payment_key      ║
║  7   order_key          recon_src.sales.order_dim         order_key        ║
║  8   promo_key          recon_src.sales.promotion_dim     promo_key        ║
║  9   store_key          recon_src.sales.store_dim         store_key        ║
║  10  care_key           recon_src.sales.care_interaction_dim  care_key     ║
║                                                                            ║
║  Set drilldown_mode = sql_full in the Advanced Config panel.               ║
║                                                                            ║
║  Expected result: generated notebook contains a single spark.sql() call   ║
║  with two CTEs (src / tgt), each with 10 INNER JOINs, a FULL OUTER JOIN    ║
║  on transaction_id, and a WHERE clause emitting only mismatching rows.     ║
║                                                                            ║
║  Drill-down should surface ~30 rows (rows 200-229 in transaction_fact      ║
║  where transaction_amount and net_amount were zeroed to 0.01).             ║
║                                                                            ║
╚══════════════════════════════════════════════════════════════════════════════╝
""")
