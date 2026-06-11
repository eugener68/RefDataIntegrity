# Databricks notebook source
# Run this cell first — parameter dropdowns appear at the top of the notebook.
dbutils.widgets.dropdown("recreate", "true", ["true", "false"], "Drop and recreate recon_tgt.gold (+ staging/keymap)")
dbutils.widgets.dropdown(
    "bootstrap", "auto", ["auto", "always", "never"],
    "auto=create missing base; always=reset 00+product base; never=require base exists",
)

# COMMAND ----------

# MAGIC %md
# MAGIC # SK Integrity Test Environment — Setup (on Recon catalogs)
# MAGIC
# MAGIC Builds **surrogate-key corruption** on top of the existing Recon Generator environment.
# MAGIC Does **not** modify `recon_tgt.silver` (hash-mismatch recon tests keep working).
# MAGIC
# MAGIC | Catalog.schema | Role |
# MAGIC |---|---|
# MAGIC | `recon_src.sales` | **Golden source** (SQL Server stand-in) — existing tables + new golden facts |
# MAGIC | `recon_tgt.gold` | **Broken Databricks** — reloaded dims with wrong SKs + mixed-cohort facts |
# MAGIC | `recon_tgt.staging` / `recon_tgt.keymap` | Repair notebook outputs |
# MAGIC
# MAGIC ### Two setup paths — same SK test end state
# MAGIC | Path | When | What to run |
# MAGIC |---|---|---|
# MAGIC | **Existing recon DB** | Already ran `00`–`04` (or partial) | This notebook only (`bootstrap=auto`, `recreate=true`) |
# MAGIC | **Empty / fresh DB** | New team, no catalogs yet | This notebook only (`bootstrap=auto`, `recreate=true`) |
# MAGIC
# MAGIC **`bootstrap=auto`** (default): creates missing base tables (00 + product bridge equivalent).
# MAGIC **`bootstrap=always`**: resets `recon_src.sales` + `recon_tgt.silver` base tables to the canonical
# MAGIC recon trial state, then builds SK fixtures. Use to recover a known baseline.
# MAGIC
# MAGIC **Recon coexistence:** refreshes `recon_tgt.silver.transaction_fact` to match canonical golden
# MAGIC `transaction_fact` (hash-mismatch pattern). Other silver tables are left unchanged unless missing
# MAGIC or `bootstrap=always`.
# MAGIC
# MAGIC ### Golden tables written/updated in `recon_src.sales`
# MAGIC - `account_dim` — SK test fixture (120 rows, NK `account_id`)
# MAGIC - `transaction_fact` — adds/replaces `account_key` (third FK)
# MAGIC - **New:** `return_fact`, `scd2_activity_fact`, hub, lookup dims, `account_type_scd2`
# MAGIC
# MAGIC ### Broken tables created (`recon_tgt.gold`)
# MAGIC **Dimensions:** `customer_dim`, `product_dim`, `customer_scd2`, lookup dims (`subscriber_dim`, `market_dim`, …)
# MAGIC **Facts / SCD2 hubs:** `transaction_fact`, `return_fact`, `scd2_activity_fact`, **`account_details_scd2`**
# MAGIC
# MAGIC ### Multi-FK SCD2 hub pattern (like real `accountDetails`)
# MAGIC One SCD2 table carries **many FK columns to different dimensions** (`account_key`, `subscriber_key`,
# MAGIC `market_key`, …). Repair notebooks run **once per dimension**, re-keying one FK column at a time.
# MAGIC See `sk_integrity_test_guide.md` § Multi-FK SCD2 hub.
# MAGIC
# MAGIC ### Scenarios covered (see `SK_INTEGRITY_TEST_CASES.md`)
# MAGIC | ID | Pattern |
# MAGIC |---|---|
# MAGIC | TC-REPAIR-SCD1-001/002 | SCD1 dim → one or two facts |
# MAGIC | TC-REPAIR-SCD1-003 | Composite NK (`product_dim`) |
# MAGIC | TC-REPAIR-SCD1-004 | Third FK — `account_dim` → `transaction_fact.account_key` |
# MAGIC | TC-REPAIR-SCD2-001/002 | SCD2 dim → fact (current + **historic** activity rows) |
# MAGIC | TC-REPAIR-HUB-001 | Hub → SCD1 lookup FK (`market_key`) |
# MAGIC | TC-REPAIR-HUB-002 | Hub → **SCD2 lookup** FK (`account_type_key` → `account_type_scd2`) |
# MAGIC | TC-REPAIR-MULTI-001 | Multiple facts / hub same dim in one pass |
# MAGIC | TC-DATA-* | Mixed cohorts, orphans, silent corruption, AMBIGUOUS SCD2 |
# MAGIC
# MAGIC See `sk_integrity_test_guide.md` for repair notebook widget presets.
# MAGIC See **`RUNBOOK.md`** for the unified team runbook.
# MAGIC
# MAGIC **Parameters:** use the **`recreate`** and **`bootstrap`** dropdowns at the top of the notebook
# MAGIC (created by the first code cell). Defaults: `recreate=true`, `bootstrap=auto`.

# COMMAND ----------

RECREATE = dbutils.widgets.get("recreate") == "true"
BOOTSTRAP = dbutils.widgets.get("bootstrap")

SRC = "recon_src.sales"
TGT = "recon_tgt.gold"
STAGING = "recon_tgt.staging"
KEYMAP = "recon_tgt.keymap"
SILVER = "recon_tgt.silver"


def _table_exists(full_table: str) -> bool:
    try:
        spark.table(full_table).limit(1).collect()
        return True
    except Exception:
        return False


spark.sql("CREATE CATALOG IF NOT EXISTS recon_src COMMENT 'Simulates source catalog (Lakehouse Federation stand-in)'")
spark.sql("CREATE CATALOG IF NOT EXISTS recon_tgt COMMENT 'Target Unity Catalog (native Delta)'")
spark.sql("CREATE SCHEMA IF NOT EXISTS recon_src.sales   COMMENT 'Source sales schema'")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER}  COMMENT 'Target silver schema (recon hash-mismatch)'")

spark.sql("CREATE SCHEMA IF NOT EXISTS recon_tgt.gold")
spark.sql("CREATE SCHEMA IF NOT EXISTS recon_tgt.staging")
spark.sql("CREATE SCHEMA IF NOT EXISTS recon_tgt.keymap")

if RECREATE:
    for schema in ["recon_tgt.gold", STAGING, KEYMAP]:
        for row in spark.sql(f"SHOW TABLES IN {schema}").collect():
            spark.sql(f"DROP TABLE IF EXISTS {schema}.{row.tableName}")
    print("Dropped existing tables in gold / staging / keymap.")

print("Schemas ready.")

# COMMAND ----------

from datetime import date, datetime, timedelta
import numpy as np
import pandas as pd

SEED = 42
_rng = np.random.default_rng(SEED)

ORPHAN_OLD_COUNT = 30
ORPHAN_NEW_COUNT = 15
INCREMENTAL_PCT  = 0.35
N_ACCOUNTS         = 120   # account_dim rows + transaction_fact.account_key range
TODAY = date.today()
D_MINUS_1 = TODAY - timedelta(days=1)


def _write(df: pd.DataFrame, table: str) -> None:
    spark.createDataFrame(df).write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(table)
    print(f"  {table:58s}  {spark.table(table).count():>6,} rows")


def _to_date(val):
    if isinstance(val, date) and not isinstance(val, datetime):
        return val
    return pd.Timestamp(val).date()


def _normalize_scd2_df(df: pd.DataFrame, table_name: str = "SCD2 table") -> pd.DataFrame:
    """Map legacy column names; derive recordStatus when missing."""
    out = df.copy()
    rename = {
        "record_status": "recordStatus",
        "RecordStatus": "recordStatus",
        "effective_start_date": "effectiveStartDate",
        "effective_end_date": "effectiveEndDate",
        "EffectiveStartDate": "effectiveStartDate",
        "EffectiveEndDate": "effectiveEndDate",
        "valid_from": "effectiveStartDate",
        "valid_to": "effectiveEndDate",
        "is_current": "recordStatus",
    }
    out = out.rename(columns={k: v for k, v in rename.items() if k in out.columns})
    if "recordStatus" in out.columns and out["recordStatus"].dtype != object:
        out["recordStatus"] = out["recordStatus"].map({True: "C", False: "H", 1: "C", 0: "H"}).fillna(out["recordStatus"])
    if "recordStatus" not in out.columns:
        if "effectiveEndDate" in out.columns:
            out["recordStatus"] = out["effectiveEndDate"].apply(
                lambda d: "C" if _to_date(d) == date(2999, 12, 31) else "H"
            )
        else:
            raise KeyError(
                f"{table_name} missing recordStatus and effectiveEndDate — "
                f"found columns: {list(out.columns)}. "
                "Re-run with bootstrap=auto (default) or bootstrap=always."
            )
    for col in ("effectiveStartDate", "effectiveEndDate"):
        if col not in out.columns:
            raise KeyError(
                f"{table_name} missing {col} — found columns: {list(out.columns)}. "
                "Re-run with bootstrap=auto (default) or bootstrap=always."
            )
    return out


def _lookup_dim(name, sk_col, nk_col, n, code_prefix):
    """Small SCD1 reference dim with unknown member."""
    codes = [f"{code_prefix}{str(i).zfill(4)}" for i in range(1, n + 1)]
    df = pd.DataFrame({
        sk_col:  np.arange(1, n + 1, dtype="int64"),
        nk_col:  codes,
        f"{name}_name": [f"{name.title()} {c}" for c in codes],
    })
    unknown = pd.DataFrame({sk_col: [-1], nk_col: ["UNKNOWN"], f"{name}_name": ["Unknown"]})
    return pd.concat([unknown, df], ignore_index=True)


print(f"Reference dates: D-1={D_MINUS_1}, today={TODAY}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Bootstrap base recon + SK golden tables (00 + product bridge equivalent)
# MAGIC
# MAGIC Deterministic **SEED=42** generators — same data whether you start from an empty workspace
# MAGIC or an existing DB that already ran recon setup notebooks.

# COMMAND ----------

# ── Shared constants (mirrors 00_setup_trial.py + 02_setup_bridge_test.py) ────
N_DIM = 1_000
N_FACT = 1_000
N_SCD2_CURRENT = 300
N_SCD2_HIST = 700
HASH_MISMATCH = 30
N_PRODUCTS = 50
PRODUCT_HASH_MISMATCH = 5

_CITIES = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
           "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose"]
_STATES = ["NY", "CA", "IL", "TX", "AZ", "PA", "TX", "CA", "TX", "CA"]
_SEGMENTS = ["Bronze", "Silver", "Gold", "Platinum"]
_CATEGORIES = ["Electronics", "Clothing", "Food & Beverage", "Home & Garden", "Sports"]
_BRANDS = ["AlphaBrand", "BetaCo", "GammaTech", "DeltaWorks", "EpsilonGoods"]


def _rand_dates(start: str, end: str, n: int):
    s, e = date.fromisoformat(start), date.fromisoformat(end)
    return [s + timedelta(days=int(d)) for d in _rng.integers(0, (e - s).days, n)]


def _rand_timestamps(n: int):
    base = datetime(2025, 1, 1)
    offsets = [timedelta(seconds=int(s)) for s in _rng.integers(0, 86_400 * 365, n)]
    return pd.to_datetime([base + o for o in offsets]).astype("datetime64[us]")


def _dim_src() -> pd.DataFrame:
    n = N_DIM
    ci = _rng.integers(0, len(_CITIES), n)
    return pd.DataFrame({
        "customer_key":  np.arange(1, n + 1, dtype="int64"),
        "customer_id":   np.arange(1, n + 1, dtype="int32"),
        "customer_name": [f"Customer {i}" for i in range(1, n + 1)],
        "email":         [f"customer{i}@example.com" for i in range(1, n + 1)],
        "city":          [_CITIES[i] for i in ci],
        "state_code":    [_STATES[i] for i in ci],
        "segment":       [_SEGMENTS[i % len(_SEGMENTS)] for i in range(n)],
        "created_date":  _rand_dates("2020-01-01", "2024-12-31", n),
        "is_active":     [True] * n,
    })


def _dim_tgt_hash_mismatch(src: pd.DataFrame) -> pd.DataFrame:
    tgt = src.copy()
    tgt.loc[100:100 + HASH_MISMATCH - 1, "city"] = "Boston"
    tgt.loc[100:100 + HASH_MISMATCH - 1, "segment"] = "Unknown"
    return tgt


def _fact_src() -> pd.DataFrame:
    n = N_FACT
    qty = _rng.integers(1, 21, n).astype("int32")
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
    tgt.loc[200:200 + HASH_MISMATCH - 1, "net_amount"] = 0.01
    return tgt


def _scd2_src(seed: int = SEED + 2) -> pd.DataFrame:
    """Deterministic SCD2 fixture — fixed sub-seed so bootstrap path order does not matter."""
    rng = np.random.default_rng(seed)
    nc, nh = N_SCD2_CURRENT, N_SCD2_HIST

    def _scd2_timestamps(n: int):
        base = datetime(2025, 1, 1)
        offsets = [timedelta(seconds=int(s)) for s in rng.integers(0, 86_400 * 365, n)]
        return pd.to_datetime([base + o for o in offsets]).astype("datetime64[us]")

    ci_c = rng.integers(0, len(_CITIES), nc)
    current = pd.DataFrame({
        "surrogate_key":  np.arange(1, nc + 1, dtype="int64"),
        "customer_id":    np.arange(1, nc + 1, dtype="int32"),
        "customer_name":  [f"Customer {i}" for i in range(1, nc + 1)],
        "email":          [f"customer{i}@example.com" for i in range(1, nc + 1)],
        "city":           [_CITIES[i] for i in ci_c],
        "state_code":     [_STATES[i] for i in ci_c],
        "segment":        [_SEGMENTS[i % len(_SEGMENTS)] for i in range(nc)],
        "recordStatus":       ["C"] * nc,
        "effectiveStartDate": [date(2024, 1, 1)] * nc,
        "effectiveEndDate":   [date(2999, 12, 31)] * nc,
        "load_ts":        _scd2_timestamps(nc),
    })
    ci_h = rng.integers(0, len(_CITIES), nh)
    cids = rng.integers(1, nc + 1, nh).astype("int32")
    historical = pd.DataFrame({
        "surrogate_key":  np.arange(nc + 1, nc + nh + 1, dtype="int64"),
        "customer_id":    cids,
        "customer_name":  [f"Customer {i}" for i in cids],
        "email":          [f"customer{i}.old@example.com" for i in cids],
        "city":           [_CITIES[i] for i in ci_h],
        "state_code":     [_STATES[i] for i in ci_h],
        "segment":        [_SEGMENTS[i % len(_SEGMENTS)] for i in range(nh)],
        "recordStatus":       ["H"] * nh,
        "effectiveStartDate": [date(2022, 1, 1)] * nh,
        "effectiveEndDate":   [date(2023, 12, 31)] * nh,
        "load_ts":        _scd2_timestamps(nh),
    })
    return pd.concat([current, historical], ignore_index=True)


def _scd2_tgt_hash_mismatch(src: pd.DataFrame) -> pd.DataFrame:
    tgt = _normalize_scd2_df(src.copy(), "customer_scd2")
    curr_idx = tgt.index[tgt["recordStatus"] == "C"][:HASH_MISMATCH]
    tgt.loc[curr_idx, "city"] = "Boston"
    tgt.loc[curr_idx, "segment"] = "Unknown"
    return tgt


def _product_src() -> pd.DataFrame:
    n = N_PRODUCTS
    cats = [_CATEGORIES[i % len(_CATEGORIES)] for i in range(n)]
    brands = [_BRANDS[i % len(_BRANDS)] for i in range(n)]
    return pd.DataFrame({
        "product_key":  np.arange(1, n + 1, dtype="int32"),
        "product_code": [f"PRD-{str(i).zfill(3)}" for i in range(1, n + 1)],
        "product_name": [f"Product {chr(65 + (i % 26))}{i}" for i in range(1, n + 1)],
        "category":     cats,
        "brand":        brands,
        "unit_cost":    np.round(_rng.uniform(5.0, 250.0, n), 2),
        "is_active":    [True] * n,
    })


def _product_tgt_hash_mismatch(src: pd.DataFrame) -> pd.DataFrame:
    tgt = src.copy()
    tgt.loc[10:10 + PRODUCT_HASH_MISMATCH - 1, "category"] = "Uncategorised"
    tgt.loc[10:10 + PRODUCT_HASH_MISMATCH - 1, "brand"] = "UnknownBrand"
    return tgt


BASE_TABLES = [
    f"{SRC}.customer_dim",
    f"{SRC}.customer_scd2",
]

need_bootstrap = BOOTSTRAP == "always" or (
    BOOTSTRAP == "auto" and not _table_exists(f"{SRC}.customer_dim")
)
if BOOTSTRAP == "never":
    missing = [t for t in BASE_TABLES if not _table_exists(t)]
    if missing:
        raise RuntimeError(
            f"bootstrap=never but missing base tables: {missing}. "
            "Use bootstrap=auto (default) or run 00_setup_trial.py first."
        )
    print("bootstrap=never — using existing base tables.")
elif need_bootstrap:
    print(f"Bootstrapping base recon tables (bootstrap={BOOTSTRAP})…")
    dim_src = _dim_src()
    scd2_src = _scd2_src()
    prod_base = _product_src()
    prod_src = prod_base.copy()
    prod_src["source_system"] = "ERP"

    _write(dim_src, f"{SRC}.customer_dim")
    _write(scd2_src, f"{SRC}.customer_scd2")
    _write(prod_src, f"{SRC}.product_dim")

    _write(_dim_tgt_hash_mismatch(dim_src), f"{SILVER}.customer_dim")
    _write(_scd2_tgt_hash_mismatch(scd2_src), f"{SILVER}.customer_scd2")
    _write(_product_tgt_hash_mismatch(prod_base), f"{SILVER}.product_dim")
    print("Base recon + silver hash-mismatch tables ready (transaction_fact → §1b).")
else:
    print("bootstrap=auto — base tables exist.")

if not need_bootstrap:
    prod_base = _product_src()
    prod_src = prod_base.copy()
    prod_src["source_system"] = "ERP"
    _write(prod_src, f"{SRC}.product_dim")
    if not _table_exists(f"{SILVER}.product_dim"):
        _write(_product_tgt_hash_mismatch(prod_base), f"{SILVER}.product_dim")
        print(f"  Created missing {SILVER}.product_dim for recon bridge tests.")
    else:
        print("  Refreshed SK-canonical recon_src.sales.product_dim (source_system=ERP).")

# SK-canonical customer_scd2 — every run (org-standard columns incl. recordStatus)
scd2_canonical = _scd2_src()
_write(scd2_canonical, f"{SRC}.customer_scd2")
print("  Refreshed SK-canonical recon_src.sales.customer_scd2 (recordStatus, effective*Date).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load golden dimensions from `recon_src.sales`

# COMMAND ----------

cust_ss = spark.table(f"{SRC}.customer_dim").toPandas()
prod_ss = spark.table(f"{SRC}.product_dim").toPandas()
scd2_ss = _normalize_scd2_df(
    spark.table(f"{SRC}.customer_scd2").toPandas(),
    f"{SRC}.customer_scd2",
)

if "source_system" not in prod_ss.columns:
    prod_ss["source_system"] = "ERP"

# Unknown member rows (SK = -1) if missing
if (cust_ss["customer_key"] >= 0).all():
    cust_ss = pd.concat([pd.DataFrame({
        "customer_key": [-1], "customer_id": [-1], "customer_name": ["Unknown"],
        "email": ["unknown@example.com"], "city": ["N/A"], "state_code": ["NA"],
        "segment": ["N/A"], "created_date": [date(1900, 1, 1)], "is_active": [True],
    }), cust_ss], ignore_index=True)

print(f"Golden dims: customer={len(cust_ss):,}, product={len(prod_ss):,}, scd2={len(scd2_ss):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1b. Golden `account_dim` + third FK on `transaction_fact`
# MAGIC
# MAGIC SK-canonical `account_dim` in **`recon_src` only**. Ensures `transaction_fact` has
# MAGIC `customer_key`, `product_key`, and `account_key` on the canonical 1 000-row grain.

# COMMAND ----------

acct_ss = _lookup_dim("account", "account_key", "account_id", N_ACCOUNTS, "AID")
_write(acct_ss, f"{SRC}.account_dim")
valid_acct_keys = acct_ss[acct_ss["account_key"] > 0]["account_key"].values

# Canonical transaction_fact (SEED=42) — identical SK gold whether bootstrap ran or not
tx_ss = _fact_src()
tx_ss["account_key"] = _rng.choice(valid_acct_keys, size=len(tx_ss)).astype("int64")
_write(tx_ss, f"{SRC}.transaction_fact")
# Keep silver transaction_fact aligned with canonical src (recon hash-mismatch scenario)
_write(_fact_tgt_hash_mismatch(tx_ss.drop(columns=["account_key"])), f"{SILVER}.transaction_fact")
print(f"transaction_fact: canonical grain + account_key (third FK), {len(tx_ss):,} rows")
n_tx = len(tx_ss)
n_ret = max(200, n_tx // 5)
ret_ss = pd.DataFrame({
    "return_id":       np.arange(1, n_ret + 1, dtype="int64"),
    "customer_key":    _rng.choice(tx_ss["customer_key"].values, n_ret),
    "product_key":     _rng.choice(tx_ss["product_key"].values, n_ret),
    "return_date":     [_to_date(d) for d in tx_ss["transaction_date"].sample(n_ret, replace=True).values],
    "return_amount":   np.round(_rng.uniform(5, 500, n_ret), 2),
    "linked_transaction_id": _rng.choice(tx_ss["transaction_id"].values, n_ret),
})

# ── scd2_activity_fact: current + historic rows (TC-REPAIR-SCD2-002) ─────────
scd2_current = scd2_ss[scd2_ss["recordStatus"] == "C"].copy()
scd2_hist    = scd2_ss[scd2_ss["recordStatus"] == "H"].copy()
n_scd2f_cur  = max(280, len(scd2_current))
n_scd2f_hist = max(120, len(scd2_hist) // 2)

pick_cur = scd2_current.sample(n=n_scd2f_cur, replace=True, random_state=SEED)
pick_hist = scd2_hist.sample(n=n_scd2f_hist, replace=True, random_state=SEED + 1)

def _event_in_window(row, fact_dates):
    vf, vt = _to_date(row["effectiveStartDate"]), _to_date(row["effectiveEndDate"])
    d = _to_date(_rng.choice(fact_dates))
    if d < vf:
        d = vf
    if d >= vt and vt != date(2999, 12, 31):
        d = vf
    return d

tx_dates = [_to_date(d) for d in tx_ss["transaction_date"].values]
cur_facts = pd.DataFrame({
    "activity_id":     np.arange(1, n_scd2f_cur + 1, dtype="int64"),
    "surrogate_key":   pick_cur["surrogate_key"].values[:n_scd2f_cur],
    "customer_id":     pick_cur["customer_id"].values[:n_scd2f_cur],
    "event_date":      [_event_in_window(r, tx_dates) for _, r in pick_cur.iterrows()],
    "activity_amount": np.round(_rng.uniform(10, 2000, n_scd2f_cur), 2),
    "activity_cohort": "CURRENT",
})
hist_facts = pd.DataFrame({
    "activity_id":     np.arange(n_scd2f_cur + 1, n_scd2f_cur + n_scd2f_hist + 1, dtype="int64"),
    "surrogate_key":   pick_hist["surrogate_key"].values[:n_scd2f_hist],
    "customer_id":     pick_hist["customer_id"].values[:n_scd2f_hist],
    "event_date":      [_event_in_window(r, tx_dates) for _, r in pick_hist.iterrows()],
    "activity_amount": np.round(_rng.uniform(10, 2000, n_scd2f_hist), 2),
    "activity_cohort": "HISTORIC",
})
scd2_fact_ss = pd.concat([cur_facts, hist_facts], ignore_index=True)

_write(ret_ss,       f"{SRC}.return_fact")
_write(scd2_fact_ss, f"{SRC}.scd2_activity_fact")
print("Golden facts return_fact + scd2_activity_fact written.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2b. Account-details SCD2 hub + lookup dimensions (multi-FK pattern)
# MAGIC
# MAGIC Models a real **SCD2 snapshot table** (`account_details_scd2`) with FKs to **separate**
# MAGIC reference dimensions — e.g. `keyAccount`, `keySubscriber`, `keyMarket`, `keyAutoPay`.
# MAGIC Reports join this hub to denormalize natural columns from each dim into cubes.
# MAGIC
# MAGIC SCD2 columns follow the org standard: `effectiveStartDate`, `effectiveEndDate`,
# MAGIC `recordStatus` (`C` = current, `H` = historic).

# COMMAND ----------

# Lookup dimension sizes (SCD1 reference tables)
N_SUBSCRIBERS      = 80
N_ACCOUNT_TYPES    = 12
N_MARKETS          = 20
N_AUTOPAY          = 8
N_BILLING_PRODUCTS = 40
N_PAYMENT_METHODS  = 15
N_ACCOUNT_BK       = N_ACCOUNTS
N_DETAIL_VERSIONS  = 2      # SCD2 versions per account (avg)

def _break_scd1_dim(golden, sk_col, nk_col, sort_cols=None):
    """Reverse-sort by NK, assign new sequential SKs. Returns (broken_df, old_to_new dict)."""
    sort_cols = sort_cols or [nk_col]
    active = golden[golden[sk_col] > 0].sort_values(sort_cols, ascending=False).reset_index(drop=True)
    old_sks = active[sk_col].tolist()
    active[sk_col] = np.arange(1, len(active) + 1)
    broken = pd.concat([golden[golden[sk_col] < 0], active], ignore_index=True)
    old_to_new = dict(zip(old_sks, active[sk_col].tolist()))
    return broken, old_to_new


def _break_scd2_dim(golden, sk_col, nk_col, vf_col, vt_col, shift_days=15):
    """Regenerate SKs and shift validity (creates AMBIGUOUS overlaps). Returns (broken_df, old_to_new)."""
    shift = timedelta(days=shift_days)
    active = golden[golden[sk_col] > 0].copy()
    old_sks = active[sk_col].tolist()
    active = active.sort_values([nk_col, vf_col]).reset_index(drop=True)
    active[sk_col] = np.arange(1, len(active) + 1)
    for col in [vf_col, vt_col]:
        active[col] = active[col].apply(
            lambda d: _to_date(d) + shift if _to_date(d) != date(2999, 12, 31) else date(2999, 12, 31)
        )
    broken = pd.concat([golden[golden[sk_col] < 0], active], ignore_index=True)
    old_to_new = dict(zip(old_sks, active[sk_col].tolist()))
    return broken, old_to_new


def _scd2_lookup_dim(name, sk_col, nk_col, codes, versions_per_code=2):
    """SCD2 reference dim (e.g. accountType) with H/C versions per business code."""
    rows = []
    sk = 1
    for code in codes:
        for v in range(versions_per_code):
            vf = date(2022, 1, 1) + timedelta(days=365 * v)
            is_current = v == versions_per_code - 1
            vt = date(2999, 12, 31) if is_current else vf + timedelta(days=364)
            rows.append({
                sk_col: sk,
                nk_col: code,
                f"{name}_name": f"{name.title()} {code} v{v + 1}",
                "effectiveStartDate": vf,
                "effectiveEndDate": vt,
                "recordStatus": "C" if is_current else "H",
            })
            sk += 1
    df = pd.DataFrame(rows)
    unknown = pd.DataFrame({
        sk_col: [-1], nk_col: ["UNKNOWN"], f"{name}_name": ["Unknown"],
        "effectiveStartDate": [date(1900, 1, 1)], "effectiveEndDate": [date(1900, 1, 1)],
        "recordStatus": ["C"],
    })
    return pd.concat([unknown, df], ignore_index=True)


subscriber_ss = _lookup_dim("subscriber", "subscriber_key", "subscriber_id", N_SUBSCRIBERS, "SUB")
account_type_ss = _lookup_dim("account_type", "account_type_key", "account_type_code", N_ACCOUNT_TYPES, "AT")
market_ss = _lookup_dim("market", "market_key", "market_code", N_MARKETS, "MKT")
autopay_ss = _lookup_dim("autopay", "autopay_key", "autopay_code", N_AUTOPAY, "AP")
billing_product_ss = _lookup_dim("billing_product", "billing_product_key", "billing_product_code", N_BILLING_PRODUCTS, "BP")
payment_method_ss = _lookup_dim("payment_method", "payment_method_key", "payment_method_code", N_PAYMENT_METHODS, "PM")

for df, tbl in [
    (subscriber_ss, "subscriber_dim"),
    (account_type_ss, "account_type_dim"),
    (market_ss, "market_dim"),
    (autopay_ss, "autopay_dim"),
    (billing_product_ss, "billing_product_dim"),
    (payment_method_ss, "payment_method_dim"),
]:
    _write(df, f"{SRC}.{tbl}")

# SCD2 lookup dim (hub FK → accountType-style SCD2, not SCD1)
AT_CODES = [f"AT{str(i).zfill(4)}" for i in range(1, N_ACCOUNT_TYPES + 1)]
account_type_scd2_ss = _scd2_lookup_dim(
    "account_type", "account_type_key", "account_type_code", AT_CODES, versions_per_code=2,
)
_write(account_type_scd2_ss, f"{SRC}.account_type_scd2")

at_keys_current = account_type_scd2_ss[account_type_scd2_ss["recordStatus"] == "C"]["account_type_key"].values
at_keys_hist    = account_type_scd2_ss[account_type_scd2_ss["recordStatus"] == "H"]["account_type_key"].values

# ── account_details_scd2: SCD2 hub with FKs to separate lookup dims ───────────
account_numbers = [f"ACC{str(i).zfill(6)}" for i in range(1, N_ACCOUNT_BK + 1)]
valid_acct_keys = acct_ss[acct_ss["account_key"] > 0]["account_key"].values
detail_rows = []
detail_sk = 1
for acc_num in account_numbers:
    for v in range(N_DETAIL_VERSIONS):
        vf = date(2023, 1, 1) + timedelta(days=180 * v + detail_sk % 30)
        vt = date(2999, 12, 31) if v == N_DETAIL_VERSIONS - 1 else vf + timedelta(days=179)
        is_current = v == N_DETAIL_VERSIONS - 1
        detail_rows.append({
            "detail_sk":            detail_sk,
            "account_number":       acc_num,
            "account_key":          int(_rng.choice(valid_acct_keys)),
            "subscriber_key":       int(_rng.integers(1, N_SUBSCRIBERS + 1)),
            "account_type_key":     int(_rng.choice(at_keys_current if is_current else at_keys_hist)),
            "market_key":           int(_rng.integers(1, N_MARKETS + 1)),
            "autopay_key":          int(_rng.integers(1, N_AUTOPAY + 1)),
            "billing_product_key":  int(_rng.integers(1, N_BILLING_PRODUCTS + 1)),
            "payment_method_key":   int(_rng.integers(1, N_PAYMENT_METHODS + 1)),
            "effectiveStartDate":   vf,
            "effectiveEndDate":     vt,
            "recordStatus":         "C" if is_current else "H",
            "current_balance":      round(float(_rng.uniform(-200, 5000)), 2),
            "credit_score":         int(_rng.integers(300, 850)),
        })
        detail_sk += 1

account_details_ss = pd.DataFrame(detail_rows)

# Registry: FK column → (golden dim df, sk col, nk col) — used for broken reload + cohort repair
LOOKUP_DIMS = [
    {"fk": "account_key",         "table": "account_dim",          "sk": "account_key",         "nk": "account_id",           "scd": 1},
    {"fk": "subscriber_key",      "table": "subscriber_dim",     "sk": "subscriber_key",      "nk": "subscriber_id",        "scd": 1},
    {"fk": "account_type_key",    "table": "account_type_scd2",  "sk": "account_type_key",    "nk": "account_type_code",    "scd": 2,
     "vf": "effectiveStartDate", "vt": "effectiveEndDate"},
    {"fk": "market_key",          "table": "market_dim",         "sk": "market_key",          "nk": "market_code",          "scd": 1},
    {"fk": "autopay_key",         "table": "autopay_dim",        "sk": "autopay_key",         "nk": "autopay_code",         "scd": 1},
    {"fk": "billing_product_key", "table": "billing_product_dim","sk": "billing_product_key", "nk": "billing_product_code", "scd": 1},
    {"fk": "payment_method_key",  "table": "payment_method_dim", "sk": "payment_method_key",  "nk": "payment_method_code",  "scd": 1},
]
_write(account_details_ss, f"{SRC}.account_details_scd2")

HUB_FK_COLS = [d["fk"] for d in LOOKUP_DIMS]
print(f"account_details_scd2: {len(account_details_ss):,} rows, {len(HUB_FK_COLS)} FK columns → separate dims")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Build broken dimensions in `recon_tgt.gold` (regenerated SKs)

# COMMAND ----------

active_cust = cust_ss[cust_ss["customer_key"] > 0].copy()
orphan_old_ids = sorted(active_cust.nlargest(ORPHAN_OLD_COUNT, "customer_id")["customer_id"].tolist())
reload_cust = active_cust[~active_cust["customer_id"].isin(orphan_old_ids)].sort_values(
    "customer_id", ascending=False
).reset_index(drop=True)
reload_cust["customer_key"] = np.arange(1, len(reload_cust) + 1)

extra_cust = pd.DataFrame({
    # High SK range — do not recycle dropped customers' golden keys (971–1000)
    "customer_key":  np.arange(
        int(active_cust["customer_key"].max()) + 100,
        int(active_cust["customer_key"].max()) + 100 + ORPHAN_NEW_COUNT,
    ),
    "customer_id":   np.arange(active_cust["customer_id"].max() + 1,
                               active_cust["customer_id"].max() + ORPHAN_NEW_COUNT + 1),
    "customer_name": [f"New Customer {i}" for i in range(1, ORPHAN_NEW_COUNT + 1)],
    "email":         [f"new{i}@example.com" for i in range(1, ORPHAN_NEW_COUNT + 1)],
    "city":          ["Boston"] * ORPHAN_NEW_COUNT,
    "state_code":    ["MA"] * ORPHAN_NEW_COUNT,
    "segment":       ["Gold"] * ORPHAN_NEW_COUNT,
    "created_date":  [TODAY] * ORPHAN_NEW_COUNT,
    "is_active":     [True] * ORPHAN_NEW_COUNT,
})

cust_db = pd.concat([
    cust_ss[cust_ss["customer_key"] < 0],
    reload_cust,
    extra_cust,
], ignore_index=True)

# Product — reverse-sort by NK, new SKs
prod_active = prod_ss[prod_ss["product_key"] > 0].sort_values(
    ["product_code", "source_system"], ascending=False
).reset_index(drop=True)
prod_active["product_key"] = np.arange(1, len(prod_active) + 1)
prod_db = pd.concat([prod_ss[prod_ss["product_key"] < 0], prod_active], ignore_index=True)

cust_nk_to_db = dict(zip(reload_cust["customer_id"], reload_cust["customer_key"]))
for _, r in extra_cust.iterrows():
    cust_nk_to_db[r["customer_id"]] = r["customer_key"]

prod_nk_to_db = dict(zip(
    zip(prod_active["product_code"], prod_active["source_system"]),
    prod_active["product_key"],
))

# SCD2 — shift validity +15 days (AMBIGUOUS overlaps)
scd2_db, scd2_old_to_new = _break_scd2_dim(
    scd2_ss, "surrogate_key", "customer_id", "effectiveStartDate", "effectiveEndDate", shift_days=15,
)

# Account dim (written in §1b; broken reload here)
acct_db, acct_old_to_new = _break_scd1_dim(acct_ss, "account_key", "account_id")
acct_nk_to_db = {
    r["account_id"]: acct_old_to_new.get(int(r["account_key"]), -1)
    for _, r in acct_ss[acct_ss["account_key"] > 0].iterrows()
}

# ── Lookup dims (subscriber, market, account_type_scd2, …) ───────────────────
lookup_golden = {}
lookup_broken = {}
ss_to_db_sk = {}

for spec in LOOKUP_DIMS:
    tbl = spec["table"]
    if tbl == "account_dim":
        golden = acct_ss
        broken = acct_db
        old_to_new = acct_old_to_new
    else:
        golden = spark.table(f"{SRC}.{tbl}").toPandas()
        if spec.get("scd", 1) == 2:
            broken, old_to_new = _break_scd2_dim(
                golden, spec["sk"], spec["nk"], spec["vf"], spec["vt"], shift_days=15,
            )
        else:
            sort_cols = [spec["nk"]] if spec["nk"] not in ("product_code",) else [spec["nk"]]
            broken, old_to_new = _break_scd1_dim(golden, spec["sk"], spec["nk"], sort_cols=sort_cols)

    lookup_golden[tbl] = golden
    lookup_broken[tbl] = broken

    def _make_resolver(o2n):
        def _resolver(ss_key):
            return o2n.get(int(ss_key), -1)
        return _resolver

    ss_to_db_sk[spec["fk"]] = _make_resolver(old_to_new)
    _write(broken, f"{TGT}.{tbl}")

_write(cust_db, f"{TGT}.customer_dim")
_write(prod_db, f"{TGT}.product_dim")
_write(scd2_db, f"{TGT}.customer_scd2")
# account_dim written in lookup loop above

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Build broken facts in `recon_tgt.gold` (mixed SK cohorts)

# COMMAND ----------

cust_key_to_id = dict(zip(
    cust_ss[cust_ss["customer_key"] > 0]["customer_key"],
    cust_ss[cust_ss["customer_key"] > 0]["customer_id"],
))
prod_key_to_code = dict(zip(prod_ss["product_key"], prod_ss["product_code"]))
acct_key_to_id = dict(zip(acct_ss["account_key"], acct_ss["account_id"]))

broken_cust_keys = set(cust_db[cust_db["customer_key"] > 0]["customer_key"].astype(int))
orphan_cust_keys = [
    int(k) for k in cust_ss[cust_ss["customer_id"].isin(orphan_old_ids)]["customer_key"]
    if int(k) not in broken_cust_keys
]
if len(orphan_cust_keys) < 10:
    base = max(broken_cust_keys, default=0) + 1000
    orphan_cust_keys.extend(range(base, base + 20))

broken_acct_keys = set(acct_db[acct_db["account_key"] > 0]["account_key"].astype(int))
orphan_acct_keys = list(range(max(broken_acct_keys, default=0) + 1000, max(broken_acct_keys, default=0) + 1020))

broken_market_keys = set(lookup_broken["market_dim"][lookup_broken["market_dim"]["market_key"] > 0]["market_key"].astype(int))
orphan_market_keys = list(range(max(broken_market_keys, default=0) + 1000, max(broken_market_keys, default=0) + 1020))


def _db_cust_sk(ss_key):
    cid = cust_key_to_id.get(int(ss_key))
    return cust_nk_to_db.get(cid, -1) if cid is not None else -1


def _db_prod_sk(ss_key):
    code = prod_key_to_code.get(int(ss_key))
    return prod_nk_to_db.get((code, "ERP"), -1) if code else -1


def _apply_cohorts(df, sk_cols, date_col, fk_resolvers=None, orphan_keys_by_col=None):
    """Apply mixed INITIAL_SS / DB_INCREMENTAL SK cohorts. fk_resolvers maps col → fn(ss_key)."""
    fk_resolvers = fk_resolvers or {}
    orphan_keys_by_col = orphan_keys_by_col or {}
    out = df.copy()
    out["load_batch"] = "INITIAL_SS"
    incr = _rng.choice(len(out), size=int(len(out) * INCREMENTAL_PCT), replace=False)
    out.loc[incr, "load_batch"] = "DB_INCREMENTAL"
    for idx, row in out.iterrows():
        if row["load_batch"] != "DB_INCREMENTAL":
            continue
        for col in sk_cols:
            if col in fk_resolvers:
                out.at[idx, col] = fk_resolvers[col](row[col])
            elif col == "customer_key":
                out.at[idx, col] = _db_cust_sk(row[col])
            elif col == "product_key":
                out.at[idx, col] = _db_prod_sk(row[col])
            elif col == "account_key":
                aid = acct_key_to_id.get(int(row[col]))
                out.at[idx, col] = acct_nk_to_db.get(aid, -1) if aid is not None else -1
    # Orphan injection: SK values that do not exist in broken dims (true pre-repair orphans)
    for col, orphan_keys in orphan_keys_by_col.items():
        if col not in sk_cols or not orphan_keys:
            continue
        oidx = _rng.choice(len(out), size=min(20, len(out)), replace=False)
        for i, idx in enumerate(oidx[:10]):
            out.at[idx, col] = orphan_keys[i % len(orphan_keys)]
            out.at[idx, "load_batch"] = "INITIAL_SS_ORPHAN"
    if "customer_key" in out.columns and "customer_id" not in out.columns:
        out["customer_id"] = out["customer_key"].map(cust_key_to_id)
    elif "customer_key" in out.columns:
        out["customer_id"] = out["customer_key"].map(cust_key_to_id)
    if "product_key" in out.columns:
        out["product_code"] = out["product_key"].map(prod_key_to_code)
        out["source_system"] = "ERP"
    # D+0 rows for alignment cutoff tests (broken copy only — golden src unchanged)
    if date_col in out.columns:
        today_idx = _rng.choice(len(out), size=max(1, int(len(out) * 0.05)), replace=False)
        out.loc[today_idx, date_col] = TODAY
    return out


tx_db = _apply_cohorts(
    tx_ss, ["customer_key", "product_key", "account_key"], "transaction_date",
    orphan_keys_by_col={"customer_key": orphan_cust_keys, "account_key": orphan_acct_keys},
)
ret_db = _apply_cohorts(
    ret_ss, ["customer_key", "product_key"], "return_date",
    orphan_keys_by_col={"customer_key": orphan_cust_keys},
)

# SCD2 fact keeps SS surrogate_key (all INITIAL_SS — wrong after dim reload)
scd2_fact_db = scd2_fact_ss.copy()
scd2_fact_db["load_batch"] = "INITIAL_SS"

# SCD2 hub — all FK columns get mixed cohorts (each FK → different dim)
hub_resolvers = {**ss_to_db_sk}
account_details_db = _apply_cohorts(
    account_details_ss, HUB_FK_COLS, "effectiveStartDate", fk_resolvers=hub_resolvers,
    orphan_keys_by_col={"market_key": orphan_market_keys},
)

_write(tx_db,              f"{TGT}.transaction_fact")
_write(ret_db,             f"{TGT}.return_fact")
_write(scd2_fact_db,       f"{TGT}.scd2_activity_fact")
_write(account_details_db, f"{TGT}.account_details_scd2")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4b. Scenario manifest (for automated tests + operator checklist)

# COMMAND ----------

SCENARIOS = [
    ("TC-SETUP-001", "Prerequisites", "recon_src + recon_tgt.gold exist", "11_run_sk_integrity_tests.py", "setup"),
    ("TC-DATA-001", "Silent SK corruption", "customer_key=1 → different customer_id src vs tgt", "11_run_sk_integrity_tests.py", "data"),
    ("TC-DATA-002", "Mixed fact cohorts", "load_batch IN (INITIAL_SS, DB_INCREMENTAL, INITIAL_SS_ORPHAN)", "11_run_sk_integrity_tests.py", "data"),
    ("TC-DATA-003", "Pre-repair orphans", "Anti-join facts/hub → dims > 0", "11_run_sk_integrity_tests.py", "data"),
    ("TC-REPAIR-SCD1-001", "SCD1 single fact", "customer_dim → transaction_fact", "surrogate_key_repair_notebook", "repair"),
    ("TC-REPAIR-SCD1-002", "SCD1 multiple facts", "customer_dim → transaction_fact + return_fact", "surrogate_key_repair_notebook", "repair"),
    ("TC-REPAIR-SCD1-003", "SCD1 composite NK", "product_dim → transaction_fact + return_fact", "surrogate_key_repair_notebook", "repair"),
    ("TC-REPAIR-SCD1-004", "SCD1 third FK on fact", "account_dim → transaction_fact.account_key (+ hub)", "surrogate_key_repair_notebook", "repair"),
    ("TC-REPAIR-SCD2-001", "SCD2 dim current facts", "customer_scd2 → scd2_activity_fact (activity_cohort=CURRENT)", "surrogate_key_repair_notebook", "repair"),
    ("TC-REPAIR-SCD2-002", "SCD2 dim historic facts", "customer_scd2 → scd2_activity_fact (activity_cohort=HISTORIC)", "surrogate_key_repair_notebook", "repair"),
    ("TC-REPAIR-SCD2-003", "SCD2 AMBIGUOUS key-map", "customer_scd2 validity +15d → AMBIGUOUS rows", "surrogate_key_repair_notebook", "repair"),
    ("TC-REPAIR-HUB-001", "Hub → SCD1 FK", "market_dim → account_details_scd2:market_key", "surrogate_key_repair_notebook", "repair"),
    ("TC-REPAIR-HUB-002", "Hub → SCD2 lookup FK", "account_type_scd2 → account_details_scd2:account_type_key + effectiveStartDate", "surrogate_key_repair_notebook", "repair"),
    ("TC-REPAIR-HUB-003", "Hub full matrix", "7 FK passes (swap hub between passes)", "surrogate_key_repair_notebook", "repair"),
    ("TC-REPAIR-MULTI-001", "Multiple tables same dim", "customer_dim → transaction_fact + return_fact one run", "surrogate_key_repair_notebook", "repair"),
    ("TC-ALIGN-001", "SQL Server alignment customer", "transaction_fact via transaction_id BK", "sqlserver_sk_alignment_notebook", "align"),
    ("TC-ALIGN-002", "SQL Server alignment product", "composite NK + two facts", "sqlserver_sk_alignment_notebook", "align"),
    ("TC-ORPHAN-001", "ORPHAN_OLD / ORPHAN_NEW", "customer_dim key-map audit", "surrogate_key_repair_notebook", "repair"),
    ("TC-POST-001", "Post-repair RI", "zero orphans on *_fixed", "surrogate_key_repair_notebook", "repair"),
    ("TC-POST-002", "Row-count invariant", "source count = *_fixed count", "surrogate_key_repair_notebook", "repair"),
]
manifest = pd.DataFrame(SCENARIOS, columns=[
    "scenario_id", "title", "description", "notebook", "category",
])
_write(manifest, f"{TGT}.sk_test_scenario_manifest")
print(f"Scenario manifest: {len(manifest)} test cases registered.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Verification

# COMMAND ----------

print("=" * 72)
print("GOLDEN (recon_src.sales)")
print("=" * 72)
for t in ["customer_dim", "product_dim", "customer_scd2", "transaction_fact",
          "return_fact", "scd2_activity_fact", "account_details_scd2",
          "subscriber_dim", "account_type_dim", "account_type_scd2", "market_dim", "autopay_dim",
          "billing_product_dim", "payment_method_dim", "account_dim"]:
    fqn = f"{SRC}.{t}"
    print(f"  {fqn:55s}  {spark.table(fqn).count():>6,} rows")

print("\nBROKEN (recon_tgt.gold)")
print("=" * 72)
for t in spark.sql("SHOW TABLES IN recon_tgt.gold").collect():
    fqn = f"recon_tgt.gold.{t.tableName}"
    print(f"  {fqn:55s}  {spark.table(fqn).count():>6,} rows")

# COMMAND ----------

# Silent corruption: SK=1 means different customers
ss1 = spark.sql(f"SELECT customer_key, customer_id, customer_name FROM {SRC}.customer_dim WHERE customer_key = 1").collect()
db1 = spark.sql(f"SELECT customer_key, customer_id, customer_name FROM {TGT}.customer_dim WHERE customer_key = 1").collect()
print("\nSILENT CORRUPTION (customer_key = 1)")
if ss1 and db1:
    print(f"  Golden:  id={ss1[0]['customer_id']}  {ss1[0]['customer_name']}")
    print(f"  Broken:  id={db1[0]['customer_id']}  {db1[0]['customer_name']}")

# COMMAND ----------

display(spark.table(f"{TGT}.transaction_fact").groupBy("load_batch").count().orderBy("load_batch"))

# COMMAND ----------

orphans = spark.sql(f"""
    SELECT COUNT(*) AS c FROM {TGT}.transaction_fact f
    LEFT ANTI JOIN {TGT}.customer_dim d ON f.customer_key = d.customer_key
    WHERE f.customer_key <> -1
""").first()["c"]
print(f"Orphan transaction_fact.customer_key rows (pre-repair): {orphans:,}  (expected > 0)")

# COMMAND ----------

# Multi-FK hub: orphan count per FK column (each points to a different dim)
hub_orphan_rows = []
for spec in LOOKUP_DIMS:
    fk, tbl, sk = spec["fk"], spec["table"], spec["sk"]
    c = spark.sql(f"""
        SELECT COUNT(*) AS c FROM {TGT}.account_details_scd2 f
        LEFT ANTI JOIN {TGT}.{tbl} d ON f.{fk} = d.{sk}
        WHERE f.{fk} <> -1
    """).first()["c"]
    hub_orphan_rows.append((fk, tbl, c))
print("\nOrphan account_details_scd2 FK rows by column (pre-repair):")
display(spark.createDataFrame(hub_orphan_rows, ["fk_column", "dim_table", "orphan_rows"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Repair notebook — multi-FK SCD2 hub workflow
# MAGIC
# MAGIC `account_details_scd2` has **7 FK columns → 7 separate dimensions**. The repair notebook
# MAGIC runs **once per dimension**, re-keying **one FK column** per pass. Swap the hub table
# MAGIC after each pass (or point the next run at `account_details_scd2_fixed`).
# MAGIC
# MAGIC **Example — repair `market_dim`, re-key only `market_key`:**
# MAGIC ```
# MAGIC legacy_dim_table     = market_dim
# MAGIC legacy_sk_col        = market_key
# MAGIC legacy_nk_cols       = market_code
# MAGIC new_dim_table        = market_dim
# MAGIC new_sk_col           = market_key
# MAGIC new_nk_cols          = market_code
# MAGIC fact_tables          = recon_tgt.gold.account_details_scd2:market_key
# MAGIC ```
# MAGIC Repeat for `subscriber_key` → `subscriber_dim`, `autopay_key` → `autopay_dim`, etc.
# MAGIC
# MAGIC Full matrix: `sk_integrity_test_guide.md` § Multi-FK SCD2 hub.
# MAGIC
# MAGIC **Key-map repair — customer_dim (SCD1), two facts:**
# MAGIC ```
# MAGIC foreign_catalog      = recon_src
# MAGIC legacy_schema        = sales
# MAGIC legacy_dim_table     = customer_dim
# MAGIC legacy_sk_col        = customer_key
# MAGIC legacy_nk_cols       = customer_id
# MAGIC new_catalog_schema   = recon_tgt.gold
# MAGIC new_dim_table        = customer_dim
# MAGIC new_sk_col           = customer_key
# MAGIC new_nk_cols          = customer_id
# MAGIC scd_type             = 1
# MAGIC fact_tables          = recon_tgt.gold.transaction_fact:customer_key,recon_tgt.gold.return_fact:customer_key
# MAGIC staging_schema       = recon_tgt.staging
# MAGIC keymap_schema        = recon_tgt.keymap
# MAGIC dry_run              = true
# MAGIC ```
# MAGIC
# MAGIC **SQL Server alignment — transaction_fact:**
# MAGIC ```
# MAGIC foreign_catalog      = recon_src
# MAGIC legacy_schema        = sales
# MAGIC legacy_dim_table     = customer_dim
# MAGIC legacy_sk_col        = customer_key
# MAGIC legacy_nk_cols       = customer_id
# MAGIC new_catalog_schema   = recon_tgt.gold
# MAGIC new_dim_table        = customer_dim
# MAGIC new_sk_col           = customer_key
# MAGIC new_nk_cols          = customer_id
# MAGIC fact_tables          = recon_tgt.gold.transaction_fact:customer_key
# MAGIC legacy_fact_tables   = transaction_fact
# MAGIC legacy_fact_fk_cols  = customer_key
# MAGIC legacy_fact_bk_cols  = transaction_id
# MAGIC new_fact_bk_cols     = transaction_id
# MAGIC fact_event_date_cols = transaction_date
# MAGIC fact_nk_cols         = customer_id
# MAGIC cutoff_mode          = d_minus_1
# MAGIC staging_schema       = recon_tgt.staging
# MAGIC keymap_schema        = recon_tgt.keymap
# MAGIC ```
# MAGIC
# MAGIC ## 7. Repair notebook widget presets (single-dim / single-FK)
