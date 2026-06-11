# Databricks notebook source
# TITLE: Recon Generator — Bridge Table Test Setup
#
# Adds product dimension tables to both catalogs so you can test the new
# "Bridge table / Join Resolver" feature in the Recon Generator UI.
#
# After 00_setup_trial.py created:
#   recon_src.sales.transaction_fact   (product_key int, customer_key int, ...)
#   recon_tgt.silver.transaction_fact  (same columns)
#
# This notebook creates:
#   recon_src.sales.product_dim        (50 rows — source)
#   recon_tgt.silver.product_dim       (50 rows — 5 rows differ → hash mismatch)
#
# These let you test two independent bridge-table resolvers on transaction_fact:
#
#   Resolver A  surrogate: customer_key  bridge: customer_dim  (already exists!)
#               natural cols: customer_id, customer_name
#
#   Resolver B  surrogate: product_key   bridge: product_dim   (created here)
#               natural cols: product_code, product_name, category
#
# ─── Prerequisites ────────────────────────────────────────────────────────────
#  • 00_setup_trial.py must have run (catalogs + schemas + transaction_fact exist)
#  • DBR 13+, Unity Catalog enabled
# ──────────────────────────────────────────────────────────────────────────────

# COMMAND ----------

# ── Step 1: Verify prerequisites ──────────────────────────────────────────────

required = [
    "recon_src.sales.transaction_fact",
    "recon_tgt.silver.transaction_fact",
    "recon_src.sales.customer_dim",
    "recon_tgt.silver.customer_dim",
]
for tbl in required:
    try:
        cnt = spark.table(tbl).count()
        print(f"  ✓ {tbl}  ({cnt:,} rows)")
    except Exception as exc:
        raise RuntimeError(
            f"Required table '{tbl}' not found — please run 00_setup_trial.py first.\n{exc}"
        )

print("\nPrerequisites OK.")

# COMMAND ----------

# ── Step 2: Confirm product_key range in transaction_fact ─────────────────────
# 00_setup_trial.py generates product_key in range [1, 50].
# We will create exactly 50 product_dim rows to cover every key.

from pyspark.sql import functions as F

pk_stats = (
    spark.table("recon_src.sales.transaction_fact")
    .select(F.min("product_key").alias("min_pk"), F.max("product_key").alias("max_pk"))
    .collect()[0]
)
print(f"transaction_fact.product_key range: {pk_stats.min_pk} – {pk_stats.max_pk}")
assert pk_stats.min_pk >= 1 and pk_stats.max_pk <= 50, \
    "Unexpected product_key range — adjust N_PRODUCTS below."

# COMMAND ----------

# ── Step 3: Generate product_dim data ─────────────────────────────────────────

import numpy as np
import pandas as pd

SEED             = 42
N_PRODUCTS       = 50          # product_key range in transaction_fact is [1, 50]
HASH_MISMATCH    = 5           # 5 rows differ in target (small — easier to spot)

_CATEGORIES = ["Electronics", "Clothing", "Food & Beverage", "Home & Garden", "Sports"]
_BRANDS     = ["AlphaBrand", "BetaCo", "GammaTech", "DeltaWorks", "EpsilonGoods"]

_rng = np.random.default_rng(SEED)


def _product_src() -> pd.DataFrame:
    n = N_PRODUCTS
    cats  = [_CATEGORIES[i % len(_CATEGORIES)] for i in range(n)]
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
    """5 rows have a different category and brand in the target."""
    tgt = src.copy()
    tgt.loc[10:10 + HASH_MISMATCH - 1, "category"] = "Uncategorised"
    tgt.loc[10:10 + HASH_MISMATCH - 1, "brand"]    = "UnknownBrand"
    return tgt


prod_src = _product_src()
prod_tgt = _product_tgt_hash_mismatch(prod_src)

print(f"Generated {len(prod_src)} source rows, {len(prod_tgt)} target rows.")
print(f"Mismatch rows (product_key 11-{10 + HASH_MISMATCH}): category='Uncategorised', brand='UnknownBrand'")

# COMMAND ----------

# ── Step 4: Write product_dim tables ──────────────────────────────────────────

def _write(df: pd.DataFrame, full_table: str) -> None:
    spark.createDataFrame(df).write.format("delta").mode("overwrite").saveAsTable(full_table)
    cnt = spark.table(full_table).count()
    print(f"  {full_table:55s}  {cnt:>4} rows")


print("Writing product_dim tables…")
_write(prod_src, "recon_src.sales.product_dim")
_write(prod_tgt, "recon_tgt.silver.product_dim")

# COMMAND ----------

# ── Step 5: Verify all tables ─────────────────────────────────────────────────

rows = []
for full_table in [
    "recon_src.sales.transaction_fact",
    "recon_src.sales.customer_dim",
    "recon_src.sales.product_dim",
    "recon_tgt.silver.transaction_fact",
    "recon_tgt.silver.customer_dim",
    "recon_tgt.silver.product_dim",
]:
    cnt = spark.table(full_table).count()
    catalog, schema, table = full_table.split(".")
    rows.append((catalog, schema, table, cnt))

display(spark.createDataFrame(rows, ["catalog", "schema", "table_name", "num_rows"]))

# COMMAND ----------

# ── Step 6: Quick sanity — show mismatch rows in product_dim ──────────────────

src_pd = spark.table("recon_src.sales.product_dim").toPandas()
tgt_pd = spark.table("recon_tgt.silver.product_dim").toPandas()

merged = src_pd.merge(tgt_pd, on="product_key", suffixes=("_src", "_tgt"))
diffs  = merged[merged["category_src"] != merged["category_tgt"]]
print(f"\nproduct_dim rows that differ between src and tgt ({len(diffs)} rows):")
display(spark.createDataFrame(
    diffs[["product_key", "product_code_src", "category_src", "category_tgt", "brand_src", "brand_tgt"]]
))

# COMMAND ----------

# ── Step 7: Test instructions ─────────────────────────────────────────────────

print("""
╔══════════════════════════════════════════════════════════════════════════════╗
║          Recon Generator — Bridge Table Test Instructions                  ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                            ║
║  MAIN TABLE (both sides):   transaction_fact                               ║
║  SOURCE catalog/schema:     recon_src / sales                              ║
║  TARGET catalog/schema:     recon_tgt / silver                             ║
║                                                                            ║
╠═ Bridge Resolver A — customer_dim (table already existed) ══════════════════╣
║                                                                            ║
║  Surrogate key (main table) : customer_key                                 ║
║  Bridge table               : recon_src.sales.customer_dim  (source side) ║
║                               recon_tgt.silver.customer_dim (target side) ║
║  Bridge join key            : customer_key                                 ║
║  Natural cols to include    : customer_id, customer_name                   ║
║                                                                            ║
║  Expected: customer_dim is identical src=tgt → no hash mismatch here.     ║
║                                                                            ║
╠═ Bridge Resolver B — product_dim (created by this notebook) ════════════════╣
║                                                                            ║
║  Surrogate key (main table) : product_key                                  ║
║  Bridge table               : recon_src.sales.product_dim   (source side) ║
║                               recon_tgt.silver.product_dim  (target side) ║
║  Bridge join key            : product_key                                  ║
║  Natural cols to include    : product_code, product_name, category, brand  ║
║                                                                            ║
║  Expected: 5 rows differ (product_key 11-15) in category + brand.         ║
║            The hash comparison should surface these mismatches.            ║
║                                                                            ║
╠═ Column selection notes ════════════════════════════════════════════════════╣
║                                                                            ║
║  • Include in hash: transaction_id, transaction_date, transaction_amount,  ║
║    quantity, net_amount  +  the natural keys from both resolvers           ║
║  • Exclude from hash (surrogates): customer_key, product_key               ║
║    (just don't tick them in the column selector — they stay in the DF      ║
║     for the JOIN but don't enter the hash)                                 ║
║                                                                            ║
╚══════════════════════════════════════════════════════════════════════════════╝
""")
