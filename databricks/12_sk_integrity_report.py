# Databricks notebook source
# Run this cell first — parameter dropdowns appear at the top of the notebook.
dbutils.widgets.dropdown(
    "report_mode", "pre_repair", ["pre_repair", "post_repair", "compare"],
    "pre_repair | post_repair | compare (both)",
)
dbutils.widgets.text("golden_catalog_schema", "recon_src.sales", "Golden catalog.schema")
dbutils.widgets.text("broken_catalog_schema", "recon_tgt.gold", "Broken / fixed catalog.schema")
dbutils.widgets.text("unknown_sk", "-1", "Unknown-member SK (excluded from RI checks)")

# COMMAND ----------

# MAGIC %md
# MAGIC # SK Integrity — Pre / Post Repair Report (per SK)
# MAGIC
# MAGIC Lists **referential integrity and silent-corruption issues** grouped by surrogate key,
# MAGIC with **every fact/hub table** that references each problematic SK.
# MAGIC
# MAGIC | Mode | Reads |
# MAGIC |---|---|
# MAGIC | `pre_repair` | Broken facts/hubs → broken dims; golden dim for NK comparison |
# MAGIC | `post_repair` | `*_fixed` tables → broken dims (after repair, before swap) |
# MAGIC | `compare` | Side-by-side pre vs post orphan counts per SK |
# MAGIC
# MAGIC **Prerequisites:** `10_setup_sk_integrity_test_env.py` (pre); repair notebook `*_fixed` outputs (post).
# MAGIC
# MAGIC **Docs:** `SK_INTEGRITY_TEST_CASES.md`, `RUNBOOK.md`

# COMMAND ----------

from pyspark.sql import functions as F

REPORT_MODE = dbutils.widgets.get("report_mode")
GOLD = dbutils.widgets.get("golden_catalog_schema").strip()
TGT = dbutils.widgets.get("broken_catalog_schema").strip()
UNKNOWN_SK = int(dbutils.widgets.get("unknown_sk"))

print(f"Mode={REPORT_MODE}  golden={GOLD}  target={TGT}  unknown_sk={UNKNOWN_SK}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## FK registry (synthetic test environment)
# MAGIC
# MAGIC Extend this list for production tables. Each entry: dimension + all fact/hub FK columns.

# COMMAND ----------

# dim_table, sk_col, nk_cols (list), [(fact_table, fk_col), ...]
FK_REGISTRY = [
    ("customer_dim", "customer_key", ["customer_id"], [
        ("transaction_fact", "customer_key"),
        ("return_fact", "customer_key"),
    ]),
    ("product_dim", "product_key", ["product_code", "source_system"], [
        ("transaction_fact", "product_key"),
        ("return_fact", "product_key"),
    ]),
    ("account_dim", "account_key", ["account_id"], [
        ("transaction_fact", "account_key"),
        ("account_details_scd2", "account_key"),
    ]),
    ("customer_scd2", "surrogate_key", ["customer_id"], [
        ("scd2_activity_fact", "surrogate_key"),
    ]),
    ("subscriber_dim", "subscriber_key", ["subscriber_id"], [
        ("account_details_scd2", "subscriber_key"),
    ]),
    ("account_type_scd2", "account_type_key", ["account_type_code"], [
        ("account_details_scd2", "account_type_key"),
    ]),
    ("market_dim", "market_key", ["market_code"], [
        ("account_details_scd2", "market_key"),
    ]),
    ("autopay_dim", "autopay_key", ["autopay_code"], [
        ("account_details_scd2", "autopay_key"),
    ]),
    ("billing_product_dim", "billing_product_key", ["billing_product_code"], [
        ("account_details_scd2", "billing_product_key"),
    ]),
    ("payment_method_dim", "payment_method_key", ["payment_method_code"], [
        ("account_details_scd2", "payment_method_key"),
    ]),
]


def _table_exists(fqn: str) -> bool:
    try:
        spark.table(fqn).limit(1).collect()
        return True
    except Exception:
        return False


def _resolve_fact_table(fact_table: str, use_fixed: bool) -> str | None:
    base = f"{TGT}.{fact_table}"
    if not use_fixed:
        return base if _table_exists(base) else None
    fixed = f"{TGT}.{fact_table}_fixed"
    if _table_exists(fixed):
        return fixed
    return base if _table_exists(base) else None


def _nk_expr(alias: str, nk_cols: list[str]):
    if len(nk_cols) == 1:
        return F.col(f"{alias}.{nk_cols[0]}").cast("string")
    return F.concat_ws("|", *[F.col(f"{alias}.{c}").cast("string") for c in nk_cols])


def _dim_nk_map(catalog_schema: str, dim_table: str, sk_col: str, nk_cols: list[str]):
    fqn = f"{catalog_schema}.{dim_table}"
    if not _table_exists(fqn):
        return None
    d = spark.table(fqn).alias("d")
    return (
        d.filter(F.col(sk_col) != F.lit(UNKNOWN_SK))
        .select(
            F.col(sk_col).alias("sk"),
            _nk_expr("d", nk_cols).alias("nk"),
        )
        .distinct()
    )


def _fact_fk_usage(fact_fqn: str, fk_col: str, sk_col: str):
    if not _table_exists(fact_fqn):
        return None
    f = spark.table(fact_fqn)
    if fk_col not in f.columns:
        return None
    cohort_col = "load_batch" if "load_batch" in f.columns else F.lit(None).cast("string")
    if isinstance(cohort_col, str):
        cohort_col = F.col(cohort_col)
    return (
        f.filter(F.col(fk_col) != F.lit(UNKNOWN_SK))
        .groupBy(F.col(fk_col).alias("sk"))
        .agg(
            F.count(F.lit(1)).alias("row_count"),
            F.collect_set(cohort_col).alias("load_batches"),
        )
        .withColumn("fact_table", F.lit(fact_fqn.split(".")[-1]))
        .withColumn("fk_column", F.lit(fk_col))
    )


def _build_sk_issues(use_fixed: bool, label: str):
    """Return detail + summary DataFrames for one pass (pre or post)."""
    detail_parts = []
    summary_parts = []

    for dim_table, sk_col, nk_cols, fact_pairs in FK_REGISTRY:
        dim_fqn = f"{TGT}.{dim_table}"
        gold_fqn = f"{GOLD}.{dim_table}"
        if not _table_exists(dim_fqn):
            print(f"  skip dim (missing): {dim_fqn}")
            continue

        tgt_map = _dim_nk_map(TGT, dim_table, sk_col, nk_cols).alias("t")
        gold_map = _dim_nk_map(GOLD, dim_table, sk_col, nk_cols).alias("g") if _table_exists(gold_fqn) else None

        # Collect FK usage across all referencing tables
        usage_parts = []
        for fact_table, fk_col in fact_pairs:
            fact_fqn = _resolve_fact_table(fact_table, use_fixed)
            if not fact_fqn:
                continue
            u = _fact_fk_usage(fact_fqn, fk_col, sk_col)
            if u is not None:
                usage_parts.append(u)

        if not usage_parts:
            continue

        usage = usage_parts[0]
        for u in usage_parts[1:]:
            usage = usage.unionByName(u)

        usage_agg = (
            usage.groupBy("sk")
            .agg(
                F.sum("row_count").alias("total_fact_rows"),
                F.collect_list(F.struct("fact_table", "fk_column", "row_count", "load_batches")).alias("fact_refs"),
            )
        )

        dim_sks = (
            spark.table(dim_fqn)
            .filter(F.col(sk_col) != F.lit(UNKNOWN_SK))
            .select(F.col(sk_col).alias("sk"))
            .distinct()
        )

        issues = (
            usage_agg.alias("u")
            .join(dim_sks.alias("d"), "sk", "left")
            .withColumn(
                "issue_type",
                F.when(F.col("d.sk").isNull(), F.lit("FK_ORPHAN")).otherwise(F.lit("FK_PRESENT")),
            )
            .select("u.sk", "u.total_fact_rows", "u.fact_refs", "issue_type")
        )

        if gold_map is not None:
            joined = (
                issues.alias("i")
                .join(tgt_map, "sk", "left")
                .join(gold_map, "sk", "left")
                .withColumn(
                    "golden_nk",
                    F.col("g.nk"),
                )
                .withColumn(
                    "broken_nk",
                    F.col("t.nk"),
                )
                .withColumn(
                    "issue_type",
                    F.when(F.col("issue_type") == "FK_ORPHAN", F.lit("FK_ORPHAN"))
                    .when(F.col("g.nk").isNull() & F.col("t.nk").isNotNull(), F.lit("ORPHAN_NEW_DIM_ONLY"))
                    .when(F.col("t.nk").isNull() & F.col("g.nk").isNotNull(), F.lit("ORPHAN_OLD_DIM_MISSING"))
                    .when(
                        F.col("g.nk").isNotNull() & F.col("t.nk").isNotNull() & (F.col("g.nk") != F.col("t.nk")),
                        F.lit("SILENT_CORRUPTION"),
                    )
                    .when(
                        F.col("g.nk").isNotNull() & F.col("t.nk").isNotNull() & (F.col("g.nk") == F.col("t.nk")),
                        F.lit("NK_MATCH"),
                    )
                    .otherwise(F.col("issue_type")),
                )
            )
        else:
            joined = (
                issues.alias("i")
                .join(tgt_map, "sk", "left")
                .withColumn("golden_nk", F.lit(None).cast("string"))
                .withColumn("broken_nk", F.col("t.nk"))
            )

        detail = (
            joined
            .filter(F.col("issue_type") != "NK_MATCH")
            .withColumn("pass", F.lit(label))
            .withColumn("dimension", F.lit(dim_table))
            .withColumn("sk_column", F.lit(sk_col))
            .select(
                "pass", "dimension", "sk_column", "sk", "issue_type",
                "golden_nk", "broken_nk", "total_fact_rows", "fact_refs",
            )
            .orderBy("dimension", "issue_type", F.desc("total_fact_rows"))
        )

        summary = (
            joined
            .filter(F.col("issue_type") != "NK_MATCH")
            .withColumn("pass", F.lit(label))
            .withColumn("dimension", F.lit(dim_table))
            .withColumn("sk_column", F.lit(sk_col))
            .groupBy("pass", "dimension", "sk_column", "issue_type")
            .agg(
                F.count(F.lit(1)).alias("sk_count"),
                F.sum("total_fact_rows").alias("fact_rows_affected"),
            )
            .orderBy("dimension", "issue_type")
        )

        detail_parts.append(detail)
        summary_parts.append(summary)

    if not detail_parts:
        empty_detail = spark.createDataFrame(
            [], "pass STRING, dimension STRING, sk_column STRING, sk BIGINT, issue_type STRING, "
            "golden_nk STRING, broken_nk STRING, total_fact_rows LONG, fact_refs ARRAY<STRUCT<fact_table:STRING,fk_column:STRING,row_count:LONG,load_batches:ARRAY<STRING>>>"
        )
        empty_summary = spark.createDataFrame(
            [], "dimension STRING, sk_column STRING, issue_type STRING, sk_count LONG, fact_rows_affected LONG, pass STRING"
        )
        return empty_detail, empty_summary

    all_detail = detail_parts[0]
    for d in detail_parts[1:]:
        all_detail = all_detail.unionByName(d)
    all_summary = summary_parts[0]
    for s in summary_parts[1:]:
        all_summary = all_summary.unionByName(s)
    return all_detail, all_summary


def _flatten_fact_refs(detail_df):
    """One row per (SK, fact_table, fk_column) for easy filtering."""
    return (
        detail_df
        .withColumn("ref", F.explode("fact_refs"))
        .select(
            "pass", "dimension", "sk_column", "sk", "issue_type",
            "golden_nk", "broken_nk",
            F.col("ref.fact_table").alias("fact_table"),
            F.col("ref.fk_column").alias("fk_column"),
            F.col("ref.row_count").alias("row_count"),
            F.col("ref.load_batches").alias("load_batches"),
        )
        .orderBy("dimension", "sk", "fact_table")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run report

# COMMAND ----------

pre_detail = pre_summary = post_detail = post_summary = None

if REPORT_MODE in ("pre_repair", "compare"):
    print("=" * 72)
    print("PRE-REPAIR (broken facts → broken dims, NK vs golden)")
    print("=" * 72)
    pre_detail, pre_summary = _build_sk_issues(use_fixed=False, label="pre_repair")
    n_pre = pre_detail.count()
    print(f"Problematic SK entries: {n_pre:,}")
    display(pre_summary)
    display(pre_detail)
    display(_flatten_fact_refs(pre_detail))

if REPORT_MODE in ("post_repair", "compare"):
    print("\n" + "=" * 72)
    print("POST-REPAIR (*_fixed facts → broken dims)")
    print("=" * 72)
    post_detail, post_summary = _build_sk_issues(use_fixed=True, label="post_repair")
    n_post = post_detail.count()
    print(f"Problematic SK entries: {n_post:,}")
    if n_post == 0:
        print("No RI issues on *_fixed tables (or no *_fixed tables found — run repair first).")
    display(post_summary)
    display(post_detail)
    display(_flatten_fact_refs(post_detail))

if REPORT_MODE == "compare" and pre_detail is not None and post_detail is not None:
    print("\n" + "=" * 72)
    print("COMPARE — orphan / corruption SK counts pre vs post")
    print("=" * 72)
    pre_keys = (
        pre_detail.select("dimension", "sk_column", "sk", "issue_type", "total_fact_rows")
        .withColumnRenamed("total_fact_rows", "pre_fact_rows")
    )
    post_keys = (
        post_detail.select("dimension", "sk_column", "sk", "issue_type", "total_fact_rows")
        .withColumnRenamed("total_fact_rows", "post_fact_rows")
    )
    compare = (
        pre_keys.alias("p")
        .join(post_keys.alias("q"), ["dimension", "sk_column", "sk", "issue_type"], "full_outer")
        .withColumn(
            "status",
            F.when(F.col("post_fact_rows").isNull(), F.lit("FIXED (gone post-repair)"))
            .when(F.col("pre_fact_rows").isNull(), F.lit("NEW post-repair"))
            .when(F.col("post_fact_rows") == 0, F.lit("FIXED"))
            .when(F.col("post_fact_rows") < F.col("pre_fact_rows"), F.lit("IMPROVED"))
            .otherwise(F.lit("UNCHANGED")),
        )
        .orderBy("dimension", "sk", "issue_type")
    )
    display(compare)

    fixed_count = compare.filter(F.col("status").like("FIXED%")).count()
    open_count = post_detail.count() if post_detail else 0
    print(f"SK issue rows fixed/removed: {fixed_count:,}  |  Still open post-repair: {open_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optional: write report snapshot
# MAGIC
# MAGIC Uncomment to persist the latest run for auditing.

# COMMAND ----------

snapshot_schema = "recon_tgt.staging"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {snapshot_schema}")
if pre_detail is not None:
    pre_detail.write.format("delta").mode("overwrite").saveAsTable(f"{snapshot_schema}.sk_integrity_report_pre")
if post_detail is not None:
    post_detail.write.format("delta").mode("overwrite").saveAsTable(f"{snapshot_schema}.sk_integrity_report_post")
print("Report snapshots written.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Issue types
# MAGIC
# MAGIC | `issue_type` | Meaning |
# MAGIC |---|---|
# MAGIC | `FK_ORPHAN` | Fact/hub FK value not in broken dimension |
# MAGIC | `SILENT_CORRUPTION` | FK exists in broken dim but **natural key ≠ golden** (wrong-join succeeds) |
# MAGIC | `ORPHAN_OLD_DIM_MISSING` | Golden NK exists for SK; row dropped from broken dim |
# MAGIC | `ORPHAN_NEW_DIM_ONLY` | SK only in broken dim (new member after reload) |
# MAGIC
# MAGIC **Note:** Post-repair mode reads `*_fixed` tables when present. After table swap, re-run with
# MAGIC `report_mode=pre_repair` on the swapped tables (or point `broken_catalog_schema` at production).
