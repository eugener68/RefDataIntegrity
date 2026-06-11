# Databricks notebook source
# MAGIC %md
# MAGIC # SK Integrity — Automated Test Runner
# MAGIC
# MAGIC Validates synthetic test data and (optionally) post-repair state.
# MAGIC
# MAGIC **Prerequisites:** `00_setup_trial.py`, `02_setup_bridge_test.py`, `10_setup_sk_integrity_test_env.py`
# MAGIC
# MAGIC **Docs:** `SK_INTEGRITY_TEST_CASES.md`, `RUNBOOK.md`

# COMMAND ----------

dbutils.widgets.dropdown("phase", "setup", ["setup", "post_repair"], "Test phase")
dbutils.widgets.dropdown("fail_fast", "false", ["true", "false"], "Stop on first failure")

# COMMAND ----------

PHASE = dbutils.widgets.get("phase")
FAIL_FAST = dbutils.widgets.get("fail_fast") == "true"
SRC = "recon_src.sales"
TGT = "recon_tgt.gold"

results = []


def _check(tc_id, name, condition, detail=""):
    status = "PASS" if condition else "FAIL"
    results.append((tc_id, name, status, detail))
    icon = "✅" if condition else "❌"
    print(f"{icon} [{tc_id}] {name}" + (f" — {detail}" if detail else ""))
    if not condition and FAIL_FAST:
        raise RuntimeError(f"{tc_id} failed: {name} {detail}")


def _table_exists(fqn):
    try:
        spark.table(fqn).limit(1).collect()
        return True
    except Exception:
        return False


def _count(fqn):
    return spark.table(fqn).count()


print(f"Phase: {PHASE}\n{'=' * 60}")

# COMMAND ----------

if PHASE == "setup":
    required_src = [
        "customer_dim", "product_dim", "customer_scd2", "transaction_fact",
        "return_fact", "scd2_activity_fact", "account_details_scd2",
        "account_type_scd2", "market_dim", "subscriber_dim",
    ]
    required_tgt = [
        "customer_dim", "product_dim", "customer_scd2", "transaction_fact",
        "return_fact", "scd2_activity_fact", "account_details_scd2",
        "account_type_scd2", "market_dim", "account_dim", "sk_test_scenario_manifest",
    ]

    for tbl in required_src:
        _check("TC-SETUP-001", f"Golden {SRC}.{tbl}", _table_exists(f"{SRC}.{tbl}"))

    for tbl in required_tgt:
        _check("TC-SETUP-001", f"Broken {TGT}.{tbl}", _table_exists(f"{TGT}.{tbl}"))

    manifest_n = _count(f"{TGT}.sk_test_scenario_manifest")
    _check("TC-SETUP-001", "Scenario manifest", manifest_n >= 15, f"{manifest_n} scenarios")

    row = spark.sql(f"""
        SELECT s.customer_id AS src_id, t.customer_id AS tgt_id
        FROM {SRC}.customer_dim s
        JOIN {TGT}.customer_dim t ON s.customer_key = t.customer_key
        WHERE s.customer_key = 1
    """).collect()
    if row:
        _check("TC-DATA-001", "Silent corruption SK=1", row[0]["src_id"] != row[0]["tgt_id"],
               f"src={row[0]['src_id']} tgt={row[0]['tgt_id']}")
    else:
        _check("TC-DATA-001", "Silent corruption SK=1", False, "missing SK=1")

    batches = {r.load_batch for r in spark.table(f"{TGT}.transaction_fact").select("load_batch").distinct().collect()}
    _check("TC-DATA-002", "INITIAL_SS cohort", "INITIAL_SS" in batches)
    _check("TC-DATA-002", "DB_INCREMENTAL cohort", "DB_INCREMENTAL" in batches)
    _check("TC-DATA-002", "INITIAL_SS_ORPHAN cohort", "INITIAL_SS_ORPHAN" in batches)

    orphans_tx = spark.sql(f"""
        SELECT COUNT(*) c FROM {TGT}.transaction_fact f
        LEFT ANTI JOIN {TGT}.customer_dim d ON f.customer_key = d.customer_key
        WHERE f.customer_key <> -1
    """).first()["c"]
    _check("TC-DATA-003", "Pre-repair tx orphans", orphans_tx > 0, f"{orphans_tx:,}")

    hub_orphans = spark.sql(f"""
        SELECT COUNT(*) c FROM {TGT}.account_details_scd2 f
        LEFT ANTI JOIN {TGT}.market_dim d ON f.market_key = d.market_key
        WHERE f.market_key <> -1
    """).first()["c"]
    _check("TC-DATA-003", "Pre-repair hub market orphans", hub_orphans > 0, f"{hub_orphans:,}")

    tx_cols = spark.table(f"{SRC}.transaction_fact").columns
    _check("TC-REPAIR-SCD1-004", "transaction_fact.account_key column", "account_key" in tx_cols)
    acct_n = _count(f"{SRC}.account_dim")
    _check("TC-REPAIR-SCD1-004", "account_dim SK fixture", acct_n >= 120, f"{acct_n} rows")
    orphans_acct = spark.sql(f"""
        SELECT COUNT(*) c FROM {TGT}.transaction_fact f
        LEFT ANTI JOIN {TGT}.account_dim d ON f.account_key = d.account_key
        WHERE f.account_key <> -1
    """).first()["c"]
    _check("TC-REPAIR-SCD1-004", "Pre-repair account_key orphans", orphans_acct > 0, f"{orphans_acct:,}")

    if "activity_cohort" in spark.table(f"{SRC}.scd2_activity_fact").columns:
        hist_n = spark.table(f"{SRC}.scd2_activity_fact").filter("activity_cohort = 'HISTORIC'").count()
        cur_n = spark.table(f"{SRC}.scd2_activity_fact").filter("activity_cohort = 'CURRENT'").count()
        _check("TC-REPAIR-SCD2-002", "Historic activity rows", hist_n > 0, f"{hist_n:,}")
        _check("TC-REPAIR-SCD2-001", "Current activity rows", cur_n > 0, f"{cur_n:,}")

        bad_hist = spark.sql(f"""
            SELECT COUNT(*) c FROM {SRC}.scd2_activity_fact f
            JOIN {SRC}.customer_scd2 d ON f.surrogate_key = d.surrogate_key
            WHERE f.activity_cohort = 'HISTORIC'
              AND (f.event_date < d.effectiveStartDate OR f.event_date >= d.effectiveEndDate)
        """).first()["c"]
        _check("TC-REPAIR-SCD2-002", "Historic event_date in window", bad_hist == 0)
    else:
        _check("TC-REPAIR-SCD2-002", "activity_cohort column", False, "re-run 10_setup")

    for tbl in ["customer_scd2", "account_type_scd2"]:
        stats = {r.recordStatus: r["count"] for r in spark.table(f"{SRC}.{tbl}").groupBy("recordStatus").count().collect()}
        _check("TC-REPAIR-SCD2-003", f"{tbl} historic versions", stats.get("H", 0) > 0)
        _check("TC-REPAIR-SCD2-003", f"{tbl} current versions", stats.get("C", 0) > 0)

    hub_hist = spark.sql(f"""
        SELECT COUNT(*) c FROM {SRC}.account_details_scd2 h
        JOIN {SRC}.account_type_scd2 d ON h.account_type_key = d.account_type_key
        WHERE h.recordStatus = 'H' AND d.recordStatus = 'H'
    """).first()["c"]
    _check("TC-REPAIR-HUB-002", "Hub historic → SCD2 account_type", hub_hist > 0, f"{hub_hist:,}")

    for tbl in ["customer_scd2", "account_details_scd2", "account_type_scd2"]:
        cols = spark.table(f"{SRC}.{tbl}").columns
        for c in ["effectiveStartDate", "effectiveEndDate", "recordStatus"]:
            _check("TC-SETUP-001", f"{tbl}.{c}", c in cols)

# COMMAND ----------

if PHASE == "post_repair":
    repair_pairs = [
        (f"{TGT}.transaction_fact", f"{TGT}.transaction_fact_fixed", "customer_key", f"{TGT}.customer_dim", "customer_key"),
        (f"{TGT}.return_fact", f"{TGT}.return_fact_fixed", "customer_key", f"{TGT}.customer_dim", "customer_key"),
        (f"{TGT}.scd2_activity_fact", f"{TGT}.scd2_activity_fact_fixed", "surrogate_key", f"{TGT}.customer_scd2", "surrogate_key"),
        (f"{TGT}.transaction_fact", f"{TGT}.transaction_fact_fixed", "account_key", f"{TGT}.account_dim", "account_key"),
        (f"{TGT}.account_details_scd2", f"{TGT}.account_details_scd2_fixed", "market_key", f"{TGT}.market_dim", "market_key"),
    ]
    any_fixed = False
    for src_t, fix_t, fk, dim_t, sk in repair_pairs:
        if not _table_exists(fix_t):
            print(f"– skip {fix_t}")
            continue
        any_fixed = True
        src_n, fix_n = _count(src_t), _count(fix_t)
        _check("TC-POST-002", f"Row count {fix_t}", src_n == fix_n, f"{src_n} vs {fix_n}")
        orphans = spark.sql(f"""
            SELECT COUNT(*) c FROM {fix_t} f
            LEFT ANTI JOIN {dim_t} d ON f.{fk} = d.{sk}
            WHERE f.{fk} <> -1
        """).first()["c"]
        _check("TC-POST-001", f"RI {fix_t}.{fk}", orphans == 0, f"{orphans} orphans")
    if not any_fixed:
        _check("TC-POST-001", "At least one *_fixed table", False, "run repair first")

# COMMAND ----------

summary = spark.createDataFrame(results, ["test_id", "test_name", "status", "detail"])
display(summary)

passed = summary.filter("status = 'PASS'").count()
failed = summary.filter("status = 'FAIL'").count()
total = summary.count()
print(f"\nRESULT: {passed}/{total} passed, {failed} failed")
if failed > 0:
    display(summary.filter("status = 'FAIL'"))
    raise RuntimeError(f"SK integrity tests failed: {failed} failure(s)")
print("✅ ALL TESTS PASSED")
