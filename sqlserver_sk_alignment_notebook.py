# Databricks notebook source
# MAGIC %md
# MAGIC # SQL Server SK Alignment (D-1 Golden Source)
# MAGIC
# MAGIC **Purpose:** Align Databricks dimensions and facts to **SQL Server surrogate keys** while
# MAGIC restoring referential integrity — without wiping the lakehouse.
# MAGIC
# MAGIC **When to use this notebook instead of `surrogate_key_repair_notebook`:**
# MAGIC - SQL Server remains the golden source through **D-1** (yesterday).
# MAGIC - Historical facts may already carry SQL Server SKs; incremental Databricks loads carry
# MAGIC   regenerated SKs — a single FK integer is ambiguous without business-key reconciliation.
# MAGIC - Target end-state: **SQL Server SK values** on dims and facts, not Databricks reloaded SKs.
# MAGIC
# MAGIC **Strategy:**
# MAGIC 1. Snapshot the SQL Server dimension (current through D-1) from Lakehouse Federation.
# MAGIC 2. Pre-check natural-key quality; build a key-map for audit and D+0 fallback.
# MAGIC 3. Rebuild the dimension into `*_fixed` with SQL Server SKs (via natural key).
# MAGIC 4. Rebuild each fact into `*_fixed`:
# MAGIC    - **event_date ≤ cutoff (default D-1):** copy FK from the federated SQL Server fact
# MAGIC      joined on **business keys** (transaction grain).
# MAGIC    - **event_date > cutoff (today-only in Databricks):** resolve SK via natural key on the
# MAGIC      fixed dim, or reverse key-map (`new_sk → sql_server_sk`) when the fact has no NK.
# MAGIC 5. Validate referential integrity and match rates; swap tables manually.
# MAGIC
# MAGIC **Safety model:** same as the repair notebook — `dry_run=true` by default, rebuild never
# MAGIC in-place, no automatic RENAME of production tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Parameters
# MAGIC
# MAGIC Run **once per dimension**. Fact entries are positional lists — same index = same fact.
# MAGIC
# MAGIC | Widget | Example | Meaning |
# MAGIC |---|---|---|
# MAGIC | `foreign_catalog` | `sqlserver_dw` | Lakehouse Federation catalog for SQL Server |
# MAGIC | `legacy_schema` | `dbo` | SQL Server schema |
# MAGIC | `legacy_dim_table` | `DimCustomer` | SQL Server dimension table |
# MAGIC | `legacy_sk_col` | `CustomerSK` | SK column in SQL Server dim |
# MAGIC | `legacy_nk_cols` | `CustomerCode` | NK column(s) in SQL Server dim, comma-sep |
# MAGIC | `new_catalog_schema` | `main.gold` | catalog.schema of current Databricks dim |
# MAGIC | `new_dim_table` | `dim_customer` | Current Databricks dim table |
# MAGIC | `new_sk_col` | `customer_sk` | SK column in current Databricks dim |
# MAGIC | `new_nk_cols` | `customer_code` | NK column(s) in current dim — same order as legacy |
# MAGIC | `scd_type` | `1` or `2` | SCD type |
# MAGIC | `legacy_valid_from/to`, `new_valid_from/to` | | SCD2 validity columns (blank for SCD1) |
# MAGIC | `fact_tables` | `main.gold.fact_sales:customer_sk` | `table:fk_col` pairs, comma-sep |
# MAGIC | `legacy_fact_tables` | `FactSales` | SQL Server fact table per entry, comma-sep |
# MAGIC | `legacy_fact_fk_cols` | `CustomerSK` | FK column in SQL Server fact, comma-sep |
# MAGIC | `legacy_fact_bk_cols` | `OrderID;LineID` | Business keys on SQL Server fact; use `;` between keys for one fact, `,` between facts |
# MAGIC | `new_fact_bk_cols` | `order_id;line_id` | Business keys on Databricks fact — same order |
# MAGIC | `fact_event_date_cols` | `order_date` | Event-date column per fact (cutoff + SCD2) |
# MAGIC | `fact_nk_cols` | `customer_code` | NK on fact for D+0 dim lookup; blank = key-map fallback only |
# MAGIC | `cutoff_mode` | `d_minus_1` | `d_minus_1` or `explicit` — boundary for SQL Server fact join |
# MAGIC | `cutoff_date` | `2026-06-10` | Used when `cutoff_mode=explicit`; inclusive through this date |
# MAGIC | `orphan_new_dim_sk_policy` | `keep_current` | Databricks-only dim members: `keep_current` or `allocate_after_max` |
# MAGIC | `staging_schema` | `main.staging` | Snapshots |
# MAGIC | `keymap_schema` | `main.keymap` | Key-map tables |
# MAGIC | `unknown_member_sk` | `-1` | Unknown-member SK |
# MAGIC | `dry_run` | `true` | If true, no `*_fixed` tables are created |

# COMMAND ----------

dbutils.widgets.text("foreign_catalog",           "sqlserver_dw",  "0. Foreign catalog")
dbutils.widgets.text("legacy_schema",             "dbo",           "1. Legacy schema")
dbutils.widgets.text("legacy_dim_table",          "DimCustomer",   "2. Legacy dim table")
dbutils.widgets.text("legacy_sk_col",             "CustomerSK",    "3. Legacy dim SK column")
dbutils.widgets.text("legacy_nk_cols",            "CustomerCode",  "4. Legacy dim NK(s), comma-sep")
dbutils.widgets.text("new_catalog_schema",        "main.gold",     "5. Current catalog.schema")
dbutils.widgets.text("new_dim_table",             "dim_customer",  "6. Current dim table")
dbutils.widgets.text("new_sk_col",                "customer_sk",   "7. Current dim SK column")
dbutils.widgets.text("new_nk_cols",               "customer_code", "8. Current dim NK(s), comma-sep")
dbutils.widgets.dropdown("scd_type", "1", ["1", "2"],              "9. SCD type")
dbutils.widgets.text("legacy_valid_from",         "",              "10. Legacy valid_from (SCD2)")
dbutils.widgets.text("legacy_valid_to",           "",              "11. Legacy valid_to (SCD2)")
dbutils.widgets.text("new_valid_from",            "",              "12. Current valid_from (SCD2)")
dbutils.widgets.text("new_valid_to",              "",              "13. Current valid_to (SCD2)")
dbutils.widgets.text("fact_tables",               "main.gold.fact_sales:customer_sk", "14. Facts table:fk")
dbutils.widgets.text("legacy_fact_tables",        "FactSales",     "15. Legacy fact table(s), comma-sep")
dbutils.widgets.text("legacy_fact_fk_cols",       "CustomerSK",    "16. Legacy fact FK col(s), comma-sep")
dbutils.widgets.text("legacy_fact_bk_cols",       "OrderID;LineID","17. Legacy fact BK(s): ; within fact, , between facts")
dbutils.widgets.text("new_fact_bk_cols",          "order_id;line_id", "18. Current fact BK(s): ; within fact, , between facts")
dbutils.widgets.text("fact_event_date_cols",      "order_date",    "19. Fact event-date col(s), comma-sep")
dbutils.widgets.text("fact_nk_cols",              "customer_code", "20. Fact NK for D+0 (blank=keymap only), comma-sep")
dbutils.widgets.dropdown("cutoff_mode", "d_minus_1", ["d_minus_1", "explicit"], "21. Cutoff mode")
dbutils.widgets.text("cutoff_date",               "",              "22. Cutoff date (explicit mode, inclusive)")
dbutils.widgets.dropdown("orphan_new_dim_sk_policy", "keep_current", ["keep_current", "allocate_after_max"], "23. ORPHAN_NEW dim SK policy")
dbutils.widgets.text("staging_schema",            "main.staging",  "24. Staging schema")
dbutils.widgets.text("keymap_schema",             "main.keymap",   "25. Key-map schema")
dbutils.widgets.text("unknown_member_sk",         "-1",            "26. Unknown-member SK")
dbutils.widgets.dropdown("dry_run", "true", ["true", "false"],    "27. Dry run")

# COMMAND ----------

from datetime import datetime, timezone

WIDGET_NAMES = [
    "foreign_catalog", "legacy_schema", "legacy_dim_table", "legacy_sk_col",
    "legacy_nk_cols", "new_catalog_schema", "new_dim_table", "new_sk_col",
    "new_nk_cols", "scd_type", "legacy_valid_from", "legacy_valid_to",
    "new_valid_from", "new_valid_to", "fact_tables", "legacy_fact_tables",
    "legacy_fact_fk_cols", "legacy_fact_bk_cols", "new_fact_bk_cols",
    "fact_event_date_cols", "fact_nk_cols", "cutoff_mode", "cutoff_date",
    "orphan_new_dim_sk_policy", "staging_schema", "keymap_schema",
    "unknown_member_sk", "dry_run",
]
p = {name: dbutils.widgets.get(name).strip() for name in WIDGET_NAMES}

legacy_nks = [c.strip() for c in p["legacy_nk_cols"].split(",") if c.strip()]
new_nks    = [c.strip() for c in p["new_nk_cols"].split(",")    if c.strip()]

facts = []
for entry in [e.strip() for e in p["fact_tables"].split(",") if e.strip()]:
    table, _, fk = entry.partition(":")
    if not fk:
        raise ValueError(f"fact_tables entry '{entry}' must be 'table:fk_column'")
    facts.append({"table": table.strip(), "fk": fk.strip()})

def parse_fact_list_groups(raw, label):
    """Split comma-separated fact groups; semicolon separates BK cols within one fact."""
    groups = [g.strip() for g in raw.split(",") if g.strip()]
    parsed = []
    for g in groups:
        cols = [c.strip() for c in g.split(";") if c.strip()]
        if not cols:
            raise ValueError(f"{label} group '{g}' has no columns (use ; between keys)")
        parsed.append(cols)
    return parsed

legacy_fact_tables = [t.strip() for t in p["legacy_fact_tables"].split(",") if t.strip()]
legacy_fact_fks    = [c.strip() for c in p["legacy_fact_fk_cols"].split(",") if c.strip()]
legacy_fact_bks    = parse_fact_list_groups(p["legacy_fact_bk_cols"], "legacy_fact_bk_cols")
new_fact_bks       = parse_fact_list_groups(p["new_fact_bk_cols"], "new_fact_bk_cols")
event_date_cols    = [c.strip() for c in p["fact_event_date_cols"].split(",") if c.strip()]
fact_nk_groups     = parse_fact_list_groups(p["fact_nk_cols"], "fact_nk_cols") if p["fact_nk_cols"] else []

is_scd2 = p["scd_type"] == "2"
dry_run = p["dry_run"] == "true"
allocate_orphan_new = p["orphan_new_dim_sk_policy"] == "allocate_after_max"

n_facts = len(facts)
if len(legacy_fact_tables) != n_facts:
    raise ValueError(f"legacy_fact_tables ({len(legacy_fact_tables)}) must match fact_tables ({n_facts})")
if len(legacy_fact_fks) != n_facts:
    raise ValueError(f"legacy_fact_fk_cols ({len(legacy_fact_fks)}) must match fact_tables ({n_facts})")
if len(legacy_fact_bks) != n_facts:
    raise ValueError(f"legacy_fact_bk_cols ({len(legacy_fact_bks)}) must match fact_tables ({n_facts})")
if len(new_fact_bks) != n_facts:
    raise ValueError(f"new_fact_bk_cols ({len(new_fact_bks)}) must match fact_tables ({n_facts})")
if len(event_date_cols) != n_facts:
    raise ValueError(f"fact_event_date_cols ({len(event_date_cols)}) must match fact_tables ({n_facts})")
if p["fact_nk_cols"] and len(fact_nk_groups) != n_facts:
    raise ValueError(f"fact_nk_cols ({len(fact_nk_groups)}) must match fact_tables ({n_facts}) or be blank")

if len(legacy_nks) != len(new_nks):
    raise ValueError(
        f"legacy_nk_cols ({legacy_nks}) and new_nk_cols ({new_nks}) must have the "
        f"same number of columns, matched positionally."
    )

if is_scd2:
    for w in ["legacy_valid_from", "legacy_valid_to", "new_valid_from", "new_valid_to"]:
        if not p[w]:
            raise ValueError(f"SCD2 selected but widget '{w}' is empty.")

for i, f in enumerate(facts):
    if len(legacy_fact_bks[i]) != len(new_fact_bks[i]):
        raise ValueError(
            f"Fact {f['table']}: legacy BK count ({legacy_fact_bks[i]}) != "
            f"new BK count ({new_fact_bks[i]})"
        )

if p["cutoff_mode"] == "explicit" and not p["cutoff_date"]:
    raise ValueError("cutoff_mode=explicit requires cutoff_date (inclusive through that date).")

if p["cutoff_mode"] == "d_minus_1":
    cutoff_predicate = "date_sub(current_date(), 1)"
    cutoff_label = "D-1 (date_sub(current_date(), 1))"
else:
    cutoff_predicate = f"date'{p['cutoff_date']}'"
    cutoff_label = p["cutoff_date"]

dim_id       = p["legacy_dim_table"].lower()
legacy_fqn   = f"{p['foreign_catalog']}.{p['legacy_schema']}.{p['legacy_dim_table']}"
new_dim_fqn  = f"{p['new_catalog_schema']}.{p['new_dim_table']}"
dim_fixed_fqn = f"{new_dim_fqn}_fixed"
snapshot_fqn = f"{p['staging_schema']}.legacy_{dim_id}"
keymap_fqn   = f"{p['keymap_schema']}.{dim_id}_keymap"
run_ts       = datetime.now(timezone.utc).isoformat()

for i, f in enumerate(facts):
    f["legacy_fact_fqn"] = f"{p['foreign_catalog']}.{p['legacy_schema']}.{legacy_fact_tables[i]}"
    f["legacy_fk"] = legacy_fact_fks[i]
    f["legacy_bks"] = legacy_fact_bks[i]
    f["new_bks"] = new_fact_bks[i]
    f["event_date"] = event_date_cols[i]
    f["fact_nks"] = fact_nk_groups[i] if fact_nk_groups else []

print(f"""
Configuration summary
=====================
Legacy dim (SQL Server) : {legacy_fqn}
Current dim             : {new_dim_fqn}
Dim fixed output        : {dim_fixed_fqn}
SCD type                : {p['scd_type']}
Cutoff (SQL Server FK)  : event_date <= {cutoff_label}
ORPHAN_NEW dim policy   : {p['orphan_new_dim_sk_policy']}
Facts                   : {n_facts}
Snapshot / key-map      : {snapshot_fqn} / {keymap_fqn}
DRY RUN                 : {dry_run}
""")

# COMMAND ----------

def nk_expr(cols, prefix=""):
    parts = [
        f"coalesce(upper(trim(cast({prefix}{c} as string))), '~NULL~')"
        for c in cols
    ]
    return " || '||' || ".join(parts)

def bk_join_sql(legacy_alias, new_alias, legacy_cols, new_cols):
    return " AND ".join(
        f"coalesce(upper(trim(cast({legacy_alias}.{lc} as string))), '~NULL~') = "
        f"coalesce(upper(trim(cast({new_alias}.{nc} as string))), '~NULL~')"
        for lc, nc in zip(legacy_cols, new_cols)
    )

legacy_nk_sql = nk_expr(legacy_nks)
new_nk_sql    = nk_expr(new_nks)
print("Legacy dim NK :", legacy_nk_sql)
print("Current dim NK:", new_nk_sql)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Snapshot the SQL Server dimension (golden source through D-1)

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {p['staging_schema']}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {p['keymap_schema']}")

validity_select = (
    f", {p['legacy_valid_from']} AS legacy_valid_from"
    f", {p['legacy_valid_to']}   AS legacy_valid_to"
) if is_scd2 else ""

spark.sql(f"""
CREATE OR REPLACE TABLE {snapshot_fqn} AS
SELECT
  {p['legacy_sk_col']} AS sql_server_sk,
  {legacy_nk_sql}      AS natural_key
  {validity_select}
FROM {legacy_fqn}
""")

spark.sql(f"""
ALTER TABLE {snapshot_fqn} SET TBLPROPERTIES (
  'snapshot_source' = '{legacy_fqn}',
  'snapshot_at'     = '{run_ts}',
  'created_by'      = 'sqlserver_sk_alignment_notebook'
)
""")

cnt = spark.table(snapshot_fqn).count()
max_sk = spark.sql(f"SELECT coalesce(max(sql_server_sk), 0) AS m FROM {snapshot_fqn}").first()["m"]
print(f"Snapshot {snapshot_fqn}: {cnt:,} rows, max sql_server_sk = {max_sk:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Pre-checks

# COMMAND ----------

if not is_scd2:
    legacy_dups = spark.sql(f"""
    SELECT natural_key, COUNT(DISTINCT sql_server_sk) AS distinct_sks
    FROM {snapshot_fqn}
    GROUP BY natural_key
    HAVING COUNT(DISTINCT sql_server_sk) > 1
    """)
    n = legacy_dups.count()
    if n > 0:
        print(f"⚠️  {n} legacy natural keys map to multiple SQL Server SKs (SCD1):")
        display(legacy_dups.orderBy("natural_key"))
    else:
        print("✅ Legacy natural keys are unique (SCD1).")
else:
    print("SCD2 dim — version counts by natural key (informational):")
    display(spark.sql(f"""
        SELECT natural_key, COUNT(*) AS versions
        FROM {snapshot_fqn}
        GROUP BY natural_key
        ORDER BY versions DESC
        LIMIT 50
    """))

# COMMAND ----------

if not is_scd2:
    current_dups = spark.sql(f"""
    SELECT {new_nk_sql} AS natural_key, COUNT(DISTINCT {p['new_sk_col']}) AS distinct_sks
    FROM {new_dim_fqn}
    GROUP BY {new_nk_sql}
    HAVING COUNT(DISTINCT {p['new_sk_col']}) > 1
    """)
    n = current_dups.count()
    if n > 0:
        print(f"⚠️  {n} current natural keys map to multiple SKs — dedup dim before proceeding!")
        display(current_dups.orderBy("natural_key"))
    else:
        print("✅ Current dim natural keys are unique (SCD1).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Build key-map (audit + D+0 fallback)
# MAGIC
# MAGIC Maps `natural_key → sql_server_sk (old_sk) → current_databricks_sk (new_sk)`.

# COMMAND ----------

if not is_scd2:
    spark.sql(f"""
    CREATE OR REPLACE TABLE {keymap_fqn} AS
    SELECT
      coalesce(s.natural_key, n.natural_key) AS natural_key,
      s.sql_server_sk                        AS old_sk,
      n.new_sk                               AS new_sk,
      CAST(NULL AS TIMESTAMP)                AS valid_from,
      CAST(NULL AS TIMESTAMP)                AS valid_to,
      CASE
        WHEN s.natural_key IS NULL THEN 'ORPHAN_NEW'
        WHEN n.natural_key IS NULL THEN 'ORPHAN_OLD'
        ELSE 'MATCHED'
      END                                    AS map_status,
      current_timestamp()                    AS created_at
    FROM {snapshot_fqn} s
    FULL OUTER JOIN (
        SELECT {new_nk_sql} AS natural_key, {p['new_sk_col']} AS new_sk
        FROM {new_dim_fqn}
    ) n ON s.natural_key = n.natural_key
    """)
else:
    spark.sql(f"""
    CREATE OR REPLACE TABLE {keymap_fqn} AS
    WITH joined AS (
      SELECT
        s.natural_key,
        s.sql_server_sk AS old_sk,
        n.new_sk,
        n.new_valid_from AS valid_from,
        n.new_valid_to   AS valid_to,
        COUNT(n.new_sk) OVER (PARTITION BY s.sql_server_sk) AS match_cnt
      FROM {snapshot_fqn} s
      LEFT JOIN (
          SELECT {new_nk_sql} AS natural_key,
                 {p['new_sk_col']} AS new_sk,
                 {p['new_valid_from']} AS new_valid_from,
                 {p['new_valid_to']}   AS new_valid_to
          FROM {new_dim_fqn}
      ) n
        ON  s.natural_key = n.natural_key
        AND s.legacy_valid_from < coalesce(n.new_valid_to,   timestamp'9999-12-31')
        AND n.new_valid_from    < coalesce(s.legacy_valid_to, timestamp'9999-12-31')
    )
    SELECT
      natural_key, old_sk, new_sk, valid_from, valid_to,
      CASE
        WHEN new_sk IS NULL     THEN 'ORPHAN_OLD'
        WHEN match_cnt > 1      THEN 'AMBIGUOUS'
        ELSE 'MATCHED'
      END AS map_status,
      current_timestamp() AS created_at
    FROM joined
    """)

print(f"Key-map {keymap_fqn} built.")
display(spark.sql(f"SELECT map_status, COUNT(*) AS rows FROM {keymap_fqn} GROUP BY 1 ORDER BY 1"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Audit key-map — read before rebuilding

# COMMAND ----------

print("Key-map status distribution:")
display(spark.sql(f"""
SELECT map_status, COUNT(*) AS rows FROM {keymap_fqn} GROUP BY map_status ORDER BY map_status
"""))

orphan_new_cnt = spark.sql(f"""
    SELECT COUNT(*) AS c FROM {keymap_fqn} WHERE map_status = 'ORPHAN_NEW'
""").first()["c"]
if orphan_new_cnt > 0:
    print(f"ℹ️  {orphan_new_cnt:,} ORPHAN_NEW dim members (Databricks-only). Policy: {p['orphan_new_dim_sk_policy']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Rebuild dimension with SQL Server SKs
# MAGIC
# MAGIC Skipped when `dry_run=true`.

# COMMAND ----------

if dry_run:
    print("DRY RUN — skipping dim rebuild. Set dry_run=false to create dim_*_fixed.")
else:
    if not is_scd2:
        if allocate_orphan_new:
            spark.sql(f"""
            CREATE OR REPLACE TABLE {dim_fixed_fqn} AS
            WITH base AS (
              SELECT {new_nk_sql} AS natural_key, * FROM {new_dim_fqn}
            ),
            orphan_new AS (
              SELECT natural_key,
                     row_number() OVER (ORDER BY natural_key) AS rn
              FROM {keymap_fqn}
              WHERE map_status = 'ORPHAN_NEW'
            )
            SELECT
              coalesce(km.old_sk, {max_sk} + onw.rn, d.{p['new_sk_col']}) AS {p['new_sk_col']},
              d.* EXCEPT ({p['new_sk_col']}, natural_key)
            FROM base d
            LEFT JOIN {keymap_fqn} km
              ON d.natural_key = km.natural_key AND km.map_status = 'MATCHED'
            LEFT JOIN orphan_new onw
              ON d.natural_key = onw.natural_key
            """)
        else:
            spark.sql(f"""
            CREATE OR REPLACE TABLE {dim_fixed_fqn} AS
            WITH base AS (
              SELECT {new_nk_sql} AS natural_key, * FROM {new_dim_fqn}
            )
            SELECT
              coalesce(km.old_sk, d.{p['new_sk_col']}) AS {p['new_sk_col']},
              d.* EXCEPT ({p['new_sk_col']}, natural_key)
            FROM base d
            LEFT JOIN {keymap_fqn} km
              ON d.natural_key = km.natural_key AND km.map_status = 'MATCHED'
            """)
    else:
        spark.sql(f"""
        CREATE OR REPLACE TABLE {dim_fixed_fqn} AS
        WITH base AS (
          SELECT
            {new_nk_sql} AS natural_key,
            {p['new_sk_col']} AS current_sk,
            {p['new_valid_from']} AS valid_from,
            {p['new_valid_to']}   AS valid_to,
            * EXCEPT ({p['new_sk_col']})
          FROM {new_dim_fqn}
        ),
        matched AS (
          SELECT
            b.*,
            s.sql_server_sk,
            row_number() OVER (
              PARTITION BY b.natural_key, b.valid_from, b.valid_to
              ORDER BY s.sql_server_sk
            ) AS rn
          FROM base b
          LEFT JOIN {snapshot_fqn} s
            ON  b.natural_key = s.natural_key
            AND b.valid_from  < coalesce(s.legacy_valid_to, timestamp'9999-12-31')
            AND s.legacy_valid_from < coalesce(b.valid_to, timestamp'9999-12-31')
        )
        SELECT
          coalesce(sql_server_sk, current_sk) AS {p['new_sk_col']},
          * EXCEPT (sql_server_sk, current_sk, rn)
        FROM matched
        WHERE rn = 1 OR sql_server_sk IS NULL
        """)

    dim_src = spark.table(new_dim_fqn).count()
    dim_fix = spark.table(dim_fixed_fqn).count()
    flag = "✅" if dim_src == dim_fix else "❌ ROW COUNT CHANGED"
    print(f"{flag}  {new_dim_fqn}: {dim_src:,} -> {dim_fixed_fqn}: {dim_fix:,}")

    if dim_src == dim_fix:
        changed = spark.sql(f"""
            SELECT COUNT(*) AS c
            FROM (
              SELECT {new_nk_sql} AS natural_key, {p['new_sk_col']} AS sk FROM {new_dim_fqn}
            ) o
            JOIN (
              SELECT {new_nk_sql} AS natural_key, {p['new_sk_col']} AS sk FROM {dim_fixed_fqn}
            ) n USING (natural_key)
            WHERE o.sk <> n.sk
        """).first()["c"]
        print(f"    SK values changed on {changed:,} rows (matched by natural key)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Audit fact match rates (SQL Server join through cutoff)
# MAGIC
# MAGIC Runs in dry-run and live mode — read-only against facts.

# COMMAND ----------

for f in facts:
    bk_join = bk_join_sql("ss", "fct", f["legacy_bks"], f["new_bks"])
    ev = f["event_date"]
    audit = spark.sql(f"""
    SELECT
      CASE
        WHEN fct.{ev} <= {cutoff_predicate} AND ss.{f['legacy_fk']} IS NOT NULL THEN 'MATCHED_SS_FACT'
        WHEN fct.{ev} <= {cutoff_predicate} AND ss.{f['legacy_fk']} IS NULL     THEN 'MISSING_SS_FACT'
        WHEN fct.{ev} >  {cutoff_predicate}                                       THEN 'AFTER_CUTOFF'
      END AS bucket,
      COUNT(*) AS fact_rows
    FROM {f['table']} fct
    LEFT JOIN {f['legacy_fact_fqn']} ss ON {bk_join}
    GROUP BY 1 ORDER BY 1
    """)
    print(f"\nFact match audit: {f['table']} (FK {f['fk']}, legacy {f['legacy_fact_fqn']})")
    display(audit)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Rebuild facts with SQL Server SKs
# MAGIC
# MAGIC - **Through cutoff:** FK from SQL Server fact (business-key join).
# MAGIC - **After cutoff:** NK lookup on `dim_*_fixed`, else key-map `new_sk → old_sk`.
# MAGIC
# MAGIC Skipped when `dry_run=true`.

# COMMAND ----------

if dry_run:
    print("DRY RUN — skipping fact rebuild.")
else:
    dim_for_lookup = dim_fixed_fqn

    for f in facts:
        fixed_fqn = f"{f['table']}_fixed"
        bk_join = bk_join_sql("ss", "fct", f["legacy_bks"], f["new_bks"])
        ev = f["event_date"]
        fk = f["fk"]

        # Resolution chain for the FK column
        ss_fk = f"ss.{f['legacy_fk']}"

        if f["fact_nks"]:
            fact_nk_sql = nk_expr(f["fact_nks"], prefix="fct.")
            dim_nk_join = f"""
            LEFT JOIN {dim_for_lookup} dim_nk
              ON {fact_nk_sql} = {nk_expr(new_nks, prefix="dim_nk.")}
            """
            nk_sk = f"dim_nk.{p['new_sk_col']}"
        else:
            dim_nk_join = ""
            nk_sk = "NULL"

        if is_scd2:
            km_join = f"""
            LEFT JOIN {keymap_fqn} km
              ON  fct.{fk} = km.new_sk
              AND km.map_status IN ('MATCHED', 'AMBIGUOUS')
              AND fct.{ev} >= km.valid_from
              AND fct.{ev} <  coalesce(km.valid_to, timestamp'9999-12-31')
            """
        else:
            km_join = f"""
            LEFT JOIN {keymap_fqn} km
              ON  fct.{fk} = km.new_sk
              AND km.map_status = 'MATCHED'
            """

        resolved_fk = f"""
          CASE
            WHEN fct.{ev} <= {cutoff_predicate} THEN coalesce({ss_fk}, {nk_sk}, km.old_sk, {p['unknown_member_sk']})
            ELSE coalesce({nk_sk}, km.old_sk, {p['unknown_member_sk']})
          END
        """

        spark.sql(f"""
        CREATE OR REPLACE TABLE {fixed_fqn} AS
        SELECT
          fct.* EXCEPT ({fk}),
          {resolved_fk} AS {fk}
        FROM {f['table']} fct
        LEFT JOIN {f['legacy_fact_fqn']} ss
          ON {bk_join}
        {dim_nk_join}
        {km_join}
        """)

        src_cnt = spark.table(f["table"]).count()
        fix_cnt = spark.table(fixed_fqn).count()
        flag = "✅" if src_cnt == fix_cnt else "❌ ROW COUNT CHANGED — DO NOT SWAP"
        print(f"{flag}  {f['table']}: {src_cnt:,} -> {fixed_fqn}: {fix_cnt:,}")

        through_cutoff = spark.sql(f"""
            SELECT COUNT(*) AS c FROM {fixed_fqn} WHERE {ev} <= {cutoff_predicate}
        """).first()["c"]
        ss_matched = spark.sql(f"""
            SELECT COUNT(*) AS c
            FROM {f['table']} fct
            INNER JOIN {f['legacy_fact_fqn']} ss ON {bk_join}
            WHERE fct.{ev} <= {cutoff_predicate}
        """).first()["c"]
        print(f"    through cutoff: {through_cutoff:,} rows; SS fact join matched: {ss_matched:,}")

        unknown_cnt = spark.sql(f"""
            SELECT COUNT(*) AS c FROM {fixed_fqn} WHERE {fk} = {p['unknown_member_sk']}
        """).first()["c"]
        print(f"    rows on unknown member ({p['unknown_member_sk']}): {unknown_cnt:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Post-repair validation

# COMMAND ----------

if dry_run:
    print("DRY RUN — run with dry_run=false, then validate.")
else:
    orphans = spark.sql(f"""
        SELECT COUNT(*) AS c
        FROM {dim_fixed_fqn} d
        LEFT ANTI JOIN {snapshot_fqn} s ON d.{p['new_sk_col']} = s.sql_server_sk
        WHERE d.{p['new_sk_col']} <> {p['unknown_member_sk']}
          AND d.{p['new_sk_col']} <= {max_sk}
    """).first()["c"]
    flag = "✅" if orphans == 0 else "⚠️"
    print(f"{flag} Dim SKs in SQL Server range (<= {max_sk:,}): {orphans:,} not found in snapshot (may be ORPHAN_NEW policy)")

    for f in facts:
        fixed_fqn = f"{f['table']}_fixed"
        fk = f["fk"]
        orphan_fks = spark.sql(f"""
            SELECT COUNT(*) AS c
            FROM {fixed_fqn} fct
            LEFT ANTI JOIN {dim_fixed_fqn} d ON fct.{fk} = d.{p['new_sk_col']}
            WHERE fct.{fk} <> {p['unknown_member_sk']}
        """).first()["c"]
        flag = "✅" if orphan_fks == 0 else "❌"
        print(f"{flag} {fixed_fqn}: {orphan_fks:,} orphaned FK rows against {dim_fixed_fqn}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Manual swap
# MAGIC
# MAGIC When validation is clean:
# MAGIC
# MAGIC ```sql
# MAGIC -- Dimension
# MAGIC ALTER TABLE main.gold.dim_customer        RENAME TO main.gold.dim_customer_broken;
# MAGIC ALTER TABLE main.gold.dim_customer_fixed    RENAME TO main.gold.dim_customer;
# MAGIC
# MAGIC -- Facts (one at a time)
# MAGIC ALTER TABLE main.gold.fact_sales            RENAME TO main.gold.fact_sales_broken;
# MAGIC ALTER TABLE main.gold.fact_sales_fixed      RENAME TO main.gold.fact_sales;
# MAGIC ```
# MAGIC
# MAGIC ## 10. Going forward
# MAGIC
# MAGIC - Incremental loads: resolve SK by **natural key** against the dim (SQL Server SK column).
# MAGIC - Dim reloads: copy SK from SQL Server federation by NK — never regenerate identity SKs.
# MAGIC - Keep key-map tables permanently for lineage.
