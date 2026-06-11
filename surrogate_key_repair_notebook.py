# Databricks notebook source
# MAGIC %md
# MAGIC # Surrogate Key Repair via Key-Map Tables
# MAGIC
# MAGIC **Purpose:** Restore referential integrity between fact and dimension tables after
# MAGIC dimensions were reloaded with regenerated surrogate keys (SKs), leaving facts
# MAGIC pointing at wrong or non-existent dimension rows.
# MAGIC
# MAGIC **Strategy:**
# MAGIC 1. Snapshot the *legacy* dimension (as it was when the facts were keyed) from the
# MAGIC    SQL Server foreign catalog (Lakehouse Federation).
# MAGIC 2. Run data-quality pre-checks (collation/case duplicates, NK uniqueness).
# MAGIC 3. Build a **key-map table**: `natural_key -> old_sk -> new_sk` with a match status.
# MAGIC 4. Audit the map (orphans on both sides) BEFORE touching any fact.
# MAGIC 5. Repair facts by *rebuilding* into a `_fixed` table (never in-place — old and new
# MAGIC    SK ranges usually overlap, which makes in-place UPDATEs dangerous).
# MAGIC 6. Reconcile measures against the legacy source, then swap tables manually.
# MAGIC
# MAGIC **How to run:** Fill in the widgets at the top, then run cells top-to-bottom.
# MAGIC Run the notebook **once per dimension**. Steps that modify data are clearly
# MAGIC marked and gated behind an explicit `dry_run` parameter.
# MAGIC
# MAGIC **Safety model:**
# MAGIC - `dry_run = true` (default): builds snapshots + key-map + audits, but does NOT
# MAGIC   create the repaired fact tables.
# MAGIC - Nothing in this notebook ever drops or renames your existing fact tables.
# MAGIC   The final swap (RENAME) is intentionally left as a manual, copy-paste step.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Parameters
# MAGIC
# MAGIC | Widget | Example | Meaning |
# MAGIC |---|---|---|
# MAGIC | `foreign_catalog` | `sqlserver_dw` | Name of the Lakehouse Federation catalog pointing at SQL Server |
# MAGIC | `legacy_schema` | `dbo` | Schema of the legacy dim inside the foreign catalog |
# MAGIC | `legacy_dim_table` | `DimCustomer` | Legacy dimension table name (SQL Server side) |
# MAGIC | `legacy_sk_col` | `CustomerSK` | Surrogate key column in the LEGACY dim |
# MAGIC | `legacy_nk_cols` | `CustomerCode` or `ProductCode,SourceSystem` | Comma-separated natural key column(s) in the LEGACY dim |
# MAGIC | `new_catalog_schema` | `main.gold` | catalog.schema of the CURRENT (reloaded) dim in Databricks |
# MAGIC | `new_dim_table` | `dim_customer` | Current dimension table name |
# MAGIC | `new_sk_col` | `customer_sk` | Surrogate key column in the CURRENT dim |
# MAGIC | `new_nk_cols` | `customer_code` | Comma-separated natural key column(s) in the CURRENT dim — **same order as legacy_nk_cols** |
# MAGIC | `scd_type` | `1` or `2` | SCD type of this dimension |
# MAGIC | `legacy_valid_from` / `legacy_valid_to` | `ValidFrom` / `ValidTo` | SCD2 only: validity columns in legacy dim (leave blank for SCD1) |
# MAGIC | `new_valid_from` / `new_valid_to` | `valid_from` / `valid_to` | SCD2 only: validity columns in current dim |
# MAGIC | `fact_tables` | `main.gold.fact_sales:customer_sk,main.gold.fact_returns:customer_sk` | Comma-separated `table:fk_column` pairs of facts referencing this dim |
# MAGIC | `fact_event_date_cols` | `order_date,return_date` | SCD2 only: event-date column per fact, same order as `fact_tables` |
# MAGIC | `staging_schema` | `main.staging` | Where legacy snapshots are written |
# MAGIC | `keymap_schema` | `main.keymap` | Where key-map tables are written |
# MAGIC | `unknown_member_sk` | `-1` | SK used for facts that cannot be mapped |
# MAGIC | `dry_run` | `true` | If `true`, no `_fixed` fact tables are created |

# COMMAND ----------

# ---------------------------------------------------------------------------
# Widget definitions. dbutils.widgets gives you editable text boxes at the top
# of the notebook UI, and lets you drive this notebook from Jobs/Workflows with
# different parameter sets per dimension.
# ---------------------------------------------------------------------------
dbutils.widgets.text("foreign_catalog",      "sqlserver_dw",  "0. Foreign catalog (Federation)")
dbutils.widgets.text("legacy_schema",        "dbo",           "1. Legacy schema")
dbutils.widgets.text("legacy_dim_table",     "DimCustomer",   "2. Legacy dim table")
dbutils.widgets.text("legacy_sk_col",        "CustomerSK",    "3. Legacy SK column")
dbutils.widgets.text("legacy_nk_cols",       "CustomerCode",  "4. Legacy NK column(s), comma-sep")
dbutils.widgets.text("new_catalog_schema",   "main.gold",     "5. Current catalog.schema")
dbutils.widgets.text("new_dim_table",        "dim_customer",  "6. Current dim table")
dbutils.widgets.text("new_sk_col",           "customer_sk",   "7. Current SK column")
dbutils.widgets.text("new_nk_cols",          "customer_code", "8. Current NK column(s), comma-sep")
dbutils.widgets.dropdown("scd_type", "1", ["1", "2"],         "9. SCD type")
dbutils.widgets.text("legacy_valid_from",    "",              "10. Legacy valid_from (SCD2)")
dbutils.widgets.text("legacy_valid_to",      "",              "11. Legacy valid_to (SCD2)")
dbutils.widgets.text("new_valid_from",       "",              "12. Current valid_from (SCD2)")
dbutils.widgets.text("new_valid_to",         "",              "13. Current valid_to (SCD2)")
dbutils.widgets.text("fact_tables",          "main.gold.fact_sales:customer_sk", "14. Facts table:fk, comma-sep")
dbutils.widgets.text("fact_event_date_cols", "",              "15. Fact event-date cols (SCD2), comma-sep")
dbutils.widgets.text("staging_schema",       "main.staging",  "16. Staging schema")
dbutils.widgets.text("keymap_schema",        "main.keymap",   "17. Key-map schema")
dbutils.widgets.text("unknown_member_sk",    "-1",            "18. Unknown-member SK")
dbutils.widgets.dropdown("dry_run", "true", ["true", "false"],"19. Dry run (no fact rebuild)")

# COMMAND ----------

# ---------------------------------------------------------------------------
# Read + validate parameters. Fail fast and loudly on inconsistent input —
# a half-configured run against production facts is exactly what we're
# trying to avoid.
# ---------------------------------------------------------------------------
from datetime import datetime, timezone

p = {name: dbutils.widgets.get(name).strip() for name in [
    "foreign_catalog", "legacy_schema", "legacy_dim_table", "legacy_sk_col",
    "legacy_nk_cols", "new_catalog_schema", "new_dim_table", "new_sk_col",
    "new_nk_cols", "scd_type", "legacy_valid_from", "legacy_valid_to",
    "new_valid_from", "new_valid_to", "fact_tables", "fact_event_date_cols",
    "staging_schema", "keymap_schema", "unknown_member_sk", "dry_run",
]}

# --- Parse list-style parameters -------------------------------------------
legacy_nks = [c.strip() for c in p["legacy_nk_cols"].split(",") if c.strip()]
new_nks    = [c.strip() for c in p["new_nk_cols"].split(",")    if c.strip()]

# fact_tables format: "catalog.schema.table:fk_col" or "table:fk1;fk2" (same dim, one rebuild)
facts = []
for entry in [e.strip() for e in p["fact_tables"].split(",") if e.strip()]:
    table, _, fk_part = entry.partition(":")
    if not fk_part:
        raise ValueError(f"fact_tables entry '{entry}' must be 'table:fk_column' or 'table:fk1;fk2'")
    fk_cols = [c.strip() for c in fk_part.replace(";", ",").split(",") if c.strip()]
    facts.append({"table": table.strip(), "fks": fk_cols})

# Merge entries for the same table (one rebuild pass, multiple FK columns)
_merged = {}
for f in facts:
    _merged.setdefault(f["table"], [])
    for fk in f["fks"]:
        if fk not in _merged[f["table"]]:
            _merged[f["table"]].append(fk)
facts = [{"table": t, "fks": fks} for t, fks in _merged.items()]

event_date_cols = [c.strip() for c in p["fact_event_date_cols"].split(",") if c.strip()]

is_scd2 = p["scd_type"] == "2"
dry_run = p["dry_run"] == "true"

# --- Consistency checks ------------------------------------------------------
if len(legacy_nks) != len(new_nks):
    raise ValueError(
        f"legacy_nk_cols ({legacy_nks}) and new_nk_cols ({new_nks}) must have the "
        f"same number of columns, in the same order — they are matched positionally."
    )

if is_scd2:
    for w in ["legacy_valid_from", "legacy_valid_to", "new_valid_from", "new_valid_to"]:
        if not p[w]:
            raise ValueError(f"SCD2 selected but widget '{w}' is empty.")
    if len(event_date_cols) != len(facts):
        raise ValueError(
            "SCD2 selected: fact_event_date_cols must list one event-date column "
            f"per fact table ({len(facts)} facts, {len(event_date_cols)} date cols given)."
        )

# --- Derived names used throughout -------------------------------------------
dim_id          = p["legacy_dim_table"].lower()                      # e.g. 'dimcustomer'
legacy_fqn      = f"{p['foreign_catalog']}.{p['legacy_schema']}.{p['legacy_dim_table']}"
new_dim_fqn     = f"{p['new_catalog_schema']}.{p['new_dim_table']}"
snapshot_fqn    = f"{p['staging_schema']}.legacy_{dim_id}"
keymap_fqn      = f"{p['keymap_schema']}.{dim_id}_keymap"
run_ts          = datetime.now(timezone.utc).isoformat()

print(f"""
Configuration summary
=====================
Legacy dim (federation) : {legacy_fqn}
  SK column             : {p['legacy_sk_col']}
  NK columns            : {legacy_nks}
Current dim             : {new_dim_fqn}
  SK column             : {p['new_sk_col']}
  NK columns            : {new_nks}
SCD type                : {p['scd_type']}
Facts to repair         : {facts}
Snapshot table          : {snapshot_fqn}
Key-map table           : {keymap_fqn}
Unknown-member SK       : {p['unknown_member_sk']}
DRY RUN                 : {dry_run}
""")

# COMMAND ----------

# ---------------------------------------------------------------------------
# Helper: build the normalized natural-key expression.
#
# Why normalize at all?
#   SQL Server's default collation is CASE-INSENSITIVE and often ignores
#   trailing whitespace in comparisons. Spark string comparisons are exact.
#   So 'ABC01' and 'abc01 ' were "the same customer" in SQL Server, but they
#   are different strings in Databricks. Without normalization, the key-map
#   silently fails to match rows and you under-repair the facts.
#
# Normalization applied:  upper(trim(CAST(col AS STRING)))
# Composite keys:         joined with the literal '||' separator so
#                         ('AB','C') never collides with ('A','BC').
# NULL handling:          coalesce to a sentinel so a NULL component doesn't
#                         null out the whole key (NULL || 'x' = NULL in SQL).
# ---------------------------------------------------------------------------
def nk_expr(cols):
    parts = [f"coalesce(upper(trim(cast({c} as string))), '~NULL~')" for c in cols]
    return " || '||' || ".join(parts)

legacy_nk_sql = nk_expr(legacy_nks)
new_nk_sql    = nk_expr(new_nks)
print("Legacy NK expression :", legacy_nk_sql)
print("Current NK expression:", new_nk_sql)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Snapshot the legacy dimension from the foreign catalog
# MAGIC
# MAGIC We **materialize** a snapshot instead of joining against the foreign catalog live:
# MAGIC - **Stability** — if the SQL Server dim is still receiving changes, a live join could
# MAGIC   shift between your audit run and your repair run. The snapshot freezes the reference.
# MAGIC - **Performance** — the full-outer-join + normalization would pull the whole table
# MAGIC   over the wire on every iteration. You will iterate; snapshot once.
# MAGIC - **Audit trail** — the snapshot is durable evidence of what you mapped against.

# COMMAND ----------

# Make sure target schemas exist (harmless if they already do).
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {p['staging_schema']}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {p['keymap_schema']}")

# SCD2 needs the validity columns carried into the snapshot; SCD1 does not.
validity_select = (
    f", {p['legacy_valid_from']} AS legacy_valid_from"
    f", {p['legacy_valid_to']}   AS legacy_valid_to"
) if is_scd2 else ""

spark.sql(f"""
CREATE OR REPLACE TABLE {snapshot_fqn} AS
SELECT
  {p['legacy_sk_col']}      AS old_sk,
  {legacy_nk_sql}           AS natural_key
  {validity_select}
FROM {legacy_fqn}
""")

# Tag the snapshot with its provenance — future-you will thank present-you.
spark.sql(f"""
ALTER TABLE {snapshot_fqn} SET TBLPROPERTIES (
  'snapshot_source' = '{legacy_fqn}',
  'snapshot_at'     = '{run_ts}',
  'created_by'      = 'surrogate_key_repair_notebook'
)
""")

cnt = spark.table(snapshot_fqn).count()
print(f"Snapshot {snapshot_fqn} created with {cnt:,} rows.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Pre-checks (run these BEFORE trusting the key-map)
# MAGIC
# MAGIC ### 2a. Collation / case-duplicate check on the LEGACY side
# MAGIC If the same normalized natural key maps to multiple legacy SKs, SQL Server's
# MAGIC case-insensitive collation was probably merging values that your Databricks
# MAGIC reload split into separate rows. These need a **human decision** (dedup rule)
# MAGIC before the key-map can be trusted for those keys.

# COMMAND ----------

legacy_dups = spark.sql(f"""
SELECT natural_key,
       COUNT(*)              AS row_cnt,
       COUNT(DISTINCT old_sk) AS distinct_sks
FROM {snapshot_fqn}
GROUP BY natural_key
HAVING COUNT(DISTINCT old_sk) > { '1' if not is_scd2 else '0' }
""")
# Note: for SCD2 a natural key legitimately has many rows (versions), so the
# SCD1 "must be unique" rule does not apply; we only surface the counts.

if not is_scd2:
    n = legacy_dups.count()
    if n > 0:
        print(f"⚠️  {n} natural keys map to MULTIPLE legacy SKs (SCD1 dim — this is a problem):")
        display(legacy_dups.orderBy("natural_key"))
    else:
        print("✅ Legacy natural keys are unique (SCD1).")
else:
    print("SCD2 dim — natural key version counts (informational):")
    display(legacy_dups.orderBy("natural_key").limit(50))

# COMMAND ----------

# ### 2b. Same uniqueness check on the CURRENT dim.
# A reload that double-inserted rows (very common in botched migrations) shows up here.
current_dups = spark.sql(f"""
SELECT {new_nk_sql} AS natural_key,
       COUNT(*) AS row_cnt,
       COUNT(DISTINCT {p['new_sk_col']}) AS distinct_sks
FROM {new_dim_fqn}
GROUP BY {new_nk_sql}
HAVING COUNT(DISTINCT {p['new_sk_col']}) > 1
""")

if not is_scd2:
    n = current_dups.count()
    if n > 0:
        print(f"⚠️  {n} natural keys map to MULTIPLE current SKs — dedup the dim before mapping!")
        display(current_dups.orderBy("natural_key"))
    else:
        print("✅ Current dim natural keys are unique (SCD1).")
else:
    print("SCD2 dim — duplicate check is by (natural_key, validity window); see step 3 overlap audit.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Build the key-map table
# MAGIC
# MAGIC The key-map is the contract: `natural_key -> old_sk -> new_sk`, plus a status:
# MAGIC
# MAGIC | map_status | Meaning | Action |
# MAGIC |---|---|---|
# MAGIC | `MATCHED` | NK exists on both sides | facts re-keyed old_sk → new_sk |
# MAGIC | `ORPHAN_OLD` | NK existed in legacy dim, missing after reload | facts using it go to unknown member; investigate why the reload dropped it |
# MAGIC | `ORPHAN_NEW` | NK only in current dim | no fact impact, but tells you the reload added members |
# MAGIC | `AMBIGUOUS` | (SCD2) one legacy row overlaps several new version windows | resolved per-fact by event date in step 5 |
# MAGIC
# MAGIC A **FULL OUTER JOIN** is used deliberately so both orphan classes are visible.

# COMMAND ----------

if not is_scd2:
    # ----------------------------- SCD1 map ----------------------------------
    spark.sql(f"""
    CREATE OR REPLACE TABLE {keymap_fqn} AS
    SELECT
      coalesce(o.natural_key, n.natural_key) AS natural_key,
      o.old_sk                               AS old_sk,
      n.new_sk                               AS new_sk,
      CAST(NULL AS TIMESTAMP)                AS valid_from,   -- unused for SCD1,
      CAST(NULL AS TIMESTAMP)                AS valid_to,     -- kept for schema parity
      CASE
        WHEN o.natural_key IS NULL THEN 'ORPHAN_NEW'
        WHEN n.natural_key IS NULL THEN 'ORPHAN_OLD'
        ELSE 'MATCHED'
      END                                    AS map_status,
      current_timestamp()                    AS created_at
    FROM {snapshot_fqn} o
    FULL OUTER JOIN (
        SELECT {new_nk_sql} AS natural_key, {p['new_sk_col']} AS new_sk
        FROM {new_dim_fqn}
    ) n
      ON o.natural_key = n.natural_key
    """)
else:
    # ----------------------------- SCD2 map ----------------------------------
    # Match on natural key + OVERLAPPING validity windows.
    # Overlap predicate:  o.start < n.end  AND  n.start < o.end
    # Open-ended rows (NULL valid_to) are treated as 9999-12-31.
    # One legacy row can overlap several new rows if the reload re-cut the
    # version boundaries — those rows are flagged AMBIGUOUS and resolved at
    # fact-repair time using the fact's event date (the only correct arbiter).
    spark.sql(f"""
    CREATE OR REPLACE TABLE {keymap_fqn} AS
    WITH joined AS (
      SELECT
        o.natural_key,
        o.old_sk,
        n.new_sk,
        n.new_valid_from  AS valid_from,
        n.new_valid_to    AS valid_to,
        COUNT(n.new_sk) OVER (PARTITION BY o.old_sk) AS match_cnt
      FROM {snapshot_fqn} o
      LEFT JOIN (
          SELECT {new_nk_sql}        AS natural_key,
                 {p['new_sk_col']}   AS new_sk,
                 {p['new_valid_from']} AS new_valid_from,
                 {p['new_valid_to']}   AS new_valid_to
          FROM {new_dim_fqn}
      ) n
        ON  o.natural_key = n.natural_key
        AND o.legacy_valid_from              < coalesce(n.new_valid_to,   timestamp'9999-12-31')
        AND n.new_valid_from                 < coalesce(o.legacy_valid_to, timestamp'9999-12-31')
    )
    SELECT
      natural_key, old_sk, new_sk, valid_from, valid_to,
      CASE
        WHEN new_sk IS NULL THEN 'ORPHAN_OLD'
        WHEN match_cnt > 1  THEN 'AMBIGUOUS'
        ELSE 'MATCHED'
      END                  AS map_status,
      current_timestamp()  AS created_at
    FROM joined
    """)

spark.sql(f"""
ALTER TABLE {keymap_fqn} SET TBLPROPERTIES (
  'maps_legacy' = '{legacy_fqn}',
  'maps_current'= '{new_dim_fqn}',
  'built_at'    = '{run_ts}'
)
""")
print(f"Key-map {keymap_fqn} built.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Audit the key-map — STOP HERE AND READ THE OUTPUT
# MAGIC
# MAGIC Do not proceed to fact repair until the numbers below make sense to you.
# MAGIC A large `ORPHAN_OLD` count means the reload *dropped* members — fixing the
# MAGIC facts won't help until the dimension itself is fixed.

# COMMAND ----------

print("Key-map status distribution:")
display(spark.sql(f"""
SELECT map_status, COUNT(*) AS rows
FROM {keymap_fqn}
GROUP BY map_status
ORDER BY map_status
"""))

# How much of each FACT is actually affected? (counts rows per status by FK)
for f in facts:
    for fk in f["fks"]:
        print(f"\nImpact on {f['table']} (FK: {fk}):")
        display(spark.sql(f"""
        SELECT coalesce(km.map_status, 'NOT_IN_LEGACY_DIM') AS map_status,
               COUNT(*) AS fact_rows
        FROM {f['table']} fct
        LEFT JOIN {keymap_fqn} km ON fct.{fk} = km.old_sk
        GROUP BY 1 ORDER BY 1
        """))
    # 'NOT_IN_LEGACY_DIM' rows carry an SK that never existed in the legacy dim.
    # Likely causes: that fact was ALSO reloaded after the dims (already has new
    # SKs — must be EXCLUDED from re-keying!), or the SK was already broken
    # before migration. Investigate before running step 5 on that fact.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Repair the facts (rebuild, never in-place)
# MAGIC
# MAGIC **Why rebuild into a `_fixed` table instead of UPDATE/MERGE in place?**
# MAGIC Old and new SKs were both identity-style sequences starting near 1, so the
# MAGIC ranges overlap. An in-place pass can re-map a row that was *already* re-mapped
# MAGIC (old 17 → new 42, then new 42 is itself someone's old key...). A single
# MAGIC LEFT JOIN into a fresh table maps every row exactly once. It also gives you a
# MAGIC free rollback path: the original fact is untouched until you swap.
# MAGIC
# MAGIC Skipped entirely when `dry_run = true`.

# COMMAND ----------

if dry_run:
    print("DRY RUN — skipping fact rebuild. Set dry_run=false to materialize *_fixed tables.")
else:
    for i, f in enumerate(facts):
        fixed_fqn = f"{f['table']}_fixed"
        fks = f["fks"]
        fk_list = ", ".join(fks)
        ev = event_date_cols[i] if is_scd2 else None

        if len(fks) == 1:
            fk = fks[0]
            if not is_scd2:
                join_sql = f"""
            LEFT JOIN {keymap_fqn} km
              ON fct.{fk} = km.old_sk
             AND km.map_status = 'MATCHED'
            """
                select_fk = f"coalesce(km.new_sk, {p['unknown_member_sk']}) AS {fk}"
            else:
                join_sql = f"""
            LEFT JOIN {keymap_fqn} km
              ON  fct.{fk} = km.old_sk
              AND km.map_status IN ('MATCHED', 'AMBIGUOUS')
              AND fct.{ev} >= km.valid_from
              AND fct.{ev} <  coalesce(km.valid_to, timestamp'9999-12-31')
            """
                select_fk = f"coalesce(km.new_sk, {p['unknown_member_sk']}) AS {fk}"
        else:
            join_parts = []
            select_parts = []
            for j, fk in enumerate(fks):
                alias = f"km{j}"
                if not is_scd2:
                    join_parts.append(f"""
            LEFT JOIN {keymap_fqn} {alias}
              ON fct.{fk} = {alias}.old_sk
             AND {alias}.map_status = 'MATCHED'""")
                else:
                    join_parts.append(f"""
            LEFT JOIN {keymap_fqn} {alias}
              ON  fct.{fk} = {alias}.old_sk
              AND {alias}.map_status IN ('MATCHED', 'AMBIGUOUS')
              AND fct.{ev} >= {alias}.valid_from
              AND fct.{ev} <  coalesce({alias}.valid_to, timestamp'9999-12-31')""")
                select_parts.append(
                    f"coalesce({alias}.new_sk, {p['unknown_member_sk']}) AS {fk}"
                )
            join_sql = "\n".join(join_parts)
            select_fk = ",\n          ".join(select_parts)

        if len(fks) == 1:
            spark.sql(f"""
        CREATE OR REPLACE TABLE {fixed_fqn} AS
        SELECT
          fct.* EXCEPT ({fk}),
          {select_fk}
        FROM {f['table']} fct
        {join_sql}
        """)
        else:
            spark.sql(f"""
        CREATE OR REPLACE TABLE {fixed_fqn} AS
        SELECT
          fct.* EXCEPT ({fk_list}),
          {select_fk}
        FROM {f['table']} fct
        {join_sql}
        """)

        src_cnt = spark.table(f["table"]).count()
        fix_cnt = spark.table(fixed_fqn).count()
        status  = "✅" if src_cnt == fix_cnt else "❌ ROW COUNT CHANGED — DO NOT SWAP"
        print(f"{status}  {f['table']}: {src_cnt:,} rows -> {fixed_fqn}: {fix_cnt:,} rows")

        for fk in fks:
            unknown_cnt = spark.sql(f"""
                SELECT COUNT(*) AS c FROM {fixed_fqn}
                WHERE {fk} = {p['unknown_member_sk']}
            """).first()["c"]
            print(f"    {fk} → unknown member ({p['unknown_member_sk']}): {unknown_cnt:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Post-repair validation
# MAGIC
# MAGIC Two layers:
# MAGIC 1. **Referential integrity** — zero orphans between `*_fixed` and the current dim.
# MAGIC 2. **Measure reconciliation** — totals per natural key vs. the legacy source.
# MAGIC    The orphan count alone can't catch re-keying that *shifted* measures between
# MAGIC    members (right totals, wrong customer); the per-NK comparison can.
# MAGIC    Edit the measure column below for your fact.

# COMMAND ----------

if not dry_run:
    for f in facts:
        fixed_fqn = f"{f['table']}_fixed"
        for fk in f["fks"]:
            orphans = spark.sql(f"""
            SELECT COUNT(*) AS c
            FROM {fixed_fqn} fct
            LEFT ANTI JOIN {new_dim_fqn} d
              ON fct.{fk} = d.{p['new_sk_col']}
            WHERE fct.{fk} <> {p['unknown_member_sk']}
        """).first()["c"]
            flag = "✅" if orphans == 0 else "❌"
            print(f"{flag} {fixed_fqn}.{fk}: {orphans:,} orphaned FK rows against {new_dim_fqn}")
else:
    print("DRY RUN — nothing to validate yet.")

# COMMAND ----------

# OPTIONAL measure reconciliation template — copy, set your measure column, run.
# Compares per-natural-key totals between the LEGACY fact (via federation) and
# the repaired fact, surfacing any member whose total moved.
#
# spark.sql(f"""
# WITH legacy AS (
#   SELECT {legacy_nk_sql.replace('(', '(lf.').replace('coalesce(lf.upper', 'coalesce(upper')}  -- adjust aliases as needed
#          , SUM(lf.SalesAmount) AS amt
#   FROM {p['foreign_catalog']}.{p['legacy_schema']}.FactSales lf
#   JOIN {legacy_fqn} ld ON lf.CustomerSK = ld.{p['legacy_sk_col']}
#   GROUP BY 1
# ),
# fixed AS (
#   SELECT {new_nk_sql} AS natural_key, SUM(ff.sales_amount) AS amt
#   FROM main.gold.fact_sales_fixed ff
#   JOIN {new_dim_fqn} d ON ff.customer_sk = d.{p['new_sk_col']}
#   GROUP BY 1
# )
# SELECT coalesce(l.natural_key, f.natural_key) AS natural_key,
#        l.amt AS legacy_amt, f.amt AS fixed_amt,
#        coalesce(f.amt,0) - coalesce(l.amt,0) AS diff
# FROM legacy l FULL OUTER JOIN fixed f USING (natural_key)
# WHERE abs(coalesce(f.amt,0) - coalesce(l.amt,0)) > 0.01
# ORDER BY abs(diff) DESC
# """).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. The swap — deliberately manual
# MAGIC
# MAGIC When (and only when) step 6 is clean, swap each fact yourself:
# MAGIC
# MAGIC ```sql
# MAGIC ALTER TABLE main.gold.fact_sales        RENAME TO main.gold.fact_sales_broken;
# MAGIC ALTER TABLE main.gold.fact_sales_fixed  RENAME TO main.gold.fact_sales;
# MAGIC -- keep *_broken around until business sign-off, then drop it.
# MAGIC ```
# MAGIC
# MAGIC ## 8. Prevent recurrence
# MAGIC
# MAGIC Keep the key-map tables permanently — they are lineage documentation.
# MAGIC Then make SK generation **deterministic** so reloads become idempotent:
# MAGIC
# MAGIC ```sql
# MAGIC -- hash-based surrogate key: same natural key always yields the same SK,
# MAGIC -- no matter how many times the dimension is reloaded.
# MAGIC SELECT xxhash64({new_nk_sql}) AS customer_sk, ...
# MAGIC ```
# MAGIC
# MAGIC (If you must keep sequential SKs, persist the key-map and make every load
# MAGIC look up existing keys before assigning new ones — the map IS the contract.)
