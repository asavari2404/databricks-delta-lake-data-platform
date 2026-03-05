# Databricks notebook source
# Import libraries
import dlt
from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

#Silver: Clean + keep CDF metadata (needed for downstream)
#@dp.expect_all_or_drop(dataset_rules) # while creating silver view log metrics and drop the bad rows
    #"LIVE." => refer to another DLT-managed table in the same pipeline graph. Never use "LIVE." outside the pipeline
@dlt.view
def silver_employee_cdf():
    df = dlt.read_stream("bronze_employee_cdf") # or spark.readStream.table("LIVE.bronze_employee_cdf")

    return df.select(
        col("id").cast("int").alias("id"),
        trim(col("fullname")).alias("fullname"),
        col("sal").cast("double").alias("sal"),
        to_date(col("doj"), "yyyy-MM-dd").alias("doj"),

        # alias Delta CDF reserved columns to non-reserved names
        col("_change_type").alias("cdf_change_type"),
        col("_commit_timestamp").alias("cdf_commit_timestamp"),
        col("_commit_version").alias("cdf_commit_version"),
    )

# GOLD latest-state table (never deletes)
# Purpose: preserve last known business values per id for archiving deletes.
# NO CDF metadata stored here.
dlt.create_streaming_table("gold_employee_latest_state")
dlt.apply_changes(
    target="gold_employee_latest_state",
    source="silver_employee_cdf",
    keys=["id"],
    # safer sequencing than timestamp alone (optional but recommended)
    sequence_by=struct(col("cdf_commit_timestamp"), col("cdf_commit_version")),
    ignore_null_updates=True,
    # IMPORTANT: no apply_as_deletes here (so record remains as last known)
    apply_as_truncates=expr("cdf_change_type = 'truncate'"),
    # Remove CDF columns from this table (business-only)
    except_column_list=["cdf_change_type", "cdf_commit_timestamp", "cdf_commit_version"],
    stored_as_scd_type=1
)


# COMMAND ----------

# GOLD SCD TYPE 1 (current state)
# - Physically delete on CDF delete
# - Do NOT store CDF metadata columns in the target

dlt.create_streaming_table("gold_employee_cdf_type1")
#doing in place update - don't want to track history
#target table is gold_employee_cdf_type1
#read data from silver employee cdf
dlt.apply_changes(
    target="gold_employee_cdf_type1",
    source="silver_employee_cdf",
    keys=["id"],
    sequence_by=struct(col("cdf_commit_timestamp"), col("cdf_commit_version")),
    ignore_null_updates=True,
    apply_as_deletes=expr("cdf_change_type = 'delete'"),
    apply_as_truncates=expr("cdf_change_type = 'truncate'"),
    # Drop CDF common schema columns from the GOLD SCD1 table
    except_column_list=["cdf_change_type", "cdf_commit_timestamp", "cdf_commit_version"],
    stored_as_scd_type=1
)


# COMMAND ----------

# GOLD ARCHIVE TABLE (for deleted rows)
# Requirement:
# - Archive table includes CDF common schema attributes
# - Archive row should contain the most complete "last-known" business values
# Strategy:
# Join deletes to gold_employee_latest_state (NOT gold_employee_cdf_type1)
#   because gold_employee_cdf_type1 may already have deleted the row.
# - Use coalesce so if delete event carries some business columns, we keep them.
@dlt.table(name="gold_employee_cdf_archive")
def gold_employee_cdf_archive():
    deletes = (
        dlt.read_stream("silver_employee_cdf")
        .filter(col("cdf_change_type") == lit("delete"))
        .select(
            "id",
            "fullname",  # may be null on delete
            "sal",       # may be null on delete
            "doj",       # may be null on delete
            "cdf_change_type",
            "cdf_commit_timestamp",
            "cdf_commit_version"
        )
    )

    latest = dlt.read("gold_employee_latest_state").select(
        "id", "fullname", "sal", "doj"
    )

    archived = (
        deletes.alias("d")
        .join(latest.alias("l"), on="id", how="left")
        .select(
            col("d.id").alias("id"),
            # prefer values present in delete payload, else fallback to last-known
            coalesce(col("d.fullname"), col("l.fullname")).alias("fullname"),
            coalesce(col("d.sal"), col("l.sal")).alias("sal"),
            coalesce(col("d.doj"), col("l.doj")).alias("doj"),
            # required CDF metadata in archive
            col("d.cdf_change_type").alias("cdf_change_type"),
            col("d.cdf_commit_timestamp").alias("cdf_commit_timestamp"),
            col("d.cdf_commit_version").alias("cdf_commit_version"),
            # archive bookkeeping
            current_timestamp().alias("archived_at")
        )
    )

    return archived