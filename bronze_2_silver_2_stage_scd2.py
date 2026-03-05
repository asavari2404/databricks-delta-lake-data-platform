# Databricks notebook source
import dlt
from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# SILVER (normalize + keep CDF metadata for sequencing/deletes)

@dlt.view
def silver_employee_cdf_v2():
    df = spark.readStream.table("LIVE.bronze_employee_cdf")
    df = df.withColumn("id", df["id"].cast("int"))
    df = df.withColumn("sal", df["sal"].cast("double"))
    df = df.withColumn("doj", to_date(df["doj"], "yyyy-MM-dd"))
    df = df.withColumn("fullname", trim(col("fullname")))
    # Keep CDF info but don't push to GOLD later
    df = df.withColumn("cdf_change_type", col("_change_type"))
    df = df.withColumn("cdf_commit_version", col("_commit_version"))
    df = df.withColumn("cdf_commit_timestamp", col("_commit_timestamp"))
    return df
# Fixed: removed incorrect .alias usage, corrected withColumn chaining, ensured function returns DataFrame

# COMMAND ----------

dlt.create_streaming_table("stage_employee_cdf_type2")
dlt.apply_changes(
    target = "stage_employee_cdf_type2",
    source = "silver_employee_cdf_v2",
    keys = ["id"],
    sequence_by=struct(col("cdf_commit_timestamp"), col("cdf_commit_version")),
    ignore_null_updates = True, #columns with a null retain existing values in the target during update
    apply_as_deletes = expr("cdf_change_type = 'delete'"),
    #except_column_list = ["operation", "sequenceNum"],
    stored_as_scd_type = 2
)

# COMMAND ----------

# GOLD (drop CDF attrs + rename/convert dates + Active/Inactive row_status)
@dlt.table
def gold_employee_cdf_type2():
    df = dlt.read("stage_employee_cdf_type2")
    # Decide which SCD2 boundary columns exist
    start_col = "__start_date" if "__start_date" in df.columns else ("__START_AT" if "__START_AT" in df.columns else None)
    end_col   = "__end_date"   if "__end_date"   in df.columns else ("__END_AT"   if "__END_AT"   in df.columns else None)
    
    def boundary_to_date(boundary_col_name: str):
        if boundary_col_name is None:
            return lit(None).cast("date")

        dt = df.schema[boundary_col_name].dataType
        # If DLT stores boundaries as a struct, pull the timestamp field that actually exists
        if isinstance(dt, StructType):
            field_names = [f.name for f in dt.fields]

            if "cdf_commit_timestamp" in field_names:
                ts_expr = col(boundary_col_name).getField("cdf_commit_timestamp")
            elif "_commit_timestamp" in field_names:
                ts_expr = col(boundary_col_name).getField("_commit_timestamp")
            else:
                # fall back: try first field if it looks like timestamp-like
                ts_expr = col(boundary_col_name).getField(field_names[0])

            return to_date(ts_expr.cast("timestamp"))
        
        # Otherwise boundary is already timestamp/date-ish
        return to_date(col(boundary_col_name).cast("timestamp"))
    
    # Create new DATE columns with date-only content
    df = df.withColumn("start_date", boundary_to_date(start_col))
    df = df.withColumn("end_date", boundary_to_date(end_col))

    # Active/Inactive logic (A when current row)
    df = df.withColumn("row_status", when(col("end_date").isNull(), lit("A")).otherwise(lit("I")))

    # Drop CDF + internal boundary columns from GOLD
    drop_cols = [
        "_change_type", "_commit_version", "_commit_timestamp",
        "cdf_change_type", "cdf_commit_version", "cdf_commit_timestamp",
        "__start_date", "__end_date", "__START_AT", "__END_AT"
    ]
    df = df.drop(*[c for c in drop_cols if c in df.columns])

    return df

    # Helper: extract a DATE from a column that might be:
    # - a timestamp/date column
    # - OR a struct containing _commit_timestamp

# COMMAND ----------

'''
dlt.create_streaming_table("gold_employee_cdf_streaming_type2")

dlt.apply_changes(
    target = "gold_employee_cdf_streaming_type2",
    source = "silver_employee_cdf",
    keys = ["id"],
    sequence_by = struct("id","_commit_timestamp"),
    ignore_null_updates = True, #columns with a null retain existing values in the target during update
    apply_as_deletes = expr("_change_type = 'delete'"),
    #except_column_list = ["operation", "sequenceNum"],
    stored_as_scd_type = 2
) 
'''

# COMMAND ----------

'''
@dlt.table
def gold_employee_cdf_type2_stage_v():
    df = dlt.read("gold_employee_cdf_type2_stage")
    return df

@dlt.table
def gold_employee_cdf_type2():
    df = dlt.read("gold_employee_cdf_type2_stage_v")
    rename_dict = {"__start_at": "start_dt", "__end_at": "end_dt"}
    for old_name, new_name in rename_dict.items():
        df = df.withColumn(\
            new_name,\
            when(\
                col(old_name).isNotNull(),\
                col(old_name).getField("_commit_timestamp")\
                )\
            .otherwise(lit(None))\
            )
    df = df.withColumn("is_active", when(col("end_dt").isNull(), lit("Y")).otherwise(lit("N")))
    df = df.drop("_change_type", "_commit_version","_commit_timestamp")
      
    return df
    '''