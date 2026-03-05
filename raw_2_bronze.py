# Databricks notebook source
# MAGIC %skip
# MAGIC # Demo Step -> Create a catalog -> create schema -> Create a table -> Insert sample data -> convert to CDF
# MAGIC create table workspace.demo.landing_employee_cdf (
# MAGIC     id string, 
# MAGIC     fullname string, 
# MAGIC     sal string, 
# MAGIC     doj string
# MAGIC );
# MAGIC
# MAGIC insert into workspace.demo.landing_employee_cdf (id,fullname,sal,doj) 
# MAGIC values 
# MAGIC ('1','John Doe','10000','2022-01-01'),
# MAGIC ('3','Jack Henry','30000','2022-03-01'),
# MAGIC ('5','Jill Adam','40000','2022-04-01'),
# MAGIC ('6',' Edwin Hall ','100000','2022-10-01');
# MAGIC
# MAGIC select * from workspace.demo.landing_employee_cdf;
# MAGIC
# MAGIC ALTER TABLE workspace.demo.landing_employee_cdf SET TBLPROPERTIES ('delta.enableChangeDataFeed' = true);

# COMMAND ----------

# Import libraries
import dlt
from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

#expectations actions:
#    except         => retain invalid rows but collect metrics like number of records pass/fail
#    expect_or_drop => violations are dropped from the target dataset but pipeline contineues
#    expect_or_fail => violations are dropped and pipeline is stopped
#Below are the rules defined in a dictionary
dataset_rules = {
    "Id_cannot_be_null" : "id is not null",
    "fullname_non_empty": "fullname is not null and length(fullname) > 0"
}


# COMMAND ----------

# MAGIC %md
# MAGIC **Change data feed has additional schema**
# MAGIC - _change_type => contains values such as insert, delete, update (preimage shows before update value, postimage shows after update value)
# MAGIC - _cmmit_version => delta log version
# MAGIC - _commit_timestamp => date and time when commit occured

# COMMAND ----------

@dlt.table(name="bronze_employee_cdf")

#use expect_all to define all expectations in a dictionary dataset_rules
@dlt.expect_all(dataset_rules) 

def bronze_employee_cdf():
    cdf = spark.readStream \
        .format("delta") \
        .option("readChangeFeed", "true") \
        .table("landing_employee_cdf")  
    return cdf
    