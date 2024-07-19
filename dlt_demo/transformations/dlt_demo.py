# Databricks notebook source
# DBTITLE 1,Temporary Quarantine Table
import dlt
import pyspark.sql.functions as f

from pyspark.sql.functions import when

rules = {
    "valid_bonus": "bonus > 200",
    "hays_salary": "salary > 2000",
    "valid_id": "worker_id >= 1"
}

quarantine_rules = {
    "invalid_row": f"NOT ({' AND '.join(rules.values())})"
}

@dlt.table(
    name="temp_worker",
    temporary=True,
    partition_cols=["is_quarantined"]
)
@dlt.expect_all(rules)
def temp_worker():
    """
    Temporary worker table that applies the quarantine rules to identify invalid rows.
    """
    return (
        dlt
         .read_stream("bronze_worker")
         .withColumn("is_quarantined", f.expr(quarantine_rules["invalid_row"]))
    )


# COMMAND ----------

# DBTITLE 1,Silver table
@dlt.table(
    table_properties={
        "quality": "silver",
        "skipChangeCommits": "true",
    }
)
@dlt.expect_all_or_drop(rules)
def silver_worker():
    """Silver table containing cleaned and transformed data."""
    return (
        dlt.read_stream("temp_worker")
            .where(f.col("is_quarantined") == False)
            .withColumn("bonus", f.col("bonus").cast("int"))
            .withColumn("salary", f.col("salary").cast("int"))
            .withColumn("worker_id", f.col("worker_id").cast("int"))
            .withColumn("created_at", f.col("created_at").cast("long"))
            .withColumn("random", f.col("random").cast("double"))
            .withColumn("total", (f.col("salary") + f.col("bonus")).cast("int"))
            .withColumn("gender", when(f.col("gender") == "F", "female")\
                                  .when(f.col("gender") == "M", "male")\
                                  .when(f.col("gender") == "O", "other")\
                                  .otherwise(f.lit(None))
            )
            .drop("_rescued_data", "is_quarantined", "file_name", "random")
    )
    


# COMMAND ----------

# DBTITLE 1,Quarantine Table

@dlt.table(
    table_properties={
        "quality": "quarantine"
    }
)
def quarantine_worker():
    """Table containing corrupt data."""
    return (
        dlt.read_stream("temp_worker")
        .where(f.col("is_quarantined") == True)
    )



# COMMAND ----------

# DBTITLE 1,SCD Type 2 experiment
dlt.create_streaming_table(
    name="workers_latest_position"
)

# Apply changes from the "silver_worker" table to the "workers_latest_position" table
# Track changes in the "position" column
dlt.apply_changes(
    target="workers_latest_position",
    source="silver_worker",
    keys=["worker_id"],
    sequence_by=f.col("created_at"),
    apply_as_deletes=f.expr("salary < 3000"),
    stored_as_scd_type="2",
    track_history_column_list=["position"]
)

# COMMAND ----------

# DBTITLE 1,Split Male/Female/Other
# Multiplex source: https://www.databricks.com/blog/2022/04/27/how-uplift-built-cdc-and-multiplexing-data-pipelines-with-databricks-delta-live-tables.html
from collections import namedtuple

def create_gender_splitter(table_name: str, gender: str):
    """Create a gender table based on arg `gender`.

    Args:
        table_name(str): Name of the table to be created.
        gender(str): Gender which the silver table should be filtered by.
    """
    @dlt.table(
        name = table_name,
        comment = f"Table containing {gender} data."
    )
    def gender_table():
        return (
            dlt.read("silver_worker")
             .where(f.col("gender") == gender)
        )

TableConfig = namedtuple("TableConfig", ["table_name", "gender"])
table_configs = [
    TableConfig("female_workers", "female"),
    TableConfig("male_workers", "male"),
    TableConfig("other_workers", "other"),
]

for config in table_configs:
    create_gender_splitter(*config)


# COMMAND ----------

# DBTITLE 1,Multiplex Data
from functools import reduce

@dlt.table
def multiplexed_workers():
    """Table that combines the filtered data from the 'female_workers', 'male_workers', and 'other_workers' tables.
    Only includes rows where the salary is greater than 8000.
    """
    tables = ["female_workers", "male_workers", "other_workers"]
    df_list = [dlt.read(table).where("salary > 8000") for table in tables]
    unioned_df = reduce(lambda df1, df2: df1.union(df2), df_list)
    return unioned_df

# COMMAND ----------

# DBTITLE 1,Gold - Salary by gender
@dlt.table(
    table_properties={
        "quality": "gold"
    }
)
def salary_by_gender():
    """Table that calculates the average salary by gender from the 'multiplexed_workers' table."""
    return (
        dlt.read("multiplexed_workers")
         .groupBy("gender")
         .agg(f.avg("salary").alias("average_salary"))
    )

# COMMAND ----------

# DBTITLE 1,Gold - Total bonus per worker
@dlt.table(
    table_properties={
        "quality": "gold"
    }
)
def total_bonus_by_worker():
    """Table that calculates the total bonus by worker from the 'multiplexed_workers' table."""
    return (
        dlt.read("multiplexed_workers")
         .groupBy("worker_name")
         .agg(f.sum("bonus").alias("total_bonus"))
    )

# COMMAND ----------

# DBTITLE 1,View - Total bonus per worker (hashed)
dlt.view(
    name="gdpr_total_bonus",
    comment="Total bonus per worker - hashed"
)
def gdpr_total_bonus():
    """View that has the worker name replaced by its sha1 hash value."""
    return (
        dlt.read("total_bonus_by_worker")
        .withColumn("worker_name", f.sha1("worker_name"))
    )
