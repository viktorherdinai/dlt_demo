# Databricks notebook source
# DBTITLE 1,Create test dataset
import dlt
import pyspark.sql.functions as f

@dlt.table(
    temporary=True,
    comment="Test dataset"
)
def bronze_worker():
    return spark.createDataFrame([
            (1, "Bob", 4201, 214, "New York", 1721048032, "M", "DS", 0.1),
            (2, "Alice", 4201, 250, "Boston", 1721048033, "F", "FE",  0.1),
            (3, "Trudy", 8000, 453, "Texas", 1721048034, "O", "BE",  0.1),
            (1, "Bob", 7000, 234, "New York", 1721048035, "M", "DE",  0.1),
            (1, "Bob", 4000, 654, "Boston", 1721048036, "O", "DE",  0.1),
        ], 
        schema="worker_id string, worker_name string, salary string, bonus string, address string, created_at string, gender string, position string, random string"
    )

# COMMAND ----------

# DBTITLE 1,Unique PK check
@dlt.table(temporary=True, name="TEST_check_unique_pk")
@dlt.expect_or_fail("unique_pk", "unique_count = 1")
def check_unique_pk():
    """Temporary table that checks if the primary key is unique. If the key is not unique the pipeline fails."""
    return (
        dlt.read("silver_worker")
         .groupBy("worker_id")
         .agg(f.countDistinct("worker_name").alias("unique_count"))
    )

# COMMAND ----------

# DBTITLE 1,Check quarantine table
@dlt.table(temporary=True, name="TEST_quarantine_worker")
@dlt.expect_or_fail("corrupt data", "bonus <= 200 OR salary <= 2000 OR worker_id < 1")
def test_quarantine_worker():
    """Temporary table that checks whether one of the health checks are violated."""
    return (
        dlt.read("quarantine_worker")
    )

# COMMAND ----------

# DBTITLE 1,Check gender tables
def test_for_gender_tables(gender: str):
    @dlt.table(temporary=True, name=f"TEST_{gender}_table")
    @dlt.expect_or_fail("single gender", f"gender = '{gender}'")
    def test_gender_table():
        return dlt.read(f"{gender}_workers")

for gender in ["male", "female", "other"]:
    test_for_gender_tables(gender)

# COMMAND ----------

# DBTITLE 1,Test workers latest position
@dlt.table(temporary=True, name="TEST_latest_position")
@dlt.expect_or_fail("row_count", "row_count = 4")
def test_latest_position():
    return (
        dlt.read("workers_latest_position").agg(f.count("*").alias("row_count"))
    )

# COMMAND ----------

# DBTITLE 1,Test multiplex table
@dlt.table(temporary=True, name="TEST_multiplex_data")
@dlt.expect_or_fail("big salary", "salary > 8000")
@dlt.expect_or_fail("genders", 'gender in ("male", "female", "other")')
def test_multiplex_data():
    return (
        dlt.read("multiplexed_workers")
    )

# COMMAND ----------


@dlt.table(temporary=True, name="TEST_bob_bonus_sum")
@dlt.expect_or_fail("bob bonus", "total_bonus = 1102")
def test_bonus_sum():
    return dlt.read("total_bonus_by_worker").where(f.col("worker_name") == "Bob")

# COMMAND ----------

# DBTITLE 1,Test salary by gender
@dlt.table(temporary=True, name="TEST_other_salary_avg")
@dlt.expect_or_fail("other avg salary", "average_salary = 6000")
def test_bonus_sum():
    return dlt.read("salary_by_gender").where(f.col("gender") == "other")
