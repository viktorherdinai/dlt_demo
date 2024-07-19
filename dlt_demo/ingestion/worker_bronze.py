# Databricks notebook source
import dlt
import pyspark.sql.functions as f

s3_bucket_root = spark.conf.get("s3_bucket_root")
schema_location = spark.conf.get("schema_location")

@dlt.table(
    table_properties={
        "quality": "bronze",
        # "pipelines.reset.allowed": "false" 
    }
)
def bronze_worker():
    """
    Bronze table containing raw data.
    If data with new column comes the pipeline will fail and restart with the evolved schema.
    """
    df = (
        spark.readStream.format("cloudFiles")
                .option("cloudFiles.format", "json")
                .option("cloudFiles.schemaLocation", schema_location)
                .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
                .option("multiline", True)
                .load(f"{s3_bucket_root}/mock_data/")
                .withColumn("file_name", f.input_file_name())
                .withColumn("current_timestamp", f.current_timestamp())
    )

    if "gender" not in df.columns:
        df = df.withColumn("gender", f.lit(None).cast("string"))

    return df

