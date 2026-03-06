# Databricks notebook source
# Notebook: 03_bronze_autoloader.py
# Purpose: Stream new JSON files from Unity Catalog Volume into Bronze Delta table
# Uses Databricks Autoloader (cloudFiles) to detect and process new files automatically

from pyspark.sql import functions as F
from pyspark.sql.types import *

# ── Config ───────────────────────────────────────────────────────────────────
RAW_PATH       = "/Volumes/ccir_workspace/default/ccir_raw/"
BRONZE_TABLE   = "ccir_workspace.default.bronze_compute_pricing"
CHECKPOINT_DIR = "/Volumes/ccir_workspace/default/ccir_checkpoints/bronze/"

from pyspark.sql.types import *

bronze_schema = StructType([
    StructField("provider",       StringType(), True),
    StructField("instance_type",  StringType(), True),
    StructField("product_name",   StringType(), True),
    StructField("gpu_type",       StringType(), True),
    StructField("gpu_count",      StringType(), True),
    StructField("vcpu",           StringType(), True),
    StructField("memory_gb",      StringType(), True),
    StructField("region",         StringType(), True),
    StructField("price_per_hour", DoubleType(),  True),
    StructField("price_type",     StringType(), True),
    StructField("currency",       StringType(), True),
    StructField("ccir_tier",      StringType(), True),
    StructField("raw_payload",    StringType(), True),
])

# ── Schema ───────────────────────────────────────────────────────────────────
# Define explicitly so Autoloader doesn't have to infer on every run
df_raw = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", CHECKPOINT_DIR + "schema/")
        .option("cloudFiles.inferColumnTypes", "true")
        .schema(bronze_schema)
        .load(RAW_PATH)
)

# ── Autoloader Stream ─────────────────────────────────────────────────────────
df_raw = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", CHECKPOINT_DIR + "schema/")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(RAW_PATH)
)

# ── Add Metadata Columns ──────────────────────────────────────────────────────
df_bronze = df_raw.select(
    "*",
    F.current_timestamp().alias("ingested_at"),
    F.col("_metadata.file_path").alias("source_file")
)

# ── Write to Bronze Delta Table ───────────────────────────────────────────────
query = (
    df_bronze.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_DIR)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)   # Process all available files then stop
        .toTable(BRONZE_TABLE)
)

query.awaitTermination()
print(f"Bronze load complete.")

# ── Validate ──────────────────────────────────────────────────────────────────
df_check = spark.table(BRONZE_TABLE)
print(f"Total records in Bronze: {df_check.count()}")
print(f"\nBreakdown by provider and tier:")
df_check.groupBy("provider", "ccir_tier").count().orderBy("provider", "ccir_tier").show()