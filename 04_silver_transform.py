# Databricks notebook source
# Notebook: 04_silver_transform.py
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

# ── Config ────────────────────────────────────────────────────────────────────
BRONZE_TABLE = "ccir_workspace.default.bronze_compute_pricing"
SILVER_TABLE = "ccir_workspace.default.silver_compute_pricing"

# ── GPU Performance Index ─────────────────────────────────────────────────────
GPU_PERF_INDEX = {
    "A100":        1.00,
    "V100":        0.45,
    "A10":         0.35,
    "A10G":        0.35,   # AWS naming for A10
    "L4":          0.40,   # Newer than A10, slightly better inference perf
    "T4":          0.20,
    "P100":        0.30,
    "Inferentia":  0.30,   # AWS proprietary — approximate
    "unknown":     0.25,
}

# ── GPU Count Per Instance ────────────────────────────────────────────────────
GPU_COUNT_MAP = {
    "p4d.24xlarge":         8,
    "p4de.24xlarge":        8,
    "p5.48xlarge":          8,
    "p3.2xlarge":           1,
    "p3.8xlarge":           4,
    "p3.16xlarge":          8,
    "p3dn.24xlarge":        8,
    "g5.xlarge":            1,
    "g5.2xlarge":           1,
    "g5.4xlarge":           1,
    "g5.8xlarge":           1,
    "g5.12xlarge":          4,
    "g5.16xlarge":          1,
    "g5.24xlarge":          4,
    "g5.48xlarge":          8,
    "g4dn.xlarge":          1,
    "g4dn.2xlarge":         1,
    "g4dn.4xlarge":         1,
    "g4dn.8xlarge":         1,
    "g4dn.12xlarge":        4,
    "g4dn.16xlarge":        1,
    "g6.xlarge":            1,
    "g6.2xlarge":           1,
    "g6.4xlarge":           1,
    "g6.8xlarge":           1,
    "g6.12xlarge":          4,
    "g6.16xlarge":          1,
    "g6.24xlarge":          4,
    "g6.48xlarge":          8,
    "g6f.xlarge":           1,
    "g6f.2xlarge":          1,
    "g6f.4xlarge":          1,
    "g6f.8xlarge":          1,
    "g6f.12xlarge":         4,
    "g6f.48xlarge":         8,
    "inf1.xlarge":          1,
    "inf1.2xlarge":         1,
    "inf1.6xlarge":         4,
    "inf1.24xlarge":        16,
    "inf2.xlarge":          1,
    "inf2.8xlarge":         1,
    "inf2.24xlarge":        6,
    "inf2.48xlarge":        12,
    "ND96asr_v4":           8,
    "ND96amsr_A100_v4":     8,
    "ND96asrA100v4_NU":     8,
    "ND40rs_v2":            8,
    "NC24ads_A100_v4":      1,
    "NC4as_T4_v3":          1,
    "NC8as_T4_v3":          1,
    "NC16as_T4_v3":         1,
    "NC64as_T4_v3":         4,
    "NV6ads_A10_v5":        1,
    "NV18ads_A10_v5":       1,
    "NV36ads_A10_v5":       1,
    "NV72ads_A10_v5":       2,
    "NC6s_v3":              1,
    "NC12s_v3":             2,
    "NC24s_v3":             4,
    "NC24rs_v3":            4,
}

# ── Load Bronze — drop legacy gpu_count column ────────────────────────────────
df_bronze = spark.table(BRONZE_TABLE).drop("gpu_count")

# ── Step 1: Deduplicate ───────────────────────────────────────────────────────
windowSpec = Window.partitionBy("provider", "instance_type", "region", "price_type") \
                   .orderBy(F.col("ingested_at").desc())

df_deduped = (
    df_bronze
        .withColumn("row_num", F.row_number().over(windowSpec))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
)

print(f"Bronze records:    {df_bronze.count()}")
print(f"After dedupe:      {df_deduped.count()}")

# ── Step 2: Filter Bad Data ───────────────────────────────────────────────────
df_clean = (
    df_deduped
        .filter(F.col("price_per_hour").isNotNull())
        .filter(F.col("price_per_hour") > 0)
        .filter(F.col("ccir_tier").isNotNull())
        .filter(F.col("ccir_tier") != "Unknown")
        .filter(F.col("gpu_type").isNotNull())
)

print(f"After cleaning:    {df_clean.count()}")

# ── Step 3: GPU Count Join ────────────────────────────────────────────────────
gpu_count_rows = [(k, v) for k, v in GPU_COUNT_MAP.items()]
df_gpu_count = spark.createDataFrame(gpu_count_rows, ["instance_type_key", "gpu_count"])

df_with_counts = (
    df_clean
        .withColumn("instance_type_key",
            F.regexp_replace(F.col("instance_type"), "^Standard_", ""))
        .join(df_gpu_count, on="instance_type_key", how="left")
        .drop("instance_type_key")
        .withColumn("gpu_count", F.coalesce(F.col("gpu_count"), F.lit(1)))
        .withColumn("price_per_gpu_hr",
            F.round(F.col("price_per_hour") / F.col("gpu_count"), 4))
)


# ── Step 5: Assemble Silver ───────────────────────────────────────────────────
df_silver = (
    df_normalized
        .withColumn("transformed_at", F.current_timestamp())
        .select(
            "provider",
            "instance_type",
            "gpu_type",
            "gpu_count",
            "perf_index",
            "region",
            "ccir_tier",
            "price_type",
            "price_per_hour",
            "price_per_gpu_hr",
            "price_per_perf_unit",
            "currency",
            "ingested_at",
            "transformed_at",
            "source_file"
        )
)

# ── Write Silver ──────────────────────────────────────────────────────────────
(
    df_silver.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(SILVER_TABLE)
)

print(f"\nSilver load complete.")

# ── Validate ──────────────────────────────────────────────────────────────────
df_check = spark.table(SILVER_TABLE)
print(f"Total Silver records: {df_check.count()}")

print(f"\nAvg price_per_gpu_hr and price_per_perf_unit by tier:")
(
    df_check
        .filter(F.col("price_type") == "on_demand")
        .groupBy("ccir_tier")
        .agg(
            F.round(F.avg("price_per_hour"), 4).alias("avg_price_per_hr"),
            F.round(F.avg("price_per_gpu_hr"), 4).alias("avg_price_per_gpu_hr"),
            F.round(F.avg("price_per_perf_unit"), 4).alias("avg_perf_adjusted_price"),
            F.count("*").alias("count")
        )
        .orderBy("avg_perf_adjusted_price")
        .show()
)