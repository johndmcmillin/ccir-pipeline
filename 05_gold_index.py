# Databricks notebook source
# Notebook: 05_gold_index.py
# Purpose: Compute CCIR index values from Silver layer
# Output: Gold Delta table with index scores, provider spreads, and tier benchmarks

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ── Config ────────────────────────────────────────────────────────────────────
SILVER_TABLE = "ccir_workspace.default.silver_compute_pricing"
GOLD_TABLE   = "ccir_workspace.default.gold_ccir_index"

# ── Load Silver ───────────────────────────────────────────────────────────────
df_silver = spark.table(SILVER_TABLE) \
    .filter(F.col("price_type") == "on_demand") \
    .filter(F.col("ccir_tier") != "Edge")

# ── Index 1: CCIR Tier Index ──────────────────────────────────────────────────
# Core index: median perf-adjusted price per tier across all providers
# Using median (percentile 50) rather than mean — more robust to outliers
# This is the headline CCIR number analogous to SOFR

df_tier_index = (
    df_silver
        .groupBy("ccir_tier")
        .agg(
            F.percentile_approx("price_per_gpu_hr", 0.50).alias("ccir_index_median"),
            F.percentile_approx("price_per_gpu_hr", 0.25).alias("ccir_index_p25"),
            F.percentile_approx("price_per_gpu_hr", 0.75).alias("ccir_index_p75"),
            F.round(F.avg("price_per_gpu_hr"), 4).alias("ccir_index_mean"),
            F.round(F.stddev("price_per_gpu_hr"), 4).alias("ccir_index_stddev"),
            F.round(F.min("price_per_gpu_hr"), 4).alias("price_floor"),
            F.round(F.max("price_per_gpu_hr"), 4).alias("price_ceiling"),
            F.count("*").alias("sample_size")
        )
        .withColumn("index_date", F.current_date())
        .withColumn("index_timestamp", F.current_timestamp())
)

# ── Index 2: Provider Spread ──────────────────────────────────────────────────
# AWS vs Azure premium/discount per tier
# Analogous to basis spread in fixed income

df_provider = (
    df_silver
        .groupBy("ccir_tier", "provider")
        .agg(
            F.percentile_approx("price_per_gpu_hr", 0.50).alias("provider_median"),
            F.round(F.avg("price_per_gpu_hr"), 4).alias("provider_mean"),
            F.count("*").alias("sample_size")
        )
)

# Pivot to get AWS and Azure side by side
df_spread = (
    df_provider
        .groupBy("ccir_tier")
        .pivot("provider", ["AWS", "Azure"])
        .agg(F.first("provider_median"))
        .withColumnRenamed("AWS", "aws_median")
        .withColumnRenamed("Azure", "azure_median")
        .withColumn("provider_spread",
            F.round(F.col("azure_median") - F.col("aws_median"), 4))
        .withColumn("spread_pct",
            F.round((F.col("azure_median") - F.col("aws_median")) / F.col("aws_median") * 100, 2))
)

# ── Index 3: Best Value Per Tier ──────────────────────────────────────────────
# Lowest perf-adjusted price per tier — the "floor" reference rate

windowSpec = Window.partitionBy("ccir_tier").orderBy(F.col("price_per_gpu_hr").asc())

df_best_value = (
    df_silver
        .withColumn("rank", F.row_number().over(windowSpec))
        .filter(F.col("rank") == 1)
        .select(
            "ccir_tier",
            F.col("provider").alias("best_value_provider"),
            F.col("instance_type").alias("best_value_instance"),
            F.col("gpu_type").alias("best_value_gpu"),
            F.col("region").alias("best_value_region"),
            F.col("price_per_hour").alias("best_value_price_hr"),
            F.col("price_per_gpu_hr").alias("best_value_perf_price")
        )
)

# ── Assemble Gold Table ───────────────────────────────────────────────────────
df_gold = (
    df_tier_index
        .join(df_spread, on="ccir_tier", how="left")
        .join(df_best_value, on="ccir_tier", how="left")
        .orderBy("ccir_index_median")
)

# ── Write Gold Delta Table ────────────────────────────────────────────────────
(
    df_gold.write
        .format("delta")
        .mode("append")          # Append so each run creates a new snapshot
        .option("mergeSchema", "true")
        .saveAsTable(GOLD_TABLE)
)

print("Gold index written successfully.\n")

# ── Display Final CCIR Index ──────────────────────────────────────────────────
df_display = spark.table(GOLD_TABLE) \
    .orderBy(F.col("index_timestamp").desc()) \
    .dropDuplicates(["ccir_tier"]) \
    .orderBy("ccir_index_median")

print("CCIR COMPUTE PRICING INDEX — {}".format(
    df_display.select("index_date").first()[0]))

display(df_display.select(
    "ccir_tier",
    "ccir_index_median",
    "ccir_index_p25",
    "ccir_index_p75",
    "ccir_index_stddev",
    "aws_median",
    "azure_median",
    "provider_spread",
    "spread_pct",
    "best_value_provider",
    "best_value_instance",
    "sample_size"
))

print("Best value per tier:")
display(df_display.select(
    "ccir_tier",
    "best_value_provider",
    "best_value_instance",
    "best_value_gpu",
    "best_value_price_hr",
    "best_value_perf_price"
))