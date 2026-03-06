# CCIR Compute Pricing Pipeline

A medallion architecture data pipeline built on Azure Databricks that ingests real-time GPU instance pricing from AWS and Azure, normalizes it across cloud compute tiers, and produces a reference pricing index for cloud GPU markets.

## Motivation

Cloud GPU pricing is opaque. The same underlying hardware is sold under different instance names, across different regions, at prices that vary dramatically by provider — with no independent reference rate to anchor against. I was curious whether meaningful price discrimination exists across different types of GPU workloads (training large models vs. running inference vs. fine-tuning), and whether AWS and Azure price those workloads differently relative to each other.

This pipeline is an attempt to answer that empirically, using publicly available pricing APIs and a structured lakehouse architecture.

---

## Architecture

```
AWS Pricing API  ─┐
                  ├──► Bronze (raw JSON) ──► Silver (normalized) ──► Gold (CCIR Index)
Azure Retail API ─┘
```

Built on the **medallion architecture** pattern using Azure Databricks and Delta Lake:

| Layer | Description |
|-------|-------------|
| **Bronze** | Raw JSON ingested via Databricks Autoloader from Unity Catalog Volumes |
| **Silver** | Cleaned, deduplicated, GPU-count normalized, and tier-classified pricing data |
| **Gold** | Aggregated CCIR index — median price per GPU hour by compute tier, provider spreads, and best value per tier |

---

## Compute Tiers

GPU workloads map to different hardware classes, each with distinct pricing dynamics:

| Tier | Typical Hardware | Use Case |
|------|-----------------|----------|
| **Training** | A100, H100 | Full model training runs |
| **Fine-tuning** | A10, V100 | Adapting pre-trained models |
| **Inference** | T4, L4 | Serving models in production |

---

## Pipeline Notebooks

| Notebook | Purpose |
|----------|---------|
| `01_ingest_aws.py` | Fetches EC2 GPU instance pricing from the AWS bulk pricing API (no auth required) and lands raw NDJSON to Unity Catalog Volume |
| `02_ingest_azure.py` | Fetches GPU VM pricing from the Azure Retail Prices API across key regions and lands raw NDJSON to Unity Catalog Volume |
| `03_bronze_autoloader.py` | Databricks Autoloader stream that monitors the raw Volume path and writes new files into the Bronze Delta table as they arrive |
| `04_silver_transform.py` | Deduplicates, filters bad data, joins a GPU count lookup table to normalize raw $/hr to $/GPU/hr, and writes the Silver Delta table |
| `05_gold_index.py` | Computes the CCIR index — median $/GPU/hr per tier, interquartile range, provider spread (AWS vs Azure), and best value instance per tier |

---

## Sample Output (March 2026)

| Tier | CCIR Index (Median $/GPU/hr) | AWS Median | Azure Median | Provider Spread |
|------|------------------------------|------------|--------------|-----------------|
| Fine-tuning | $2.04 | $2.04 | $1.60 | -21% |
| Inference | $2.18 | $2.56 | $0.75 | -71% |
| Training | $4.10 | $6.88 | $4.10 | -40% |

**Key findings:**
- Training commands a ~2x premium over Inference on a per-GPU-hour basis, consistent with hardware scarcity and longer reservation windows
- Fine-tuning and Inference prices have converged, reflecting hardware commoditization at the A10/L4/T4 tier
- Azure prices Training and Inference meaningfully cheaper than AWS; AWS prices Edge compute cheaper than Azure — the provider premium is not stable across tiers, which makes a single blended price meaningless as a reference rate

---

## Tech Stack

- **Azure Databricks** (serverless + Unity Catalog)
- **Delta Lake** — ACID transactions, time travel on all three layers
- **Databricks Autoloader** — incremental file ingestion with schema inference
- **PySpark** — distributed transforms in Silver and Gold
- **AWS EC2 Bulk Pricing API** — public, no auth
- **Azure Retail Prices API** — public, no auth

---

## Reproducing This

1. Spin up an Azure Databricks workspace (14-day trial is sufficient)
2. Run `01_ingest_aws.py` and `02_ingest_azure.py` to land raw pricing files
3. Run `03_bronze_autoloader.py` to stream files into the Bronze Delta table
4. Run `04_silver_transform.py` to produce the normalized Silver layer
5. Run `05_gold_index.py` to compute and display the index

No API keys required. All pricing data is publicly available.
