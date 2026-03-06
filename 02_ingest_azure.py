# Databricks notebook source
# Notebook: 02_ingest_azure.py
# Purpose: Fetch Azure GPU instance pricing and land raw JSON to Unity Catalog Volume

import requests
import json
import os
from datetime import datetime

# ── Config ───────────────────────────────────────────────────────────────────
OUTPUT_PATH = "/Volumes/ccir_workspace/default/ccir_raw/azure/"

GPU_SKU_KEYWORDS = [
    "ND96asr",    # A100 80GB — Training
    "ND96amsr",   # A100 80GB high mem — Training
    "ND40rs",     # V100 — Fine-tuning
    "NC24ads",    # A100 40GB — Fine-tuning
    "NC4as",      # T4 — Inference
    "NC8as",      # T4 — Inference
    "NC16as",     # T4 — Inference
    "NC64as",     # T4 — Inference
    "NV6ads",     # A10 — Inference
    "NV18ads",    # A10 — Inference
    "NV36ads",    # A10 — Inference
    "NC6s",       # P100 — Edge
    "NC12s",      # P100 — Edge
    "NC24s",      # P100 — Edge
]

REGIONS = ["eastus", "westus2", "eastus2"]


# ── Fetch Azure Pricing ───────────────────────────────────────────────────────
def fetch_azure_gpu_pricing(regions: list[str]) -> list[dict]:
    results = []

    for region in regions:
        print(f"Fetching Azure pricing for region: {region}")

        url = "https://prices.azure.com/api/retail/prices"
        params = {
            "$filter": f"serviceName eq 'Virtual Machines' and armRegionName eq '{region}' and priceType eq 'Consumption' and contains(productName, 'Windows') eq false",
            "api-version": "2023-01-01-preview"
        }

        page_count = 0
        next_url = url

        while next_url:
            if next_url == url:
                response = requests.get(next_url, params=params)
            else:
                response = requests.get(next_url)

            response.raise_for_status()
            data = response.json()
            page_count += 1

            for item in data.get("Items", []):
                sku = item.get("skuName", "")

                if not any(keyword.lower() in sku.lower() for keyword in GPU_SKU_KEYWORDS):
                    continue

                if "Spot" in sku or "Low Priority" in sku:
                    price_type = "spot"
                else:
                    price_type = "on_demand"

                tier = map_to_ccir_tier(sku)

                results.append({
                    "provider": "Azure",
                    "instance_type": sku,
                    "product_name": item.get("productName"),
                    "gpu_type": extract_gpu_type(sku),
                    "region": region,
                    "price_per_hour": item.get("unitPrice"),
                    "price_type": price_type,
                    "currency": item.get("currencyCode", "USD"),
                    "ccir_tier": tier,
                    "raw_payload": json.dumps(item)
                })

            next_url = data.get("NextPageLink")

        print(f"  → {region}: {len([r for r in results if r['region'] == region])} GPU records found across {page_count} pages")

    print(f"\nTotal Azure records: {len(results)}")
    return results


def extract_gpu_type(sku: str) -> str:
    sku_lower = sku.lower()
    if "nd96" in sku_lower:
        return "A100"
    elif "nv" in sku_lower and "ads" in sku_lower:
        return "A10"
    elif "nc" in sku_lower and "as" in sku_lower:
        return "T4"
    elif "nd40rs" in sku_lower:
        return "V100"
    elif "nc" in sku_lower and "s_v" in sku_lower:
        return "P100"
    else:
        return "unknown"


def map_to_ccir_tier(sku: str) -> str:
    sku_lower = sku.lower()
    if any(k in sku_lower for k in ["nd96asr", "nd96amsr", "nd40rs"]):
        return "Training"
    elif any(k in sku_lower for k in ["nc24ads", "nd"]):
        return "Fine-tuning"
    elif any(k in sku_lower for k in ["nc4as", "nc8as", "nc16as", "nc64as", "nv6ads", "nv18ads", "nv36ads"]):
        return "Inference"
    elif any(k in sku_lower for k in ["nc6s", "nc12s", "nc24s"]):
        return "Edge"
    else:
        return "Unknown"


# ── Write to Volume ───────────────────────────────────────────────────────────
def write_to_volume(records: list[dict], output_path: str):
    if not records:
        print("No records to write — check SKU filters or region availability")
        return None

    os.makedirs(output_path, exist_ok=True)

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"azure_{timestamp}.json"
    full_path = f"{output_path}{filename}"

    ndjson = "\n".join(json.dumps(r) for r in records)

    with open(full_path, "w") as f:
        f.write(ndjson)

    print(f"Written {len(records)} records to {full_path}")
    return full_path


# ── Main ──────────────────────────────────────────────────────────────────────
records = fetch_azure_gpu_pricing(REGIONS)
output_file = write_to_volume(records, OUTPUT_PATH)

if records:
    print(f"\nPreview of first record:")
    print(json.dumps(records[0], indent=2))

    print(f"\nTier breakdown:")
    from collections import Counter
    tiers = Counter(r["ccir_tier"] for r in records)
    for tier, count in sorted(tiers.items()):
        print(f"  {tier}: {count} records")

# COMMAND ----------

import os

aws_files = os.listdir("/Volumes/ccir_workspace/default/ccir_raw/aws/")
azure_files = os.listdir("/Volumes/ccir_workspace/default/ccir_raw/azure/")

print(f"AWS files: {aws_files}")
print(f"Azure files: {azure_files}")