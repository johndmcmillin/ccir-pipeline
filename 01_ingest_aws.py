# Databricks notebook source
spark.sql("CREATE SCHEMA IF NOT EXISTS ccir_workspace.default")
spark.sql("CREATE VOLUME IF NOT EXISTS ccir_workspace.default.ccir_raw")

# COMMAND ----------

# Notebook: 01_ingest_aws.py
# Purpose: Fetch AWS GPU instance pricing and land raw JSON to DBFS Bronze zone

import requests
import json
from datetime import datetime

# ── Config ──────────────────────────────────────────────────────────────────
REGION = "us-east-1"
OUTPUT_PATH = f"/Volumes/ccir_workspace/default/ccir_raw/aws/"

# AWS GPU instance families to capture across all 4 compute tiers
# Training: p4, p5 | Fine-tuning: p3, g5 | Inference: g4dn, g6 | Edge: inf1, inf2
GPU_FAMILIES = ["p4", "p5", "p3", "g5", "g4dn", "g6", "inf1", "inf2"]
def extract_aws_gpu_type(instance_type: str) -> str:
    if instance_type.startswith(("p4d", "p4de", "p5")):
        return "A100"
    elif instance_type.startswith("p3"):
        return "V100"
    elif instance_type.startswith("g5"):
        return "A10G"
    elif instance_type.startswith(("g4dn", "g4ad")):
        return "T4"
    elif instance_type.startswith(("g6", "g6e", "g6f")):
        return "L4"
    elif instance_type.startswith(("inf1", "inf2")):
        return "Inferentia"
    else:
        return "unknown"

# ── Fetch AWS Pricing ────────────────────────────────────────────────────────
def fetch_aws_gpu_pricing(region: str) -> list[dict]:
    """
    Pull EC2 on-demand pricing from the AWS bulk pricing JSON endpoint.
    No auth required - this is a public endpoint.
    """
    url = f"https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/current/{region}/index.json"
    
    print(f"Fetching AWS pricing for region: {region}")
    print("Note: This file is large (~500MB) — filtering in stream...")

    response = requests.get(url, stream=True)
    response.raise_for_status()

    # Parse full JSON (Databricks cluster has enough RAM)
    data = response.json()
    products = data.get("products", {})
    terms = data.get("terms", {}).get("OnDemand", {})

    results = []

    for sku, product in products.items():
        attrs = product.get("attributes", {})
        instance_type = attrs.get("instanceType", "")
        
        # Filter to GPU families only
        if not any(instance_type.startswith(f) for f in GPU_FAMILIES):
            continue
        
        # Only grab Linux on-demand compute (no reserved, no Windows)
        if attrs.get("operatingSystem") != "Linux":
            continue
        if attrs.get("tenancy") != "Shared":
            continue
        if attrs.get("capacitystatus") != "Used":
            continue

        # Pull price from terms
        price_per_hour = None
        if sku in terms:
            for offer_code, offer in terms[sku].items():
                for rate_code, rate in offer.get("priceDimensions", {}).items():
                    price_str = rate.get("pricePerUnit", {}).get("USD", "0")
                    price_per_hour = float(price_str)

        if price_per_hour is None or price_per_hour == 0:
            continue

        # Map instance family → CCIR compute tier
        tier = map_to_ccir_tier(instance_type)

        results.append({
            "provider": "AWS",
            "instance_type": instance_type,
            "gpu_type": extract_aws_gpu_type(instance_type),
            "gpu_count": attrs.get("gpuMemory", "unknown"),  # AWS doesn't expose count cleanly
            "vcpu": attrs.get("vcpu"),
            "memory_gb": attrs.get("memory"),
            "region": region,
            "price_per_hour": price_per_hour,
            "price_type": "on_demand",
            "ccir_tier": tier,
            "raw_payload": json.dumps(attrs)
        })

    print(f"Found {len(results)} GPU instance records")
    return results


def map_to_ccir_tier(instance_type: str) -> str:
    """Map AWS instance family to CCIR compute tier."""
    if instance_type.startswith(("p4", "p5")):
        return "Training"
    elif instance_type.startswith(("p3", "g5")):
        return "Fine-tuning"
    elif instance_type.startswith(("g4dn", "g6")):
        return "Inference"
    elif instance_type.startswith(("inf1", "inf2")):
        return "Edge"
    else:
        return "Unknown"


# ── Write to DBFS ────────────────────────────────────────────────────────────
def write_to_dbfs(records: list[dict], output_path: str):
    import os
    
    # Create volume directory if needed
    os.makedirs(output_path, exist_ok=True)
    
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"aws_{timestamp}.json"
    full_path = f"{output_path}{filename}"

    ndjson = "\n".join(json.dumps(r) for r in records)
    
    with open(full_path, "w") as f:
        f.write(ndjson)
    
    print(f"Written {len(records)} records to {full_path}")
    return full_path


# ── Main ─────────────────────────────────────────────────────────────────────
records = fetch_aws_gpu_pricing(REGION)
output_file = write_to_dbfs(records, OUTPUT_PATH)

print(f"\nDone. Preview of first record:")
print(json.dumps(records[0], indent=2))