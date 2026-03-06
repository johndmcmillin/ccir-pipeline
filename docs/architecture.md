# CCIR Pipeline — Architecture & Analytical Commentary

## Analytical Methodology

### Tier Classification

The central premise of this project is that GPU compute is not a homogeneous commodity. Cloud providers offer distinct hardware tiers that map to different workload classes — training, fine-tuning, and inference — and the pricing of those tiers reflects structurally different supply and demand dynamics. This project asks whether those differences are quantifiable, whether they are consistent across providers, and whether the market is pricing them efficiently.

Three tiers were defined based on the dominant GPU hardware associated with each workload class:

**Training** maps to frontier GPUs (A100, H100) running at scale. These instances are the scarcest, carry the longest procurement lead times, and are typically reserved rather than purchased on-demand. The pricing reflects not just the hardware but the difficulty of access.

**Fine-tuning** maps to mid-tier GPUs (A10, V100) capable of adapting pre-trained models without requiring full training infrastructure. These are more available than Training-class hardware but still specialized enough that pricing is elevated relative to general compute.

**Inference** maps to the most commoditized GPU tier (T4, L4) used to serve models in production. This is the highest-volume workload category in the industry, and provider competition has compressed margins considerably.

Edge compute (Inferentia, specialized inference chips) was identified as a fourth potential tier but excluded from the headline index. The primary reason is architectural heterogeneity — AWS Inferentia is a proprietary chip with no direct Azure equivalent, making cross-provider comparison misleading. The 707% provider spread observed in the raw data confirmed this: Azure has minimal participation in this market, which means there is no functional two-sided market to price from. Edge is noted as a candidate for a standalone sub-index once market participation broadens.

### GPU-Count Normalization

Raw instance pricing ($/hr) is not comparable across providers without normalization for the number of GPUs included in each instance. An AWS p4d.24xlarge at $32/hr includes 8 A100s, making its effective per-GPU cost ~$4/hr — not $32/hr. Without this normalization, multi-GPU instances appear artificially expensive and distort any cross-provider comparison.

The Silver layer applies a GPU count lookup table to convert all prices to a **$/GPU/hour** basis before any aggregation. This is the foundational normalization step that makes the Gold index meaningful.

---

## Price Discovery and the Case for a Reference Rate

The most significant finding from this pipeline is not any single price point — it is the **dispersion** within each tier.

| Tier | Median $/GPU/hr | Std Dev | Std Dev as % of Median |
|------|-----------------|---------|------------------------|
| Fine-tuning | $2.04 | $1.19 | 58% |
| Inference | $2.18 | $5.46 | 250% |
| Training | $4.10 | $17.02 | 415% |

A standard deviation of 415% of the median in Training means that buyers procuring A100 capacity without a reference rate are operating essentially blind. The market is not efficient — there is no transparent mechanism anchoring what fair value looks like for a given tier on a given day.

This is the same problem that existed in short-term lending markets before SOFR replaced LIBOR. LIBOR was a survey-based rate that reflected what banks said they would lend at, not what they actually transacted at. The result was chronic opacity and, ultimately, manipulation. SOFR replaced it with a rate grounded in observed transactions.

Cloud compute pricing has analogous structural problems. Published list prices do not reflect reservation discounts, committed use agreements, or spot market dynamics. There is no independent body publishing observed transaction prices. Buyers — particularly enterprise AI teams running large training workloads — have no reference rate to benchmark their contracts against.

A tier-specific compute pricing index built from observed public market prices, updated on a regular cadence, and published independently would materially improve price discovery for compute consumers. The pipeline here is a proof of concept for what that infrastructure could look like.

---

## Technical Architecture Decisions

### Why Medallion Architecture

The Bronze/Silver/Gold pattern was chosen because the raw pricing APIs return data in formats that are not directly comparable — AWS uses a bulk JSON file with nested product and term objects, Azure uses a paginated REST API with flat records. Bronze preserves the raw payload from each source without transformation, which is important for auditability. If the normalization logic in Silver is ever revised, Bronze can be reprocessed without re-hitting the APIs.

Silver applies all cleaning, deduplication, and normalization logic. Gold is intentionally kept as pure aggregation — no business logic lives in Gold that isn't already expressed in Silver. This separation makes the pipeline easier to debug and extend.

### Why Autoloader

Databricks Autoloader was chosen for the Bronze ingestion layer because it handles incremental file processing natively — it tracks which files have already been processed via a checkpoint, so re-running the ingestion notebooks never produces duplicates in Bronze. For a pipeline designed to run on a daily cadence, this is significantly more reliable than a batch overwrite pattern.

### Why Append Mode on Gold with Time Travel

The Gold table uses append mode rather than overwrite. Each pipeline run adds a new snapshot row per tier, preserving the full history of index values. Delta Lake's time travel feature allows querying the index at any historical point using `VERSION AS OF` or `TIMESTAMP AS OF` syntax — which is exactly the behavior you would want from a reference rate. A rate that cannot be queried historically is not a reference rate.

### Why Databricks Jobs for Orchestration

The five notebooks are wired into a single Databricks Job (`ccir-daily-index`) with sequential task dependencies:

```
ingest_aws → ingest_azure → bronze_autoloader → silver_transform → gold_index
```

This means the pipeline is fully automated — no manual intervention required after setup. The Gold table self-builds a time series with each run, and the Databricks SQL dashboard auto-refreshes against it. The combination of scheduled Jobs + append-mode Delta + time travel means the entire historical index is always queryable without maintaining any separate storage or snapshot logic.

### Why Median Over Mean for the Index

The headline index value uses the median (50th percentile) rather than the mean for each tier. The reasoning mirrors the methodology behind SOFR and similar reference rates: the mean is sensitive to outliers, and in a thin market with high price dispersion, a small number of extreme price points can significantly distort a mean-based index. The median is more robust and better represents the price a typical buyer would encounter. The interquartile range (P25 to P75) is published alongside the median to give a sense of market width.

---

## Limitations and Future Work

**Spot pricing not included.** This pipeline captures on-demand list prices only. Spot and preemptible instances can trade at 60-80% discounts to on-demand, and reserved instance pricing adds another layer of complexity. A complete reference rate would need to incorporate all three pricing modes with appropriate weighting.

**Sample size constraints.** The current index is computed from 184 normalized records across two providers and three regions. This is sufficient for a proof of concept but thin for a production reference rate. Expanding to additional regions (EU, APAC) and additional providers (CoreWeave, Lambda Labs, Vast.ai) would materially improve the index's representativeness — and would likely compress the provider spread as alternative cloud competition is factored in.

**Alternative clouds would change the picture.** CoreWeave and Lambda Labs publish GPU pricing that is generally 30-50% below AWS and Azure for comparable hardware. Including them would pull the index medians down and widen the provider spread analysis significantly. It would also introduce a new analytical question: whether hyperscaler pricing carries a premium for reliability and SLA guarantees that alternative clouds do not offer — which is itself a pricing signal worth quantifying.

**Performance adjustment removed by design.** An earlier version of this pipeline applied a GPU performance index (normalizing price by relative benchmark throughput) before computing the index. This was removed because it introduced subjective weighting that would be difficult to defend as a neutral reference rate. The current approach — reporting observed $/GPU/hour by tier, where the tier classification itself implies the performance class — is more analogous to how commodity benchmarks work. The tier is the quality grade; the index reports the price within that grade.
