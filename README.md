# 🛒 Salla E-Commerce Data Pipeline

> **End-to-end Medallion Architecture on Databricks** — ingesting data from Azure PostgreSQL and file-based landing zones, transforming through Bronze → Silver → Gold layers using Spark Declarative Pipelines, and serving a star-schema dimensional model for BI analytics.

![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache_Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Azure](https://img.shields.io/badge/Azure-0078D4?style=for-the-badge&logo=microsoftazure&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)
![GitHub Actions](https://img.shields.io/badge/GitHub_Actions-2088FF?style=for-the-badge&logo=githubactions&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-003366?style=for-the-badge&logo=delta&logoColor=white)

---

## 📐 Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                                    │
│                                                                         │
│   Azure PostgreSQL (8 tables)          UC Volume Landing Zone           │
│   ├── customers (9,952)                ├── ad_spend (CSV)               │
│   ├── products (40)                    ├── competitor_pricing (CSV)     │
│   ├── stores (46)                      ├── exchange_rates (JSON)        │
│   ├── sales_orders (1.4M)             └── payment_settlements (CSV)    │
│   ├── payment_transactions (1.4M)                                       │
│   ├── shipping_details (1.4M)          12 Synthetic Data Generators     │
│   ├── inventory_movements (548K)       └── CSV / JSON / Parquet         │
│   └── product_reviews (90K)                                             │
└──────────────┬──────────────────────────────────┬───────────────────────┘
               │ JDBC                             │ Auto Loader
               ▼                                  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  🥉 BRONZE LAYER (salla_databricks.bronze) — 12 tables                  │
│                                                                         │
│  bronze_postgresql.py          │  bronze_landing_zone.py                │
│  8 Materialized Views          │  4 Streaming Tables                    │
│  ├── bronze_customers          │  ├── bronze_ad_spend                   │
│  ├── bronze_products           │  ├── bronze_competitor_pricing         │
│  ├── bronze_stores             │  ├── bronze_exchange_rates             │
│  ├── bronze_sales_orders       │  └── bronze_payment_settlements        │
│  ├── bronze_payment_txns       │                                        │
│  ├── bronze_shipping_details   │  + Metadata: _bronze_timestamp,        │
│  ├── bronze_inventory_mvmts    │    _source_system, _source_file        │
│  └── bronze_product_reviews    │                                        │
└──────────────────────────┬──────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  🥈 SILVER LAYER (salla_databricks.silver) — 12 tables                  │
│                                                                         │
│  silver_postgresql.py          │  silver_landing_zone.py                │
│  8 Materialized Views          │  4 Streaming Tables                    │
│                                │                                        │
│  ✓ 65+ Data Quality Rules      ✓ String Normalization                  │
│  ✓ Quarantine Pattern           ✓ Deduplication (watermark-based)      │
│  ✓ FK Validation                ✓ Type Casting & Date Validation       │
│  ✓ Business Logic Checks        ✓ Rescued Data Filtering               │
└──────────────────────────┬──────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  🥇 GOLD LAYER (salla_databricks.gold) — 8 tables                      │
│                                                                         │
│  ┌─── Dimensions (5 MVs) ───┐    ┌──── Facts (3 MVs) ────────────┐    │
│  │ dim_date                  │    │ fact_sales (1.4M)              │    │
│  │ dim_customers             │◄───│   orders + payments + shipping │    │
│  │ dim_products              │    │   estimated_profit, is_refund  │    │
│  │ dim_stores                │    │                                │    │
│  │ dim_payment_method        │    │ fact_ad_spend (35K)            │    │
│  │                           │    │   ROAS, CTR, cost_per_conv     │    │
│  │ Surrogate keys (md5)     │    │                                │    │
│  │ Business enrichment:      │    │ fact_competitor_pricing (5.5K) │    │
│  │  • price_tier             │    │   price_diff, competitive_pos  │    │
│  │  • region_zone            │    └────────────────────────────────┘    │
│  │  • stock_status           │                                          │
│  │  • is_bnpl                │                                          │
│  └───────────────────────────┘                                          │
└──────────────────────────┬──────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  📊 BI LAYER                                                            │
│  Salla E-Commerce — Pipeline Observatory (Lakeview Dashboard)           │
│  Star-schema queries: fact_sales ⟕ dim_customers ⟕ dim_products ⟕ ...  │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 🛠️ Tech Stack

| Component | Technology |
| --- | --- |
| **Cloud** | Microsoft Azure |
| **Lakehouse** | Databricks (Unity Catalog) |
| **Storage** | Azure Data Lake Storage Gen2 (ADLS) |
| **Source DB** | Azure PostgreSQL Flexible Server |
| **Compute** | Databricks Serverless |
| **Pipeline Framework** | Spark Declarative Pipelines (formerly DLT) |
| **Table Format** | Delta Lake |
| **Streaming** | Auto Loader (cloudFiles) with watermark-based dedup |
| **Orchestration** | Databricks Workflows (multi-task job) |
| **Deployment** | Databricks Asset Bundles (DAB) |
| **CI/CD** | GitHub Actions |
| **Language** | Python (PySpark), SQL |

---

## 📂 Project Structure

```
salla-ecom-pipeline/
│
├── databricks.yml                      # DAB config — pipelines, jobs, targets
├── .github/
│   └── workflows/
│       ├── ci.yml                      # PR validation (bundle validate)
│       └── deploy.yml                  # CD — deploy to dev on push, prod on release
│
├── bronze-layer/
│   └── transformations/
│       ├── bronze_postgresql.py        # 8 MVs — JDBC ingestion from PostgreSQL
│       └── bronze_landing_zone.py      # 4 streaming tables — Auto Loader from UC Volume
│
├── silver-layer/
│   └── transformations/
│       ├── silver_postgresql.py        # 8 MVs — cleaned, validated, deduped
│       └── silver_landing_zone.py      # 4 streaming tables — watermark dedup
│
├── gold-layer/
│   └── transformations/
│       ├── gold_dimensions.py          # 5 dimension MVs (star schema)
│       └── gold_facts.py              # 3 fact MVs (sales, ads, competitors)
│
└── sources/
    ├── csv/                            # 6 generators (ad_spend, pricing, settlements...)
    ├── json/                           # 4 generators (exchange_rates, clickstream...)
    ├── parquet/                        # 2 generators (segmentation, forecasts)
    └── run_all_generators.py           # Orchestrates all 12 generators
```

---

## 🔄 Pipeline Orchestration

The orchestration job chains three Spark Declarative Pipelines sequentially:

```
┌───────────────────┐     ┌────────────────────────┐     ┌──────────────────┐
│ bronze_ingestion  │────►│ silver_transformation  │────►│ gold_aggregation │
│ (12 tables)       │     │ (12 tables, 65+ rules) │     │ (8 tables)       │
└───────────────────┘     └────────────────────────┘     └──────────────────┘
```

**Schedule:** Daily at 6:00 AM UTC  
**Failure Alerts:** Email notification on failure

---

## ✅ Data Quality

The Silver layer enforces **65+ data quality expectations** across all 12 tables:

| Category | Examples |
| --- | --- |
| **Primary Key** | `expect_or_drop` — rows with NULL PKs are quarantined |
| **Email Validation** | Regex: `^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$` |
| **Date Ranges** | No future dates, no dates before 2020 |
| **Foreign Keys** | `customer_id`, `store_id`, `product_id` existence checks |
| **Business Logic** | `total_amount ≈ quantity × unit_price - discount` (5% tolerance) |
| **Financial** | `net_amount ≈ amount - gateway_fee` (0.02 tolerance) |
| **Enum Validation** | Order status, payment status validated against known values |
| **Streaming Dedup** | `dropDuplicatesWithinWatermark` for landing zone tables |
| **Malformed Data** | `_rescued_data` filtering for Auto Loader sources |

---

## ⭐ Star Schema Model

```
                    ┌──────────────┐
                    │  dim_date    │
                    │  (2,677)     │
                    └──────┬───────┘
                           │
┌──────────────┐   ┌──────┴───────┐   ┌──────────────────┐
│dim_customers │───│  fact_sales  │───│  dim_products     │
│  (9,670)     │   │  (1,411,696) │   │  (40)             │
└──────────────┘   └──┬───────┬───┘   └──────────────────┘
                      │       │
              ┌───────┘       └────────┐
              │                        │
      ┌───────┴──────┐   ┌────────────┴─────┐
      │  dim_stores  │   │dim_payment_method │
      │  (46)        │   │  (8)              │
      └──────────────┘   └──────────────────┘
```

**Business Enrichment:**
- **Price Tiers:** Budget / Mid-Range / Premium / Luxury
- **Region Zones:** Central / Western / Eastern / Southern / Northern (Saudi Arabia)
- **Stock Status:** In Stock / Low Stock / Out of Stock
- **Payment Types:** Card / BNPL / Digital Wallet / Cash (Saudi market)
- **Competitive Position:** We are cheaper / Same price / We are more expensive

---

## 🚀 CI/CD Pipeline

```
┌──────────────┐     ┌──────────────────┐     ┌──────────────────┐
│  Pull Request│────►│  CI — Validate   │     │                  │
│  to main     │     │  bundle (dev +   │     │                  │
│              │     │  prod targets)   │     │                  │
└──────────────┘     └──────────────────┘     │                  │
                                               │                  │
┌──────────────┐     ┌──────────────────┐     │                  │
│  Push to     │────►│  CD — Deploy to  │────►│  Databricks      │
│  main        │     │  dev + run       │     │  Workspace       │
└──────────────┘     └──────────────────┘     │                  │
                                               │                  │
┌──────────────┐     ┌──────────────────┐     │                  │
│  GitHub      │────►│  CD — Deploy to  │────►│                  │
│  Release     │     │  prod (schedule  │     │                  │
│  (tag v*)    │     │  unpaused)       │     │                  │
└──────────────┘     └──────────────────┘     └──────────────────┘
```

**DAB Targets:**

| Target | Mode | Naming | Schedule |
| --- | --- | --- | --- |
| `dev` | Development | `[dev username] Resource_Name` | Paused |
| `prod` | Production | `Resource_Name` | Unpaused (6 AM UTC daily) |

---

## 📊 Dashboard

The **Salla E-Commerce — Pipeline Observatory** dashboard provides real-time analytics by querying the Gold star schema, including:

- Sales performance by region, category, and time
- Payment method analysis (Card vs BNPL vs Digital Wallet)
- Advertising ROAS and campaign effectiveness
- Competitor pricing intelligence

---

## ⚙️ Deployment

### Prerequisites
- Databricks workspace with Unity Catalog enabled
- Azure PostgreSQL with the Salla e-commerce schema
- ADLS storage account for landing zone files
- Databricks secrets scope (`databricks-secrets`) with:
  - `pg-username` / `pg-password` — PostgreSQL credentials
  - `adls-storage-key` — ADLS access key

### Deploy with DAB

```bash
# Install Databricks CLI
pip install databricks-cli

# Authenticate
databricks configure --token

# Validate the bundle
databricks bundle validate --target dev

# Deploy to dev
databricks bundle deploy --target dev

# Run the full pipeline
databricks bundle run salla_orchestration --target dev

# Tear down
databricks bundle destroy --target dev
```

---

## 📈 Key Metrics

| Metric | Value |
| --- | --- |
| **Total Records Processed** | ~5.3M across all layers |
| **Fact Table Size** | 1,411,696 sales transactions |
| **Data Sources** | 2 (PostgreSQL + Landing Zone files) |
| **Source Tables** | 12 (8 JDBC + 4 file-based) |
| **Data Quality Rules** | 65+ expectations |
| **Pipeline Tables** | 32 (12 Bronze + 12 Silver + 8 Gold) |
| **Dimensions** | 5 (date, customers, products, stores, payment method) |
| **Facts** | 3 (sales, ad spend, competitor pricing) |

---

## 👤 Author

**Zahoor Ishfaq**  
[LinkedIn](https://linkedin.com/in/your-profile) · [GitHub](https://github.com/your-username)

---

*Built with Databricks on Azure — Medallion Architecture, Unity Catalog, Spark Declarative Pipelines, and Databricks Asset Bundles.*
