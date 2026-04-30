# Salla E-Commerce Data Pipeline

A production-grade end-to-end data engineering pipeline built on **Azure Databricks** using **Medallion Architecture** (Bronze → Silver → Gold). Ingests data from multiple sources, transforms it through three quality layers, and serves a Kimball star schema for BI analytics via Power BI.

![CD](https://github.com/zahoor-isfhaq/salla-ecom-pipeline/actions/workflows/deploy.yml/badge.svg)
![CI](https://github.com/zahoor-isfhaq/salla-ecom-pipeline/actions/workflows/ci.yml/badge.svg)

---

## Tech Stack

**Cloud & Platform**

![Azure](https://img.shields.io/badge/Microsoft_Azure-0078D4?style=for-the-badge&logo=microsoftazure&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![Unity Catalog](https://img.shields.io/badge/Unity_Catalog-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![ADLS Gen2](https://img.shields.io/badge/ADLS_Gen2-0078D4?style=for-the-badge&logo=microsoftazure&logoColor=white)
![Azure PostgreSQL](https://img.shields.io/badge/Azure_PostgreSQL-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)
![Key Vault](https://img.shields.io/badge/Azure_Key_Vault-0078D4?style=for-the-badge&logo=microsoftazure&logoColor=white)

**Data Engineering**

![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-003366?style=for-the-badge&logo=databricks&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-4479A1?style=for-the-badge&logo=mysql&logoColor=white)
![Auto Loader](https://img.shields.io/badge/Auto_Loader-FF3621?style=for-the-badge&logo=databricks&logoColor=white)

**Orchestration & CI/CD**

![GitHub Actions](https://img.shields.io/badge/GitHub_Actions-2088FF?style=for-the-badge&logo=githubactions&logoColor=white)
![DAB](https://img.shields.io/badge/Databricks_Asset_Bundles-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![Databricks Jobs](https://img.shields.io/badge/Databricks_Jobs-FF3621?style=for-the-badge&logo=databricks&logoColor=white)

**BI & Analytics**

![Power BI](https://img.shields.io/badge/Power_BI-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)
![Lakeview](https://img.shields.io/badge/Lakeview_Dashboards-FF3621?style=for-the-badge&logo=databricks&logoColor=white)

---

## Architecture

<img width="2752" height="1458" alt="pipeline" src="https://github.com/user-attachments/assets/18ff98c8-5848-4ce3-a657-d3bb193febd5" />

> **Data flow:** Multiple sources → Unity Catalog Landing Zone (ADLS Gen2) → Bronze (raw ingestion) → Silver (cleaned & validated) → Gold (star schema) → Power BI via Serverless SQL Warehouse. Orchestrated by Databricks Jobs, deployed via GitHub Actions → Databricks Asset Bundles.

---

## Pipeline Runs

<img width="2170" height="725" alt="pipeline runs" src="https://github.com/user-attachments/assets/3eea40fc-fa41-4bea-bf11-6d43f8d74e36" />

> All 4 resources deployed and running: Orchestration Job (scheduled), Bronze, Silver, and Gold pipelines.

---

## Unity Catalog — Schemas & Tables

| Layer | Screenshot |
|---|---|
| Catalog overview (Bronze · Silver · Gold) | <img width="156" height="124" alt="image" src="https://github.com/user-attachments/assets/da6aaee8-5c29-4920-aa58-7b20d8ef64e4" /> |
| Gold layer — 5 dims + 3 facts | <img width="169" height="182" alt="image" src="https://github.com/user-attachments/assets/155654e8-8196-4feb-b07f-cfa647ed0b50" /> |
| Silver layer — 13 tables | <img width="351" height="549" alt="image" src="https://github.com/user-attachments/assets/3d5f57a6-5e53-4f69-9889-e6364af1473d" /> |

---

## Successful Pipeline Run

<img width="1050" height="602" alt="image" src="https://github.com/user-attachments/assets/d1390121-7ef1-4c71-8584-18b234ea24f3" />

> Job: `Salla E-Commerce Pipeline Orchestration` — Status: ✅ Success — Duration: 7m 39s

---

## Azure Resources

<img width="1187" height="338" alt="image" src="https://github.com/user-attachments/assets/8ef688ad-c2f5-49ea-813e-9507d9db277a" />

| Resource | Type |
|---|---|
| `salla-databricks` | Azure Databricks Service |
| `salla-keyvault2` | Key Vault (secrets) |
| `salla-pipeline-server` | Azure PostgreSQL Flexible Server |
| `sallaecomdata2` | ADLS Gen2 Storage Account |

---

## Data Sources

### PostgreSQL (via JDBC)
| Table | Description |
|---|---|
| `customers` | Customer profiles |
| `products` | Product catalog |
| `stores` | Store locations |
| `sales_orders` | Order transactions |
| `payment_transactions` | Payment records |
| `shipping_details` | Shipment tracking |
| `inventory_movements` | Stock movements |
| `product_reviews` | Customer reviews |

### REST API (via Databricks notebooks → JSON files)
| Source | Description |
|---|---|
| `clickstream` | User behaviour events |
| `exchange_rates` | Live currency rates |
| `social_media` | Social engagement data |
| `push_notifications` | Notification delivery data |

### Landing Zone — Unity Catalog Volume (`/Volumes/salla_databricks/bronze/landing_data/`)
| File Source | Format | Description |
|---|---|---|
| `ad_spend/` | CSV | Marketing campaign spend |
| `competitor_pricing/` | CSV | Scraped competitor prices |
| `payment_settlements/` | CSV | Gateway settlement reports |
| `suppliers_invoices/` | CSV | Supplier invoice data |
| `return_requests/` | CSV | Customer return records |
| `shipping_manifests/` | CSV | Shipping manifest files |
| `exchange_rates/` | JSON | Currency exchange rates |
| `customer_segmentations/` | Parquet | ML-generated segments |
| `demand_forecasts/` | Parquet | Demand forecast outputs |

---

## Project Structure

```
salla-ecom-pipeline/
├── .github/
│   └── workflows/
│       ├── ci.yml              # PR validation — databricks bundle validate
│       └── deploy.yml          # CD — deploy on push to main / release tag
├── bronze-layer/
│   └── transformations/
│       ├── bronze_postgresql.py      # 8 MVs from PostgreSQL via JDBC
│       └── bronze_landing_zone.py    # 4 Streaming Tables via Auto Loader
├── silver-layer/
│   └── transformations/
│       ├── silver_postgresql.py      # 8 MVs — cleaned PostgreSQL data
│       └── silver_landing_zone.py    # 4 Streaming Tables — cleaned file data
├── gold-layer/
│   └── transformations/
│       ├── gold_dimensions.py        # 5 Dimension tables
│       └── gold_facts.py             # 3 Fact tables
├── monitoring/
│   └── pipeline_health_monitor.py   # Post-pipeline health checks + alerting
├── sources/                         # Data generation scripts
├── .gitignore
├── databricks.yml                   # DAB config — all pipelines + jobs
└── README.md
```

---

## Layer Details

### Bronze Layer
- PostgreSQL tables ingested as **Materialized Views** via JDBC
- Landing Zone files ingested as **Streaming Tables** via Auto Loader (`cloudFiles`) with explicit schemas
- Adds `_bronze_timestamp` and `_source_file` / `_source_system` metadata columns
- Basic DLT expectations only (PK not null) — no transformations

### Silver Layer
- Full string normalisation (trim, lower/upper), type casting, null handling
- Streaming tables use watermark + `dropDuplicatesWithinWatermark` for deduplication
- `_rescued_data` filtering — malformed Auto Loader rows dropped
- Comprehensive DLT expectations: null checks, range validation, business logic (e.g. `net_amount = amount - fee ± 0.02`)
- Bronze metadata columns removed, `_silver_timestamp` added

### Gold Layer — Kimball Star Schema

**Dimensions (`gold_dimensions.py`)**
| Table | Key Business Logic |
|---|---|
| `dim_date` | Fiscal year, Saudi weekend flags, BI slicing attributes |
| `dim_customers` | Saudi region zones (Central/Western/Eastern/Southern/Northern), Unknown member |
| `dim_products` | Price tiers (Budget/Mid-Range/Premium/Luxury), stock status, margin % |
| `dim_stores` | Region zones |
| `dim_payment_method` | BNPL flags (Tamara/Tabby), payment type classification |

**Facts (`gold_facts.py`)**
| Table | Key Metrics |
|---|---|
| `fact_sales` | estimated_profit, is_refund, joins all 5 dimensions |
| `fact_ad_spend` | ROAS, CTR %, cost per conversion, net revenue |
| `fact_competitor_pricing` | price_diff_sar, price_diff_pct, competitive_position |

All Gold tables are Materialized Views. Surrogate keys use `md5()`. Orphaned FKs handled via `COALESCE` to Unknown member rows (Kimball methodology).

### Star Schema (ERD)
<img width="455" height="362" alt="ERD" src="https://github.com/user-attachments/assets/fe362878-633d-4314-ad3a-b0ba32bc0d57" />


> Star schema with `fact_sales` as the central fact table connected to all 5 dimensions. `fact_ad_spend` and `fact_competitor_pricing` share `dim_date` and `dim_products` respectively.

---

## Orchestration

The `Salla E-Commerce Pipeline Orchestration` job runs daily at **06:00 UTC**:

```
Bronze Ingestion → Silver Transformation → Gold Aggregation → Pipeline Health Monitor
```

Tasks chained with `depends_on`. Schedule is `PAUSED` in dev, `UNPAUSED` in prod.

---

## Monitoring

`pipeline_health_monitor.py` runs as the final task after Gold and checks:

- **DQ Violations** — reads DLT event logs, alerts if failure rate > 5%
- **Data Freshness** — WARNING if last update > 24h, CRITICAL if > 48h
- **Row Count Validation** — alerts if table rows fall below 50% of expected minimums
- **Filter Rate Analysis** — alerts if Bronze → Silver drop rate exceeds 50%

On CRITICAL failure, the notebook raises an exception to fail the job task, triggering Databricks email alerting.

---

## CI/CD Pipeline

### CI (`ci.yml`) — Pull Request Validation
- Triggers on PRs to `main` when pipeline files change
- Runs `databricks bundle validate` for both `dev` and `prod` targets
- Prevents broken bundles from being merged

### CD (`deploy.yml`) — Deployment
- **Push to `main`** → deploys to `dev` target (schedule paused)
- **GitHub Release tag** → deploys to `prod` target (schedule active)

---

## Deployment

### Required Secrets

**Databricks Secret Scope (`databricks-secrets`)**
| Key | Description |
|---|---|
| `pg-username` | PostgreSQL username |
| `pg-password` | PostgreSQL password |
| `adls-storage-key` | ADLS Gen2 storage account key |

**GitHub Repository Secrets**
| Secret | Description |
|---|---|
| `DATABRICKS_HOST` | Dev workspace URL |
| `DATABRICKS_TOKEN` | Dev PAT token |
| `DATABRICKS_PROD_HOST` | Prod workspace URL |
| `DATABRICKS_PROD_TOKEN` | Prod PAT token |

### Deploy Commands

```bash
# Validate bundle
databricks bundle validate --target dev

# Deploy to dev
databricks bundle deploy --target dev

# Run orchestration manually
databricks bundle run salla_orchestration --target dev

# Deploy to prod (no [dev] prefix on resources)
databricks bundle deploy --target prod

# Destroy dev environment
databricks bundle destroy --target dev
```

---

## Unity Catalog Structure

```
salla_databricks (catalog)
├── bronze (schema) — 21 tables
│   ├── bronze_customers, bronze_products, bronze_stores
│   ├── bronze_sales_orders, bronze_payment_transactions
│   ├── bronze_shipping_details, bronze_inventory_movements
│   ├── bronze_product_reviews
│   ├── bronze_ad_spend, bronze_competitor_pricing
│   ├── bronze_exchange_rates, bronze_payment_settlements
│   └── Volumes (2) — landing_data
├── silver (schema) — 13 tables
│   ├── silver_customers, silver_products, silver_stores
│   ├── silver_sales_orders, silver_payment_transactions
│   ├── silver_shipping_details, silver_inventory_movements
│   ├── silver_product_reviews, silver_ad_spend
│   ├── silver_competitor_pricing, silver_exchange_rates
│   ├── silver_payment_settlements, silver_clickstream
│   └── silver_customer_segmentation, silver_demand_forecasts
└── gold (schema) — 8 tables
    ├── dim_date, dim_customers, dim_products
    ├── dim_stores, dim_payment_method
    ├── fact_sales, fact_ad_spend
    └── fact_competitor_pricing
```

---

## Key Design Decisions

- **Serverless SQL Warehouses** — no cluster management, auto-scaling, cost-efficient
- **`PREVIEW` channel** — access to latest Spark Declarative Pipeline features
- **Self-contained pipeline files** — no cross-file imports (serverless limitation compliance)
- **Kimball star schema** — Unknown member rows handle orphaned FKs gracefully
- **Hybrid ingestion** — streaming (Auto Loader) for files, batch MVs for PostgreSQL in same pipeline
- **DAB dev/prod targets** — `dev` mode prefixes resources with `[dev username]` for isolation
- **Unity Catalog Volumes** — managed ADLS Gen2 storage with full governance

---

## ⚠️ Disclaimer

All data used in this project is **synthetically generated** using custom Python scripts
located in the `sources/` folder. No real customer, transaction, or business data was
used at any point. The Salla brand name is used purely for portfolio demonstration
purposes and has no affiliation with the actual Salla e-commerce platform.

---

## Author

**Zahoor Ishfaq**
- GitHub: [zahoor-isfhaq](https://github.com/zahoor-isfhaq)
- LinkedIn: [zahoor-isfhaq](https://linkedin.com/in/zahoor-isfhaq)
- Email: [zahoor.ishfaaq@gmail.com](mailto:zahoor.ishfaaq@gmail.com)
