# Databricks notebook source
# DBTITLE 1,Setup and Configuration
# ============================================================================
# Pipeline Health Monitor — Salla E-Commerce Data Pipeline
# ============================================================================
# Runs post-pipeline to validate data quality, freshness, and row counts.
# Triggers email alerts (via job failure) on CRITICAL issues.
# ============================================================================

from datetime import datetime, timedelta
import json

# Pipeline IDs
PIPELINES = {
    "bronze": "cd8c7c88-a926-47e5-9580-f1934c44a037",
    "silver": "f5c1e5c9-8cc9-4c57-87fc-e0b40ba53803",
    "gold":   "62279bb1-c0a1-4670-ad61-0e07b1184d71",
}

# Expected minimum row counts (alert if below 50% of these)
EXPECTED_MINS = {
    # Bronze - PostgreSQL
    "bronze.bronze_customers": 9000,
    "bronze.bronze_products": 30,
    "bronze.bronze_stores": 40,
    "bronze.bronze_sales_orders": 1000000,
    "bronze.bronze_payment_transactions": 1000000,
    "bronze.bronze_shipping_details": 1000000,
    "bronze.bronze_inventory_movements": 400000,
    "bronze.bronze_product_reviews": 80000,
    # Bronze - Landing Zone
    "bronze.bronze_ad_spend": 30000,
    "bronze.bronze_competitor_pricing": 40000,
    "bronze.bronze_exchange_rates": 4000,
    "bronze.bronze_payment_settlements": 80000,
    "bronze.bronze_supplier_invoices": 200,
    "bronze.bronze_return_requests": 1000,
    "bronze.bronze_shipping_manifests": 150000,
    "bronze.bronze_clickstream": 1000000,
    "bronze.bronze_social_media": 100000,
    "bronze.bronze_push_notifications": 50000,
    "bronze.bronze_customer_segmentation": 9000,
    "bronze.bronze_demand_forecasts": 150000,
}

# Freshness SLA
FRESHNESS_WARNING_HOURS = 24
FRESHNESS_ALERT_HOURS = 48

# DQ violation threshold
DQ_ALERT_THRESHOLD_PCT = 5.0

# Store results for final summary
check_results = {"dq": [], "freshness": [], "row_counts": [], "filter_rates": []}

print(f"Pipeline Health Monitor initialized at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Freshness SLA: WARNING > {FRESHNESS_WARNING_HOURS}h, ALERT > {FRESHNESS_ALERT_HOURS}h")
print(f"DQ alert threshold: > {DQ_ALERT_THRESHOLD_PCT}% failure rate")

# COMMAND ----------

# DBTITLE 1,Data Quality Violations Check
# MAGIC %sql
# MAGIC -- ============================================================================
# MAGIC -- DQ VIOLATIONS: Extract expectation pass/fail from pipeline event logs
# MAGIC -- ============================================================================
# MAGIC
# MAGIC WITH bronze_events AS (
# MAGIC   SELECT
# MAGIC     timestamp,
# MAGIC     details
# MAGIC   FROM event_log('cd8c7c88-a926-47e5-9580-f1934c44a037')
# MAGIC   WHERE event_type = 'flow_progress'
# MAGIC     AND timestamp >= current_timestamp() - INTERVAL 24 HOURS
# MAGIC ),
# MAGIC silver_events AS (
# MAGIC   SELECT
# MAGIC     timestamp,
# MAGIC     details
# MAGIC   FROM event_log('f5c1e5c9-8cc9-4c57-87fc-e0b40ba53803')
# MAGIC   WHERE event_type = 'flow_progress'
# MAGIC     AND timestamp >= current_timestamp() - INTERVAL 24 HOURS
# MAGIC ),
# MAGIC all_events AS (
# MAGIC   SELECT 'bronze' AS layer, * FROM bronze_events
# MAGIC   UNION ALL
# MAGIC   SELECT 'silver' AS layer, * FROM silver_events
# MAGIC ),
# MAGIC parsed AS (
# MAGIC   SELECT
# MAGIC     layer,
# MAGIC     timestamp,
# MAGIC     details:flow_progress.metrics.num_output_rows AS output_rows,
# MAGIC     explode(
# MAGIC       from_json(
# MAGIC         details:flow_progress:data_quality:expectations,
# MAGIC         'array<struct<name: string, dataset: string, passed_records: int, failed_records: int>>'
# MAGIC       )
# MAGIC     ) AS expectation
# MAGIC   FROM all_events
# MAGIC   WHERE details:flow_progress:data_quality:expectations IS NOT NULL
# MAGIC )
# MAGIC SELECT
# MAGIC   layer,
# MAGIC   expectation.dataset AS table_name,
# MAGIC   expectation.name AS expectation_name,
# MAGIC   SUM(expectation.passed_records) AS total_passed,
# MAGIC   SUM(expectation.failed_records) AS total_failed,
# MAGIC   ROUND(
# MAGIC     SUM(expectation.failed_records) * 100.0 /
# MAGIC     NULLIF(SUM(expectation.passed_records) + SUM(expectation.failed_records), 0),
# MAGIC     2
# MAGIC   ) AS failure_rate_pct,
# MAGIC   CASE
# MAGIC     WHEN SUM(expectation.failed_records) * 100.0 /
# MAGIC          NULLIF(SUM(expectation.passed_records) + SUM(expectation.failed_records), 0) > 5
# MAGIC     THEN '🔴 ALERT'
# MAGIC     WHEN SUM(expectation.failed_records) > 0
# MAGIC     THEN '🟡 WARNING'
# MAGIC     ELSE '🟢 PASS'
# MAGIC   END AS status
# MAGIC FROM parsed
# MAGIC GROUP BY layer, expectation.dataset, expectation.name
# MAGIC HAVING SUM(expectation.failed_records) > 0
# MAGIC ORDER BY failure_rate_pct DESC

# COMMAND ----------

# DBTITLE 1,Data Freshness Check
# MAGIC %sql
# MAGIC -- ============================================================================
# MAGIC -- DATA FRESHNESS: Check last update timestamps for all landing zone tables
# MAGIC -- ============================================================================
# MAGIC
# MAGIC WITH freshness AS (
# MAGIC   SELECT 'bronze_ad_spend' AS table_name, MAX(_bronze_timestamp) AS last_update FROM salla_databricks.bronze.bronze_ad_spend
# MAGIC   UNION ALL SELECT 'bronze_competitor_pricing', MAX(_bronze_timestamp) FROM salla_databricks.bronze.bronze_competitor_pricing
# MAGIC   UNION ALL SELECT 'bronze_exchange_rates', MAX(_bronze_timestamp) FROM salla_databricks.bronze.bronze_exchange_rates
# MAGIC   UNION ALL SELECT 'bronze_payment_settlements', MAX(_bronze_timestamp) FROM salla_databricks.bronze.bronze_payment_settlements
# MAGIC   UNION ALL SELECT 'bronze_supplier_invoices', MAX(_bronze_timestamp) FROM salla_databricks.bronze.bronze_supplier_invoices
# MAGIC   UNION ALL SELECT 'bronze_return_requests', MAX(_bronze_timestamp) FROM salla_databricks.bronze.bronze_return_requests
# MAGIC   UNION ALL SELECT 'bronze_shipping_manifests', MAX(_bronze_timestamp) FROM salla_databricks.bronze.bronze_shipping_manifests
# MAGIC   UNION ALL SELECT 'bronze_clickstream', MAX(_bronze_timestamp) FROM salla_databricks.bronze.bronze_clickstream
# MAGIC   UNION ALL SELECT 'bronze_social_media', MAX(_bronze_timestamp) FROM salla_databricks.bronze.bronze_social_media
# MAGIC   UNION ALL SELECT 'bronze_push_notifications', MAX(_bronze_timestamp) FROM salla_databricks.bronze.bronze_push_notifications
# MAGIC   UNION ALL SELECT 'bronze_customer_segmentation', MAX(_bronze_timestamp) FROM salla_databricks.bronze.bronze_customer_segmentation
# MAGIC   UNION ALL SELECT 'bronze_demand_forecasts', MAX(_bronze_timestamp) FROM salla_databricks.bronze.bronze_demand_forecasts
# MAGIC   UNION ALL SELECT 'silver_ad_spend', MAX(_silver_timestamp) FROM salla_databricks.silver.silver_ad_spend
# MAGIC   UNION ALL SELECT 'silver_competitor_pricing', MAX(_silver_timestamp) FROM salla_databricks.silver.silver_competitor_pricing
# MAGIC   UNION ALL SELECT 'silver_exchange_rates', MAX(_silver_timestamp) FROM salla_databricks.silver.silver_exchange_rates
# MAGIC   UNION ALL SELECT 'silver_payment_settlements', MAX(_silver_timestamp) FROM salla_databricks.silver.silver_payment_settlements
# MAGIC   UNION ALL SELECT 'silver_supplier_invoices', MAX(_silver_timestamp) FROM salla_databricks.silver.silver_supplier_invoices
# MAGIC   UNION ALL SELECT 'silver_return_requests', MAX(_silver_timestamp) FROM salla_databricks.silver.silver_return_requests
# MAGIC   UNION ALL SELECT 'silver_shipping_manifests', MAX(_silver_timestamp) FROM salla_databricks.silver.silver_shipping_manifests
# MAGIC   UNION ALL SELECT 'silver_clickstream', MAX(_silver_timestamp) FROM salla_databricks.silver.silver_clickstream
# MAGIC   UNION ALL SELECT 'silver_social_media', MAX(_silver_timestamp) FROM salla_databricks.silver.silver_social_media
# MAGIC   UNION ALL SELECT 'silver_push_notifications', MAX(_silver_timestamp) FROM salla_databricks.silver.silver_push_notifications
# MAGIC   UNION ALL SELECT 'silver_customer_segmentation', MAX(_silver_timestamp) FROM salla_databricks.silver.silver_customer_segmentation
# MAGIC   UNION ALL SELECT 'silver_demand_forecasts', MAX(_silver_timestamp) FROM salla_databricks.silver.silver_demand_forecasts
# MAGIC )
# MAGIC SELECT
# MAGIC   table_name,
# MAGIC   last_update,
# MAGIC   ROUND((unix_timestamp(current_timestamp()) - unix_timestamp(last_update)) / 3600, 1) AS hours_since_update,
# MAGIC   CASE
# MAGIC     WHEN last_update IS NULL THEN '🔴 ALERT - No data'
# MAGIC     WHEN (unix_timestamp(current_timestamp()) - unix_timestamp(last_update)) / 3600 > 48 THEN '🔴 ALERT - Stale >48h'
# MAGIC     WHEN (unix_timestamp(current_timestamp()) - unix_timestamp(last_update)) / 3600 > 24 THEN '🟡 WARNING - Stale >24h'
# MAGIC     ELSE '🟢 FRESH'
# MAGIC   END AS status
# MAGIC FROM freshness
# MAGIC ORDER BY hours_since_update DESC

# COMMAND ----------

# DBTITLE 1,Row Count Validation
# MAGIC %sql
# MAGIC -- ============================================================================
# MAGIC -- ROW COUNTS: Validate all 48 tables have expected minimum rows
# MAGIC -- ============================================================================
# MAGIC
# MAGIC WITH counts AS (
# MAGIC   -- Bronze PostgreSQL (8)
# MAGIC   SELECT 'bronze' AS layer, 'bronze_customers' AS table_name, COUNT(*) AS row_count, 9000 AS expected_min FROM salla_databricks.bronze.bronze_customers
# MAGIC   UNION ALL SELECT 'bronze', 'bronze_products', COUNT(*), 30 FROM salla_databricks.bronze.bronze_products
# MAGIC   UNION ALL SELECT 'bronze', 'bronze_stores', COUNT(*), 40 FROM salla_databricks.bronze.bronze_stores
# MAGIC   UNION ALL SELECT 'bronze', 'bronze_sales_orders', COUNT(*), 1000000 FROM salla_databricks.bronze.bronze_sales_orders
# MAGIC   UNION ALL SELECT 'bronze', 'bronze_payment_transactions', COUNT(*), 1000000 FROM salla_databricks.bronze.bronze_payment_transactions
# MAGIC   UNION ALL SELECT 'bronze', 'bronze_shipping_details', COUNT(*), 1000000 FROM salla_databricks.bronze.bronze_shipping_details
# MAGIC   UNION ALL SELECT 'bronze', 'bronze_inventory_movements', COUNT(*), 400000 FROM salla_databricks.bronze.bronze_inventory_movements
# MAGIC   UNION ALL SELECT 'bronze', 'bronze_product_reviews', COUNT(*), 80000 FROM salla_databricks.bronze.bronze_product_reviews
# MAGIC   -- Bronze Landing Zone (12)
# MAGIC   UNION ALL SELECT 'bronze', 'bronze_ad_spend', COUNT(*), 30000 FROM salla_databricks.bronze.bronze_ad_spend
# MAGIC   UNION ALL SELECT 'bronze', 'bronze_competitor_pricing', COUNT(*), 40000 FROM salla_databricks.bronze.bronze_competitor_pricing
# MAGIC   UNION ALL SELECT 'bronze', 'bronze_exchange_rates', COUNT(*), 4000 FROM salla_databricks.bronze.bronze_exchange_rates
# MAGIC   UNION ALL SELECT 'bronze', 'bronze_payment_settlements', COUNT(*), 80000 FROM salla_databricks.bronze.bronze_payment_settlements
# MAGIC   UNION ALL SELECT 'bronze', 'bronze_supplier_invoices', COUNT(*), 200 FROM salla_databricks.bronze.bronze_supplier_invoices
# MAGIC   UNION ALL SELECT 'bronze', 'bronze_return_requests', COUNT(*), 1000 FROM salla_databricks.bronze.bronze_return_requests
# MAGIC   UNION ALL SELECT 'bronze', 'bronze_shipping_manifests', COUNT(*), 150000 FROM salla_databricks.bronze.bronze_shipping_manifests
# MAGIC   UNION ALL SELECT 'bronze', 'bronze_clickstream', COUNT(*), 1000000 FROM salla_databricks.bronze.bronze_clickstream
# MAGIC   UNION ALL SELECT 'bronze', 'bronze_social_media', COUNT(*), 100000 FROM salla_databricks.bronze.bronze_social_media
# MAGIC   UNION ALL SELECT 'bronze', 'bronze_push_notifications', COUNT(*), 50000 FROM salla_databricks.bronze.bronze_push_notifications
# MAGIC   UNION ALL SELECT 'bronze', 'bronze_customer_segmentation', COUNT(*), 9000 FROM salla_databricks.bronze.bronze_customer_segmentation
# MAGIC   UNION ALL SELECT 'bronze', 'bronze_demand_forecasts', COUNT(*), 150000 FROM salla_databricks.bronze.bronze_demand_forecasts
# MAGIC   -- Silver (20)
# MAGIC   UNION ALL SELECT 'silver', 'silver_customers', COUNT(*), 9000 FROM salla_databricks.silver.silver_customers
# MAGIC   UNION ALL SELECT 'silver', 'silver_products', COUNT(*), 30 FROM salla_databricks.silver.silver_products
# MAGIC   UNION ALL SELECT 'silver', 'silver_stores', COUNT(*), 40 FROM salla_databricks.silver.silver_stores
# MAGIC   UNION ALL SELECT 'silver', 'silver_sales_orders', COUNT(*), 1000000 FROM salla_databricks.silver.silver_sales_orders
# MAGIC   UNION ALL SELECT 'silver', 'silver_payment_transactions', COUNT(*), 1000000 FROM salla_databricks.silver.silver_payment_transactions
# MAGIC   UNION ALL SELECT 'silver', 'silver_shipping_details', COUNT(*), 1000000 FROM salla_databricks.silver.silver_shipping_details
# MAGIC   UNION ALL SELECT 'silver', 'silver_inventory_movements', COUNT(*), 400000 FROM salla_databricks.silver.silver_inventory_movements
# MAGIC   UNION ALL SELECT 'silver', 'silver_product_reviews', COUNT(*), 80000 FROM salla_databricks.silver.silver_product_reviews
# MAGIC   UNION ALL SELECT 'silver', 'silver_ad_spend', COUNT(*), 25000 FROM salla_databricks.silver.silver_ad_spend
# MAGIC   UNION ALL SELECT 'silver', 'silver_competitor_pricing', COUNT(*), 4000 FROM salla_databricks.silver.silver_competitor_pricing
# MAGIC   UNION ALL SELECT 'silver', 'silver_exchange_rates', COUNT(*), 3500 FROM salla_databricks.silver.silver_exchange_rates
# MAGIC   UNION ALL SELECT 'silver', 'silver_payment_settlements', COUNT(*), 75000 FROM salla_databricks.silver.silver_payment_settlements
# MAGIC   UNION ALL SELECT 'silver', 'silver_supplier_invoices', COUNT(*), 180 FROM salla_databricks.silver.silver_supplier_invoices
# MAGIC   UNION ALL SELECT 'silver', 'silver_return_requests', COUNT(*), 900 FROM salla_databricks.silver.silver_return_requests
# MAGIC   UNION ALL SELECT 'silver', 'silver_shipping_manifests', COUNT(*), 140000 FROM salla_databricks.silver.silver_shipping_manifests
# MAGIC   UNION ALL SELECT 'silver', 'silver_clickstream', COUNT(*), 900000 FROM salla_databricks.silver.silver_clickstream
# MAGIC   UNION ALL SELECT 'silver', 'silver_social_media', COUNT(*), 90000 FROM salla_databricks.silver.silver_social_media
# MAGIC   UNION ALL SELECT 'silver', 'silver_push_notifications', COUNT(*), 45000 FROM salla_databricks.silver.silver_push_notifications
# MAGIC   UNION ALL SELECT 'silver', 'silver_customer_segmentation', COUNT(*), 9000 FROM salla_databricks.silver.silver_customer_segmentation
# MAGIC   UNION ALL SELECT 'silver', 'silver_demand_forecasts', COUNT(*), 140000 FROM salla_databricks.silver.silver_demand_forecasts
# MAGIC   -- Gold (8)
# MAGIC   UNION ALL SELECT 'gold', 'dim_customers', COUNT(*), 9000 FROM salla_databricks.gold.dim_customers
# MAGIC   UNION ALL SELECT 'gold', 'dim_products', COUNT(*), 30 FROM salla_databricks.gold.dim_products
# MAGIC   UNION ALL SELECT 'gold', 'dim_stores', COUNT(*), 40 FROM salla_databricks.gold.dim_stores
# MAGIC   UNION ALL SELECT 'gold', 'dim_date', COUNT(*), 2000 FROM salla_databricks.gold.dim_date
# MAGIC   UNION ALL SELECT 'gold', 'dim_payment_method', COUNT(*), 5 FROM salla_databricks.gold.dim_payment_method
# MAGIC   UNION ALL SELECT 'gold', 'fact_sales', COUNT(*), 1000000 FROM salla_databricks.gold.fact_sales
# MAGIC   UNION ALL SELECT 'gold', 'fact_ad_spend', COUNT(*), 25000 FROM salla_databricks.gold.fact_ad_spend
# MAGIC   UNION ALL SELECT 'gold', 'fact_competitor_pricing', COUNT(*), 4000 FROM salla_databricks.gold.fact_competitor_pricing
# MAGIC )
# MAGIC SELECT
# MAGIC   layer,
# MAGIC   table_name,
# MAGIC   row_count,
# MAGIC   expected_min,
# MAGIC   ROUND(row_count * 100.0 / NULLIF(expected_min, 0), 1) AS pct_of_expected,
# MAGIC   CASE
# MAGIC     WHEN row_count = 0 THEN '🔴 ALERT - Empty table'
# MAGIC     WHEN row_count < expected_min * 0.5 THEN '🔴 ALERT - Below 50%'
# MAGIC     WHEN row_count < expected_min THEN '🟡 WARNING - Below expected'
# MAGIC     ELSE '🟢 PASS'
# MAGIC   END AS status
# MAGIC FROM counts
# MAGIC ORDER BY
# MAGIC   CASE WHEN row_count = 0 THEN 0 WHEN row_count < expected_min * 0.5 THEN 1 ELSE 2 END,
# MAGIC   pct_of_expected ASC

# COMMAND ----------

# DBTITLE 1,DQ Filter Rate Analysis (Bronze vs Silver)
# MAGIC %sql
# MAGIC -- ============================================================================
# MAGIC -- DQ FILTER RATE: Compare Bronze vs Silver to measure DQ effectiveness
# MAGIC -- ============================================================================
# MAGIC
# MAGIC WITH bronze_counts AS (
# MAGIC   SELECT 'ad_spend' AS source, COUNT(*) AS bronze_rows FROM salla_databricks.bronze.bronze_ad_spend
# MAGIC   UNION ALL SELECT 'competitor_pricing', COUNT(*) FROM salla_databricks.bronze.bronze_competitor_pricing
# MAGIC   UNION ALL SELECT 'exchange_rates', COUNT(*) FROM salla_databricks.bronze.bronze_exchange_rates
# MAGIC   UNION ALL SELECT 'payment_settlements', COUNT(*) FROM salla_databricks.bronze.bronze_payment_settlements
# MAGIC   UNION ALL SELECT 'supplier_invoices', COUNT(*) FROM salla_databricks.bronze.bronze_supplier_invoices
# MAGIC   UNION ALL SELECT 'return_requests', COUNT(*) FROM salla_databricks.bronze.bronze_return_requests
# MAGIC   UNION ALL SELECT 'shipping_manifests', COUNT(*) FROM salla_databricks.bronze.bronze_shipping_manifests
# MAGIC   UNION ALL SELECT 'clickstream', COUNT(*) FROM salla_databricks.bronze.bronze_clickstream
# MAGIC   UNION ALL SELECT 'social_media', COUNT(*) FROM salla_databricks.bronze.bronze_social_media
# MAGIC   UNION ALL SELECT 'push_notifications', COUNT(*) FROM salla_databricks.bronze.bronze_push_notifications
# MAGIC   UNION ALL SELECT 'customer_segmentation', COUNT(*) FROM salla_databricks.bronze.bronze_customer_segmentation
# MAGIC   UNION ALL SELECT 'demand_forecasts', COUNT(*) FROM salla_databricks.bronze.bronze_demand_forecasts
# MAGIC ),
# MAGIC silver_counts AS (
# MAGIC   SELECT 'ad_spend' AS source, COUNT(*) AS silver_rows FROM salla_databricks.silver.silver_ad_spend
# MAGIC   UNION ALL SELECT 'competitor_pricing', COUNT(*) FROM salla_databricks.silver.silver_competitor_pricing
# MAGIC   UNION ALL SELECT 'exchange_rates', COUNT(*) FROM salla_databricks.silver.silver_exchange_rates
# MAGIC   UNION ALL SELECT 'payment_settlements', COUNT(*) FROM salla_databricks.silver.silver_payment_settlements
# MAGIC   UNION ALL SELECT 'supplier_invoices', COUNT(*) FROM salla_databricks.silver.silver_supplier_invoices
# MAGIC   UNION ALL SELECT 'return_requests', COUNT(*) FROM salla_databricks.silver.silver_return_requests
# MAGIC   UNION ALL SELECT 'shipping_manifests', COUNT(*) FROM salla_databricks.silver.silver_shipping_manifests
# MAGIC   UNION ALL SELECT 'clickstream', COUNT(*) FROM salla_databricks.silver.silver_clickstream
# MAGIC   UNION ALL SELECT 'social_media', COUNT(*) FROM salla_databricks.silver.silver_social_media
# MAGIC   UNION ALL SELECT 'push_notifications', COUNT(*) FROM salla_databricks.silver.silver_push_notifications
# MAGIC   UNION ALL SELECT 'customer_segmentation', COUNT(*) FROM salla_databricks.silver.silver_customer_segmentation
# MAGIC   UNION ALL SELECT 'demand_forecasts', COUNT(*) FROM salla_databricks.silver.silver_demand_forecasts
# MAGIC )
# MAGIC SELECT
# MAGIC   b.source,
# MAGIC   b.bronze_rows,
# MAGIC   s.silver_rows,
# MAGIC   b.bronze_rows - s.silver_rows AS rows_filtered,
# MAGIC   ROUND((1 - s.silver_rows * 1.0 / NULLIF(b.bronze_rows, 0)) * 100, 1) AS filter_pct,
# MAGIC   CASE
# MAGIC     WHEN b.bronze_rows = 0 THEN '🔴 ALERT - No data'
# MAGIC     WHEN b.source = 'competitor_pricing' AND (1 - s.silver_rows * 1.0 / b.bronze_rows) * 100 > 80
# MAGIC       THEN '🟢 EXPECTED - Strict price validation'
# MAGIC     WHEN (1 - s.silver_rows * 1.0 / NULLIF(b.bronze_rows, 0)) * 100 > 50
# MAGIC       THEN '🔴 ALERT - Excessive filtering'
# MAGIC     WHEN (1 - s.silver_rows * 1.0 / NULLIF(b.bronze_rows, 0)) * 100 > 20
# MAGIC       THEN '🟡 WARNING - High filter rate'
# MAGIC     ELSE '🟢 NORMAL'
# MAGIC   END AS status
# MAGIC FROM bronze_counts b
# MAGIC JOIN silver_counts s ON b.source = s.source
# MAGIC ORDER BY filter_pct DESC

# COMMAND ----------

# DBTITLE 1,Summary and Alerting
# ============================================================================
# SUMMARY & ALERTING: Aggregate all checks, fail job on CRITICAL issues
# ============================================================================

from datetime import datetime
import json

# Run summary queries to collect results
print("="*70)
print("PIPELINE HEALTH CHECK SUMMARY")
print(f"Run time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("="*70)

critical_issues = []
warnings = []

# 1. Check for empty tables (CRITICAL)
empty_tables = spark.sql("""
  SELECT table_name, row_count FROM (
    SELECT 'bronze_sales_orders' AS table_name, COUNT(*) AS row_count FROM salla_databricks.bronze.bronze_sales_orders
    UNION ALL SELECT 'silver_sales_orders', COUNT(*) FROM salla_databricks.silver.silver_sales_orders
    UNION ALL SELECT 'gold.fact_sales', COUNT(*) FROM salla_databricks.gold.fact_sales
    UNION ALL SELECT 'bronze_clickstream', COUNT(*) FROM salla_databricks.bronze.bronze_clickstream
  ) WHERE row_count = 0
""").collect()

for row in empty_tables:
    critical_issues.append(f"EMPTY TABLE: {row.table_name}")

# 2. Check data freshness (WARNING if >24h, CRITICAL if >48h)
freshness_check = spark.sql("""
  SELECT table_name, hours_since FROM (
    SELECT 'bronze_clickstream' AS table_name,
           ROUND((unix_timestamp(current_timestamp()) - unix_timestamp(MAX(_bronze_timestamp))) / 3600, 1) AS hours_since
    FROM salla_databricks.bronze.bronze_clickstream
    UNION ALL
    SELECT 'bronze_sales_orders',
           ROUND((unix_timestamp(current_timestamp()) - unix_timestamp(MAX(_bronze_timestamp))) / 3600, 1)
    FROM salla_databricks.bronze.bronze_sales_orders
  ) WHERE hours_since > 24
""").collect()

for row in freshness_check:
    if row.hours_since and row.hours_since > 48:
        critical_issues.append(f"STALE DATA (>48h): {row.table_name} - {row.hours_since}h")
    elif row.hours_since and row.hours_since > 24:
        warnings.append(f"Stale data (>24h): {row.table_name} - {row.hours_since}h")

# 3. Check row counts below 50% threshold
low_count_tables = spark.sql("""
  SELECT * FROM (
    SELECT 'fact_sales' AS table_name, COUNT(*) AS cnt, 1000000 AS expected FROM salla_databricks.gold.fact_sales
    UNION ALL SELECT 'bronze_clickstream', COUNT(*), 1000000 FROM salla_databricks.bronze.bronze_clickstream
    UNION ALL SELECT 'silver_clickstream', COUNT(*), 900000 FROM salla_databricks.silver.silver_clickstream
  ) WHERE cnt < expected * 0.5
""").collect()

for row in low_count_tables:
    critical_issues.append(f"LOW ROW COUNT: {row.table_name} has {row.cnt:,} rows (expected {row.expected:,})")

# 4. Check for abnormal DQ filter rates (excluding competitor_pricing which is expected high)
filter_check = spark.sql("""
  WITH b AS (SELECT 'social_media' AS src, COUNT(*) AS cnt FROM salla_databricks.bronze.bronze_social_media
             UNION ALL SELECT 'clickstream', COUNT(*) FROM salla_databricks.bronze.bronze_clickstream),
       s AS (SELECT 'social_media' AS src, COUNT(*) AS cnt FROM salla_databricks.silver.silver_social_media
             UNION ALL SELECT 'clickstream', COUNT(*) FROM salla_databricks.silver.silver_clickstream)
  SELECT b.src, ROUND((1 - s.cnt * 1.0 / b.cnt) * 100, 1) AS filter_pct
  FROM b JOIN s ON b.src = s.src
  WHERE (1 - s.cnt * 1.0 / b.cnt) * 100 > 50
""").collect()

for row in filter_check:
    critical_issues.append(f"EXCESSIVE DQ FILTERING: {row.src} - {row.filter_pct}% filtered")

# Print summary
print("\n" + "-"*70)
print("CRITICAL ISSUES:")
if critical_issues:
    for issue in critical_issues:
        print(f"  🔴 {issue}")
else:
    print("  🟢 None")

print("\nWARNINGS:")
if warnings:
    for warn in warnings:
        print(f"  🟡 {warn}")
else:
    print("  🟢 None")

print("-"*70)

# Build result JSON
result = {
    "status": "FAIL" if critical_issues else "PASS",
    "timestamp": datetime.now().isoformat(),
    "critical_count": len(critical_issues),
    "warning_count": len(warnings),
    "critical_issues": critical_issues,
    "warnings": warnings,
}

print(f"\nOVERALL STATUS: {'\u274c FAIL' if critical_issues else '\u2705 PASS'}")
print("="*70)

# Exit with result JSON (for downstream consumption)
if critical_issues:
    # Raise exception to fail the job task, triggering email notification
    dbutils.notebook.exit(json.dumps(result))
    raise Exception(f"Pipeline health check FAILED with {len(critical_issues)} critical issue(s): {'; '.join(critical_issues)}")
else:
    dbutils.notebook.exit(json.dumps(result))
