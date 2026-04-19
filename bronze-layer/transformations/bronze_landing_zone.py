# Databricks notebook source
# ============================================================
# Bronze Layer — Landing Zone Ingestion (12 Streaming Tables)
# ============================================================
# Source  : Unity Catalog managed volume (landing_data)
# Target  : salla_databricks.bronze
# Auth    : UC-managed — no external credentials needed
# Tables  : 4 original + 8 new = 12 total
#   CSV    : ad_spend, competitor_pricing, payment_settlements,
#            supplier_invoices, return_requests, shipping_manifests
#   JSON   : exchange_rates, clickstream, social_media, push_notifications
#   Parquet: customer_segmentation, demand_forecasts
# ============================================================

from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col

VOLUME_BASE = "/Volumes/salla_databricks/bronze/landing_data"

def _read_landing(file_path: str, file_format: str, schema: str):
    """Read from UC managed volume via Auto Loader with explicit schema."""
    spark = SparkSession.builder.getOrCreate()
    full_path = f"{VOLUME_BASE}/{file_path}"
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", file_format)
        .option("header", "true")
        .schema(schema)
        .load(full_path)
        .withColumn("_bronze_timestamp", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )


# ====================================================================
# SCHEMAS
# ====================================================================

# -- CSV Sources -----------------------------------------------------
AD_SPEND_SCHEMA = "date DATE, campaign_name STRING, campaign_type STRING, campaign_category STRING, platform STRING, impressions INT, clicks INT, spend_sar DOUBLE, conversions INT, revenue_sar DOUBLE, roas DOUBLE, cpc DOUBLE, cpm DOUBLE, currency STRING, country STRING, year INT, month INT, day INT, _rescued_data STRING"

COMPETITOR_PRICING_SCHEMA = "scrape_id INT, scrape_timestamp TIMESTAMP, scrape_date DATE, competitor_id STRING, competitor_name STRING, competitor_country STRING, product_name STRING, product_category STRING, product_subcategory STRING, current_price DOUBLE, original_price DOUBLE, discount_percentage DOUBLE, currency STRING, in_stock BOOLEAN, rating DOUBLE, review_count INT, year INT, month INT, day INT, _rescued_data STRING"

PAYMENT_SETTLEMENTS_SCHEMA = "gateway_transaction_id STRING, merchant_order_id INT, payment_method STRING, transaction_date DATE, amount DOUBLE, currency STRING, gateway_fee DOUBLE, net_amount DOUBLE, status STRING, settlement_date DATE, gateway STRING, country STRING, year INT, month INT, day INT, _rescued_data STRING"

SUPPLIER_INVOICES_SCHEMA = "invoice_number STRING, invoice_date DATE, due_date DATE, supplier_id STRING, supplier_name STRING, contact_email STRING, contact_phone STRING, supplier_city STRING, po_number STRING, product_id STRING, product_name STRING, brand STRING, quantity INT, unit_cost DOUBLE, line_total DOUBLE, vat_amount DOUBLE, total_amount DOUBLE, currency STRING, payment_terms STRING, status STRING, amount_paid DOUBLE, year INT, month INT, _rescued_data STRING"

RETURN_REQUESTS_SCHEMA = "return_request_id STRING, order_id STRING, customer_id STRING, product_id STRING, store_id STRING, request_date DATE, return_reason STRING, status STRING, refund_amount DOUBLE, resolution_date DATE, resolution_notes STRING, return_quantity INT, year INT, month INT, day INT, _rescued_data STRING"

SHIPPING_MANIFESTS_SCHEMA = "scan_id LONG, order_id STRING, tracking_number STRING, carrier STRING, scan_date DATE, scan_timestamp STRING, event_type STRING, scan_location STRING, weight_kg DOUBLE, customer_city STRING, customer_region STRING, year INT, month INT, day INT, _rescued_data STRING"

# -- JSON Sources ----------------------------------------------------
EXCHANGE_RATES_SCHEMA = "api_url STRING, base_currency STRING, date STRING, day BIGINT, exchange_rate DOUBLE, fetch_timestamp STRING, inverse_rate DOUBLE, month BIGINT, source STRING, target_currency STRING, year INT, _rescued_data STRING"

CLICKSTREAM_SCHEMA = "event_id STRING, session_id STRING, user_id STRING, event_type STRING, event_timestamp STRING, page_url STRING, product_id STRING, store_id STRING, device_type STRING, user_agent STRING, referrer STRING, year INT, month INT, day INT, _rescued_data STRING"

SOCIAL_MEDIA_SCHEMA = "post_id STRING, platform STRING, post_timestamp STRING, post_date DATE, author_id STRING, language STRING, text STRING, sentiment STRING, store_mentioned STRING, product_mentioned STRING, likes INT, shares INT, comments INT, year INT, month INT, day INT, _rescued_data STRING"

PUSH_NOTIFICATIONS_SCHEMA = "notification_id STRING, customer_id STRING, order_id STRING, notification_type STRING, channel STRING, device_token STRING, title STRING, sent_at STRING, delivery_status STRING, opened BOOLEAN, opened_at STRING, year INT, month INT, day INT, _rescued_data STRING"

# -- Parquet Sources -------------------------------------------------
CUSTOMER_SEGMENTATION_SCHEMA = "customer_id STRING, customer_name STRING, email STRING, city STRING, region STRING, customer_since TIMESTAMP, last_order_date TIMESTAMP, recency_days INT, total_orders LONG, total_spend DOUBLE, avg_order_value DOUBLE, recency_score INT, frequency_score INT, monetary_score INT, rfm_total INT, segment STRING, clv_12_months DOUBLE, churn_probability DOUBLE, predicted_annual_orders INT, model_version STRING, model_algorithm STRING, scoring_date DATE, _scored_at TIMESTAMP, _rescued_data STRING"

DEMAND_FORECASTS_SCHEMA = "product_id STRING, product_name STRING, category STRING, store_id STRING, store_name STRING, store_city STRING, store_region STRING, forecast_date DATE, predicted_demand DOUBLE, lower_bound DOUBLE, upper_bound DOUBLE, confidence_level DOUBLE, prediction_std DOUBLE, historical_avg_demand DOUBLE, season_factor DOUBLE, category_boost DOUBLE, model_algorithm STRING, model_version STRING, training_date DATE, mape_score DOUBLE, forecast_generated_at TIMESTAMP, year INT, month INT, day INT, _rescued_data STRING"


# ====================================================================
# ORIGINAL 4 STREAMING TABLES
# ====================================================================

@dp.table(name="bronze_ad_spend", comment="Raw ad spend data ingested from CSV files in landing zone")
@dp.expect("valid_date", "date IS NOT NULL")
def bronze_ad_spend():
    return _read_landing("ad_spend/", "csv", AD_SPEND_SCHEMA)

@dp.table(name="bronze_competitor_pricing", comment="Raw competitor pricing data ingested from CSV files in landing zone")
@dp.expect("valid_scrape_id", "scrape_id IS NOT NULL")
def bronze_competitor_pricing():
    return _read_landing("competitor_pricing/", "csv", COMPETITOR_PRICING_SCHEMA)

@dp.table(name="bronze_exchange_rates", comment="Raw exchange rates ingested from JSON files in landing zone")
@dp.expect("valid_base_currency", "base_currency IS NOT NULL")
def bronze_exchange_rates():
    return _read_landing("exchange_rates/", "json", EXCHANGE_RATES_SCHEMA)

@dp.table(name="bronze_payment_settlements", comment="Raw payment settlements ingested from CSV files in landing zone")
@dp.expect("valid_gateway_transaction_id", "gateway_transaction_id IS NOT NULL")
def bronze_payment_settlements():
    return _read_landing("payment_settlements/", "csv", PAYMENT_SETTLEMENTS_SCHEMA)


# ====================================================================
# 8 NEW STREAMING TABLES
# ====================================================================

# -- 5. Supplier Invoices (CSV) --------------------------------------
@dp.table(name="bronze_supplier_invoices", comment="Raw supplier/procurement invoices from ERP system")
@dp.expect("valid_invoice_number", "invoice_number IS NOT NULL")
def bronze_supplier_invoices():
    return _read_landing("supplier_invoices/", "csv", SUPPLIER_INVOICES_SCHEMA)

# -- 6. Return Requests (CSV) ----------------------------------------
@dp.table(name="bronze_return_requests", comment="Raw customer return/refund requests from RMA system")
@dp.expect("valid_return_request_id", "return_request_id IS NOT NULL")
def bronze_return_requests():
    return _read_landing("return_requests/", "csv", RETURN_REQUESTS_SCHEMA)

# -- 7. Shipping Manifests (CSV) -------------------------------------
@dp.table(name="bronze_shipping_manifests", comment="Raw 3PL carrier tracking/scan events from logistics partners")
@dp.expect("valid_scan_id", "scan_id IS NOT NULL")
def bronze_shipping_manifests():
    return _read_landing("shipping_manifests/", "csv", SHIPPING_MANIFESTS_SCHEMA)

# -- 8. Clickstream (JSON) -------------------------------------------
@dp.table(name="bronze_clickstream", comment="Raw website analytics events from GTM/analytics platform")
@dp.expect("valid_event_id", "event_id IS NOT NULL")
def bronze_clickstream():
    return _read_landing("clickstream/", "json", CLICKSTREAM_SCHEMA)

# -- 9. Social Media (JSON) ------------------------------------------
@dp.table(name="bronze_social_media", comment="Raw social listening mentions from Twitter/X, Instagram, TikTok, Snapchat")
@dp.expect("valid_post_id", "post_id IS NOT NULL")
def bronze_social_media():
    return _read_landing("social_media/", "json", SOCIAL_MEDIA_SCHEMA)

# -- 10. Push Notifications (JSON) -----------------------------------
@dp.table(name="bronze_push_notifications", comment="Raw mobile/web push notification delivery logs from Firebase/APNs")
@dp.expect("valid_notification_id", "notification_id IS NOT NULL")
def bronze_push_notifications():
    return _read_landing("push_notifications/", "json", PUSH_NOTIFICATIONS_SCHEMA)

# -- 11. Customer Segmentation (Parquet) ------------------------------
@dp.table(name="bronze_customer_segmentation", comment="Raw ML model output with RFM segmentation, CLV, and churn scores")
@dp.expect("valid_customer_id", "customer_id IS NOT NULL")
def bronze_customer_segmentation():
    return _read_landing("customer_segmentation/", "parquet", CUSTOMER_SEGMENTATION_SCHEMA)

# -- 12. Demand Forecasts (Parquet) -----------------------------------
@dp.table(name="bronze_demand_forecasts", comment="Raw ML model output with 90-day demand predictions per product x store")
@dp.expect("valid_product_id", "product_id IS NOT NULL")
def bronze_demand_forecasts():
    return _read_landing("demand_forecasts/", "parquet", DEMAND_FORECASTS_SCHEMA)
