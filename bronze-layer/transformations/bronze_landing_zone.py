# ============================================================
# Bronze Layer — Landing Zone Ingestion (4 Streaming Tables)
# ============================================================
# Source  : Unity Catalog managed volume (landing_data)
# Target  : salla_databricks.bronze
# Auth    : UC-managed — no external credentials needed
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

AD_SPEND_SCHEMA = "date DATE, campaign_name STRING, campaign_type STRING, campaign_category STRING, platform STRING, impressions INT, clicks INT, spend_sar DOUBLE, conversions INT, revenue_sar DOUBLE, roas DOUBLE, cpc DOUBLE, cpm DOUBLE, currency STRING, country STRING, year INT, month INT, day INT, _rescued_data STRING"
COMPETITOR_PRICING_SCHEMA = "scrape_id INT, scrape_timestamp TIMESTAMP, scrape_date DATE, competitor_id STRING, competitor_name STRING, competitor_country STRING, product_name STRING, product_category STRING, product_subcategory STRING, current_price DOUBLE, original_price DOUBLE, discount_percentage DOUBLE, currency STRING, in_stock BOOLEAN, rating DOUBLE, review_count INT, year INT, month INT, day INT, _rescued_data STRING"
EXCHANGE_RATES_SCHEMA = "api_url STRING, base_currency STRING, date STRING, day BIGINT, exchange_rate DOUBLE, fetch_timestamp STRING, inverse_rate DOUBLE, month BIGINT, source STRING, target_currency STRING, year INT, _rescued_data STRING"
PAYMENT_SETTLEMENTS_SCHEMA = "gateway_transaction_id STRING, merchant_order_id INT, payment_method STRING, transaction_date DATE, amount DOUBLE, currency STRING, gateway_fee DOUBLE, net_amount DOUBLE, status STRING, settlement_date DATE, gateway STRING, country STRING, year INT, month INT, day INT, _rescued_data STRING"


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