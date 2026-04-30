# Databricks notebook source
# ============================================================================
# Silver Layer — Landing Zone Transformations (4 Streaming Tables)
# ============================================================================
# INDUSTRY-STANDARD per Databricks Docs:
#   ✓ _rescued_data handling (FILTERED in transformation — malformed rows dropped)
#   ✓ Year/month/day partition columns carried forward
#   ✓ Watermark + dropDuplicatesWithinWatermark for streaming dedup
#   ✓ Comprehensive null handling
#   ✓ Bronze metadata excluded (_bronze_timestamp, _source_file removed)
#   ✓ Date validation (no future dates)
#   ✓ Full string normalization (trim ALL strings)
#   ✓ Type casting (exchange_rates.date STRING→DATE, etc.)
#   ✓ Business logic validation
# ============================================================================

from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, lower, upper, current_timestamp, to_date, to_timestamp, when
)
from pyspark.sql.types import StringType

BRONZE = SparkSession.builder.getOrCreate().conf.get("pipeline.bronze_schema", "salla_databricks.bronze")

def _stream_bronze(t):
    return SparkSession.builder.getOrCreate().readStream.table(f"{BRONZE}.bronze_{t}")

def _trim_strings(df):
    for f in df.schema.fields:
        if isinstance(f.dataType, StringType):
            df = df.withColumn(f.name, trim(col(f.name)))
    return df

def _filter_rescued(df):
    """Filter out rows with malformed data (captured in _rescued_data by Auto Loader)."""
    if "_rescued_data" in df.columns:
        return df.filter(col("_rescued_data").isNull())
    return df


# ══════════════════════════════════════════════════════════════════════════
# 1. AD SPEND
# ══════════════════════════════════════════════════════════════════════════
@dp.table(name="silver_ad_spend", comment="Cleaned ad spend — validated metrics, malformed rows filtered")
@dp.expect_or_drop("date_not_null", "date IS NOT NULL")
@dp.expect_or_drop("spend_positive", "spend_sar IS NOT NULL AND spend_sar > 0")
@dp.expect("date_not_future", "date <= current_date()")
@dp.expect("impressions_valid", "impressions IS NOT NULL AND impressions >= 0")
@dp.expect("clicks_valid", "clicks IS NOT NULL AND clicks >= 0")
@dp.expect("clicks_lte_impressions", "impressions IS NULL OR clicks <= impressions")
@dp.expect("conversions_valid", "conversions IS NOT NULL AND conversions >= 0")
@dp.expect("platform_not_null", "platform IS NOT NULL")
@dp.expect("campaign_not_null", "campaign_name IS NOT NULL")
def silver_ad_spend():
    return _filter_rescued(_trim_strings(_stream_bronze("ad_spend"))) \
        .withWatermark("_bronze_timestamp", "1 hour") \
        .dropDuplicatesWithinWatermark(["date", "campaign_name", "platform", "country"]) \
        .select(
            col("date"),
            col("campaign_name"), lower(col("campaign_type")).alias("campaign_type"),
            lower(col("campaign_category")).alias("campaign_category"),
            lower(col("platform")).alias("platform"),
            col("impressions"), col("clicks"), col("spend_sar"),
            col("conversions"), col("revenue_sar"),
            col("roas"), col("cpc"), col("cpm"),
            upper(col("currency")).alias("currency"),
            upper(col("country")).alias("country"),
            col("year"), col("month"), col("day"),
            current_timestamp().alias("_silver_timestamp"))


# ══════════════════════════════════════════════════════════════════════════
# 2. COMPETITOR PRICING
# ══════════════════════════════════════════════════════════════════════════
@dp.table(name="silver_competitor_pricing", comment="Cleaned competitor pricing — malformed rows filtered")
@dp.expect_or_drop("pk_not_null", "scrape_id IS NOT NULL")
@dp.expect("price_positive", "current_price IS NOT NULL AND current_price > 0")
@dp.expect("original_price_valid", "original_price IS NULL OR original_price > 0")
@dp.expect("discount_valid", "discount_percentage IS NULL OR (discount_percentage >= 0 AND discount_percentage <= 100)")
@dp.expect("competitor_not_null", "competitor_name IS NOT NULL")
@dp.expect("product_not_null", "product_name IS NOT NULL")
@dp.expect("scrape_date_valid", "scrape_date IS NOT NULL AND scrape_date <= current_date()")
@dp.expect("rating_valid", "rating IS NULL OR (rating >= 0 AND rating <= 5)")
def silver_competitor_pricing():
    return _filter_rescued(_trim_strings(_stream_bronze("competitor_pricing"))) \
        .withWatermark("_bronze_timestamp", "1 hour") \
        .dropDuplicatesWithinWatermark(["scrape_id"]) \
        .select(
            col("scrape_id").cast("string").alias("scrape_id"),
            col("scrape_timestamp"), col("scrape_date"),
            col("competitor_id"), col("competitor_name"),
            upper(col("competitor_country")).alias("competitor_country"),
            col("product_name"), col("product_category"), col("product_subcategory"),
            col("current_price"), col("original_price"), col("discount_percentage"),
            upper(col("currency")).alias("currency"),
            col("in_stock"), col("rating"), col("review_count"),
            col("year"), col("month"), col("day"),
            current_timestamp().alias("_silver_timestamp"))


# ══════════════════════════════════════════════════════════════════════════
# 3. EXCHANGE RATES
# ══════════════════════════════════════════════════════════════════════════
@dp.table(name="silver_exchange_rates", comment="Cleaned exchange rates — proper DATE/TIMESTAMP types")
@dp.expect_or_drop("date_not_null", "date IS NOT NULL")
@dp.expect("rate_positive", "exchange_rate IS NOT NULL AND exchange_rate > 0")
@dp.expect("inverse_valid", "inverse_rate IS NULL OR inverse_rate > 0")
@dp.expect("base_not_null", "base_currency IS NOT NULL")
@dp.expect("target_not_null", "target_currency IS NOT NULL")
@dp.expect("date_not_future", "date <= current_date()")
def silver_exchange_rates():
    return _filter_rescued(_trim_strings(_stream_bronze("exchange_rates"))) \
        .withWatermark("_bronze_timestamp", "1 hour") \
        .dropDuplicatesWithinWatermark(["date", "base_currency", "target_currency", "source"]) \
        .select(
            to_date(col("date")).alias("date"),
            upper(col("base_currency")).alias("base_currency"),
            upper(col("target_currency")).alias("target_currency"),
            col("exchange_rate"), col("inverse_rate"),
            col("source"),
            to_timestamp(col("fetch_timestamp")).alias("fetch_timestamp"),
            col("year").cast("int").alias("year"),
            col("month").cast("int").alias("month"),
            col("day").cast("int").alias("day"),
            current_timestamp().alias("_silver_timestamp"))


# ══════════════════════════════════════════════════════════════════════════
# 4. PAYMENT SETTLEMENTS
# ══════════════════════════════════════════════════════════════════════════
@dp.table(name="silver_payment_settlements", comment="Cleaned payment settlements — net amount validation")
@dp.expect_or_drop("pk_not_null", "gateway_transaction_id IS NOT NULL")
@dp.expect("amount_positive", "amount IS NOT NULL AND amount > 0")
@dp.expect("fee_valid", "gateway_fee IS NOT NULL AND gateway_fee >= 0")
@dp.expect("net_positive", "net_amount IS NOT NULL AND net_amount > 0")
@dp.expect("net_calc_valid", "ABS(net_amount - (amount - gateway_fee)) <= 0.02")
@dp.expect("status_not_null", "status IS NOT NULL")
@dp.expect("txn_date_valid", "transaction_date IS NOT NULL AND transaction_date <= current_date()")
@dp.expect("settlement_logic", "settlement_date IS NULL OR settlement_date >= transaction_date")
@dp.expect("method_not_null", "payment_method IS NOT NULL")
@dp.expect("gateway_not_null", "gateway IS NOT NULL")
def silver_payment_settlements():
    return _filter_rescued(_trim_strings(_stream_bronze("payment_settlements"))) \
        .withWatermark("_bronze_timestamp", "1 hour") \
        .dropDuplicatesWithinWatermark(["gateway_transaction_id"]) \
        .select(
            col("gateway_transaction_id"),
            col("merchant_order_id").cast("string").alias("merchant_order_id"),
            lower(col("payment_method")).alias("payment_method"),
            col("transaction_date"),
            col("amount"), upper(col("currency")).alias("currency"),
            col("gateway_fee"), col("net_amount"),
            lower(col("status")).alias("status"),
            col("settlement_date"),
            lower(col("gateway")).alias("gateway"),
            upper(col("country")).alias("country"),
            col("year"), col("month"), col("day"),
            current_timestamp().alias("_silver_timestamp"))