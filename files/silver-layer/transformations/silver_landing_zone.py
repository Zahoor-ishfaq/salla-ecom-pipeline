# Databricks notebook source
# ============================================================================
# Silver Layer — Landing Zone Transformations (12 Streaming Tables)
# ============================================================================
# INDUSTRY-STANDARD per Databricks Docs:
#   - Quarantine pattern (expect_or_drop for critical, expect for soft)
#   - _rescued_data handling (malformed rows filtered)
#   - Watermark + dropDuplicatesWithinWatermark for streaming dedup
#   - Full string normalization, type casting, date validation
#   - Bronze metadata excluded (_bronze_timestamp, _source_file removed)
#   - 120+ total data quality expectations across all 12 tables
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


# ====================================================================
# ORIGINAL 4 SILVER TABLES
# ====================================================================

# -- 1. AD SPEND ----------------------------------------------------
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


# -- 2. COMPETITOR PRICING -------------------------------------------
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


# -- 3. EXCHANGE RATES -----------------------------------------------
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


# -- 4. PAYMENT SETTLEMENTS ------------------------------------------
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


# ====================================================================
# 8 NEW SILVER TABLES
# ====================================================================

# -- 5. SUPPLIER INVOICES --------------------------------------------
@dp.table(name="silver_supplier_invoices", comment="Cleaned supplier invoices — VAT math, payment status, FK validation")
@dp.expect_or_drop("pk_not_null", "invoice_number IS NOT NULL")
@dp.expect("supplier_fk", "supplier_id IS NOT NULL")
@dp.expect("product_fk", "product_id IS NOT NULL")
@dp.expect("date_not_null", "invoice_date IS NOT NULL")
@dp.expect("date_not_future", "invoice_date <= current_date()")
@dp.expect("due_after_invoice", "due_date IS NULL OR due_date >= invoice_date")
@dp.expect("quantity_positive", "quantity IS NOT NULL AND quantity > 0")
@dp.expect("unit_cost_positive", "unit_cost IS NOT NULL AND unit_cost > 0")
@dp.expect("line_total_calc", "ABS(line_total - (quantity * unit_cost)) <= line_total * 0.05")
@dp.expect("vat_15_pct", "ABS(vat_amount - (line_total * 0.15)) <= 1.0")
@dp.expect("total_calc", "ABS(total_amount - (line_total + vat_amount)) <= 1.0")
@dp.expect("status_valid", "status IN ('paid','pending','overdue','partially_paid','cancelled')")
@dp.expect("amount_paid_valid", "amount_paid IS NOT NULL AND amount_paid >= 0")
@dp.expect("paid_lte_total", "amount_paid <= total_amount * 1.01")
@dp.expect("terms_valid", "payment_terms IN ('Net30','Net60','Net90','COD','Prepaid')")
@dp.expect("email_valid", "contact_email IS NULL OR contact_email RLIKE '^[^@]+@[^@]+\\\\.[^@]+$'")
def silver_supplier_invoices():
    return _filter_rescued(_trim_strings(_stream_bronze("supplier_invoices"))) \
        .withWatermark("_bronze_timestamp", "1 hour") \
        .dropDuplicatesWithinWatermark(["invoice_number"]) \
        .select(
            col("invoice_number"), col("invoice_date"), col("due_date"),
            col("supplier_id"), col("supplier_name"),
            lower(col("contact_email")).alias("contact_email"),
            col("contact_phone"), col("supplier_city"),
            col("po_number"), col("product_id"), col("product_name"), col("brand"),
            col("quantity"), col("unit_cost"), col("line_total"),
            col("vat_amount"), col("total_amount"), upper(col("currency")).alias("currency"),
            lower(col("payment_terms")).alias("payment_terms"),
            lower(col("status")).alias("status"), col("amount_paid"),
            col("year"), col("month"),
            current_timestamp().alias("_silver_timestamp"))


# -- 6. RETURN REQUESTS ----------------------------------------------
@dp.table(name="silver_return_requests", comment="Cleaned return requests — refund validation, FK integrity, status flow")
@dp.expect_or_drop("pk_not_null", "return_request_id IS NOT NULL")
@dp.expect("order_fk", "order_id IS NOT NULL")
@dp.expect("customer_fk", "customer_id IS NOT NULL")
@dp.expect("product_fk", "product_id IS NOT NULL")
@dp.expect("store_fk", "store_id IS NOT NULL")
@dp.expect("date_not_null", "request_date IS NOT NULL")
@dp.expect("date_not_future", "request_date <= current_date()")
@dp.expect("reason_not_null", "return_reason IS NOT NULL")
@dp.expect("reason_valid", "return_reason IN ('changed_mind','defective','not_as_described','wrong_item','late_delivery','size_issue','other')")
@dp.expect("status_valid", "status IN ('requested','approved','rejected','refunded','completed','processing')")
@dp.expect("refund_not_negative", "refund_amount IS NOT NULL AND refund_amount >= 0")
@dp.expect("resolution_after_request", "resolution_date IS NULL OR resolution_date >= request_date")
@dp.expect("quantity_positive", "return_quantity IS NOT NULL AND return_quantity > 0")
@dp.expect("resolved_has_date", "status NOT IN ('completed','refunded','rejected') OR resolution_date IS NOT NULL")
def silver_return_requests():
    return _filter_rescued(_trim_strings(_stream_bronze("return_requests"))) \
        .withWatermark("_bronze_timestamp", "1 hour") \
        .dropDuplicatesWithinWatermark(["return_request_id"]) \
        .select(
            col("return_request_id"),
            col("order_id").cast("string").alias("order_id"),
            col("customer_id").cast("string").alias("customer_id"),
            col("product_id").cast("string").alias("product_id"),
            col("store_id").cast("string").alias("store_id"),
            col("request_date"),
            lower(col("return_reason")).alias("return_reason"),
            lower(col("status")).alias("status"),
            col("refund_amount"), col("resolution_date"), col("resolution_notes"),
            col("return_quantity"),
            col("year"), col("month"), col("day"),
            current_timestamp().alias("_silver_timestamp"))


# -- 7. SHIPPING MANIFESTS -------------------------------------------
@dp.table(name="silver_shipping_manifests", comment="Cleaned 3PL tracking events — carrier validation, event sequence, weight checks")
@dp.expect_or_drop("pk_not_null", "scan_id IS NOT NULL")
@dp.expect("order_fk", "order_id IS NOT NULL")
@dp.expect("tracking_not_null", "tracking_number IS NOT NULL")
@dp.expect("carrier_not_null", "carrier IS NOT NULL")
@dp.expect("carrier_valid", "carrier IN ('Aramex','SMSA Express','DHL','J&T Express','Fetchr','FedEx','UPS')")
@dp.expect("date_not_null", "scan_date IS NOT NULL")
@dp.expect("date_not_future", "scan_date <= current_date()")
@dp.expect("event_type_valid", "event_type IN ('picked_up','in_transit','out_for_delivery','delivered','failed_attempt','returned','customs_hold')")
@dp.expect("weight_positive", "weight_kg IS NOT NULL AND weight_kg > 0")
@dp.expect("weight_realistic", "weight_kg <= 50")
@dp.expect("location_not_null", "scan_location IS NOT NULL")
@dp.expect("city_not_null", "customer_city IS NOT NULL")
@dp.expect("region_not_null", "customer_region IS NOT NULL")
def silver_shipping_manifests():
    return _filter_rescued(_trim_strings(_stream_bronze("shipping_manifests"))) \
        .withWatermark("_bronze_timestamp", "1 hour") \
        .dropDuplicatesWithinWatermark(["scan_id"]) \
        .select(
            col("scan_id"), col("order_id").cast("string").alias("order_id"),
            col("tracking_number"), col("carrier"),
            col("scan_date"), to_timestamp(col("scan_timestamp")).alias("scan_timestamp"),
            lower(col("event_type")).alias("event_type"),
            col("scan_location"), col("weight_kg"),
            col("customer_city"), col("customer_region"),
            col("year"), col("month"), col("day"),
            current_timestamp().alias("_silver_timestamp"))


# -- 8. CLICKSTREAM --------------------------------------------------
@dp.table(name="silver_clickstream", comment="Cleaned website analytics — bot filtering, session validation, funnel events")
@dp.expect_or_drop("pk_not_null", "event_id IS NOT NULL")
@dp.expect("session_not_null", "session_id IS NOT NULL")
@dp.expect("event_type_valid", "event_type IN ('page_view','product_view','search','add_to_cart','remove_from_cart','checkout_start','checkout_complete','wishlist_add')")
@dp.expect("timestamp_not_null", "event_timestamp IS NOT NULL")
@dp.expect("device_valid", "device_type IS NOT NULL AND device_type IN ('mobile','desktop','tablet')")
@dp.expect("referrer_not_bot", "referrer IS NULL OR referrer != 'bot/crawler'")
@dp.expect("url_not_null", "page_url IS NOT NULL")
@dp.expect("product_on_product_event", "event_type NOT IN ('product_view','add_to_cart','checkout_start','checkout_complete') OR product_id IS NOT NULL")
@dp.expect("user_agent_not_null", "user_agent IS NOT NULL")
def silver_clickstream():
    return _filter_rescued(_trim_strings(_stream_bronze("clickstream"))) \
        .withWatermark("_bronze_timestamp", "1 hour") \
        .dropDuplicatesWithinWatermark(["event_id"]) \
        .select(
            col("event_id"), col("session_id"), col("user_id"),
            lower(col("event_type")).alias("event_type"),
            to_timestamp(col("event_timestamp")).alias("event_timestamp"),
            col("page_url"), col("product_id").cast("string").alias("product_id"),
            col("store_id").cast("string").alias("store_id"),
            lower(col("device_type")).alias("device_type"),
            col("user_agent"),
            lower(col("referrer")).alias("referrer"),
            col("year"), col("month"), col("day"),
            current_timestamp().alias("_silver_timestamp"))


# -- 9. SOCIAL MEDIA -------------------------------------------------
@dp.table(name="silver_social_media", comment="Cleaned social mentions — sentiment validation, engagement metrics, language")
@dp.expect_or_drop("pk_not_null", "post_id IS NOT NULL")
@dp.expect("platform_valid", "platform IN ('Twitter/X','Instagram','TikTok','Snapchat','YouTube','LinkedIn')")
@dp.expect("timestamp_not_null", "post_timestamp IS NOT NULL")
@dp.expect("date_not_null", "post_date IS NOT NULL")
@dp.expect("date_not_future", "post_date <= current_date()")
@dp.expect("author_not_null", "author_id IS NOT NULL")
@dp.expect("language_valid", "language IS NOT NULL AND language IN ('ar','en','ur','hi','tl')")
@dp.expect("text_not_empty", "text IS NOT NULL AND length(trim(text)) > 0")
@dp.expect("sentiment_valid", "sentiment IN ('positive','negative','neutral')")
@dp.expect("likes_valid", "likes IS NULL OR likes >= 0")
@dp.expect("shares_valid", "shares IS NULL OR shares >= 0")
@dp.expect("comments_valid", "comments IS NULL OR comments >= 0")
@dp.expect("likes_not_bot", "likes IS NULL OR likes < 50000")
@dp.expect("store_not_null", "store_mentioned IS NOT NULL")
def silver_social_media():
    return _filter_rescued(_trim_strings(_stream_bronze("social_media"))) \
        .withWatermark("_bronze_timestamp", "1 hour") \
        .dropDuplicatesWithinWatermark(["post_id"]) \
        .select(
            col("post_id"), col("platform"),
            to_timestamp(col("post_timestamp")).alias("post_timestamp"),
            col("post_date"), col("author_id"),
            lower(col("language")).alias("language"),
            col("text"),
            lower(col("sentiment")).alias("sentiment"),
            col("store_mentioned"), col("product_mentioned"),
            col("likes"), col("shares"), col("comments"),
            col("year"), col("month"), col("day"),
            current_timestamp().alias("_silver_timestamp"))


# -- 10. PUSH NOTIFICATIONS ------------------------------------------
@dp.table(name="silver_push_notifications", comment="Cleaned push logs — delivery validation, open rate tracking, channel checks")
@dp.expect_or_drop("pk_not_null", "notification_id IS NOT NULL")
@dp.expect("customer_fk", "customer_id IS NOT NULL")
@dp.expect("customer_valid", "customer_id != 'CUST_INVALID'")
@dp.expect("type_valid", "notification_type IN ('order_update','promotion','abandoned_cart','delivery_update','price_drop','welcome','restock')")
@dp.expect("channel_valid", "channel IN ('Firebase','APNs','Web Push')")
@dp.expect("device_token_not_null", "device_token IS NOT NULL")
@dp.expect("title_not_empty", "title IS NOT NULL AND length(trim(title)) > 0")
@dp.expect("sent_at_not_null", "sent_at IS NOT NULL")
@dp.expect("delivery_status_valid", "delivery_status IN ('delivered','failed','pending','throttled')")
@dp.expect("opened_requires_delivery", "opened = false OR delivery_status = 'delivered'")
@dp.expect("opened_at_if_opened", "opened = false OR opened_at IS NOT NULL")
@dp.expect("order_on_order_notif", "notification_type NOT IN ('order_update','delivery_update') OR order_id IS NOT NULL")
def silver_push_notifications():
    return _filter_rescued(_trim_strings(_stream_bronze("push_notifications"))) \
        .withWatermark("_bronze_timestamp", "1 hour") \
        .dropDuplicatesWithinWatermark(["notification_id"]) \
        .select(
            col("notification_id"),
            col("customer_id").cast("string").alias("customer_id"),
            col("order_id").cast("string").alias("order_id"),
            lower(col("notification_type")).alias("notification_type"),
            lower(col("channel")).alias("channel"),
            col("device_token"), col("title"),
            to_timestamp(col("sent_at")).alias("sent_at"),
            lower(col("delivery_status")).alias("delivery_status"),
            col("opened"),
            to_timestamp(col("opened_at")).alias("opened_at"),
            col("year"), col("month"), col("day"),
            current_timestamp().alias("_silver_timestamp"))


# -- 11. CUSTOMER SEGMENTATION (Parquet) ------------------------------
@dp.table(name="silver_customer_segmentation", comment="Cleaned ML segmentation — RFM score validation, CLV bounds, churn probability")
@dp.expect_or_drop("pk_not_null", "customer_id IS NOT NULL")
@dp.expect("name_not_null", "customer_name IS NOT NULL")
@dp.expect("email_valid", "email IS NULL OR email RLIKE '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\\\.[a-zA-Z]{2,}$'")
@dp.expect("city_not_null", "city IS NOT NULL")
@dp.expect("region_not_null", "region IS NOT NULL")
@dp.expect("recency_score_range", "recency_score IS NOT NULL AND recency_score BETWEEN 1 AND 5")
@dp.expect("frequency_score_range", "frequency_score IS NOT NULL AND frequency_score BETWEEN 1 AND 5")
@dp.expect("monetary_score_range", "monetary_score IS NOT NULL AND monetary_score BETWEEN 1 AND 5")
@dp.expect("rfm_total_calc", "rfm_total = recency_score + frequency_score + monetary_score")
@dp.expect("segment_valid", "segment IN ('Champions','Loyal','New','Potential Loyalist','Promising','Need Attention','About to Sleep','Cant Lose','At Risk','Hibernating')")
@dp.expect("clv_positive", "clv_12_months IS NOT NULL AND clv_12_months >= 0")
@dp.expect("churn_probability_range", "churn_probability >= 0 AND churn_probability <= 1")
@dp.expect("total_spend_valid", "total_spend IS NULL OR total_spend >= 0")
@dp.expect("total_orders_valid", "total_orders IS NULL OR total_orders >= 0")
@dp.expect("model_version_not_null", "model_version IS NOT NULL")
def silver_customer_segmentation():
    return _filter_rescued(_trim_strings(_stream_bronze("customer_segmentation"))) \
        .withWatermark("_bronze_timestamp", "1 hour") \
        .dropDuplicatesWithinWatermark(["customer_id"]) \
        .select(
            col("customer_id").cast("string").alias("customer_id"),
            col("customer_name"), lower(col("email")).alias("email"),
            col("city"), col("region"),
            col("customer_since"), col("last_order_date"),
            col("recency_days"), col("total_orders"), col("total_spend"),
            col("avg_order_value"),
            col("recency_score"), col("frequency_score"), col("monetary_score"),
            col("rfm_total"), col("segment"),
            col("clv_12_months"), col("churn_probability"),
            col("predicted_annual_orders"),
            col("model_version"), col("model_algorithm"),
            col("scoring_date"),
            current_timestamp().alias("_silver_timestamp"))


# -- 12. DEMAND FORECASTS (Parquet) -----------------------------------
@dp.table(name="silver_demand_forecasts", comment="Cleaned ML forecasts — confidence interval validation, MAPE bounds, FK checks")
@dp.expect_or_drop("product_not_null", "product_id IS NOT NULL")
@dp.expect_or_drop("store_not_null", "store_id IS NOT NULL")
@dp.expect_or_drop("date_not_null", "forecast_date IS NOT NULL")
@dp.expect("demand_not_negative", "predicted_demand >= 0")
@dp.expect("lower_bound_valid", "lower_bound >= 0")
@dp.expect("upper_bound_valid", "upper_bound >= predicted_demand")
@dp.expect("bounds_consistent", "lower_bound <= predicted_demand AND predicted_demand <= upper_bound")
@dp.expect("confidence_valid", "confidence_level IS NOT NULL AND confidence_level BETWEEN 0 AND 1")
@dp.expect("std_positive", "prediction_std >= 0")
@dp.expect("historical_valid", "historical_avg_demand IS NULL OR historical_avg_demand >= 0")
@dp.expect("season_factor_range", "season_factor > 0 AND season_factor < 10")
@dp.expect("mape_valid", "mape_score IS NOT NULL AND mape_score >= 0 AND mape_score < 1")
@dp.expect("model_version_not_null", "model_version IS NOT NULL")
@dp.expect("training_date_valid", "training_date IS NOT NULL AND training_date <= current_date()")
@dp.expect("forecast_in_future", "forecast_date >= training_date")
def silver_demand_forecasts():
    return _filter_rescued(_trim_strings(_stream_bronze("demand_forecasts"))) \
        .withWatermark("_bronze_timestamp", "1 hour") \
        .dropDuplicatesWithinWatermark(["product_id", "store_id", "forecast_date"]) \
        .select(
            col("product_id").cast("string").alias("product_id"),
            col("product_name"), col("category"),
            col("store_id").cast("string").alias("store_id"),
            col("store_name"), col("store_city"), col("store_region"),
            col("forecast_date"),
            col("predicted_demand"), col("lower_bound"), col("upper_bound"),
            col("confidence_level"), col("prediction_std"),
            col("historical_avg_demand"),
            col("season_factor"), col("category_boost"),
            col("model_algorithm"), col("model_version"),
            col("training_date"), col("mape_score"),
            col("year"), col("month"), col("day"),
            current_timestamp().alias("_silver_timestamp"))
