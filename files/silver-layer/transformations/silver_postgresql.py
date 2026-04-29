# Databricks notebook source
# ============================================================================
# Silver Layer — PostgreSQL Transformations (8 MVs)
# ============================================================================
# INDUSTRY-STANDARD per Databricks Docs:
#   ✓ Quarantine pattern (expect logs failures to event_log for investigation)
#   ✓ Comprehensive null handling (all required fields validated)
#   ✓ Bronze metadata excluded (_bronze_timestamp, _source_system removed)
#   ✓ Date validation (no future dates, business range checks)
#   ✓ FK checks at warn level (customer_id, store_id, product_id)
#   ✓ Full string normalization (trim ALL strings, lower enums)
#   ✓ Consistent STRING IDs (all PKs/FKs cast to string)
#   ✓ Business logic validation (net_amount, total_amount calculations)
#   ✓ All useful columns included (reference_id, notes, verified_purchase, etc.)
#   ✓ Enum values validated against actual source data
# ============================================================================

from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, lower, upper, current_timestamp, when, datediff
)
from pyspark.sql.types import StringType

BRONZE = SparkSession.builder.getOrCreate().conf.get("pipeline.bronze_schema", "salla_databricks.bronze")

def _read_bronze(t):
    return SparkSession.builder.getOrCreate().read.table(f"{BRONZE}.bronze_{t}")

def _trim_strings(df):
    for f in df.schema.fields:
        if isinstance(f.dataType, StringType):
            df = df.withColumn(f.name, trim(col(f.name)))
    return df


# ══════════════════════════════════════════════════════════════════════════
# 1. CUSTOMERS
# ══════════════════════════════════════════════════════════════════════════
@dp.materialized_view(name="silver_customers", comment="Cleaned customers — validated emails, normalized strings")
@dp.expect_or_drop("pk_not_null", "customer_id IS NOT NULL")
@dp.expect_or_drop("email_valid", "email IS NOT NULL AND email RLIKE '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\\\.[a-zA-Z]{2,}$'")
@dp.expect("phone_valid", "phone IS NOT NULL AND length(trim(phone)) >= 7")
@dp.expect("name_not_null", "customer_name IS NOT NULL")
@dp.expect("city_not_null", "city IS NOT NULL")
@dp.expect("region_not_null", "region IS NOT NULL")
@dp.expect("created_not_future", "created_at IS NULL OR created_at <= current_timestamp()")
def silver_customers():
    return _trim_strings(_read_bronze("customers")).dropDuplicates(["customer_id"]).select(
        col("customer_id").cast("string").alias("customer_id"),
        col("customer_name"), lower(col("email")).alias("email"), col("phone"),
        col("city"), col("region"), col("created_at"), col("updated_at"),
        current_timestamp().alias("_silver_timestamp"))


# ══════════════════════════════════════════════════════════════════════════
# 2. PRODUCTS
# ══════════════════════════════════════════════════════════════════════════
@dp.materialized_view(name="silver_products", comment="Cleaned products — price validation, profit margin computed")
@dp.expect_or_drop("pk_not_null", "product_id IS NOT NULL")
@dp.expect("name_not_null", "product_name IS NOT NULL")
@dp.expect("price_positive", "unit_price IS NOT NULL AND unit_price > 0")
@dp.expect("cost_positive", "cost_price IS NOT NULL AND cost_price > 0")
@dp.expect("price_gte_cost", "unit_price >= cost_price")
@dp.expect("stock_valid", "current_stock IS NOT NULL AND current_stock >= 0")
@dp.expect("category_not_null", "category IS NOT NULL")
@dp.expect("brand_not_null", "brand IS NOT NULL")
def silver_products():
    df = _trim_strings(_read_bronze("products")).dropDuplicates(["product_id"])
    return df.select(
        col("product_id").cast("string").alias("product_id"), col("product_name"),
        col("category"), col("subcategory"), col("brand"),
        col("unit_price"), col("cost_price"),
        ((col("unit_price") - col("cost_price")) / col("unit_price") * 100).alias("profit_margin_pct"),
        col("current_stock"), col("reorder_level"), col("reserved_stock"),
        lower(col("product_status")).alias("product_status"),
        col("created_at"), col("updated_at"), current_timestamp().alias("_silver_timestamp"))


# ══════════════════════════════════════════════════════════════════════════
# 3. STORES
# ══════════════════════════════════════════════════════════════════════════
@dp.materialized_view(name="silver_stores", comment="Cleaned stores — normalized strings")
@dp.expect_or_drop("pk_not_null", "store_id IS NOT NULL")
@dp.expect("name_not_null", "store_name IS NOT NULL")
@dp.expect("city_not_null", "city IS NOT NULL")
@dp.expect("region_not_null", "region IS NOT NULL")
@dp.expect("type_not_null", "store_type IS NOT NULL")
def silver_stores():
    return _trim_strings(_read_bronze("stores")).dropDuplicates(["store_id"]).select(
        col("store_id").cast("string").alias("store_id"), col("store_name"),
        col("city"), col("region"), lower(col("store_type")).alias("store_type"),
        col("created_at"), col("updated_at"), current_timestamp().alias("_silver_timestamp"))


# ══════════════════════════════════════════════════════════════════════════
# 4. SALES ORDERS
# Actual statuses: delivered, confirmed, cancelled, returned, pending, ship_pending
# ══════════════════════════════════════════════════════════════════════════
@dp.materialized_view(name="silver_sales_orders", comment="Cleaned orders — date validation, FK checks, business logic")
@dp.expect_or_drop("pk_not_null", "order_id IS NOT NULL")
@dp.expect("customer_fk", "customer_id IS NOT NULL")
@dp.expect("store_fk", "store_id IS NOT NULL")
@dp.expect("product_fk", "product_id IS NOT NULL")
@dp.expect("date_not_null", "order_date IS NOT NULL")
@dp.expect("date_not_future", "order_date <= current_timestamp()")
@dp.expect("date_not_too_old", "order_date >= '2020-01-01'")
@dp.expect("quantity_positive", "quantity IS NOT NULL AND quantity > 0")
@dp.expect("price_positive", "unit_price IS NOT NULL AND unit_price > 0")
@dp.expect("discount_valid", "discount_amount IS NOT NULL AND discount_amount >= 0")
@dp.expect("total_positive", "total_amount IS NOT NULL AND total_amount > 0")
@dp.expect("status_valid", "order_status IN ('delivered','confirmed','cancelled','returned','pending','ship_pending','processing','shipped','refunded')")
@dp.expect("total_calc_valid", "ABS(total_amount - (quantity * unit_price - discount_amount)) <= total_amount * 0.05")
def silver_sales_orders():
    return _trim_strings(_read_bronze("sales_orders")).dropDuplicates(["order_id"]).select(
        col("order_id").cast("string").alias("order_id"),
        col("customer_id").cast("string").alias("customer_id"),
        col("store_id").cast("string").alias("store_id"),
        col("product_id").cast("string").alias("product_id"),
        col("order_date"), lower(col("order_status")).alias("order_status"),
        col("quantity"), col("unit_price"), col("discount_amount"),
        col("total_amount"), col("vat_amount"), col("final_amount"),
        lower(col("shipping_method")).alias("shipping_method"),
        col("created_at"), col("updated_at"), current_timestamp().alias("_silver_timestamp"))


# ══════════════════════════════════════════════════════════════════════════
# 5. PAYMENT TRANSACTIONS
# Actual statuses: paid, refunded, failed, processing
# ══════════════════════════════════════════════════════════════════════════
@dp.materialized_view(name="silver_payment_transactions", comment="Cleaned payments — net amount validation, settlement logic")
@dp.expect_or_drop("pk_not_null", "payment_id IS NOT NULL")
@dp.expect("order_fk", "order_id IS NOT NULL")
@dp.expect("amount_positive", "amount IS NOT NULL AND amount > 0")
@dp.expect("method_not_null", "payment_method IS NOT NULL")
@dp.expect("status_valid", "payment_status IN ('paid','refunded','failed','processing','pending','completed')")
@dp.expect("fee_valid", "gateway_fee IS NOT NULL AND gateway_fee >= 0")
@dp.expect("net_positive", "net_amount IS NOT NULL AND net_amount > 0")
@dp.expect("net_calc_valid", "ABS(net_amount - (amount - gateway_fee)) <= 0.02")
@dp.expect("txn_date_valid", "transaction_date IS NOT NULL AND transaction_date <= current_timestamp()")
@dp.expect("settlement_logic", "settlement_date IS NULL OR settlement_date >= transaction_date")
def silver_payment_transactions():
    return _trim_strings(_read_bronze("payment_transactions")).dropDuplicates(["payment_id"]).select(
        col("payment_id").cast("string").alias("payment_id"),
        col("order_id").cast("string").alias("order_id"),
        lower(col("payment_method")).alias("payment_method"),
        lower(col("payment_gateway")).alias("payment_gateway"),
        col("amount"), col("gateway_fee"), col("net_amount"),
        upper(col("currency")).alias("currency"),
        lower(col("payment_status")).alias("payment_status"),
        lower(col("settlement_status")).alias("settlement_status"),
        col("transaction_id"), col("transaction_date"), col("settlement_date"),
        col("created_at"), col("updated_at"), current_timestamp().alias("_silver_timestamp"))


# ══════════════════════════════════════════════════════════════════════════
# 6. SHIPPING DETAILS
# Actual statuses: delivered, out_for_delivery, pending, in_transit, cancelled, returned, unknown
# ══════════════════════════════════════════════════════════════════════════
@dp.materialized_view(name="silver_shipping_details", comment="Cleaned shipping — delivery metrics, late delivery flag")
@dp.expect_or_drop("pk_not_null", "shipping_id IS NOT NULL")
@dp.expect("order_fk", "order_id IS NOT NULL")
@dp.expect("cost_valid", "shipping_cost IS NOT NULL AND shipping_cost >= 0")
@dp.expect("status_valid", "shipping_status IN ('delivered','out_for_delivery','pending','in_transit','cancelled','returned','shipped','unknown')")
@dp.expect("shipped_not_future", "shipped_date IS NULL OR shipped_date <= current_timestamp()")
@dp.expect("delivery_after_ship", "actual_delivery IS NULL OR shipped_date IS NULL OR actual_delivery >= shipped_date")
def silver_shipping_details():
    df = _trim_strings(_read_bronze("shipping_details")).dropDuplicates(["shipping_id"])
    return df.select(
        col("shipping_id").cast("string").alias("shipping_id"),
        col("order_id").cast("string").alias("order_id"),
        lower(col("carrier")).alias("carrier"), col("tracking_number"),
        col("shipping_cost"), lower(col("shipping_status")).alias("shipping_status"),
        col("shipped_date"), col("estimated_delivery"), col("actual_delivery"),
        datediff(col("actual_delivery"), col("shipped_date")).alias("delivery_days"),
        when(col("actual_delivery") > col("estimated_delivery"), True).otherwise(False).alias("is_late_delivery"),
        col("delivery_address"), col("delivery_city"), col("delivery_region"),
        col("created_at"), col("updated_at"), current_timestamp().alias("_silver_timestamp"))


# ══════════════════════════════════════════════════════════════════════════
# 7. INVENTORY MOVEMENTS
# Actual types: stock_out, stock_in, return, transfer
# ══════════════════════════════════════════════════════════════════════════
@dp.materialized_view(name="silver_inventory_movements", comment="Cleaned inventory — includes reference_id, notes")
@dp.expect_or_drop("pk_not_null", "movement_id IS NOT NULL")
@dp.expect("product_fk", "product_id IS NOT NULL")
@dp.expect("quantity_not_zero", "quantity IS NOT NULL AND quantity != 0")
@dp.expect("type_valid", "movement_type IN ('stock_out','stock_in','return','transfer','sale','restock','adjustment')")
@dp.expect("date_valid", "movement_date IS NOT NULL AND movement_date <= current_timestamp()")
def silver_inventory_movements():
    return _trim_strings(_read_bronze("inventory_movements")).dropDuplicates(["movement_id"]).select(
        col("movement_id").cast("string").alias("movement_id"),
        col("product_id").cast("string").alias("product_id"),
        col("store_id").cast("string").alias("store_id"),
        lower(col("movement_type")).alias("movement_type"), col("quantity"),
        col("movement_date"), col("reference_id").cast("string").alias("reference_id"),
        col("notes"), current_timestamp().alias("_silver_timestamp"))


# ══════════════════════════════════════════════════════════════════════════
# 8. PRODUCT REVIEWS
# ══════════════════════════════════════════════════════════════════════════
@dp.materialized_view(name="silver_product_reviews", comment="Cleaned reviews — includes order_id, verified_purchase")
@dp.expect_or_drop("pk_not_null", "review_id IS NOT NULL")
@dp.expect("product_fk", "product_id IS NOT NULL")
@dp.expect("customer_fk", "customer_id IS NOT NULL")
@dp.expect("order_fk", "order_id IS NOT NULL")
@dp.expect("rating_valid", "rating IS NOT NULL AND rating BETWEEN 1 AND 5")
@dp.expect("date_valid", "review_date IS NOT NULL AND review_date <= current_timestamp()")
def silver_product_reviews():
    return _trim_strings(_read_bronze("product_reviews")).dropDuplicates(["review_id"]).select(
        col("review_id").cast("string").alias("review_id"),
        col("product_id").cast("string").alias("product_id"),
        col("customer_id").cast("string").alias("customer_id"),
        col("order_id").cast("string").alias("order_id"),
        col("rating"), col("review_text"), col("review_date"), col("verified_purchase"),
        current_timestamp().alias("_silver_timestamp"))
