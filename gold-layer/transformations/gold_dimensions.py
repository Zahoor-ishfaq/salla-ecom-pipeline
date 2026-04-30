# Databricks notebook source
# ============================================================================
# Gold Layer — Dimension Tables (5 MVs)
# ============================================================================
# Per Databricks Official Docs:
#   ✓ Materialized views for pre-computed Gold layer (best practice)
#   ✓ Self-contained file (no cross-file imports — serverless limitation)
#   ✓ Parameterized silver schema via spark.conf.get()
#   ✓ Surrogate keys via md5() for star schema joins
#   ✓ Business enrichment (price_tier, region_zone, stock_status, is_bnpl)
#   ✓ Dimensional modeling aligned with business logic
# ============================================================================

from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, md5, round, when, current_timestamp,
)

_spark = SparkSession.builder.getOrCreate()
SILVER = _spark.conf.get("pipeline.silver_schema", "salla_databricks.silver")


def _read_silver(t: str):
    """Read a Silver table. Usage: _read_silver('customers') → {SILVER}.silver_customers"""
    return _spark.read.table(f"{SILVER}.silver_{t}")


# ══════════════════════════════════════════════════════════════════════════
# 1. DIM_DATE — Generated date dimension with fiscal year & weekend flags
# ══════════════════════════════════════════════════════════════════════════
@dp.materialized_view(
    name="dim_date",
    comment="Date dimension with fiscal year, Saudi weekend flags, and BI slicing attributes",
)
@dp.expect("valid_date_key", "date_key IS NOT NULL")
@dp.expect("valid_full_date", "full_date IS NOT NULL")
def dim_date():
    return _spark.sql(f"""
        WITH date_range AS (
            SELECT
                date_add(MIN(to_date(order_date)), -30)  AS min_d,
                date_add(MAX(to_date(order_date)), 730)  AS max_d
            FROM {SILVER}.silver_sales_orders
        ),
        dates AS (
            SELECT explode(sequence(min_d, max_d, interval 1 day)) AS full_date
            FROM date_range
        )
        SELECT
            CAST(date_format(full_date, 'yyyyMMdd') AS INT) AS date_key,
            full_date,
            year(full_date)                           AS year,
            quarter(full_date)                        AS quarter,
            month(full_date)                          AS month,
            dayofmonth(full_date)                     AS day,
            dayofweek(full_date)                      AS day_of_week,
            dayofyear(full_date)                      AS day_of_year,
            weekofyear(full_date)                     AS week_of_year,
            date_format(full_date, 'MMMM')            AS month_name,
            date_format(full_date, 'MMM')             AS month_short,
            date_format(full_date, 'EEEE')            AS day_name,
            date_format(full_date, 'EEE')             AS day_short,
            concat('Q', quarter(full_date), '-', year(full_date)) AS quarter_label,
            date_format(full_date, 'yyyy-MM')         AS year_month,
            year(full_date)                           AS fiscal_year,
            CASE WHEN dayofweek(full_date) IN (6, 7) THEN 1 ELSE 0 END AS is_weekend,
            CASE WHEN dayofweek(full_date) IN (6, 7) THEN 0 ELSE 1 END AS is_weekday,
            current_timestamp()                       AS _gold_timestamp
        FROM dates
    """)


# ══════════════════════════════════════════════════════════════════════════
# 2. DIM_CUSTOMERS — Customer dimension with Saudi region zones
# ══════════════════════════════════════════════════════════════════════════
@dp.materialized_view(
    name="dim_customers",
    comment="Customer dimension with region zones for BI grouping",
)
@dp.expect("valid_customer_key", "customer_key IS NOT NULL")
@dp.expect("valid_customer_id", "customer_id IS NOT NULL")
def dim_customers():
    return (
        _read_silver("customers")
        .select(
            md5(col("customer_id")).alias("customer_key"),
            col("customer_id"),
            col("customer_name"),
            col("email"),
            col("phone"),
            col("city"),
            col("region"),
            when(col("region") == "Riyadh Region", lit("Central"))
            .when(col("region").isin("Makkah Region", "Madinah Region"), lit("Western"))
            .when(col("region") == "Eastern Region", lit("Eastern"))
            .when(
                col("region").isin("Asir Region", "Jizan Region", "Najran Region"),
                lit("Southern"),
            )
            .otherwise(lit("Northern"))
            .alias("region_zone"),
            col("created_at").alias("customer_since"),
            col("updated_at").alias("last_updated"),
            current_timestamp().alias("_gold_timestamp"),
        )
        .dropDuplicates(["customer_id"])
    )


# ══════════════════════════════════════════════════════════════════════════
# 3. DIM_PRODUCTS — Product dimension with pricing tiers & stock status
# ══════════════════════════════════════════════════════════════════════════
@dp.materialized_view(
    name="dim_products",
    comment="Product dimension with pricing tiers, margins, and stock status",
)
@dp.expect("valid_product_key", "product_key IS NOT NULL")
@dp.expect("valid_unit_price", "unit_price >= 0")
@dp.expect("valid_cost_price", "cost_price >= 0")
def dim_products():
    return (
        _read_silver("products")
        .select(
            md5(col("product_id")).alias("product_key"),
            col("product_id"),
            col("product_name"),
            col("category"),
            col("subcategory"),
            col("brand"),
            col("product_status"),
            col("unit_price"),
            col("cost_price"),
            round(
                ((col("unit_price") - col("cost_price")) / col("unit_price")) * 100, 2
            ).alias("margin_pct"),
            when(col("unit_price") < 100, lit("Budget"))
            .when(col("unit_price") < 500, lit("Mid-Range"))
            .when(col("unit_price") < 2000, lit("Premium"))
            .otherwise(lit("Luxury"))
            .alias("price_tier"),
            col("current_stock"),
            col("reserved_stock"),
            col("reorder_level"),
            when(col("current_stock") <= 0, lit("Out of Stock"))
            .when(col("current_stock") <= col("reorder_level"), lit("Low Stock"))
            .otherwise(lit("In Stock"))
            .alias("stock_status"),
            col("created_at").alias("listed_at"),
            col("updated_at").alias("last_updated"),
            current_timestamp().alias("_gold_timestamp"),
        )
        .dropDuplicates(["product_id"])
    )


# ══════════════════════════════════════════════════════════════════════════
# 4. DIM_STORES — Store dimension with region zones
# ══════════════════════════════════════════════════════════════════════════
@dp.materialized_view(
    name="dim_stores",
    comment="Store dimension with region zones for BI grouping",
)
@dp.expect("valid_store_key", "store_key IS NOT NULL")
def dim_stores():
    return (
        _read_silver("stores")
        .select(
            md5(col("store_id")).alias("store_key"),
            col("store_id"),
            col("store_name"),
            col("city"),
            col("region"),
            col("store_type"),
            when(col("region") == "Riyadh Region", lit("Central"))
            .when(col("region").isin("Makkah Region", "Madinah Region"), lit("Western"))
            .when(col("region") == "Eastern Region", lit("Eastern"))
            .otherwise(lit("Other"))
            .alias("region_zone"),
            col("created_at").alias("opened_at"),
            col("updated_at").alias("last_updated"),
            current_timestamp().alias("_gold_timestamp"),
        )
        .dropDuplicates(["store_id"])
    )


# ══════════════════════════════════════════════════════════════════════════
# 5. DIM_PAYMENT_METHOD — Payment method dimension with BNPL flags
# ══════════════════════════════════════════════════════════════════════════
@dp.materialized_view(
    name="dim_payment_method",
    comment="Payment method dimension with BNPL flags for Saudi market",
)
@dp.expect("valid_payment_key", "payment_method_key IS NOT NULL")
def dim_payment_method():
    return (
        _read_silver("payment_transactions")
        .select("payment_method", "payment_gateway")
        .dropDuplicates(["payment_method"])
        .select(
            md5(col("payment_method")).alias("payment_method_key"),
            col("payment_method"),
            col("payment_gateway"),
            when(col("payment_method").isin("visa", "mastercard", "mada"), lit("Card"))
            .when(col("payment_method").isin("tamara", "tabby"), lit("BNPL"))
            .when(col("payment_method") == "stc pay", lit("Digital Wallet"))
            .when(col("payment_method") == "cash on delivery", lit("Cash"))
            .otherwise(lit("Other"))
            .alias("payment_type"),
            when(col("payment_method") == "cash on delivery", lit(0))
            .otherwise(lit(1))
            .alias("is_digital"),
            when(col("payment_method").isin("tamara", "tabby"), lit(1))
            .otherwise(lit(0))
            .alias("is_bnpl"),
            current_timestamp().alias("_gold_timestamp"),
        )
    )
