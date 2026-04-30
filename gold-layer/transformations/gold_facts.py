# ============================================================================
# Gold Layer — Fact Tables (3 MVs)
# ============================================================================
# Per Databricks Official Docs:
#   ✓ Materialized views for pre-computed Gold layer
#   ✓ Self-contained file (no cross-file imports)
#   ✓ Parameterized silver + gold schema
#   ✓ Star schema joins with dimension surrogate keys
#   ✓ Business metrics: estimated_profit, CTR, cost_per_conversion
#   ✓ Competitive intelligence: price_diff_sar, competitive_position
# ============================================================================

from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, md5, round, when, coalesce, concat_ws, lower, trim,
    current_timestamp, to_date,
)

_spark = SparkSession.builder.getOrCreate()
SILVER = _spark.conf.get("pipeline.silver_schema", "salla_databricks.silver")
GOLD = _spark.conf.get("pipeline.gold_schema", "salla_databricks.gold")


def _read_silver(t: str):
    """Read a Silver table. Usage: _read_silver('sales_orders')"""
    return _spark.read.table(f"{SILVER}.silver_{t}")


def _read_gold(t: str):
    """Read a Gold dimension table. Usage: _read_gold('dim_customers')"""
    return _spark.read.table(f"{GOLD}.{t}")


# ══════════════════════════════════════════════════════════════════════════
# 1. FACT_SALES — Core sales fact (orders + payments + shipping + all dims)
# ══════════════════════════════════════════════════════════════════════════
@dp.materialized_view(
    name="fact_sales",
    comment="Core sales fact table joining orders, payments, and shipping with all dimensions",
)
@dp.expect("valid_fact_key", "fact_sales_key IS NOT NULL")
@dp.expect("valid_quantity", "quantity > 0")
@dp.expect("valid_final_amount", "final_amount >= 0 OR is_refund = 1")
def fact_sales():
    df_orders = _read_silver("sales_orders")
    df_payments = _read_silver("payment_transactions").select(
        "order_id", "payment_method", "payment_status",
        "gateway_fee", "settlement_status",
    )
    df_shipping = _read_silver("shipping_details").select(
        "order_id", "carrier", "shipping_status", "shipping_cost",
        "actual_delivery", "shipped_date", "delivery_days",
    )

    # ── Dimension lookups ────────────────────────────────────────────────
    df_dim_customers = _read_gold("dim_customers").select("customer_key", "customer_id")
    df_dim_products = _read_gold("dim_products").select(
        "product_key", "product_id", col("cost_price").alias("product_cost_price"),
    )
    df_dim_stores = _read_gold("dim_stores").select("store_key", "store_id")
    df_dim_date = _read_gold("dim_date").select("date_key", "full_date")
    df_dim_payment = _read_gold("dim_payment_method").select(
        "payment_method_key", col("payment_method").alias("pm_method"),
    )

    return (
        df_orders
        .join(df_payments, "order_id", "left")
        .join(df_shipping, "order_id", "left")
        .join(df_dim_customers, "customer_id", "left")
        .join(df_dim_products, "product_id", "left")
        .join(df_dim_stores, "store_id", "left")
        .join(df_dim_payment, col("payment_method") == col("pm_method"), "left")
        .join(
            df_dim_date.withColumnRenamed("full_date", "order_date_join"),
            to_date(col("order_date")) == col("order_date_join"),
            "left",
        )
        .select(
            md5(col("order_id").cast("string")).alias("fact_sales_key"),
            col("date_key"), col("customer_key"), col("product_key"),
            col("store_key"), col("payment_method_key"), col("order_id"),
            col("quantity"), col("order_status"), col("carrier"),
            col("payment_method"), col("shipping_status"),
            col("payment_status"), col("settlement_status"),
            col("delivery_days"),
            col("unit_price"), col("discount_amount"), col("total_amount"),
            col("vat_amount"), col("final_amount"),
            col("gateway_fee"), col("shipping_cost"),
            col("product_cost_price").alias("cost_price"),
            round(
                col("final_amount")
                - coalesce(col("product_cost_price"), lit(0)) * col("quantity")
                - coalesce(col("gateway_fee"), lit(0))
                - coalesce(col("shipping_cost"), lit(0)),
                2,
            ).alias("estimated_profit"),
            when(col("order_status") == "refunded", lit(1))
            .otherwise(lit(0))
            .alias("is_refund"),
            col("order_date"), col("shipped_date"), col("actual_delivery"),
            current_timestamp().alias("_gold_timestamp"),
        )
    )


# ══════════════════════════════════════════════════════════════════════════
# 2. FACT_AD_SPEND — Advertising spend with ROAS and CTR metrics
# ══════════════════════════════════════════════════════════════════════════
@dp.materialized_view(
    name="fact_ad_spend",
    comment="Advertising spend fact with ROAS and CTR metrics",
)
@dp.expect("valid_ad_key", "fact_ad_spend_key IS NOT NULL")
def fact_ad_spend():
    df_silver = _read_silver("ad_spend")
    df_dim_date = _read_gold("dim_date").select("date_key", "full_date")

    return (
        df_silver
        .join(df_dim_date, df_silver["date"] == df_dim_date["full_date"], "left")
        .select(
            md5(concat_ws("|", col("date"), col("campaign_name"), col("platform"), col("country"))).alias("fact_ad_spend_key"),
            col("date_key"),
            col("date"), col("campaign_name"), col("platform"),
            col("currency"), col("country"),
            col("campaign_type"), col("campaign_category"),
            col("impressions"), col("clicks"), col("spend_sar"),
            col("conversions"), col("revenue_sar"), col("roas"),
            col("cpc").alias("cost_per_click"),
            col("cpm").alias("cost_per_mille"),
            round(col("clicks") / col("impressions") * 100, 4).alias("ctr_pct"),
            when(col("conversions") > 0, round(col("spend_sar") / col("conversions"), 2))
            .otherwise(lit(None))
            .alias("cost_per_conversion"),
            round(col("revenue_sar") - col("spend_sar"), 2).alias("net_revenue_sar"),
            current_timestamp().alias("_gold_timestamp"),
        )
    )


# ══════════════════════════════════════════════════════════════════════════
# 3. FACT_COMPETITOR_PRICING — Competitive intelligence & price comparison
# ══════════════════════════════════════════════════════════════════════════
@dp.materialized_view(
    name="fact_competitor_pricing",
    comment="Competitor pricing intelligence with price comparison vs our catalog",
)
@dp.expect("valid_competitor_key", "fact_competitor_key IS NOT NULL")
def fact_competitor_pricing():
    df_silver = _read_silver("competitor_pricing")
    df_dim_date = _read_gold("dim_date").select("date_key", "full_date")
    df_dim_prod = _read_gold("dim_products").select(
        "product_key",
        col("product_name").alias("our_product_name"),
        col("unit_price").alias("our_unit_price"),
    )

    return (
        df_silver
        .join(df_dim_date, df_silver["scrape_date"] == df_dim_date["full_date"], "left")
        .join(
            df_dim_prod,
            lower(trim(df_silver["product_name"])) == lower(trim(col("our_product_name"))),
            "left",
        )
        .select(
            md5(concat_ws("|", col("scrape_id").cast("string"), col("competitor_id"))).alias("fact_competitor_key"),
            col("date_key"), col("product_key"),
            col("scrape_id"), col("scrape_date"), col("scrape_timestamp"),
            col("competitor_id"), col("competitor_name"), col("competitor_country"),
            df_silver["product_name"].alias("product_name"),
            col("product_category"), col("product_subcategory"),
            col("currency"), col("in_stock"),
            col("current_price"), col("original_price"),
            col("discount_percentage"), col("rating"), col("review_count"),
            col("our_unit_price"),
            round(col("current_price") - col("our_unit_price"), 2).alias("price_diff_sar"),
            round(
                ((col("current_price") - col("our_unit_price")) / col("our_unit_price")) * 100, 2
            ).alias("price_diff_pct"),
            when(col("current_price") < col("our_unit_price"), lit("We are more expensive"))
            .when(col("current_price") > col("our_unit_price"), lit("We are cheaper"))
            .otherwise(lit("Same price"))
            .alias("competitive_position"),
            current_timestamp().alias("_gold_timestamp"),
        )
    )
