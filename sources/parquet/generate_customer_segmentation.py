# Databricks notebook source
# ============================================================================
# Source Generator: Customer Segmentation (Parquet)
# ============================================================================
# Simulates weekly ML model output — RFM segmentation + CLV + churn scores
# for every customer. Mimics a data science team's production model export.
#
# Output : /Volumes/salla_databricks/bronze/landing_data/customer_segmentation/
# Format : Parquet (no partitioning — small dataset, full snapshot each run)
# Volume : ~9 670 rows (one per customer)
# ============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, expr, rand, round as spark_round, when, floor,
    current_timestamp, date_format, datediff, max as spark_max,
    count, sum as spark_sum, avg,
)

spark = SparkSession.builder.getOrCreate()

ADLS_PATH = "/Volumes/salla_databricks/bronze/landing_data/customer_segmentation/"

# ── 1. Compute actual RFM from Silver data ────────────────────────────
# This makes the segmentation realistic — based on REAL transaction history
rfm_raw = spark.sql("""
    SELECT
        c.customer_id,
        c.customer_name,
        c.email,
        c.city,
        c.region,
        c.created_at AS customer_since,
        MAX(o.order_date) AS last_order_date,
        COUNT(DISTINCT o.order_id) AS total_orders,
        ROUND(SUM(o.final_amount), 2) AS total_spend,
        ROUND(AVG(o.final_amount), 2) AS avg_order_value,
        DATEDIFF('2025-01-01', MAX(o.order_date)) AS recency_days
    FROM salla_databricks.silver.silver_customers c
    LEFT JOIN salla_databricks.silver.silver_sales_orders o
        ON c.customer_id = o.customer_id
    GROUP BY c.customer_id, c.customer_name, c.email, c.city, c.region, c.created_at
""")

print(f"Customers loaded: {rfm_raw.count():,}")

# ── 2. Calculate RFM scores (1-5 quintiles) ───────────────────────────
# Using approximate percentile boundaries
rfm_scored = rfm_raw \
    .withColumn("recency_score",
        when(col("recency_days").isNull(), lit(1))
        .when(col("recency_days") <= 30, lit(5))
        .when(col("recency_days") <= 90, lit(4))
        .when(col("recency_days") <= 180, lit(3))
        .when(col("recency_days") <= 365, lit(2))
        .otherwise(lit(1))
    ) \
    .withColumn("frequency_score",
        when(col("total_orders").isNull() | (col("total_orders") == 0), lit(1))
        .when(col("total_orders") >= 200, lit(5))
        .when(col("total_orders") >= 100, lit(4))
        .when(col("total_orders") >= 50, lit(3))
        .when(col("total_orders") >= 10, lit(2))
        .otherwise(lit(1))
    ) \
    .withColumn("monetary_score",
        when(col("total_spend").isNull() | (col("total_spend") == 0), lit(1))
        .when(col("total_spend") >= 50000, lit(5))
        .when(col("total_spend") >= 20000, lit(4))
        .when(col("total_spend") >= 5000, lit(3))
        .when(col("total_spend") >= 1000, lit(2))
        .otherwise(lit(1))
    ) \
    .withColumn("rfm_total", col("recency_score") + col("frequency_score") + col("monetary_score"))

# ── 3. Assign segments based on RFM combination ──────────────────────
segmented = rfm_scored \
    .withColumn("segment",
        when((col("recency_score") >= 4) & (col("frequency_score") >= 4) & (col("monetary_score") >= 4),
             lit("Champions"))
        .when((col("recency_score") >= 3) & (col("frequency_score") >= 3) & (col("monetary_score") >= 3),
             lit("Loyal"))
        .when((col("recency_score") >= 4) & (col("frequency_score") <= 2),
             lit("New"))
        .when((col("recency_score") >= 3) & (col("frequency_score") >= 2) & (col("monetary_score") >= 2),
             lit("Potential Loyalist"))
        .when((col("recency_score") >= 3) & (col("frequency_score") <= 2) & (col("monetary_score") >= 2),
             lit("Promising"))
        .when((col("recency_score") == 2) & (col("frequency_score") >= 3),
             lit("Need Attention"))
        .when((col("recency_score") == 2) & (col("frequency_score") <= 2),
             lit("About to Sleep"))
        .when((col("recency_score") == 1) & (col("frequency_score") >= 4),
             lit("Cant Lose"))
        .when((col("recency_score") == 1) & (col("frequency_score") >= 2),
             lit("At Risk"))
        .otherwise(lit("Hibernating"))
    )

# ── 4. CLV estimate (simplified: avg_order_value × predicted_orders/year) ─
segmented = segmented \
    .withColumn("predicted_annual_orders",
        when(col("segment") == "Champions", col("frequency_score") * 50 + rand() * 30)
        .when(col("segment") == "Loyal", col("frequency_score") * 30 + rand() * 20)
        .when(col("segment").isin("New", "Promising"), 5 + rand() * 15)
        .when(col("segment").isin("At Risk", "Cant Lose"), col("frequency_score") * 10 + rand() * 10)
        .otherwise(1 + rand() * 5)
    ) \
    .withColumn("clv_12_months",
        spark_round(col("avg_order_value") * col("predicted_annual_orders"), 2)
    ) \
    .withColumn("clv_12_months",
        when(col("clv_12_months").isNull(), spark_round(50 + rand() * 200, 2))
        .otherwise(col("clv_12_months"))
    )

# ── 5. Churn probability ──────────────────────────────────────────────
segmented = segmented \
    .withColumn("churn_probability",
        spark_round(
            when(col("segment") == "Champions", 0.02 + rand() * 0.05)
            .when(col("segment") == "Loyal", 0.05 + rand() * 0.10)
            .when(col("segment") == "Potential Loyalist", 0.10 + rand() * 0.15)
            .when(col("segment").isin("New", "Promising"), 0.20 + rand() * 0.20)
            .when(col("segment") == "Need Attention", 0.35 + rand() * 0.20)
            .when(col("segment") == "About to Sleep", 0.50 + rand() * 0.20)
            .when(col("segment") == "At Risk", 0.60 + rand() * 0.20)
            .when(col("segment") == "Cant Lose", 0.40 + rand() * 0.25)
            .otherwise(0.75 + rand() * 0.20),  # Hibernating
            4
        )
    ) \
    .withColumn("model_version", lit("rfm_v2.1_20250101")) \
    .withColumn("model_algorithm", lit("RFM_Quintile + Logistic_Regression")) \
    .withColumn("scoring_date", lit("2025-01-01").cast("date")) \
    .withColumn("_scored_at", current_timestamp())

# ── 6. Final select ───────────────────────────────────────────────────
final = segmented.select(
    col("customer_id"), col("customer_name"), col("email"),
    col("city"), col("region"), col("customer_since"),
    col("last_order_date"), col("recency_days"),
    col("total_orders"), col("total_spend"), col("avg_order_value"),
    col("recency_score"), col("frequency_score"), col("monetary_score"),
    col("rfm_total"), col("segment"),
    col("clv_12_months"), col("churn_probability"),
    col("predicted_annual_orders").cast("int"),
    col("model_version"), col("model_algorithm"), col("scoring_date"),
    col("_scored_at"),
)

# ── 7. Write as Parquet to ADLS ───────────────────────────────────────
final.write \
    .mode("overwrite") \
    .parquet(ADLS_PATH)

row_count = final.count()
print(f"✅ Customer segmentation written to {ADLS_PATH}")
print(f"   Rows: {row_count:,}")
print(f"   Segments: Champions, Loyal, Potential Loyalist, New, Promising, etc.")
print(f"   Features: RFM scores, CLV, churn probability")
print(f"   Model: rfm_v2.1 (RFM + Logistic Regression)")

# Show segment distribution
final.groupBy("segment").count().orderBy(col("count").desc()).show()
