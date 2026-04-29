# Databricks notebook source
# ============================================================================
# Source Generator: Return Requests (CSV)
# ============================================================================
# Simulates customer return/refund requests from the RMA system.
# Cross-references real order_ids from Silver for FK integrity.
#
# Output : /Volumes/salla_databricks/bronze/landing_data/return_requests/
# Format : CSV, partitioned by year/month/day
# Volume : ~15 000 rows (~3-4 % of 2024 orders)
# Noise  : ~7 % — duplicate requests, missing reasons, wrong statuses
# ============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, expr, rand, round as spark_round, when, concat, floor,
    date_format, month as spark_month, date_add, monotonically_increasing_id,
    datediff,
)

spark = SparkSession.builder.getOrCreate()

ADLS_PATH = "/Volumes/salla_databricks/bronze/landing_data/return_requests/"

# ── 1. Sample real orders from Silver (completed + refunded in 2024) ───
orders = spark.sql("""
    SELECT
        o.order_id, o.customer_id, o.product_id, o.store_id,
        o.order_date, o.order_status, o.final_amount, o.quantity
    FROM salla_databricks.silver.silver_sales_orders o
    WHERE o.order_date >= '2024-01-01' AND o.order_date < '2025-01-01'
      AND o.order_status IN ('completed', 'refunded', 'cancelled')
""")

# ~3.5% return rate (realistic for Saudi e-commerce)
returns = orders.sample(False, 0.035)
print(f"Return requests to generate: {returns.count():,}")

# ── 2. Return reasons (weighted by real-world Saudi e-commerce data) ──
returns = returns \
    .withColumn("_reason_rand", rand()) \
    .withColumn("return_reason",
        when(col("_reason_rand") < 0.25, lit("changed_mind"))         # 25% — most common
        .when(col("_reason_rand") < 0.45, lit("defective"))           # 20%
        .when(col("_reason_rand") < 0.60, lit("not_as_described"))    # 15%
        .when(col("_reason_rand") < 0.73, lit("wrong_item"))          # 13%
        .when(col("_reason_rand") < 0.85, lit("late_delivery"))       # 12%
        .when(col("_reason_rand") < 0.93, lit("size_issue"))          # 8%
        .otherwise(lit("other"))                                       # 7%
    )

# ── 3. Request date = 1-30 days after order ────────────────────────────
returns = returns \
    .withColumn("request_id", monotonically_increasing_id()) \
    .withColumn("return_request_id",
        concat(lit("RET-"), date_format(col("order_date"), "yyyyMM"),
               lit("-"), col("request_id").cast("string"))
    ) \
    .withColumn("days_after_order", (1 + floor(rand() * 29)).cast("int")) \
    .withColumn("request_date",
        date_add(col("order_date").cast("date"), col("days_after_order"))
    )

# ── 4. Status flow ────────────────────────────────────────────────────
returns = returns \
    .withColumn("_status_rand", rand()) \
    .withColumn("status",
        when(col("order_status") == "refunded", lit("refunded"))      # match order status
        .when(col("_status_rand") < 0.35, lit("completed"))
        .when(col("_status_rand") < 0.55, lit("approved"))
        .when(col("_status_rand") < 0.70, lit("refunded"))
        .when(col("_status_rand") < 0.85, lit("requested"))           # still pending
        .otherwise(lit("rejected"))
    ) \
    .withColumn("refund_amount",
        when(col("status").isin("refunded", "completed"),
             spark_round(col("final_amount") * (0.85 + rand() * 0.15), 2))  # 85-100% refund
        .when(col("status") == "approved",
             spark_round(col("final_amount") * (0.80 + rand() * 0.20), 2))
        .otherwise(lit(0.0))
    ) \
    .withColumn("resolution_date",
        when(col("status").isin("completed", "refunded", "rejected"),
             date_add(col("request_date"), (3 + floor(rand() * 12)).cast("int")))
        .otherwise(lit(None).cast("date"))
    ) \
    .withColumn("resolution_notes",
        when(col("status") == "rejected", lit("Return policy window exceeded"))
        .when(col("return_reason") == "defective", lit("Quality inspection confirmed defect"))
        .when(col("return_reason") == "wrong_item", lit("Warehouse mis-pick verified"))
        .otherwise(lit(None).cast("string"))
    )

# ── 5. Inject noise (~7 %) ────────────────────────────────────────────
noisy = returns \
    .withColumn("_noise", rand()) \
    .withColumn("return_reason",                                     # 3% missing reason
        when(col("_noise") < 0.03, lit(None).cast("string")).otherwise(col("return_reason"))
    ) \
    .withColumn("refund_amount",                                     # 2% negative refund (data bug)
        when((col("_noise") >= 0.03) & (col("_noise") < 0.05),
             spark_round(-col("refund_amount"), 2)).otherwise(col("refund_amount"))
    )

# ── 6. Add ~3 % duplicate return requests ─────────────────────────────
dupes = noisy.sample(False, 0.03)
noisy = noisy.unionAll(dupes)

# ── 7. Final select ───────────────────────────────────────────────────
final = noisy.select(
    col("return_request_id"),
    col("order_id"),
    col("customer_id"),
    col("product_id"),
    col("store_id"),
    col("request_date"),
    col("return_reason"),
    col("status"),
    col("refund_amount"),
    col("resolution_date"),
    col("resolution_notes"),
    col("quantity").alias("return_quantity"),
    date_format(col("request_date"), "yyyy").cast("int").alias("year"),
    spark_month(col("request_date")).alias("month"),
    date_format(col("request_date"), "d").cast("int").alias("day"),
)

# ── 8. Write to ADLS ──────────────────────────────────────────────────
final.write \
    .mode("overwrite") \
    .option("header", "true") \
    .partitionBy("year", "month", "day") \
    .csv(ADLS_PATH)

row_count = final.count()
print(f"✅ Return requests written to {ADLS_PATH}")
print(f"   Rows: {row_count:,}")
print(f"   Return rate: ~3.5% of 2024 orders")
print(f"   Noise: ~7% (missing reasons, negative amounts, duplicates)")
