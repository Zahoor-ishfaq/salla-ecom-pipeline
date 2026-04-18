# Databricks notebook source
# ============================================================================
# Source Generator: Payment Settlements (CSV)
# ============================================================================
# Simulates bank/gateway reconciliation files from Saudi payment processors.
# Gateways : HyperPay, Moyasar, Tap Payments, PayTabs
#
# Output : /Volumes/salla_databricks/bronze/landing_data/payment_settlements/
# Format : CSV, partitioned by year/month/day
# Volume : ~100 000 rows (subset of 1.4M orders)
# Noise  : ~8 % — duplicates, rounding mismatches, null settlement dates
# ============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, expr, rand, round as spark_round, when, date_add, date_format,
    month as spark_month, floor, concat, datediff,
)

spark = SparkSession.builder.getOrCreate()

ADLS_PATH = "/Volumes/salla_databricks/bronze/landing_data/payment_settlements/"

# ── 1. Read real orders + payments from Silver ─────────────────────────
# Sample ~100K orders from 2024 for settlement generation
orders = spark.sql("""
    SELECT
        o.order_id,
        o.order_date,
        o.final_amount,
        o.order_status,
        p.payment_method,
        p.gateway_fee,
        p.payment_status
    FROM salla_databricks.silver.silver_sales_orders o
    JOIN salla_databricks.silver.silver_payment_transactions p
        ON o.order_id = p.order_id
    WHERE o.order_date >= '2024-01-01' AND o.order_date < '2025-01-01'
""").sample(False, 0.25)  # ~25% sample ≈ 100K rows

print(f"Orders sampled: {orders.count():,}")

# ── 2. Map payment methods → gateways (realistic Saudi routing) ───────
#   mada, visa, mastercard → HyperPay or Moyasar (card processors)
#   tamara, tabby           → Tap Payments (BNPL aggregator)
#   stc pay                 → PayTabs (digital wallet)
#   cash on delivery        → PayTabs (COD reconciliation)
settlements = orders \
    .withColumn("gateway",
        when(col("payment_method").isin("visa", "mastercard", "mada"),
             when(rand() < 0.6, lit("HyperPay")).otherwise(lit("Moyasar")))
        .when(col("payment_method").isin("tamara", "tabby"), lit("Tap Payments"))
        .otherwise(lit("PayTabs"))
    ) \
    .withColumn("gateway_transaction_id",
        concat(
            when(col("gateway") == "HyperPay", lit("HP-"))
            .when(col("gateway") == "Moyasar", lit("MY-"))
            .when(col("gateway") == "Tap Payments", lit("TAP-"))
            .otherwise(lit("PT-")),
            col("order_id").cast("string"),
            lit("-"),
            floor(rand() * 9000 + 1000).cast("string")
        )
    ) \
    .withColumn("merchant_order_id", col("order_id"))

# ── 3. Transaction date = order_date, settlement date = +1 to 5 days ──
settlements = settlements \
    .withColumn("transaction_date", col("order_date").cast("date")) \
    .withColumn("settlement_delay_days", (1 + floor(rand() * 5)).cast("int")) \
    .withColumn("settlement_date",
        date_add(col("transaction_date"), col("settlement_delay_days"))
    )

# ── 4. Settlement amounts with realistic rounding differences ─────────
settlements = settlements \
    .withColumn("amount", spark_round(col("final_amount"), 2)) \
    .withColumn("gateway_fee_calc",
        spark_round(col("amount") * (0.015 + rand() * 0.015), 2)  # 1.5-3% gateway fee
    ) \
    .withColumn("net_amount",
        spark_round(col("amount") - col("gateway_fee_calc"), 2)
    ) \
    .withColumn("currency", lit("SAR"))

# ── 5. Settlement status (realistic distribution) ─────────────────────
settlements = settlements \
    .withColumn("_status_rand", rand()) \
    .withColumn("status",
        when(col("order_status") == "cancelled", lit("failed"))
        .when(col("order_status") == "refunded",
              when(col("_status_rand") < 0.7, lit("chargeback")).otherwise(lit("disputed")))
        .when(col("_status_rand") < 0.85, lit("settled"))
        .when(col("_status_rand") < 0.92, lit("pending"))
        .when(col("_status_rand") < 0.97, lit("failed"))
        .when(col("_status_rand") < 0.99, lit("disputed"))
        .otherwise(lit("chargeback"))
    ) \
    .withColumn("country", lit("SA"))

# ── 6. Inject noise (~8 %) ────────────────────────────────────────────
noisy = settlements \
    .withColumn("_noise", rand()) \
    .withColumn("settlement_date",                                   # 3% null settlement_date (pending)
        when(col("_noise") < 0.03, lit(None).cast("date")).otherwise(col("settlement_date"))
    ) \
    .withColumn("amount",                                            # 2% rounding mismatch (off by 0.01–0.05)
        when((col("_noise") >= 0.03) & (col("_noise") < 0.05),
             spark_round(col("amount") + (rand() * 0.05 - 0.025), 2))
        .otherwise(col("amount"))
    ) \
    .withColumn("gateway_fee_calc",                                  # 1% null gateway fee
        when((col("_noise") >= 0.05) & (col("_noise") < 0.06),
             lit(None).cast("double")).otherwise(col("gateway_fee_calc"))
    )

# ── 7. Add ~3 % duplicate settlements ─────────────────────────────────
dupes = noisy.sample(False, 0.03)
noisy = noisy.unionAll(dupes)

# ── 8. Final select ───────────────────────────────────────────────────
final = noisy.select(
    col("gateway_transaction_id"),
    col("merchant_order_id"),
    col("payment_method"),
    col("transaction_date"),
    col("amount"),
    col("currency"),
    col("gateway_fee_calc").alias("gateway_fee"),
    col("net_amount"),
    col("status"),
    col("settlement_date"),
    col("gateway"),
    col("country"),
    date_format(col("transaction_date"), "yyyy").cast("int").alias("year"),
    spark_month(col("transaction_date")).alias("month"),
    date_format(col("transaction_date"), "d").cast("int").alias("day"),
)

# ── 9. Write to ADLS ──────────────────────────────────────────────────
final.write \
    .mode("overwrite") \
    .option("header", "true") \
    .partitionBy("year", "month", "day") \
    .csv(ADLS_PATH)

row_count = final.count()
print(f"✅ Payment settlements written to {ADLS_PATH}")
print(f"   Rows: {row_count:,}")
print(f"   Gateways: HyperPay, Moyasar, Tap Payments, PayTabs")
print(f"   Noise: ~8% (null dates, rounding mismatches, duplicates)")
