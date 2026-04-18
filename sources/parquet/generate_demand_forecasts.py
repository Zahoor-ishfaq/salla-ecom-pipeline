# Databricks notebook source
# ============================================================================
# Source Generator: Demand Forecasts (Parquet)
# ============================================================================
# Simulates ML model output — 90-day forward demand predictions per
# product × store combination. Mimics a forecasting pipeline output.
#
# Output : /Volumes/salla_databricks/bronze/landing_data/demand_forecasts/
# Format : Parquet
# Volume : ~165 000 rows (40 products × 46 stores × 90 days)
# ============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, expr, rand, round as spark_round, when, floor,
    date_format, month as spark_month, dayofweek, current_timestamp,
    explode, greatest,
)

spark = SparkSession.builder.getOrCreate()

ADLS_PATH = "/Volumes/salla_databricks/bronze/landing_data/demand_forecasts/"

# ── 1. Read real products + stores from Silver ─────────────────────────
products = spark.sql("""
    SELECT product_id, product_name, category, unit_price
    FROM salla_databricks.silver.silver_products
""")
stores = spark.sql("""
    SELECT store_id, store_name, city, region
    FROM salla_databricks.silver.silver_stores
""")

print(f"Products: {products.count()}, Stores: {stores.count()}")

# ── 2. Compute historical avg daily demand per product (from real data) ─
hist_demand = spark.sql("""
    SELECT
        product_id,
        store_id,
        ROUND(COUNT(*) / DATEDIFF(MAX(order_date), MIN(order_date)), 2) AS avg_daily_demand
    FROM salla_databricks.silver.silver_sales_orders
    WHERE order_status NOT IN ('cancelled')
    GROUP BY product_id, store_id
""")

# ── 3. 90-day forecast window ─────────────────────────────────────────
forecast_dates = spark.sql("""
    SELECT explode(sequence(
        to_date('2025-01-01'), to_date('2025-03-31'), interval 1 day
    )) AS forecast_date
""")

# ── 4. Cross join: products × stores × dates ──────────────────────────
base = products.crossJoin(stores).crossJoin(forecast_dates)

# Join with historical demand (where available)
base = base.join(hist_demand, ["product_id", "store_id"], "left") \
    .withColumn("avg_daily_demand",
        when(col("avg_daily_demand").isNull(), 0.5 + rand() * 2.0)  # default for no history
        .otherwise(col("avg_daily_demand"))
    )

# ── 5. Seasonality adjustments ────────────────────────────────────────
# Ramadan 2025: ~Feb 28 – Mar 29 (approximate)
base = base \
    .withColumn("dow", dayofweek(col("forecast_date"))) \
    .withColumn("season_factor",
        when((col("forecast_date") >= "2025-02-28") & (col("forecast_date") <= "2025-03-29"),
             1.5 + rand() * 0.5)   # Ramadan: 1.5-2.0× demand
        .when(col("dow").isin(6, 7),
             0.7 + rand() * 0.1)    # Weekend dip (Fri-Sat)
        .otherwise(1.0 + (rand() - 0.5) * 0.2)  # Normal: ±10%
    ) \
    .withColumn("category_boost",
        when(col("category").isin("Food", "Grocery", "Personal Care"), lit(1.3))  # staples during Ramadan
        .when(col("category").isin("Electronics", "Fashion"), lit(1.1))
        .otherwise(lit(1.0))
    )

# ── 6. Generate forecast values ───────────────────────────────────────
forecasts = base \
    .withColumn("predicted_demand",
        greatest(
            lit(0),
            spark_round(
                col("avg_daily_demand") * col("season_factor") * col("category_boost")
                * (1.0 + (rand() - 0.5) * 0.3),  # ±15% noise
                1
            )
        )
    ) \
    .withColumn("prediction_std",
        spark_round(col("predicted_demand") * (0.15 + rand() * 0.20), 1)
    ) \
    .withColumn("lower_bound",
        greatest(lit(0.0), spark_round(col("predicted_demand") - 1.96 * col("prediction_std"), 1))
    ) \
    .withColumn("upper_bound",
        spark_round(col("predicted_demand") + 1.96 * col("prediction_std"), 1)
    ) \
    .withColumn("confidence_level", lit(0.95))

# ── 7. Model metadata ─────────────────────────────────────────────────
forecasts = forecasts \
    .withColumn("model_algorithm",
        when(rand() < 0.6, lit("Prophet"))
        .when(rand() < 0.8, lit("LightGBM"))
        .otherwise(lit("ARIMA"))
    ) \
    .withColumn("model_version", lit("forecast_v3.2_20250101")) \
    .withColumn("training_date", lit("2024-12-28").cast("date")) \
    .withColumn("mape_score", spark_round(0.08 + rand() * 0.15, 4)) \
    .withColumn("forecast_generated_at", current_timestamp())

# ── 8. Final select ───────────────────────────────────────────────────
final = forecasts.select(
    col("product_id"), col("product_name"), col("category"),
    col("store_id"), col("store_name"), col("city").alias("store_city"),
    col("region").alias("store_region"),
    col("forecast_date"),
    col("predicted_demand"), col("lower_bound"), col("upper_bound"),
    col("confidence_level"), col("prediction_std"),
    col("avg_daily_demand").alias("historical_avg_demand"),
    col("season_factor"), col("category_boost"),
    col("model_algorithm"), col("model_version"),
    col("training_date"), col("mape_score"),
    col("forecast_generated_at"),
    date_format(col("forecast_date"), "yyyy").cast("int").alias("year"),
    spark_month(col("forecast_date")).alias("month"),
    date_format(col("forecast_date"), "d").cast("int").alias("day"),
)

# ── 9. Write as Parquet to ADLS ───────────────────────────────────────
final.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet(ADLS_PATH)

row_count = final.count()
print(f"✅ Demand forecasts written to {ADLS_PATH}")
print(f"   Rows: {row_count:,}")
print(f"   Forecast window: 2025-01-01 → 2025-03-31 (90 days)")
print(f"   Products: {products.count()}, Stores: {stores.count()}")
print(f"   Models: Prophet, LightGBM, ARIMA")
print(f"   Features: predicted_demand, confidence intervals, MAPE scores")
