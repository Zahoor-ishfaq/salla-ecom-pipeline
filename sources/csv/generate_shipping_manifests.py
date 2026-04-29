# Databricks notebook source
# ============================================================================
# Source Generator: Shipping Manifests (CSV)
# ============================================================================
# Simulates 3PL carrier scan/tracking events for Saudi last-mile delivery.
# Carriers : Aramex, SMSA Express, DHL, J&T Express, Fetchr
#
# Output : /Volumes/salla_databricks/bronze/landing_data/shipping_manifests/
# Format : CSV, partitioned by year/month/day
# Volume : ~200 000 rows (multiple scan events per shipment)
# Noise  : ~6 % — missing tracking #s, duplicate scans, weight mismatches
# ============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, expr, rand, round as spark_round, when, concat, floor,
    date_format, month as spark_month, date_add, hour, explode, array,
    monotonically_increasing_id, struct,
)
from pyspark.sql.types import *

spark = SparkSession.builder.getOrCreate()

ADLS_PATH = "/Volumes/salla_databricks/bronze/landing_data/shipping_manifests/"

# ── 1. Sample real orders from Silver (2024, non-cancelled) ────────────
orders = spark.sql("""
    SELECT
        o.order_id, o.customer_id, o.store_id, o.order_date,
        s.carrier AS original_carrier, s.shipping_status,
        c.city AS customer_city, c.region AS customer_region
    FROM salla_databricks.silver.silver_sales_orders o
    LEFT JOIN salla_databricks.silver.silver_shipping_details s ON o.order_id = s.order_id
    LEFT JOIN salla_databricks.silver.silver_customers c ON o.customer_id = c.customer_id
    WHERE o.order_date >= '2024-01-01' AND o.order_date < '2025-01-01'
      AND o.order_status NOT IN ('cancelled')
""").sample(False, 0.15)  # ~15% ≈ 50K shipments, each with ~4 scan events

print(f"Shipments sampled: {orders.count():,}")

# ── 2. Assign carriers (weighted by Saudi market share) ───────────────
shipments = orders \
    .withColumn("_carrier_rand", rand()) \
    .withColumn("carrier",
        when(col("_carrier_rand") < 0.30, lit("Aramex"))            # 30% — biggest in KSA
        .when(col("_carrier_rand") < 0.55, lit("SMSA Express"))     # 25%
        .when(col("_carrier_rand") < 0.72, lit("DHL"))              # 17%
        .when(col("_carrier_rand") < 0.88, lit("J&T Express"))      # 16% — fast growing
        .otherwise(lit("Fetchr"))                                     # 12%
    ) \
    .withColumn("tracking_number",
        concat(
            when(col("carrier") == "Aramex", lit("ARX"))
            .when(col("carrier") == "SMSA Express", lit("SMS"))
            .when(col("carrier") == "DHL", lit("DHL"))
            .when(col("carrier") == "J&T Express", lit("JNT"))
            .otherwise(lit("FTR")),
            lit("-SA-"),
            col("order_id").cast("string"),
            floor(rand() * 9000 + 1000).cast("string")
        )
    ) \
    .withColumn("weight_kg", spark_round(0.2 + rand() * 14.8, 2)) \
    .withColumn("ship_date", date_add(col("order_date").cast("date"), 1))

# ── 3. Delivery speed by region (Riyadh fastest, remote slowest) ──────
shipments = shipments \
    .withColumn("delivery_days",
        when(col("customer_region") == "Riyadh Region", (1 + floor(rand() * 2)).cast("int"))
        .when(col("customer_region").isin("Makkah Region", "Eastern Region"),
              (2 + floor(rand() * 2)).cast("int"))
        .when(col("customer_region").isin("Madinah Region", "Asir Region"),
              (3 + floor(rand() * 3)).cast("int"))
        .otherwise((4 + floor(rand() * 4)).cast("int"))  # remote areas
    )

# ── 4. Generate tracking events (multiple per shipment) ───────────────
# Each shipment gets 3-6 scan events depending on delivery outcome
events = shipments \
    .withColumn("event_picked_up",
        struct(
            date_add(col("ship_date"), 0).alias("event_date"),
            lit("picked_up").alias("event_type"),
            lit("Warehouse").alias("location"),
        )
    ) \
    .withColumn("event_in_transit",
        struct(
            date_add(col("ship_date"), 1).alias("event_date"),
            lit("in_transit").alias("event_type"),
            lit("Sorting Hub").alias("location"),
        )
    ) \
    .withColumn("event_out_for_delivery",
        struct(
            date_add(col("ship_date"), col("delivery_days") - 1).alias("event_date"),
            lit("out_for_delivery").alias("event_type"),
            col("customer_city").alias("location"),
        )
    ) \
    .withColumn("_outcome_rand", rand()) \
    .withColumn("event_final",
        when(col("_outcome_rand") < 0.88,   # 88% delivered
            struct(
                date_add(col("ship_date"), col("delivery_days")).alias("event_date"),
                lit("delivered").alias("event_type"),
                col("customer_city").alias("location"),
            )
        )
        .when(col("_outcome_rand") < 0.95,  # 7% failed attempt
            struct(
                date_add(col("ship_date"), col("delivery_days")).alias("event_date"),
                lit("failed_attempt").alias("event_type"),
                col("customer_city").alias("location"),
            )
        )
        .otherwise(                           # 5% returned to sender
            struct(
                date_add(col("ship_date"), col("delivery_days") + 3).alias("event_date"),
                lit("returned").alias("event_type"),
                lit("Return Hub").alias("location"),
            )
        )
    )

# Explode into individual scan rows
scan_events = events \
    .select(
        col("order_id"), col("tracking_number"), col("carrier"),
        col("weight_kg"), col("customer_city"), col("customer_region"),
        explode(array(
            col("event_picked_up"),
            col("event_in_transit"),
            col("event_out_for_delivery"),
            col("event_final"),
        )).alias("event")
    ) \
    .select(
        col("order_id"), col("tracking_number"), col("carrier"),
        col("weight_kg"), col("customer_city"), col("customer_region"),
        col("event.event_date").alias("scan_date"),
        col("event.event_type").alias("event_type"),
        col("event.location").alias("scan_location"),
    ) \
    .withColumn("scan_id", monotonically_increasing_id()) \
    .withColumn("scan_timestamp",
        expr("concat(scan_date, 'T', lpad(cast(6 + floor(rand() * 14) as string), 2, '0'), ':', lpad(cast(floor(rand() * 60) as string), 2, '0'), ':00Z')")
    )

# ── 5. Inject noise (~6 %) ────────────────────────────────────────────
noisy = scan_events \
    .withColumn("_noise", rand()) \
    .withColumn("tracking_number",                                   # 2% missing tracking
        when(col("_noise") < 0.02, lit(None).cast("string")).otherwise(col("tracking_number"))
    ) \
    .withColumn("weight_kg",                                         # 2% weight discrepancy
        when((col("_noise") >= 0.02) & (col("_noise") < 0.04),
             spark_round(col("weight_kg") * (1.1 + rand() * 0.3), 2))
        .otherwise(col("weight_kg"))
    )

# ── 6. Add ~3 % duplicate scan events ─────────────────────────────────
dupes = noisy.sample(False, 0.03)
noisy = noisy.unionAll(dupes)

# ── 7. Final select ───────────────────────────────────────────────────
final = noisy.select(
    col("scan_id"), col("order_id"), col("tracking_number"), col("carrier"),
    col("scan_date"), col("scan_timestamp"), col("event_type"),
    col("scan_location"), col("weight_kg"),
    col("customer_city"), col("customer_region"),
    date_format(col("scan_date"), "yyyy").cast("int").alias("year"),
    spark_month(col("scan_date")).alias("month"),
    date_format(col("scan_date"), "d").cast("int").alias("day"),
)

# ── 8. Write to ADLS ──────────────────────────────────────────────────
final.write \
    .mode("overwrite") \
    .option("header", "true") \
    .partitionBy("year", "month", "day") \
    .csv(ADLS_PATH)

row_count = final.count()
print(f"✅ Shipping manifests written to {ADLS_PATH}")
print(f"   Rows: {row_count:,}")
print(f"   Carriers: Aramex, SMSA Express, DHL, J&T Express, Fetchr")
print(f"   Events: picked_up → in_transit → out_for_delivery → delivered/failed/returned")
print(f"   Noise: ~6% (missing tracking, weight mismatches, duplicate scans)")
