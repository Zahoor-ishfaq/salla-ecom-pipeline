# Databricks notebook source
# ============================================================================
# Source Generator: Website Clickstream (JSON)
# ============================================================================
# Simulates Google Tag Manager / analytics event exports capturing user
# browsing sessions on Salla storefronts. Saudi Arabia is mobile-first.
#
# Output : /Volumes/salla_databricks/bronze/landing_data/clickstream/
# Format : JSON, partitioned by year/month/day
# Volume : ~500 000 rows (session-based events)
# Noise  : ~7 % — null user agents, bot traffic, orphaned sessions
# ============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, expr, rand, round as spark_round, when, concat, concat_ws,
    date_format, month as spark_month, floor, monotonically_increasing_id,
    element_at, array, explode, sha2,
)
from pyspark.sql.types import *

spark = SparkSession.builder.getOrCreate()

ADLS_PATH = "/Volumes/salla_databricks/bronze/landing_data/clickstream/"

# ── 1. Read real product + store IDs for event context ─────────────────
products = spark.sql("""
    SELECT product_id, product_name, category
    FROM salla_databricks.silver.silver_products
""").collect()
stores = spark.sql("""
    SELECT store_id, store_name FROM salla_databricks.silver.silver_stores
""").collect()

product_ids = [p.product_id for p in products]
store_ids = [s.store_id for s in stores]
print(f"Products: {len(product_ids)}, Stores: {len(store_ids)}")

# ── 2. Generate sessions (each session = 1 user visit) ────────────────
# ~1,400 sessions/day → ~500K total, avg 3-4 events per session
dates_df = spark.sql("""
    SELECT explode(sequence(
        to_date('2024-01-01'), to_date('2024-12-31'), interval 1 day
    )) AS date
""")

sessions_per_day = dates_df.withColumn("num_sessions",
    when((col("date") >= "2024-11-25") & (col("date") <= "2024-11-30"),
         lit(4000))   # White Friday traffic spike
    .when((col("date") >= "2024-03-10") & (col("date") <= "2024-04-09"),
         lit(2200))   # Ramadan browsing
    .otherwise(lit(1400))
)

sessions = sessions_per_day.withColumn("session_idx",
    explode(expr("sequence(1, num_sessions)"))
).drop("num_sessions")

# ── 3. Session attributes ─────────────────────────────────────────────
sessions = sessions \
    .withColumn("session_id", sha2(concat(col("date").cast("string"), col("session_idx").cast("string"), rand().cast("string")), 256)) \
    .withColumn("user_id",
        when(rand() < 0.60,   # 60% logged-in users
             concat(lit("user_"), floor(rand() * 9670).cast("string")))
        .otherwise(lit(None).cast("string"))  # 40% anonymous
    ) \
    .withColumn("_device_rand", rand()) \
    .withColumn("device_type",
        when(col("_device_rand") < 0.70, lit("mobile"))   # KSA = mobile-first
        .when(col("_device_rand") < 0.90, lit("desktop"))
        .otherwise(lit("tablet"))
    ) \
    .withColumn("user_agent",
        when(col("device_type") == "mobile",
             element_at(array(
                 lit("Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15"),
                 lit("Mozilla/5.0 (Linux; Android 14; SM-S918B) AppleWebKit/537.36"),
                 lit("Mozilla/5.0 (Linux; Android 14; Pixel 8 Pro) AppleWebKit/537.36"),
             ), (rand() * 3).cast("int") + 1))
        .when(col("device_type") == "desktop",
             element_at(array(
                 lit("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0"),
                 lit("Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 Safari/17.4"),
             ), (rand() * 2).cast("int") + 1))
        .otherwise(lit("Mozilla/5.0 (iPad; CPU OS 17_4 like Mac OS X) AppleWebKit/605.1.15"))
    ) \
    .withColumn("store_id",
        element_at(array(*[lit(s) for s in store_ids]), (rand() * len(store_ids)).cast("int") + 1)
    ) \
    .withColumn("referrer",
        element_at(array(
            lit("google.com"), lit("direct"), lit("instagram.com"), lit("snapchat.com"),
            lit("twitter.com"), lit("tiktok.com"), lit("noon.com"), lit("facebook.com"),
        ), (rand() * 8).cast("int") + 1)
    )

# ── 4. Generate events per session (funnel-based) ─────────────────────
# Every session starts with page_view, then probabilistic funnel
product_id_arr = array(*[lit(p) for p in product_ids])

events = sessions \
    .withColumn("_funnel_depth", rand()) \
    .withColumn("events_list",
        when(col("_funnel_depth") < 0.30,                          # 30% bounce (1 event)
             array(lit("page_view")))
        .when(col("_funnel_depth") < 0.55,                          # 25% browse (2-3 events)
             array(lit("page_view"), lit("product_view"), lit("search")))
        .when(col("_funnel_depth") < 0.75,                          # 20% add to cart
             array(lit("page_view"), lit("product_view"), lit("add_to_cart")))
        .when(col("_funnel_depth") < 0.88,                          # 13% start checkout
             array(lit("page_view"), lit("product_view"), lit("add_to_cart"), lit("checkout_start")))
        .otherwise(                                                   # 12% complete purchase
             array(lit("page_view"), lit("product_view"), lit("add_to_cart"),
                   lit("checkout_start"), lit("checkout_complete")))
    )

# Explode events
event_rows = events \
    .select("*", explode(col("events_list")).alias("event_type")) \
    .withColumn("event_id", monotonically_increasing_id()) \
    .withColumn("product_id",
        when(col("event_type").isin("product_view", "add_to_cart", "remove_from_cart",
                                     "checkout_start", "checkout_complete"),
             element_at(product_id_arr, (rand() * len(product_ids)).cast("int") + 1))
        .otherwise(lit(None).cast("string"))
    ) \
    .withColumn("event_timestamp",
        expr("""concat(date, 'T',
            lpad(cast(8 + floor(rand() * 14) as string), 2, '0'), ':',
            lpad(cast(floor(rand() * 60) as string), 2, '0'), ':',
            lpad(cast(floor(rand() * 60) as string), 2, '0'), '.',
            lpad(cast(floor(rand() * 999) as string), 3, '0'), 'Z')""")
    ) \
    .withColumn("page_url",
        when(col("event_type") == "page_view", lit("/"))
        .when(col("event_type") == "product_view", concat(lit("/product/"), col("product_id")))
        .when(col("event_type") == "search", lit("/search?q="))
        .when(col("event_type") == "add_to_cart", concat(lit("/cart/add/"), col("product_id")))
        .when(col("event_type") == "checkout_start", lit("/checkout"))
        .when(col("event_type") == "checkout_complete", lit("/checkout/success"))
        .otherwise(lit("/"))
    )

# ── 5. Inject noise (~7 %) ────────────────────────────────────────────
noisy = event_rows \
    .withColumn("_noise", rand()) \
    .withColumn("user_agent",                                        # 3% null user agent
        when(col("_noise") < 0.03, lit(None).cast("string")).otherwise(col("user_agent"))
    ) \
    .withColumn("referrer",                                          # 2% bot traffic
        when((col("_noise") >= 0.03) & (col("_noise") < 0.05),
             lit("bot/crawler")).otherwise(col("referrer"))
    ) \
    .withColumn("session_id",                                        # 2% orphaned events (random session)
        when((col("_noise") >= 0.05) & (col("_noise") < 0.07),
             sha2(rand().cast("string"), 256)).otherwise(col("session_id"))
    )

# ── 6. Add ~2 % duplicate events ──────────────────────────────────────
dupes = noisy.sample(False, 0.02)
noisy = noisy.unionAll(dupes)

# ── 7. Final select ───────────────────────────────────────────────────
final = noisy.select(
    col("event_id").cast("string"),
    col("session_id"),
    col("user_id"),
    col("event_type"),
    col("event_timestamp"),
    col("page_url"),
    col("product_id"),
    col("store_id"),
    col("device_type"),
    col("user_agent"),
    col("referrer"),
    date_format(col("date"), "yyyy").cast("int").alias("year"),
    spark_month(col("date")).alias("month"),
    date_format(col("date"), "d").cast("int").alias("day"),
)

# ── 8. Write as JSON to ADLS ──────────────────────────────────────────
final.write \
    .mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .json(ADLS_PATH)

row_count = final.count()
print(f"✅ Clickstream events written to {ADLS_PATH}")
print(f"   Rows: {row_count:,}")
print(f"   Device split: mobile 70%, desktop 20%, tablet 10%")
print(f"   Funnel: page_view → product_view → add_to_cart → checkout → purchase")
print(f"   Noise: ~7% (null user agents, bot traffic, orphaned sessions, duplicates)")
