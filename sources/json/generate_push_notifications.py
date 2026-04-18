# Databricks notebook source
# ============================================================================
# Source Generator: Push Notifications (JSON)
# ============================================================================
# Simulates Firebase/APNs/Web Push notification delivery logs.
#
# Output : /Volumes/salla_databricks/bronze/landing_data/push_notifications/
# Format : JSON, partitioned by year/month/day
# Volume : ~50 000 rows
# Noise  : ~6 % — failed deliveries, duplicate sends, null device tokens
# ============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, expr, rand, round as spark_round, when, concat, concat_ws,
    date_format, month as spark_month, floor, sha2,
    monotonically_increasing_id, element_at, array, explode,
)

spark = SparkSession.builder.getOrCreate()

ADLS_PATH = "/Volumes/salla_databricks/bronze/landing_data/push_notifications/"

# ── 1. Read real customer + order IDs ──────────────────────────────────
customers = spark.sql("""
    SELECT customer_id FROM salla_databricks.silver.silver_customers
""").collect()
customer_ids = [c.customer_id for c in customers]

orders_2024 = spark.sql("""
    SELECT order_id, customer_id FROM salla_databricks.silver.silver_sales_orders
    WHERE order_date >= '2024-01-01' AND order_date < '2025-01-01'
""").sample(False, 0.05).collect()  # 5% sample for order-related notifications
order_ids = [o.order_id for o in orders_2024]

print(f"Customers: {len(customer_ids)}, Order sample: {len(order_ids)}")

# ── 2. Date spine ─────────────────────────────────────────────────────
dates_df = spark.sql("""
    SELECT explode(sequence(
        to_date('2024-01-01'), to_date('2024-12-31'), interval 1 day
    )) AS date
""")

# ~137 notifications/day → ~50K per year
raw = dates_df.withColumn("notifs_today",
    when((col("date") >= "2024-11-25") & (col("date") <= "2024-11-30"),
         lit(400))   # White Friday promo blast
    .when((col("date") >= "2024-03-10") & (col("date") <= "2024-04-09"),
         lit(250))   # Ramadan campaigns
    .otherwise(lit(137))
).withColumn("idx", explode(expr("sequence(1, notifs_today)"))).drop("notifs_today")

# ── 3. Notification attributes ────────────────────────────────────────
cust_arr = array(*[lit(c) for c in customer_ids])
order_arr = array(*[lit(o) for o in order_ids]) if order_ids else array(lit(None).cast("string"))

raw = raw \
    .withColumn("notification_id", sha2(concat(col("date").cast("string"), col("idx").cast("string"), rand().cast("string")), 256)) \
    .withColumn("customer_id",
        element_at(cust_arr, (rand() * len(customer_ids)).cast("int") + 1)
    ) \
    .withColumn("_type_rand", rand()) \
    .withColumn("notification_type",
        when(col("_type_rand") < 0.30, lit("order_update"))         # 30%
        .when(col("_type_rand") < 0.55, lit("promotion"))           # 25%
        .when(col("_type_rand") < 0.72, lit("abandoned_cart"))      # 17%
        .when(col("_type_rand") < 0.88, lit("delivery_update"))     # 16%
        .otherwise(lit("price_drop"))                                 # 12%
    ) \
    .withColumn("order_id",
        when(col("notification_type").isin("order_update", "delivery_update"),
             element_at(order_arr, (rand() * max(len(order_ids), 1)).cast("int") + 1))
        .otherwise(lit(None).cast("string"))
    )

# ── 4. Channel (Android-heavy in KSA) ─────────────────────────────────
raw = raw \
    .withColumn("_chan_rand", rand()) \
    .withColumn("channel",
        when(col("_chan_rand") < 0.55, lit("Firebase"))      # Android 55% in KSA
        .when(col("_chan_rand") < 0.90, lit("APNs"))         # iOS 35%
        .otherwise(lit("Web Push"))                           # Web 10%
    ) \
    .withColumn("device_token",
        sha2(concat(col("customer_id").cast("string"), col("channel"), rand().cast("string")), 256)
    )

# ── 5. Notification content ───────────────────────────────────────────
TITLES = {
    "order_update": ["Your order is confirmed! 📦", "طلبك تم تأكيده", "Order status update"],
    "promotion": ["Flash Sale! Up to 50% off 🔥", "تخفيضات حصرية اليوم", "Don't miss today's deals"],
    "abandoned_cart": ["You left something behind 🛒", "عندك منتجات بالسلة", "Complete your purchase"],
    "delivery_update": ["Your package is on its way! 🚚", "شحنتك في الطريق", "Delivery update"],
    "price_drop": ["Price dropped on your wishlist! 📉", "سعر المنتج انخفض", "Good news about pricing"],
}

title_arrays = {}
for k, v in TITLES.items():
    title_arrays[k] = array(*[lit(t) for t in v])

raw = raw \
    .withColumn("title",
        when(col("notification_type") == "order_update", element_at(title_arrays["order_update"], (rand() * 3).cast("int") + 1))
        .when(col("notification_type") == "promotion", element_at(title_arrays["promotion"], (rand() * 3).cast("int") + 1))
        .when(col("notification_type") == "abandoned_cart", element_at(title_arrays["abandoned_cart"], (rand() * 3).cast("int") + 1))
        .when(col("notification_type") == "delivery_update", element_at(title_arrays["delivery_update"], (rand() * 3).cast("int") + 1))
        .otherwise(element_at(title_arrays["price_drop"], (rand() * 3).cast("int") + 1))
    )

# ── 6. Delivery & open status ─────────────────────────────────────────
raw = raw \
    .withColumn("delivery_status",
        when(rand() < 0.95, lit("delivered")).otherwise(lit("failed"))
    ) \
    .withColumn("opened",
        when(col("delivery_status") == "delivered",
             when(rand() < 0.20, lit(True)).otherwise(lit(False)))  # ~20% open rate
        .otherwise(lit(False))
    ) \
    .withColumn("sent_at",
        expr("""concat(date, 'T',
            lpad(cast(8 + floor(rand() * 14) as string), 2, '0'), ':',
            lpad(cast(floor(rand() * 60) as string), 2, '0'), ':00Z')""")
    ) \
    .withColumn("opened_at",
        when(col("opened") == True,
             expr("""concat(date, 'T',
                lpad(cast(9 + floor(rand() * 13) as string), 2, '0'), ':',
                lpad(cast(floor(rand() * 60) as string), 2, '0'), ':00Z')"""))
        .otherwise(lit(None).cast("string"))
    )

# ── 7. Inject noise (~6 %) ────────────────────────────────────────────
noisy = raw \
    .withColumn("_noise", rand()) \
    .withColumn("device_token",                                      # 3% null device token
        when(col("_noise") < 0.03, lit(None).cast("string")).otherwise(col("device_token"))
    ) \
    .withColumn("customer_id",                                       # 1% orphaned (invalid customer)
        when((col("_noise") >= 0.03) & (col("_noise") < 0.04),
             lit("CUST_INVALID")).otherwise(col("customer_id"))
    )

# ── 8. Add ~2 % duplicate sends ───────────────────────────────────────
dupes = noisy.sample(False, 0.02)
noisy = noisy.unionAll(dupes)

# ── 9. Final select ───────────────────────────────────────────────────
final = noisy.select(
    col("notification_id"), col("customer_id"), col("order_id"),
    col("notification_type"), col("channel"), col("device_token"),
    col("title"), col("sent_at"), col("delivery_status"),
    col("opened"), col("opened_at"),
    date_format(col("date"), "yyyy").cast("int").alias("year"),
    spark_month(col("date")).alias("month"),
    date_format(col("date"), "d").cast("int").alias("day"),
)

# ── 10. Write as JSON to ADLS ─────────────────────────────────────────
final.write \
    .mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .json(ADLS_PATH)

row_count = final.count()
print(f"✅ Push notifications written to {ADLS_PATH}")
print(f"   Rows: {row_count:,}")
print(f"   Channels: Firebase 55%, APNs 35%, Web Push 10%")
print(f"   Open rate: ~20%, Delivery rate: ~95%")
print(f"   Noise: ~6% (null tokens, orphaned customers, duplicate sends)")
