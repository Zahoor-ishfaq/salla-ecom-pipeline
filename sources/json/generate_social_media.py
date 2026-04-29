# Databricks notebook source
# ============================================================================
# Source Generator: Social Media Mentions (JSON)
# ============================================================================
# Simulates social listening API pulls from Twitter/X, Instagram, TikTok,
# Snapchat — the top platforms in Saudi Arabia.
#
# Output : /Volumes/salla_databricks/bronze/landing_data/social_media/
# Format : JSON, partitioned by year/month/day
# Volume : ~100 000 rows
# Noise  : ~8 % — null engagement, duplicate posts, bot patterns
# ============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    explode,
    col, lit, expr, rand, round as spark_round, when, concat, concat_ws,
    date_format, month as spark_month, floor, abs as spark_abs,
    monotonically_increasing_id, element_at, array,
)
from pyspark.sql.types import *

spark = SparkSession.builder.getOrCreate()

ADLS_PATH = "/Volumes/salla_databricks/bronze/landing_data/social_media/"

# ── 1. Read real store names and product names for mention content ─────
stores = [r.store_name for r in spark.sql(
    "SELECT DISTINCT store_name FROM salla_databricks.silver.silver_stores"
).collect()]
products = [r.product_name for r in spark.sql(
    "SELECT DISTINCT product_name FROM salla_databricks.silver.silver_products"
).collect()]

print(f"Stores for mentions: {len(stores)}, Products: {len(products)}")

# ── 2. Social media templates (Arabic + English mix) ──────────────────
POSITIVE_TEMPLATES = [
    "طلبت من {store} ووصلني بسرعة! تجربة ممتازة 👍",
    "Just received my {product} from {store} - amazing quality! ⭐⭐⭐⭐⭐",
    "أفضل متجر إلكتروني في السعودية {store} 🇸🇦",
    "Love shopping on Salla stores! Got a great deal on {product} 🛍️",
    "تجربة شراء رائعة من {store} - توصيل سريع وجودة عالية",
    "{store} has the best prices for {product} in KSA",
    "شكراً {store} على الخدمة الممتازة والتوصيل السريع 🚀",
    "Finally found {product} at a great price on {store}! Highly recommend",
]
NEGATIVE_TEMPLATES = [
    "طلبت من {store} وتأخر التوصيل أسبوعين 😤",
    "Terrible experience with {store} - {product} arrived damaged",
    "لا أنصح بالشراء من {store} - جودة المنتج سيئة",
    "{store} customer service is unresponsive. Still waiting for refund.",
    "وصلني منتج غلط من {store} والاسترجاع صعب",
    "Paid extra for express shipping from {store} but still late",
]
NEUTRAL_TEMPLATES = [
    "Has anyone tried {product} from {store}? Looking for reviews",
    "مقارنة أسعار {product} بين {store} ونون",
    "New collection just dropped at {store} 👀",
    "{store} is having a sale on {product} this week",
    "هل يوجد كود خصم {store}؟",
    "Comparing {product} prices across Saudi stores",
]

# ── 3. Platform distribution (Saudi market) ───────────────────────────
PLATFORMS = ["Twitter/X", "Instagram", "TikTok", "Snapchat"]
PLATFORM_WEIGHTS = [0.25, 0.30, 0.25, 0.20]  # Instagram #1 in KSA

# ── 4. Date spine with hourly granularity ──────────────────────────────
dates_df = spark.sql("""
    SELECT explode(sequence(
        to_date('2024-01-01'), to_date('2024-12-31'), interval 1 day
    )) AS date
""")

# ~275 mentions per day → ~100K per year
raw = dates_df.withColumn("mentions_today",
    when((col("date") >= "2024-11-25") & (col("date") <= "2024-11-30"),
         lit(800))   # White Friday buzz
    .when((col("date") >= "2024-03-10") & (col("date") <= "2024-04-09"),
         lit(450))   # Ramadan activity
    .otherwise(lit(275))
)

# Explode into individual mentions
raw = raw.withColumn("mention_idx",
    explode(expr("sequence(1, mentions_today)"))
)

# ── 5. Assign platform, sentiment, content ─────────────────────────────
raw = raw \
    .withColumn("post_id", monotonically_increasing_id()) \
    .withColumn("_plat_rand", rand()) \
    .withColumn("platform",
        when(col("_plat_rand") < 0.25, lit("Twitter/X"))
        .when(col("_plat_rand") < 0.55, lit("Instagram"))
        .when(col("_plat_rand") < 0.80, lit("TikTok"))
        .otherwise(lit("Snapchat"))
    ) \
    .withColumn("_sent_rand", rand()) \
    .withColumn("sentiment",
        when(col("_sent_rand") < 0.70, lit("positive"))
        .when(col("_sent_rand") < 0.85, lit("negative"))
        .otherwise(lit("neutral"))
    ) \
    .withColumn("_store_idx", (rand() * len(stores)).cast("int") % len(stores)) \
    .withColumn("_prod_idx", (rand() * len(products)).cast("int") % len(products)) \
    .withColumn("store_mentioned", element_at(array(*[lit(s) for s in stores]), col("_store_idx") + 1)) \
    .withColumn("product_mentioned", element_at(array(*[lit(p) for p in products]), col("_prod_idx") + 1))

# Template selection (simplified — pick based on sentiment + index)
pos_arr = array(*[lit(t) for t in POSITIVE_TEMPLATES])
neg_arr = array(*[lit(t) for t in NEGATIVE_TEMPLATES])
neu_arr = array(*[lit(t) for t in NEUTRAL_TEMPLATES])

raw = raw \
    .withColumn("_tmpl_idx",
        when(col("sentiment") == "positive", (rand() * len(POSITIVE_TEMPLATES)).cast("int") % len(POSITIVE_TEMPLATES) + 1)
        .when(col("sentiment") == "negative", (rand() * len(NEGATIVE_TEMPLATES)).cast("int") % len(NEGATIVE_TEMPLATES) + 1)
        .otherwise((rand() * len(NEUTRAL_TEMPLATES)).cast("int") % len(NEUTRAL_TEMPLATES) + 1)
    ) \
    .withColumn("text_template",
        when(col("sentiment") == "positive", element_at(pos_arr, col("_tmpl_idx")))
        .when(col("sentiment") == "negative", element_at(neg_arr, col("_tmpl_idx")))
        .otherwise(element_at(neu_arr, col("_tmpl_idx")))
    ) \
    .withColumn("text",
        expr("replace(replace(text_template, '{store}', store_mentioned), '{product}', product_mentioned)")
    )

# ── 6. Engagement metrics ─────────────────────────────────────────────
raw = raw \
    .withColumn("likes", (rand() * 500).cast("int")) \
    .withColumn("shares", (rand() * 50).cast("int")) \
    .withColumn("comments", (rand() * 80).cast("int")) \
    .withColumn("post_timestamp",
        expr("concat(date, 'T', lpad(cast(floor(rand() * 24) as string), 2, '0'), ':', lpad(cast(floor(rand() * 60) as string), 2, '0'), ':', lpad(cast(floor(rand() * 60) as string), 2, '0'), 'Z')")
    ) \
    .withColumn("author_id", concat(lit("user_"), floor(rand() * 50000).cast("string"))) \
    .withColumn("language", when(rand() < 0.65, lit("ar")).otherwise(lit("en")))

# ── 7. Inject noise (~8 %) ────────────────────────────────────────────
noisy = raw \
    .withColumn("_noise", rand()) \
    .withColumn("likes",                                             # 3% null engagement
        when(col("_noise") < 0.03, lit(None).cast("int")).otherwise(col("likes"))
    ) \
    .withColumn("shares",
        when((col("_noise") >= 0.03) & (col("_noise") < 0.05),
             lit(None).cast("int")).otherwise(col("shares"))
    ) \
    .withColumn("likes",                                             # 2% bot-like (exact round numbers)
        when((col("_noise") >= 0.05) & (col("_noise") < 0.07),
             lit(10000)).otherwise(col("likes"))
    )

# ── 8. Add ~3 % duplicates ────────────────────────────────────────────
dupes = noisy.sample(False, 0.03)
noisy = noisy.unionAll(dupes)

# ── 9. Final select ───────────────────────────────────────────────────
final = noisy.select(
    col("post_id").cast("string"),
    col("platform"),
    col("post_timestamp"),
    col("date").alias("post_date"),
    col("author_id"),
    col("language"),
    col("text"),
    col("sentiment"),
    col("store_mentioned"),
    col("product_mentioned"),
    col("likes"), col("shares"), col("comments"),
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
print(f"✅ Social media mentions written to {ADLS_PATH}")
print(f"   Rows: {row_count:,}")
print(f"   Platforms: Twitter/X, Instagram, TikTok, Snapchat")
print(f"   Sentiment: 70% positive, 15% negative, 15% neutral")
print(f"   Languages: Arabic (65%) + English (35%)")
print(f"   Noise: ~8% (null engagement, bot patterns, duplicates)")
