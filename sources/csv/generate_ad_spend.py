# Databricks notebook source
# ============================================================================
# Source Generator: Ad Spend (CSV)
# ============================================================================
# Simulates daily marketing spend exports from Google Ads, Meta, Snapchat,
# TikTok — the dominant ad platforms in the Saudi market.
#
# Output : /Volumes/salla_databricks/bronze/landing_data/ad_spend/
# Format : CSV, partitioned by year/month/day
# Volume : ~50 000 rows (365 days × ~137 campaign-platform combos/day)
# Noise  : ~8 % — null conversions, duplicate rows, negative ROAS, outliers
# Auth   : Uses Unity Catalog managed volume (no external auth needed)
# ============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, expr, rand, round as spark_round, when, concat_ws,
    date_format, dayofweek, month as spark_month, array, element_at,
    floor, abs as spark_abs, monotonically_increasing_id, unix_timestamp,
)
from pyspark.sql.types import *
import datetime

spark = SparkSession.builder.getOrCreate()


# ── 1. Read real product categories for campaign naming ─────────────────
categories = [r.category for r in spark.sql(
    "SELECT DISTINCT category FROM salla_databricks.silver.silver_products"
).collect()]
print(f"Product categories: {categories}")

# ── 2. Reference data ──────────────────────────────────────────────────
PLATFORMS = ["Google Ads", "Meta", "Snapchat", "TikTok"]
# Snapchat gets ~35 % of Saudi digital ad spend (source: MCIT / Statista KSA)
PLATFORM_WEIGHTS = {"Google Ads": 0.25, "Meta": 0.25, "Snapchat": 0.35, "TikTok": 0.15}
CAMPAIGN_TYPES = ["awareness", "consideration", "conversion", "retargeting"]
COUNTRIES = ["SA", "AE", "KW", "BH", "OM", "QA"]  # GCC markets

ADLS_PATH = "/Volumes/salla_databricks/bronze/landing_data/ad_spend/"

# ── 3. Generate date spine (2024-01-01 → 2024-12-31) ──────────────────
dates_df = spark.sql("""
    SELECT explode(sequence(
        to_date('2024-01-01'), to_date('2024-12-31'), interval 1 day
    )) AS date
""")

# ── 4. Build campaign dimension ────────────────────────────────────────
campaign_rows = []
campaign_id = 1000
for cat in categories:
    for ctype in CAMPAIGN_TYPES:
        for platform in PLATFORMS:
            campaign_rows.append((
                f"SA-{campaign_id}",
                f"{cat} - {ctype.title()}",
                ctype,
                cat,                         # maps back to product category
                platform,
                PLATFORM_WEIGHTS[platform],
                "SAR",
                "SA",
            ))
            campaign_id += 1

campaign_schema = StructType([
    StructField("campaign_id", StringType()),
    StructField("campaign_name", StringType()),
    StructField("campaign_type", StringType()),
    StructField("campaign_category", StringType()),
    StructField("platform", StringType()),
    StructField("platform_weight", DoubleType()),
    StructField("currency", StringType()),
    StructField("country", StringType()),
])
campaigns_df = spark.createDataFrame(campaign_rows, campaign_schema)
print(f"Campaigns generated: {campaigns_df.count()}")

# ── 5. Cross join dates × campaigns ───────────────────────────────────
raw = dates_df.crossJoin(campaigns_df)

# ── 6. Seasonality multipliers ────────────────────────────────────────
#   Ramadan 2024 : Mar 10 – Apr 9  → 2× spend (brands push hard)
#   White Friday : Nov 25 – Nov 30 → 3× spend
#   Saudi Nat'l  : Sep 23          → 1.8× spend
#   KSA weekend  : Fri (6) / Sat (7) → 0.7× (business hours drop)
raw = raw.withColumn("dow", dayofweek(col("date"))) \
    .withColumn("mon", spark_month(col("date"))) \
    .withColumn("day_str", date_format(col("date"), "MM-dd")) \
    .withColumn(
        "season_mult",
        when((col("date") >= "2024-03-10") & (col("date") <= "2024-04-09"), 2.0)   # Ramadan
        .when((col("date") >= "2024-11-25") & (col("date") <= "2024-11-30"), 3.0)   # White Friday
        .when(col("day_str") == "09-23", 1.8)                                       # National Day
        .when(col("mon").isin(7, 8), 0.8)                                           # Summer lull
        .otherwise(1.0)
    ) \
    .withColumn(
        "weekend_mult",
        when(col("dow").isin(6, 7), 0.7).otherwise(1.0)  # Fri-Sat in KSA
    )

# ── 7. Generate KPIs with realistic ranges ────────────────────────────
# Base impressions vary by campaign type & platform weight
result = raw \
    .withColumn("base_imp",
        when(col("campaign_type") == "awareness", 150000)
        .when(col("campaign_type") == "consideration", 60000)
        .when(col("campaign_type") == "conversion", 25000)
        .otherwise(40000)  # retargeting
    ) \
    .withColumn("impressions",
        (col("base_imp") * col("platform_weight") * col("season_mult")
         * col("weekend_mult") * (1.0 + (rand() - 0.5) * 0.4)).cast("int")
    ) \
    .withColumn("ctr",
        when(col("campaign_type") == "retargeting", 0.03 + rand() * 0.02)
        .when(col("campaign_type") == "conversion", 0.02 + rand() * 0.015)
        .when(col("campaign_type") == "consideration", 0.01 + rand() * 0.01)
        .otherwise(0.005 + rand() * 0.005)  # awareness
    ) \
    .withColumn("clicks", (col("impressions") * col("ctr")).cast("int")) \
    .withColumn("spend_sar",
        spark_round(
            col("clicks") * (1.5 + rand() * 3.5) * col("season_mult"), 2
        )
    ) \
    .withColumn("conv_rate",
        when(col("campaign_type") == "conversion", 0.03 + rand() * 0.04)
        .when(col("campaign_type") == "retargeting", 0.02 + rand() * 0.03)
        .otherwise(0.005 + rand() * 0.01)
    ) \
    .withColumn("conversions", (col("clicks") * col("conv_rate")).cast("int")) \
    .withColumn("revenue_sar",
        spark_round(col("conversions") * (80 + rand() * 320), 2)
    ) \
    .withColumn("roas",
        spark_round(when(col("spend_sar") > 0, col("revenue_sar") / col("spend_sar"))
                    .otherwise(lit(0.0)), 2)
    ) \
    .withColumn("cpc", spark_round(
        when(col("clicks") > 0, col("spend_sar") / col("clicks")).otherwise(lit(0.0)), 2
    )) \
    .withColumn("cpm", spark_round(
        when(col("impressions") > 0, col("spend_sar") / col("impressions") * 1000)
        .otherwise(lit(0.0)), 2
    ))

# ── 8. Inject noise (~8 %) ────────────────────────────────────────────
noisy = result \
    .withColumn("_noise", rand()) \
    .withColumn("conversions",                                        # 5 % null conversions
        when(col("_noise") < 0.05, lit(None).cast("int")).otherwise(col("conversions"))
    ) \
    .withColumn("roas",                                               # 1 % negative ROAS (data error)
        when((col("_noise") >= 0.05) & (col("_noise") < 0.06),
             spark_round(-rand() * 2, 2)).otherwise(col("roas"))
    ) \
    .withColumn("spend_sar",                                          # 1 % outlier spend
        when((col("_noise") >= 0.06) & (col("_noise") < 0.07),
             spark_round(col("spend_sar") * (8 + rand() * 4), 2)).otherwise(col("spend_sar"))
    )

# ── 9. Add ~3 % duplicate rows ────────────────────────────────────────
dupes = noisy.sample(False, 0.03)
noisy = noisy.unionAll(dupes)

# ── 10. Final select & partition columns ───────────────────────────────
final = noisy.select(
    col("date"),
    col("campaign_name"),
    col("campaign_type"),
    col("campaign_category"),
    col("platform"),
    col("impressions"),
    col("clicks"),
    spark_round(col("spend_sar"), 2).alias("spend_sar"),
    col("conversions"),
    spark_round(col("revenue_sar"), 2).alias("revenue_sar"),
    col("roas"),
    col("cpc"),
    col("cpm"),
    col("currency"),
    col("country"),
    date_format(col("date"), "yyyy").cast("int").alias("year"),
    spark_month(col("date")).alias("month"),
    date_format(col("date"), "d").cast("int").alias("day"),
)

# ── 11. Write to ADLS ─────────────────────────────────────────────────
final.write \
    .mode("overwrite") \
    .option("header", "true") \
    .partitionBy("year", "month", "day") \
    .csv(ADLS_PATH)

row_count = final.count()
print(f"✅ Ad spend data written to {ADLS_PATH}")
print(f"   Rows: {row_count:,}")
print(f"   Date range: 2024-01-01 → 2024-12-31")
print(f"   Platforms: {PLATFORMS}")
print(f"   Noise: ~8% (null conversions, negative ROAS, outlier spend, duplicates)")
