# Databricks notebook source
# ============================================================================
# Source Generator: Competitor Pricing (CSV)
# ============================================================================
# Simulates daily web-scraping results from major Saudi e-commerce competitors.
# Competitors : Noon, Amazon.sa, Jarir, Extra, Lulu Hypermarket
#
# Output : /Volumes/salla_databricks/bronze/landing_data/competitor_pricing/
# Format : CSV, partitioned by year/month/day
# Volume : ~30 000 rows
# Noise  : ~7 % — missing prices, null ratings, duplicate scrapes
# ============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, expr, rand, round as spark_round, when, concat_ws,
    date_format, month as spark_month, monotonically_increasing_id,
    explode, array, struct, row_number,
)
from pyspark.sql.types import *
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()


ADLS_PATH = "/Volumes/salla_databricks/bronze/landing_data/competitor_pricing/"

# ── 1. Read real products from Silver for cross-referencing ────────────
products = spark.sql("""
    SELECT product_id, product_name, category, subcategory, unit_price
    FROM salla_databricks.silver.silver_products
""").collect()
print(f"Real products loaded: {len(products)}")

# ── 2. Competitor reference data ──────────────────────────────────────
COMPETITORS = [
    ("COMP-001", "Noon",            "SA", 0.95),   # price_factor vs our price
    ("COMP-002", "Amazon.sa",       "SA", 0.92),
    ("COMP-003", "Jarir",           "SA", 1.02),   # slightly more expensive (electronics focus)
    ("COMP-004", "Extra",           "SA", 0.98),
    ("COMP-005", "Lulu Hypermarket","SA", 1.05),   # different product mix
]
comp_schema = StructType([
    StructField("competitor_id", StringType()),
    StructField("competitor_name", StringType()),
    StructField("competitor_country", StringType()),
    StructField("base_price_factor", DoubleType()),
])
competitors_df = spark.createDataFrame(COMPETITORS, comp_schema)

# ── 3. Build product dimension from real data ─────────────────────────
prod_rows = [(p.product_id, p.product_name, p.category, p.subcategory, float(p.unit_price))
             for p in products]
prod_schema = StructType([
    StructField("product_id", StringType()),
    StructField("product_name", StringType()),
    StructField("product_category", StringType()),
    StructField("product_subcategory", StringType()),
    StructField("our_unit_price", DoubleType()),
])
products_df = spark.createDataFrame(prod_rows, prod_schema)

# ── 4. Date spine ─────────────────────────────────────────────────────
dates_df = spark.sql("""
    SELECT explode(sequence(
        to_date('2024-01-01'), to_date('2024-12-31'), interval 1 day
    )) AS scrape_date
""")

# ── 5. Cross join: dates × products × competitors ────────────────────
# Not every competitor scrapes every product every day (~60 % coverage)
raw = dates_df.crossJoin(products_df).crossJoin(competitors_df) \
    .withColumn("_keep", rand()) \
    .filter(col("_keep") < 0.60) \
    .drop("_keep")

# ── 6. Generate scrape IDs and timestamps ─────────────────────────────
raw = raw.withColumn("scrape_id", monotonically_increasing_id()) \
    .withColumn("scrape_timestamp",
        expr("concat(scrape_date, 'T', lpad(cast(floor(rand() * 24) as string), 2, '0'), ':', lpad(cast(floor(rand() * 60) as string), 2, '0'), ':00.000Z')")
    )

# ── 7. Price generation with seasonal discounts ──────────────────────
raw = raw \
    .withColumn("day_str", date_format(col("scrape_date"), "MM-dd")) \
    .withColumn(
        "discount_mult",
        when((col("scrape_date") >= "2024-11-25") & (col("scrape_date") <= "2024-11-30"),
             0.70 + rand() * 0.15)   # White Friday: 15-30% off
        .when((col("scrape_date") >= "2024-03-10") & (col("scrape_date") <= "2024-04-09"),
             0.80 + rand() * 0.10)   # Ramadan: 10-20% off
        .when(col("day_str") == "09-23",
             0.75 + rand() * 0.10)   # National Day: 15-25% off
        .otherwise(0.95 + rand() * 0.10)  # Normal: 0-5% off
    ) \
    .withColumn("price_jitter", 1.0 + (rand() - 0.5) * 0.10) \
    .withColumn("current_price",
        spark_round(
            col("our_unit_price") * col("base_price_factor")
            * col("discount_mult") * col("price_jitter"), 2
        )
    ) \
    .withColumn("original_price",
        spark_round(col("our_unit_price") * col("base_price_factor") * col("price_jitter"), 2)
    ) \
    .withColumn("discount_percentage",
        spark_round((1 - col("current_price") / col("original_price")) * 100, 1)
    ) \
    .withColumn("rating", spark_round(3.0 + rand() * 2.0, 1)) \
    .withColumn("review_count", (rand() * 500).cast("int")) \
    .withColumn("in_stock",
        when(rand() < 0.92, lit(True)).otherwise(lit(False))
    ) \
    .withColumn("currency", lit("SAR"))

# ── 8. Inject noise (~7 %) ────────────────────────────────────────────
noisy = raw \
    .withColumn("_noise", rand()) \
    .withColumn("current_price",                                     # 2% null prices
        when(col("_noise") < 0.02, lit(None).cast("double")).otherwise(col("current_price"))
    ) \
    .withColumn("rating",                                            # 3% null ratings
        when((col("_noise") >= 0.02) & (col("_noise") < 0.05),
             lit(None).cast("double")).otherwise(col("rating"))
    ) \
    .withColumn("review_count",                                      # 2% null review counts
        when((col("_noise") >= 0.05) & (col("_noise") < 0.07),
             lit(None).cast("int")).otherwise(col("review_count"))
    )

# ── 9. Add ~3 % duplicate scrapes ─────────────────────────────────────
dupes = noisy.sample(False, 0.03)
noisy = noisy.unionAll(dupes)

# ── 10. Final select ──────────────────────────────────────────────────
final = noisy.select(
    col("scrape_id"),
    col("scrape_timestamp"),
    col("scrape_date"),
    col("competitor_id"),
    col("competitor_name"),
    col("competitor_country"),
    col("product_name"),
    col("product_category"),
    col("product_subcategory"),
    col("current_price"),
    col("original_price"),
    col("discount_percentage"),
    col("currency"),
    col("in_stock"),
    col("rating"),
    col("review_count"),
    date_format(col("scrape_date"), "yyyy").cast("int").alias("year"),
    spark_month(col("scrape_date")).alias("month"),
    date_format(col("scrape_date"), "d").cast("int").alias("day"),
)

# ── 11. Write to ADLS ─────────────────────────────────────────────────
final.write \
    .mode("overwrite") \
    .option("header", "true") \
    .partitionBy("year", "month", "day") \
    .csv(ADLS_PATH)

row_count = final.count()
print(f"✅ Competitor pricing written to {ADLS_PATH}")
print(f"   Rows: {row_count:,}")
print(f"   Competitors: Noon, Amazon.sa, Jarir, Extra, Lulu Hypermarket")
print(f"   Products: {len(products)} (from silver_products)")
print(f"   Noise: ~7% (null prices/ratings, duplicates)")
