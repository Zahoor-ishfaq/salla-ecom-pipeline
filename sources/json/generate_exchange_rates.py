# Databricks notebook source
# ============================================================================
# Source Generator: Exchange Rates (JSON)
# ============================================================================
# Simulates daily REST API snapshots from exchangerate-api.com.
# Base currency SAR with targets for major trading & remittance partners.
#
# Output : /Volumes/salla_databricks/bronze/landing_data/exchange_rates/
# Format : JSON (one object per line), partitioned by year/month/day
# Volume : ~5 000 rows (365 days × ~13 currency pairs + noise)
# Noise  : ~5 % — missing currencies, duplicate fetches, stale timestamps
# ============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, expr, rand, round as spark_round, when, concat,
    date_format, month as spark_month, explode, array, struct,
    monotonically_increasing_id, floor,
)
from pyspark.sql.types import *

spark = SparkSession.builder.getOrCreate()

ADLS_PATH = "/Volumes/salla_databricks/bronze/landing_data/exchange_rates/"

# ── 1. Realistic SAR exchange rates (mid-2024 approximate) ────────────
#   SAR is pegged to USD at 3.75, so USD rate is near-constant
#   Other rates fluctuate more
BASE_RATES = {
    "USD": 0.2667,    # 1 SAR = 0.2667 USD (pegged ~3.75)
    "EUR": 0.2450,    # 1 SAR ≈ 0.245 EUR
    "GBP": 0.2110,    # 1 SAR ≈ 0.211 GBP
    "AED": 0.9793,    # 1 SAR ≈ 0.98 AED (near-peg, both pegged to USD)
    "EGP": 8.2000,    # 1 SAR ≈ 8.2 EGP (high — remittance corridor)
    "INR": 22.2200,   # 1 SAR ≈ 22.22 INR (large expat community)
    "PKR": 74.3000,   # 1 SAR ≈ 74.3 PKR (large expat community)
    "BDT": 29.3000,   # 1 SAR ≈ 29.3 BDT (expat remittances)
    "PHP": 14.9000,   # 1 SAR ≈ 14.9 PHP (expat remittances)
    "KWD": 0.0818,    # 1 SAR ≈ 0.082 KWD (GCC trade)
    "BHD": 0.1005,    # 1 SAR ≈ 0.10 BHD (GCC trade)
    "CNY": 1.9300,    # 1 SAR ≈ 1.93 CNY (import trade)
    "TRY": 8.5600,    # 1 SAR ≈ 8.56 TRY (volatile)
}

# Volatility: how much each currency fluctuates daily (as fraction)
VOLATILITY = {
    "USD": 0.001,  "AED": 0.001,  "KWD": 0.002,  "BHD": 0.002,   # pegged/stable
    "EUR": 0.005,  "GBP": 0.006,  "CNY": 0.004,                    # moderate
    "INR": 0.008,  "PKR": 0.012,  "PHP": 0.007,  "BDT": 0.009,   # emerging
    "EGP": 0.020,  "TRY": 0.025,                                    # volatile
}

# ── 2. Date spine ─────────────────────────────────────────────────────
dates_df = spark.sql("""
    SELECT explode(sequence(
        to_date('2024-01-01'), to_date('2024-12-31'), interval 1 day
    )) AS date
""")

# ── 3. Build currency rows ────────────────────────────────────────────
currency_rows = [(k, v, VOLATILITY[k]) for k, v in BASE_RATES.items()]
cur_schema = StructType([
    StructField("target_currency", StringType()),
    StructField("base_rate", DoubleType()),
    StructField("volatility", DoubleType()),
])
currencies_df = spark.createDataFrame(currency_rows, cur_schema)

# ── 4. Cross join and generate rates with daily fluctuation ───────────
raw = dates_df.crossJoin(currencies_df) \
    .withColumn("daily_jitter", (rand() - 0.5) * 2 * col("volatility")) \
    .withColumn("exchange_rate",
        spark_round(col("base_rate") * (1.0 + col("daily_jitter")), 6)
    ) \
    .withColumn("inverse_rate",
        spark_round(1.0 / col("exchange_rate"), 6)
    ) \
    .withColumn("base_currency", lit("SAR")) \
    .withColumn("source", lit("exchangerate-api.com")) \
    .withColumn("api_url",
        concat(lit("https://v6.exchangerate-api.com/v6/latest/SAR/"), col("target_currency"))
    ) \
    .withColumn("fetch_timestamp",
        expr("concat(date, 'T06:', lpad(cast(floor(rand() * 60) as string), 2, '0'), ':00Z')")
    )

# ── 5. Partition columns ──────────────────────────────────────────────
raw = raw \
    .withColumn("year", date_format(col("date"), "yyyy").cast("int")) \
    .withColumn("month", spark_month(col("date"))) \
    .withColumn("day", date_format(col("date"), "d").cast("int"))

# ── 6. Inject noise (~5 %) ────────────────────────────────────────────
noisy = raw \
    .withColumn("_noise", rand()) \
    .withColumn("exchange_rate",                                     # 2% missing rate
        when(col("_noise") < 0.02, lit(None).cast("double")).otherwise(col("exchange_rate"))
    ) \
    .withColumn("fetch_timestamp",                                   # 1% stale timestamp (yesterday)
        when((col("_noise") >= 0.02) & (col("_noise") < 0.03),
             expr("concat(date_add(date, -1), 'T23:59:00Z')"))
        .otherwise(col("fetch_timestamp"))
    )

# ── 7. Add ~2 % duplicate fetches ─────────────────────────────────────
dupes = noisy.sample(False, 0.02)
noisy = noisy.unionAll(dupes)

# ── 8. Final select ───────────────────────────────────────────────────
final = noisy.select(
    col("api_url"),
    col("base_currency"),
    col("date").cast("string").alias("date"),
    col("day").cast("long"),
    col("exchange_rate"),
    col("fetch_timestamp"),
    col("inverse_rate"),
    col("month").cast("long"),
    col("source"),
    col("target_currency"),
    col("year"),
)

# ── 9. Write as JSON to ADLS ──────────────────────────────────────────
final.write \
    .mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .json(ADLS_PATH)

row_count = final.count()
print(f"✅ Exchange rates written to {ADLS_PATH}")
print(f"   Rows: {row_count:,}")
print(f"   Base: SAR → {len(BASE_RATES)} target currencies")
print(f"   Noise: ~5% (missing rates, stale timestamps, duplicate fetches)")
