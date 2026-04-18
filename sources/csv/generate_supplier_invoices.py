# Databricks notebook source
# ============================================================================
# Source Generator: Supplier Invoices (CSV)
# ============================================================================
# Simulates weekly ERP/procurement exports — supplier invoices for inventory
# replenishment tied to real product brands from the catalog.
#
# Output : /Volumes/salla_databricks/bronze/landing_data/supplier_invoices/
# Format : CSV, partitioned by year/month
# Volume : ~5 000 rows (weekly invoices over 12 months)
# Noise  : ~6 % — duplicate invoice numbers, missing PO refs, status errors
# ============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, expr, rand, round as spark_round, when, concat, floor,
    date_format, month as spark_month, date_add, monotonically_increasing_id,
)
from pyspark.sql.types import *

spark = SparkSession.builder.getOrCreate()

ADLS_PATH = "/Volumes/salla_databricks/bronze/landing_data/supplier_invoices/"

# ── 1. Read real product brands + cost prices from Silver ──────────────
brands_data = spark.sql("""
    SELECT DISTINCT brand, category, cost_price, product_id, product_name
    FROM salla_databricks.silver.silver_products
    WHERE brand IS NOT NULL
""").collect()

# Map brands → supplier companies (realistic Saudi distributors)
SUPPLIER_MAP = {}
for b in brands_data:
    brand = b.brand
    if brand not in SUPPLIER_MAP:
        SUPPLIER_MAP[brand] = {
            "supplier_name": f"{brand} Distribution KSA",
            "supplier_id": f"SUP-{abs(hash(brand)) % 9000 + 1000}",
            "contact_email": f"accounts@{brand.lower().replace(' ', '')}.com.sa",
            "contact_phone": f"+9661{abs(hash(brand)) % 9000000 + 1000000}",
            "city": ["Riyadh", "Jeddah", "Dammam", "Khobar"][abs(hash(brand)) % 4],
        }
print(f"Unique suppliers: {len(SUPPLIER_MAP)}")

# ── 2. Generate weekly invoice dates for 2024 ─────────────────────────
dates_df = spark.sql("""
    SELECT explode(sequence(
        to_date('2024-01-07'), to_date('2024-12-31'), interval 7 day
    )) AS invoice_date
""")
print(f"Invoice periods: {dates_df.count()}")

# ── 3. Build supplier-product combinations ─────────────────────────────
sup_rows = []
for b in brands_data:
    sup = SUPPLIER_MAP[b.brand]
    sup_rows.append((
        sup["supplier_id"], sup["supplier_name"], sup["contact_email"],
        sup["contact_phone"], sup["city"],
        b.product_id, b.product_name, b.brand, float(b.cost_price),
    ))

sup_schema = StructType([
    StructField("supplier_id", StringType()),
    StructField("supplier_name", StringType()),
    StructField("contact_email", StringType()),
    StructField("contact_phone", StringType()),
    StructField("supplier_city", StringType()),
    StructField("product_id", StringType()),
    StructField("product_name", StringType()),
    StructField("brand", StringType()),
    StructField("cost_price", DoubleType()),
])
suppliers_df = spark.createDataFrame(sup_rows, sup_schema)

# ── 4. Cross join dates × suppliers (not every supplier every week) ───
raw = dates_df.crossJoin(suppliers_df) \
    .withColumn("_keep", rand()) \
    .filter(col("_keep") < 0.12) \
    .drop("_keep")

# ── 5. Generate invoice details ───────────────────────────────────────
raw = raw \
    .withColumn("invoice_id", monotonically_increasing_id()) \
    .withColumn("invoice_number",
        concat(lit("INV-"), date_format(col("invoice_date"), "yyyyMM"), lit("-"),
               col("invoice_id").cast("string"))
    ) \
    .withColumn("po_number",
        concat(lit("PO-"), col("supplier_id"), lit("-"),
               floor(rand() * 90000 + 10000).cast("string"))
    ) \
    .withColumn("quantity", (10 + floor(rand() * 490)).cast("int")) \
    .withColumn("unit_cost", spark_round(col("cost_price") * (0.85 + rand() * 0.20), 2)) \
    .withColumn("line_total", spark_round(col("quantity") * col("unit_cost"), 2)) \
    .withColumn("vat_amount", spark_round(col("line_total") * 0.15, 2)) \
    .withColumn("total_amount", spark_round(col("line_total") + col("vat_amount"), 2)) \
    .withColumn("currency", lit("SAR"))

# ── 6. Payment terms & status ─────────────────────────────────────────
raw = raw \
    .withColumn("_term_rand", rand()) \
    .withColumn("payment_terms",
        when(col("_term_rand") < 0.5, lit("Net30"))
        .when(col("_term_rand") < 0.8, lit("Net60"))
        .otherwise(lit("Net90"))
    ) \
    .withColumn("due_date",
        when(col("payment_terms") == "Net30", date_add(col("invoice_date"), 30))
        .when(col("payment_terms") == "Net60", date_add(col("invoice_date"), 60))
        .otherwise(date_add(col("invoice_date"), 90))
    ) \
    .withColumn("_status_rand", rand()) \
    .withColumn("status",
        when(col("_status_rand") < 0.65, lit("paid"))
        .when(col("_status_rand") < 0.80, lit("pending"))
        .when(col("_status_rand") < 0.92, lit("overdue"))
        .otherwise(lit("partially_paid"))
    ) \
    .withColumn("amount_paid",
        when(col("status") == "paid", col("total_amount"))
        .when(col("status") == "partially_paid",
              spark_round(col("total_amount") * (0.3 + rand() * 0.5), 2))
        .otherwise(lit(0.0))
    )

# ── 7. Inject noise (~6 %) ────────────────────────────────────────────
noisy = raw \
    .withColumn("_noise", rand()) \
    .withColumn("po_number",                                         # 3% missing PO reference
        when(col("_noise") < 0.03, lit(None).cast("string")).otherwise(col("po_number"))
    ) \
    .withColumn("contact_email",                                     # 2% typo in email
        when((col("_noise") >= 0.03) & (col("_noise") < 0.05),
             concat(col("contact_email"), lit(".invalid")))
        .otherwise(col("contact_email"))
    )

# ── 8. Add ~2 % duplicate invoice numbers ─────────────────────────────
dupes = noisy.sample(False, 0.02)
noisy = noisy.unionAll(dupes)

# ── 9. Final select ───────────────────────────────────────────────────
final = noisy.select(
    col("invoice_number"), col("invoice_date"), col("due_date"),
    col("supplier_id"), col("supplier_name"), col("contact_email"),
    col("contact_phone"), col("supplier_city"),
    col("po_number"), col("product_id"), col("product_name"), col("brand"),
    col("quantity"), col("unit_cost"), col("line_total"),
    col("vat_amount"), col("total_amount"), col("currency"),
    col("payment_terms"), col("status"), col("amount_paid"),
    date_format(col("invoice_date"), "yyyy").cast("int").alias("year"),
    spark_month(col("invoice_date")).alias("month"),
)

# ── 10. Write to ADLS ─────────────────────────────────────────────────
final.write \
    .mode("overwrite") \
    .option("header", "true") \
    .partitionBy("year", "month") \
    .csv(ADLS_PATH)

row_count = final.count()
print(f"✅ Supplier invoices written to {ADLS_PATH}")
print(f"   Rows: {row_count:,}")
print(f"   Suppliers: {len(SUPPLIER_MAP)} (mapped from product brands)")
print(f"   Noise: ~6% (missing POs, email typos, duplicate invoices)")
