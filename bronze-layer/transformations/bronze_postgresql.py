# ============================================================
# Bronze Layer — PostgreSQL Ingestion (8 Materialized Views)
# ============================================================
# Source  : Azure PostgreSQL (salla-pipeline-server)
# Target  : salla_databricks.bronze
# Auth    : databricks-secrets scope (pg-username, pg-password)
# ============================================================

from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from pyspark.dbutils import DBUtils

PG_HOST     = "salla-pipeline-server.postgres.database.azure.com"
PG_DATABASE = "salla-ecommerce-db"

def _read_pg(table_name: str):
    """Read a PostgreSQL table via JDBC. Adds Bronze metadata columns."""
    spark   = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark)
    jdbc_url = f"jdbc:postgresql://{PG_HOST}:5432/{PG_DATABASE}?sslmode=require"
    return (
        spark.read.jdbc(
            jdbc_url, table_name,
            properties={
                "user":     dbutils.secrets.get("databricks-secrets", "pg-username"),
                "password": dbutils.secrets.get("databricks-secrets", "pg-password"),
                "driver":   "org.postgresql.Driver",
            },
        )
        .withColumn("_bronze_timestamp", current_timestamp())
        .withColumn("_source_system", lit("postgresql"))
    )


@dp.materialized_view(name="bronze_customers", comment="Raw customers ingested from PostgreSQL")
@dp.expect("valid_customer_id", "customer_id IS NOT NULL")
def bronze_customers():
    return _read_pg("customers")

@dp.materialized_view(name="bronze_products", comment="Raw products ingested from PostgreSQL")
@dp.expect("valid_product_id", "product_id IS NOT NULL")
def bronze_products():
    return _read_pg("products")

@dp.materialized_view(name="bronze_stores", comment="Raw stores ingested from PostgreSQL")
@dp.expect("valid_store_id", "store_id IS NOT NULL")
def bronze_stores():
    return _read_pg("stores")

@dp.materialized_view(name="bronze_sales_orders", comment="Raw sales orders ingested from PostgreSQL")
@dp.expect("valid_order_id", "order_id IS NOT NULL")
def bronze_sales_orders():
    return _read_pg("sales_orders")

@dp.materialized_view(name="bronze_payment_transactions", comment="Raw payment transactions ingested from PostgreSQL")
@dp.expect("valid_payment_id", "payment_id IS NOT NULL")
def bronze_payment_transactions():
    return _read_pg("payment_transactions")

@dp.materialized_view(name="bronze_shipping_details", comment="Raw shipping details ingested from PostgreSQL")
@dp.expect("valid_shipping_id", "shipping_id IS NOT NULL")
def bronze_shipping_details():
    return _read_pg("shipping_details")

@dp.materialized_view(name="bronze_inventory_movements", comment="Raw inventory movements ingested from PostgreSQL")
@dp.expect("valid_movement_id", "movement_id IS NOT NULL")
def bronze_inventory_movements():
    return _read_pg("inventory_movements")

@dp.materialized_view(name="bronze_product_reviews", comment="Raw product reviews ingested from PostgreSQL")
@dp.expect("valid_review_id", "review_id IS NOT NULL")
def bronze_product_reviews():
    return _read_pg("product_reviews")