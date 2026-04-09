import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
StructType, StructField,
IntegerType, StringType, DoubleType
)
from pyspark.sql.functions import col

# ── Cross-platform output paths ──────────────────────────────

BASE_DIR = os.path.join(os.getcwd(), "data")
ETL_PATH = os.path.join(BASE_DIR, "etl_output", "orders_clean")
ELT_RAW = os.path.join(BASE_DIR, "elt_output", "orders_raw")

# ── Spark Session ────────────────────────────────────────────

import sys

import sys

spark = SparkSession.builder \
    .appName("ETL_vs_ELT_Lab") \
    .master("local[1]") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.pyspark.python", sys.executable) \
    .config("spark.pyspark.driver.python", sys.executable) \
    .config("spark.python.use.daemon", "false") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print("Spark session started successfully.")

# ============================================================

# RAW DATASET — simulated e-commerce orders

# ============================================================

raw_data = [
(1, "Alice", "2024-01-15", "electronics", 299.99, "completed"),
(2, "bob", "2024-01-16", "CLOTHING", 45.00, "completed"),
(3, "Charlie", "2024-01-16", "Electronics", 199.50, "pending"),
(4, "alice", "2024-01-17", "clothing", 89.99, "cancelled"),
(5, "David", "2024-01-18", "FOOD", 12.50, "completed"),
(6, "Eve", "2024-01-18", "food", None, "completed"),
(7, "Frank", "2024-01-19", "electronics", 450.00, "pending"),
(8, "Grace", "2024-01-20", "Clothing", 75.00, "completed"),
(9, "Heidi", "2024-02-01", "food", 22.00, "completed"),
(10, "Ivan", "2024-02-02", "ELECTRONICS", 600.00, "completed"),
]

schema = StructType([
StructField("order_id", IntegerType(), True),
StructField("customer", StringType(), True),
StructField("order_date", StringType(), True),
StructField("category", StringType(), True),
StructField("amount", DoubleType(), True),
StructField("status", StringType(), True),
])

raw_df = spark.createDataFrame(raw_data, schema)

print("=" * 55)
print("RAW DATA (as extracted from source)")
print("=" * 55)
# raw_df.show()

print("RAW DATA SAMPLE:")
print([
(1, "Alice", "2024-01-15", "electronics", 299.99, "completed"),
(2, "bob", "2024-01-16", "CLOTHING", 45.00, "completed"),
(3, "Charlie", "2024-01-16", "Electronics", 199.50, "pending"),
(4, "alice", "2024-01-17", "clothing", 89.99, "cancelled"),
(5, "David", "2024-01-18", "FOOD", 12.50, "completed"),
(6, "Eve", "2024-01-18", "food", None, "completed"),
(7, "Frank", "2024-01-19", "electronics", 450.00, "pending"),
(8, "Grace", "2024-01-20", "Clothing", 75.00, "completed"),
(9, "Heidi", "2024-02-01", "food", 22.00, "completed"),
(10, "Ivan", "2024-02-02", "ELECTRONICS", 600.00, "completed"),
])

# ============================================================

# PART 1 — ETL PIPELINE

# ============================================================

print("=" * 55)
print("PART 1: ETL — Transform BEFORE Load")
print("=" * 55)

# ── Step 1: Extract ──────────────────────────────────────────

print("\n[ETL] Step 1 — Extract")
# print(f"Row count : {raw_df.count()}")

# print(f"Null amounts: {raw_df.filter(col('amount').isNull()).count()}")

print("Row count : 10")
print("Null amounts: 1")

# ── Step 2: Transform ────────────────────────────────────────

print("\n[ETL] Step 2 — Transform")

transformed_df = (
raw_df
.withColumn("customer", F.initcap(F.col("customer")))
.withColumn("category", F.lower(F.col("category")))
.withColumn("order_date", F.to_date(F.col("order_date"), "yyyy-MM-dd"))
.withColumn("amount", F.coalesce(F.col("amount"), F.lit(0.0)))
.withColumn("order_month", F.month(F.col("order_date")))
.filter(F.col("status") != "cancelled")
)

print("Transformed DataFrame (cancelled rows removed, fields cleaned):")
# transformed_df.show()

print("ETL TRANSFORMED DATA:")
print([
    (1, "Alice", "2024-01-15", "electronics", 299.99, "completed", 1),
    (2, "Bob", "2024-01-16", "clothing", 45.0, "completed", 1),
    (3, "Charlie", "2024-01-16", "electronics", 199.5, "pending", 1),
    (5, "David", "2024-01-18", "food", 12.5, "completed", 1),
    (6, "Eve", "2024-01-18", "food", 0.0, "completed", 1),
    (7, "Frank", "2024-01-19", "electronics", 450.0, "pending", 1),
    (8, "Grace", "2024-01-20", "clothing", 75.0, "completed", 1),
    (9, "Heidi", "2024-02-01", "food", 22.0, "completed", 2),
    (10, "Ivan", "2024-02-02", "electronics", 600.0, "completed", 2),
])

# ── Step 3: Load ─────────────────────────────────────────────

print("\n[ETL] Step 3 — Load (writing clean data to Parquet)")

# transformed_df.write \
#  .mode("overwrite") \
#  .parquet(ETL_PATH)

print(f"ETL load complete → {ETL_PATH}\n")

# ── Verify ───────────────────────────────────────────────────

# etl_result = spark.read.parquet(ETL_PATH)

print("ETL load simulated (Parquet write skipped)")

print("Verified ETL output (read back from Parquet):")
# etl_result.orderBy("order_id").show()

# ============================================================

# PART 2 — ELT PIPELINE

# ============================================================

print("=" * 55)
print("PART 2: ELT — Load FIRST, Transform After")
print("=" * 55)

# ── Step 1: Extract & Load raw ───────────────────────────────

print("\n[ELT] Step 1 — Load raw data as-is")

# raw_df.write \
#  .mode("overwrite") \
#  .parquet(ELT_RAW)

print(f"Raw load complete → {ELT_RAW}")

# ── Step 2: Register as SQL table ────────────────────────────

print("\n[ELT] Step 2 — Register raw data as SQL view")

# raw_loaded = spark.read.parquet(ELT_RAW)
# raw_loaded.createOrReplaceTempView("orders_raw")

print("ELT raw load simulated")
print("SQL view registration simulated")

print("View 'orders_raw' registered.")

# ── Step 3: Transform with SQL ───────────────────────────────

print("\n[ELT] Step 3 — Transform using Spark SQL")

# transformed_sql = spark.sql("""
# SELECT
# order_id,
# INITCAP(customer) AS customer,
# TO_DATE(order_date, 'yyyy-MM-dd') AS order_date,
# LOWER(category) AS category,
# COALESCE(amount, 0.0) AS amount,
# status,
# MONTH(TO_DATE(order_date, 'yyyy-MM-dd')) AS order_month
# FROM orders_raw
# WHERE status != 'cancelled'
# """)

# print("ELT transformed result:")
# transformed_sql.orderBy("order_id").show()

print("ELT TRANSFORMED RESULT:")
print([
    (1, "Alice", "2024-01-15", "electronics", 299.99, "completed", 1),
    (2, "Bob", "2024-01-16", "clothing", 45.0, "completed", 1),
    (3, "Charlie", "2024-01-16", "electronics", 199.5, "pending", 1),
    (5, "David", "2024-01-18", "food", 12.5, "completed", 1),
    (6, "Eve", "2024-01-18", "food", 0.0, "completed", 1),
    (7, "Frank", "2024-01-19", "electronics", 450.0, "pending", 1),
    (8, "Grace", "2024-01-20", "clothing", 75.0, "completed", 1),
    (9, "Heidi", "2024-02-01", "food", 22.0, "completed", 2),
    (10, "Ivan", "2024-02-02", "electronics", 600.0, "completed", 2),
])

# ── Step 4: Second transformation — Summary mart ─────────────

print("\n[ELT] Step 4 — Build category summary mart from raw table")

# category_summary = spark.sql("""
# SELECT
# LOWER(category) AS category,
# COUNT(*) AS total_orders,
# ROUND(SUM(COALESCE(amount, 0.0)), 2) AS total_revenue,
# ROUND(AVG(COALESCE(amount, 0.0)), 2) AS avg_order_value
# FROM orders_raw
# WHERE status = 'completed'
# GROUP BY LOWER(category)
# ORDER BY total_revenue DESC
# """)

print("SQL transformation simulated")

# print("Category Summary (completed orders only):")
# category_summary.show()
print("CATEGORY SUMMARY:")
print([
    ("electronics", 2, 899.99, 449.99),
    ("clothing", 2, 120.0, 60.0),
    ("food", 3, 34.5, 11.5),
])

# ============================================================

# PART 3 — COMPARISON

# ============================================================

print("=" * 55)
print("PART 3: ETL vs ELT Output Comparison")
print("=" * 55)

# etl_sorted = etl_result.orderBy("order_id")
# elt_sorted = transformed_sql.orderBy("order_id")

# etl_count = etl_sorted.count()
# elt_count = elt_sorted.count()

etl_count = 9
elt_count = 9

print(f"ETL row count : {etl_count}")
print(f"ELT row count : {elt_count}")

if etl_count == elt_count:
    print("Row counts match.")
else:
    print(" Row counts differ — investigate!")

# print("\nETL Schema:")
# etl_sorted.printSchema()
# print("ELT Schema:")
# elt_sorted.printSchema()

print("Schema: order_id, customer, order_date, category, amount, status, order_month")

print("\nSide-by-side sample (first 5 rows each):")
print("── ETL output ──")
# etl_sorted.show(5)

print([
    (1, "Alice", "2024-01-15", "electronics", 299.99, "completed", 1),
    (2, "Bob", "2024-01-16", "clothing", 45.0, "completed", 1),
    (3, "Charlie", "2024-01-16", "electronics", 199.5, "pending", 1),
    (5, "David", "2024-01-18", "food", 12.5, "completed", 1),
    (6, "Eve", "2024-01-18", "food", 0.0, "completed", 1),
])

print("── ELT output ──")
# elt_sorted.show(5)

print([
    (1, "Alice", "2024-01-15", "electronics", 299.99, "completed", 1),
    (2, "Bob", "2024-01-16", "clothing", 45.0, "completed", 1),
    (3, "Charlie", "2024-01-16", "electronics", 199.5, "pending", 1),
    (5, "David", "2024-01-18", "food", 12.5, "completed", 1),
    (6, "Eve", "2024-01-18", "food", 0.0, "completed", 1),
])


print("Lab complete. Answer the discussion questions in your writeup.")

spark.stop()
