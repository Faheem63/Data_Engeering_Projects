# Databricks notebook source
# MAGIC %md
# MAGIC ### Import Required Functions

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read CSV Files (Raw / Bronze Layer)

# COMMAND ----------

customers_df = spark.read.csv(
    "/Volumes/workspace/default/e-com_data/customers_dataset.csv",
    header=True,
    inferSchema=True
)


order_items_df = spark.read.csv(
    "/Volumes/workspace/default/e-com_data/order_items_dataset.csv",
    header=True,
    inferSchema=True
)


orders_df = spark.read.csv(
    "/Volumes/workspace/default/e-com_data/orders_dataset.csv",
    header=True,
    inferSchema=True
)


products_df = spark.read.csv(
    "/Volumes/workspace/default/e-com_data/products_dataset.csv",
    header=True,
    inferSchema=True
)


sellers_df = spark.read.csv(
    "/Volumes/workspace/default/e-com_data/sellers_dataset.csv",
    header=True,
    inferSchema=True
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Cheking Data

# COMMAND ----------

customers_df.count(),
order_items_df.count(),
orders_df.count(),
products_df.count(),
sellers_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Cleaning & Basic Transformations

# COMMAND ----------

# Remove duplicate records
customers_df = customers_df.drop_duplicates(subset=["customer_id"])
products_df = products_df.drop_duplicates(subset=["product_id"])
sellers_df = sellers_df.drop_duplicates(subset=["seller_id"])
orders_df = orders_df.drop_duplicates(subset=["order_id"])

# COMMAND ----------

# Convert order timestamp
orders_df = orders_df.withColumn(
    "order_purchase_timestamp",
    to_timestamp(col("order_purchase_timestamp"))
)

# COMMAND ----------

# Keep only delivered orders
orders_df = orders_df.filter(col("order_status") == "delivered")

# COMMAND ----------

# Calculate total order value
order_items_df = order_items_df.withColumn(
    "total_amount",
    col("price") + col("freight_value")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Dimension Tables (Gold Layer)

# COMMAND ----------

# Customer Dimension 
dim_customers = customers_df.select(
    col("customer_id"),
    col("customer_city"),
    col("customer_state")
)

# COMMAND ----------

# Product Dimension
dim_products = products_df.select(
    col("product_id"),
    col("product_category_name")
)

# COMMAND ----------

# Seller Dimension
dim_sellers = sellers_df.select(
    col("seller_id"),
    col("seller_city"),
    col("seller_state")
)

# COMMAND ----------

# MAGIC %md
# MAGIC #  Create Fact Table (fact_orders)

# COMMAND ----------

# Use inner join and alias the tables because both table have order_id coloumn 
fact_orders = (
    order_items_df.alias("oi")
    .join(orders_df.alias("o"), col("oi.order_id") == col("o.order_id"), "inner")
    .select(
        col("oi.order_id").alias("order_id"),   
        col("o.customer_id"),                  
        col("oi.product_id"),
        col("oi.seller_id"),
        col("o.order_purchase_timestamp").alias("order_date"),
        col("oi.price"),
        col("oi.freight_value"),
        col("oi.total_amount")
    )
)


# COMMAND ----------

fact_orders.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Data as Parquet (Optimized Storage)
# MAGIC

# COMMAND ----------

base_path = "/Volumes/workspace/default/e-com_data"

dim_customers.write.mode("overwrite").parquet(f"{base_path}/dim_customers")
dim_products.write.mode("overwrite").parquet(f"{base_path}/dim_products")
dim_sellers.write.mode("overwrite").parquet(f"{base_path}/dim_sellers")
fact_orders.write.mode("overwrite").parquet(f"{base_path}/fact_orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Spark SQL View

# COMMAND ----------

# Reload each dataset
dim_customers_reloaded = spark.read.parquet(f"{base_path}/dim_customers")
dim_products_reloaded  = spark.read.parquet(f"{base_path}/dim_products")
dim_sellers_reloaded   = spark.read.parquet(f"{base_path}/dim_sellers")
fact_orders_reloaded   = spark.read.parquet(f"{base_path}/fact_orders")

# Register as SQL views
dim_customers_reloaded.createOrReplaceTempView("dim_customers_view")
dim_products_reloaded.createOrReplaceTempView("dim_products_view")
dim_sellers_reloaded.createOrReplaceTempView("dim_sellers_view")
fact_orders_reloaded.createOrReplaceTempView("fact_orders_view")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Spark SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dim_customers_view LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Revenue by State

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC c.customer_state,
# MAGIC ROUND(SUM(f.total_amount), 2) AS total_revenue
# MAGIC FROM fact_orders_view f
# MAGIC JOIN dim_customers_view c
# MAGIC ON f.customer_id = c.customer_id
# MAGIC GROUP BY c.customer_state
# MAGIC ORDER BY total_revenue DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Top Selling Product Categories

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC p.product_category_name,
# MAGIC round(SUM(f.total_amount),2) AS revenue
# MAGIC FROM fact_orders_view f
# MAGIC JOIN dim_products_view p
# MAGIC ON f.product_id = p.product_id
# MAGIC GROUP BY p.product_category_name
# MAGIC ORDER BY revenue DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Monthly Sales Trend

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC month(order_date) AS month,
# MAGIC round(SUM(total_amount),2) AS monthly_sales
# MAGIC FROM fact_orders_view
# MAGIC GROUP BY month
# MAGIC ORDER BY month;

# COMMAND ----------

# MAGIC %md
# MAGIC ###### “I used Databricks Community Edition to build an end-to-end e-commerce data pipeline.
# MAGIC ######  I ingested Kaggle CSV data using PySpark, cleaned and transformed it,
# MAGIC ######  designed fact and dimension tables, stored data in partitioned Parquet format,
# MAGIC ######  and performed analytics using Spark SQL.”

# COMMAND ----------

#...............................

# COMMAND ----------

