# 🛒 E-Commerce Data Engineering Project (End-to-End)
-------------------------------------------------------------------------------------------
# 📌 Project Overview
 This project demonstrates an end-to-end Data Engineering pipeline using PySpark on Databricks Community Edition.
A real-world E-commerce dataset from Kaggle is processed from raw CSV files into analytics-ready fact and dimension tables following industry-standard data engineering practices.
The project is designed for freshers and focuses on clarity, correctness, and production thinking. 
................................................................................................
# 🧱 High-Level Architecture

# Kaggle CSV Dataset


→ PySpark Ingestion (Databricks)


→ Data Cleaning & Transformation


→ Fact & Dimension Modeling (Star Schema)


→ Partitioned Parquet Storage


→ Analytics using Spark SQL


# 📂 Dataset Details


Source: Kaggle


Dataset Name: Brazilian E-Commerce Public Dataset by Olist


Data Characteristics


Real transactional E-commerce data


Customer, order, product, seller, and payment information


Suitable for batch processing and analytics


Storage


Uploaded to Databricks FileStore


Used as raw input for PySpark processing


# 🛠 Technologies Used


Databricks Community Edition


PySpark


Spark SQL


Parquet (Columnar Storage)


Python


# 🗂 Data Modeling Approach


⭐ Fact Table


Fact Orders


Represents transactional order-level data


Stores measures such as order value and freight cost


Optimized for analytical queries


# 📐 Dimension Tables


Customers Dimension – customer location details


Products Dimension – product category information


Sellers Dimension – seller geographic details


This follows a Star Schema, commonly used in data warehouses for fast querying and scalability.


# 🔄 Data Pipeline Flow


Raw CSV files are ingested using PySpark


Data is cleaned by removing duplicates and handling missing values


Only valid business records (delivered orders) are processed


Business metrics such as total order amount are derived


Fact and dimension tables are created


Data is stored in partitioned Parquet format


Spark SQL is used to perform analytical queries


# 📊 Analytics Performed


Revenue analysis by customer location


Top-selling product categories


Seller performance analysis


Monthly and yearly sales trends


These analytics help answer real business questions using large-scale data.


# ▶️ How to Run the Project


Create a Databricks Community Edition account


Upload the Kaggle CSV files to Databricks FileStore


Import the notebook into Databricks


Execute the notebook sequentially from start to end


# 🎯 Key Learnings from This Project


Building a complete batch data pipeline


Applying data cleaning and transformation techniques


Designing fact and dimension tables


Using Parquet and partitioning for performance


Writing analytical queries using Spark SQL

 
Understanding real-world E-commerce data


------------------------------------------------------------------------------------------
