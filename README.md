# 🛒 End-to-End E-Commerce Data Engineering Project | Databricks

## 📌 Overview
This repository contains an **end-to-end Data Engineering project** built using **Databricks, Apache Spark (PySpark), Spark SQL, and Delta Lake**, following the **Medallion Architecture (Bronze → Silver → Gold)**.

The project simulates a real-world **e-commerce analytics pipeline**, where raw transactional data is ingested, cleaned, transformed, and modeled into **analytics-ready fact and dimension tables** to support business reporting and insights.

---

## 🏗 Architecture – Medallion Architecture

### 🥉 Bronze Layer (Raw Data)
- Ingest raw CSV files from Data Lake
- Minimal transformation
- Schema inferred from source
- Stored as Delta tables
- Acts as historical and audit layer

### 🥈 Silver Layer (Cleaned & Transformed)
- Removed duplicate records
- Standardized timestamps
- Filtered only delivered orders
- Added derived columns (total order amount)
- Business-ready clean datasets

### 🥇 Gold Layer (Analytics & Business)
- Designed **Star Schema**
- Created Dimension and Fact tables
- Optimized for analytics and reporting
- Used Spark SQL for insights

---

## 📂 Datasets - ( E-Commerce )
- Customers  
- Orders  
- Order Items  
- Products  
- Sellers  

---

## ⚙️ Tech Stack
- Databricks
- Apache Spark (PySpark)
- Spark SQL
- Delta Lake
- Medallion Architecture

---

## 🔄 Data Pipeline Flow
1. Load raw CSV data into **Bronze Delta tables**
2. Clean, validate, and enrich data in **Silver layer**
3. Create **Fact and Dimension tables** in **Gold layer**
4. Perform business analysis using **Spark SQL**

---

## 📊 Business Insights Generated
- Revenue by customer state
- Top selling product categories
- Monthly sales trend analysis

---

## ▶️ How to Run the Project
1. Upload source CSV files to Databricks Volume / Data Lake
2. Execute Bronze layer notebook
3. Execute Silver layer notebook
4. Execute Gold layer notebook
5. Run Spark SQL queries for analytics

---

## 📌 Key Takeaways
- Practical implementation of Medallion Architecture
- Real-world PySpark transformations
- Delta Lake table management
- Fact & Dimension data modeling
- Business-driven analytics using Spark SQL

---
