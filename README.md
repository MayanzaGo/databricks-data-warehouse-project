# Databricks Data Warehouse Project: Star Schema & OLAP Analysis

## Overview
This project demonstrates **Data Warehouse design** and **OLAP analysis** using **PySpark** in **Databricks**.  
It follows a **Star Schema** approach with one fact table (`sales_fact`) and four dimension tables:
- `date_dim`
- `product_dim`
- `customer_dim`
- `store_dim`

The workflow includes:
1. **Synthetic Data Generation** with `Faker`
2. **Data Modeling** in a Star Schema format
3. **Delta Table storage** to simulate Data Warehouse persistence
4. **OLAP-style queries** for business insights
5. **ER Diagram visualization** of the schema

---

## Technologies Used
- **Databricks** (PySpark runtime)
- **Delta Lake** for table storage
- **Faker** for synthetic data generation
- **Matplotlib & NetworkX** for ER diagram visualization
- **Data Modeling** with Star Schema

---

## Example Queries
```python
# Total Sales by Category
spark.table("sales_fact") \
    .join(spark.table("product_dim"), "ProductID") \
    .groupBy("Category") \
    .agg(sum("Sales_Amount").alias("Total_Sales")) \
    .orderBy(desc("Total_Sales")) \
    .show()
