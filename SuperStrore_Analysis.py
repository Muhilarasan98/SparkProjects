# Databricks notebook source
### Setup and load the data

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession.builder.appName("GlobalSuperStoreAnalytics").getOrCreate()

# COMMAND ----------

df = spark.read.option("header", True).option("inferSchema", True).csv("/FileStore/superstore.csv")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.display()

# COMMAND ----------

### Data cleaning and data conversion

# COMMAND ----------

### Converting the data columns into proper format

# COMMAND ----------

df = df.withColumn("Sales", col("Sales").cast("double"))

# COMMAND ----------

df = df.withColumn("Order Date", to_date("Order Date", "MM/dd/yyyy")) \
       .withColumn("Ship Date", to_date("Ship Date", "MM/dd/yyyy"))

# COMMAND ----------

## Creating new columns for analysis

df1 = df.withColumn("Month", date_format("Order Date", "yyyy-MM")) \
    .withColumn("Shipping_Days", datediff("Ship Date","Order Date")) \
    .withColumn("Sales", col("Sales").cast("double")) \
    .withColumn("Profit", col("Profit").cast("double")) \
    .withColumn("Discount", col("Discount").cast("double"))

# COMMAND ----------

df1.display()

# COMMAND ----------

df1.printSchema()

# COMMAND ----------

## 1 - Identifying monthly sales trend

monthly_sales = df1.groupBy("Month").agg(round(sum("Sales"), 0).alias("Total Sales")).orderBy("Month")
monthly_sales.display()

# COMMAND ----------

## Top 10 cities by Sales

top_cities = df1.groupBy("City").agg(round(sum("Sales"),0).alias("City_Sales")).orderBy(desc("City_Sales")).limit(10)
top_cities.display()

# COMMAND ----------

## Top 10 Most Profitable Product
top_products = df1.groupBy("Product Name").agg(round(sum("Profit"),0).alias("Total_Profit")) \
                 .orderBy(desc("Total_Profit")).limit(10)
top_products.show()


# COMMAND ----------

## Category wise discount vs profit

category_profit = df1.groupBy("Category").agg(round(avg("Discount"),1).alias("Avg_Discount"),
                                             round(sum("Discount"),0).alias("Total_Profit"))
category_profit.display()

# COMMAND ----------

## Segmentwise Sales

segment_wise_sales = df1.groupBy("Segment").agg(round(sum("Sales"),0).alias("Segment_Sales"))
segment_wise_sales.show()

# COMMAND ----------

#High discount and  NegativeProfits
discount_loss = df1.filter((col("Discount") > 0.4) & (col("Profit") < 0)) \
                  .select("Product Name", "Discount", "Profit").distinct()
discount_loss.show(10)

# COMMAND ----------

## Repeat customers more than 3 orders
repeat_customers = df1.groupBy("Customer Name").agg(countDistinct("Order ID").alias("Order_Count")) \
    .filter("Order_Count > 3").orderBy(desc("Order_Count"))
repeat_customers.display()

# COMMAND ----------

##Regionwise Profitability
region_profit = df.groupBy("Region").agg(round(sum("Profit"),0).alias("Region_profit"))
region_profit.display()

# COMMAND ----------

