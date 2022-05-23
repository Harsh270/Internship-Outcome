# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/yellow_tripdata_2021_01.csv"
file_type = "csv"

# CSV options
infer_schema = "True"
first_row_is_header = "True"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

df.display()

# COMMAND ----------

# MAGIC %md ### Query 1
# MAGIC #####  Add a column named as "Revenue" into dataframe which is the sum of the below columns 'Fare_amount','Extra','MTA_tax','Improvement_surcharge','Tip_amount','Tolls_amount','Total_amount'

# COMMAND ----------

df=df.withColumn("Revenue",df["fare_amount"]+df["extra"]+df["mta_tax"]+df["tip_amount"]+df["tolls_amount"]+df["improvement_surcharge"]+df["total_amount"])

# COMMAND ----------

df.printSchema()

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md ###Query 2
# MAGIC #####  Increasing count of total passengers in New York City by area

# COMMAND ----------

df.groupby("PULocationID").sum("passenger_count").show()

# COMMAND ----------

# MAGIC %md ### Query 3
# MAGIC #####  Realtime Average fare/total earning amount earned by 2 vendors

# COMMAND ----------

average=df["fare_amount"]/df["total_amount"]

# COMMAND ----------

df=df.withColumn("Average of fare and Total Amount",average)

# COMMAND ----------

df.groupby("VendorID").sum("Average of fare and Total Amount").filter("VendorID=2").show()

# COMMAND ----------

# MAGIC %md ### Query 4
# MAGIC ##### Moving Count of payments made by each payment mode

# COMMAND ----------

df.groupby("payment_type").count().show()

# COMMAND ----------

# MAGIC %md ### Query 5
# MAGIC #####  Highest two gaining vendor's on a particular date with no of passenger and total distance by cab

# COMMAND ----------

display(df)

# COMMAND ----------

max_total=df.agg({"total_amount":"max"}).first()[0]
max_total

# COMMAND ----------

df.select(["VendorID","tpep_pickup_datetime","passenger_count","trip_distance"]).filter(df.total_amount==max_total).display()

# COMMAND ----------

# MAGIC %md ### Query 6
# MAGIC ##### Most no of passenger between a route of two location.

# COMMAND ----------

df.groupby("PULocationID","DOLocationID").sum("passenger_count").display()

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md ### Query 7
# MAGIC #####  Get top pickup locations with most passengers in last 5/10 seconds.

# COMMAND ----------

from pyspark.sql.window import *

# COMMAND ----------

time=df.groupby("PULocationID",window("tpep_pickup_datetime","5 seconds")).agg(sum("passenger_count").alias("Total_Passanger"))

# COMMAND ----------

time_df=time.select(time.window.start.cast("String").alias("Start_time"),time.window.end.cast("String").alias("end_time"),"Total_Passanger","PULocationID")

# COMMAND ----------

time_df.orderBy(time_df.Start_time.desc(),time_df.end_time.desc()).display()

# COMMAND ----------

