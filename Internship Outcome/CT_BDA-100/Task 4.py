# Databricks notebook source
# File location and type
file_location = "/FileStore/tables/OI_MVP_Custs_Task4_.csv"
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
  .option("multiline",True)\
.load(file_location)

display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 1
# MAGIC #####  Identify duplicate customers basis Name and Address

# COMMAND ----------

duplicate_df=df.groupby("name","address").count().filter(col("count")>1)

# COMMAND ----------

duplicate_df.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Query2
# MAGIC ##### Assign Master ID to matching customers

# COMMAND ----------

df.display()

# COMMAND ----------

window=Window.partitionBy("name").orderBy("name")

# COMMAND ----------

new_df=df.withColumn("row_no",row_number().over(window))
new_df.display()

# COMMAND ----------

new_df.withColumn("Master_id",when(col("row_no")==1,monotonically_increasing_id()).otherwise(lit("Duplicate"))).display()

# COMMAND ----------

