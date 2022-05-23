# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

data=[["Harsh",21,"MCA"],["Yash",19,"BCA"],["Kapil",25,"MBA"]]

# COMMAND ----------

schema=StructType([
    StructField("Name",StringType(),True),
    StructField("Age",IntegerType(),True),
    StructField("Course",StringType(),True),
])

# COMMAND ----------

df=spark.createDataFrame(data,schema)

# COMMAND ----------

df.display()

# COMMAND ----------

df.write.format("delta").mode("overwrite").save("/FileStore/tables/BDA101_Task2")

# COMMAND ----------

sql("SET spark.databricks.delta.formatCheck.enabled=false")

# COMMAND ----------

# MAGIC %fs ls  FileStore/tables

# COMMAND ----------

