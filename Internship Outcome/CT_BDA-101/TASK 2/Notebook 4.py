# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

df=spark.read.option("header",True).option("inferschema",True).format("delta").load("/FileStore/tables/BDA101_Task2")
df.display()

# COMMAND ----------

df=df.withColumn("Name",regexp_replace("Name","Kapil","Kajal"))
df.display()

# COMMAND ----------

df.write.format("delta").mode("append").option("mergeSchema",True).save("/FileStore/tables/BDA101_Task2")

# COMMAND ----------

dbutils.notebook.exit("Notebook 4 run sucessfully")

# COMMAND ----------

