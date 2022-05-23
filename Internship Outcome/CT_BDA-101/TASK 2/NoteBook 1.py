# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

df=spark.read.option("header",True).option("inferschema",True).format("delta").load("/FileStore/tables/BDA101_Task2")

# COMMAND ----------

df.display()

# COMMAND ----------

df=df.withColumn("Category",lit("Student"))

# COMMAND ----------

df.display()

# COMMAND ----------

df.write.format("delta").mode("append").option("mergeschema",True).save("/FileStore/tables/BDA101_Task2")

# COMMAND ----------

dbutils.notebook.exit("Notebook 1 run sucessfully")

# COMMAND ----------

