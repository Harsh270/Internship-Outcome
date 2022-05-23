# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/Task3.csv"
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
  .load(file_location)\

display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.select("distinct_id").count()

# COMMAND ----------

df.count()

# COMMAND ----------

df.select("date").distinct().show()

# COMMAND ----------

# MAGIC %md ### Query 1
# MAGIC ##### Calculate unique viewers for each day

# COMMAND ----------

df.display()

# COMMAND ----------

df_watch=df.filter(col("event")=="Video Watched")

# COMMAND ----------

df_watch.display()

# COMMAND ----------

df_unique=df_watch.groupby("date").agg(countDistinct(df.distinct_id)).sort("date")

# COMMAND ----------

df_unique.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 2
# MAGIC ##### Calculate unique viewers for all possible date ranges

# COMMAND ----------

df_watch.select("date").distinct().sort('date').display()

# COMMAND ----------

df_watch.filter(df_watch.date.between("01-10-20","02-10-20")).dropDuplicates(["distinct_id"]).display()

# COMMAND ----------

df_watch.filter(df_watch.date.between("02-10-20","09-10-20")).dropDuplicates(["distinct_id"]).sort("date").count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 6
# MAGIC ##### Calculate unique viewers basis incremental data and update result table

# COMMAND ----------

df_new=spark.read.csv("/FileStore/tables/mixpanel_incremental.csv", header=True, inferSchema=True)

# COMMAND ----------

df_new.display()

# COMMAND ----------

df_new=df_new.filter(col("event")=="Video Watched")

# COMMAND ----------

df_new.display()

# COMMAND ----------

df_new_unique=df_new.groupby("date").agg(countDistinct("distinct_id")).sort("date")

# COMMAND ----------

df_new_unique.display()

# COMMAND ----------

final_df=df_unique.union(df_new_unique)

# COMMAND ----------

final_df.display()

# COMMAND ----------

df.coalesce(1).write.mode("overwrite").format("csv").save("/FileStore/tables/coalesce_check1.csv")

# COMMAND ----------

display(dbutils.fs.ls("/FileStore/tables/"))

# COMMAND ----------

df=spark.read.csv("/FileStore/tables/coalesce_check1.csv")

# COMMAND ----------

df.display()

# COMMAND ----------

df.count()

# COMMAND ----------

