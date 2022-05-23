# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md 
# MAGIC # Exercise #2 - Batch Ingestion
# MAGIC 
# MAGIC In this exercise you will be ingesting three batches of orders, one for 2017, 2018 and 2019.
# MAGIC 
# MAGIC As each batch is ingested, we are going to append it to a new Delta table, unifying all the datasets into one single dataset.
# MAGIC 
# MAGIC Each year, different individuals and different standards were used resulting in datasets that vary slightly:
# MAGIC * In 2017 the backup was written as fixed-width text files
# MAGIC * In 2018 the backup was written a tab-separated text files
# MAGIC * In 2019 the backup was written as a "standard" comma-separted text files but the format of the column names was changed
# MAGIC 
# MAGIC Our only goal here is to unify all the datasets while tracking the source of each record (ingested file name and ingested timestamp) should additional problems arise.
# MAGIC 
# MAGIC Because we are only concerned with ingestion at this stage, the majority of the columns will be ingested as simple strings and in future exercises we will address this issue (and others) with various transformations.
# MAGIC 
# MAGIC As you progress, several "reality checks" will be provided to you help ensure that you are on track - simply run the corresponding command after implementing the corresponding solution.
# MAGIC 
# MAGIC This exercise is broken up into 3 steps:
# MAGIC * Exercise 2.A - Ingest Fixed-Width File
# MAGIC * Exercise 2.B - Ingest Tab-Separated File
# MAGIC * Exercise 2.C - Ingest Comma-Separated File

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Setup Exercise #2</h2>
# MAGIC 
# MAGIC To get started, run the following cell to setup this exercise, declaring exercise-specific variables and functions.

# COMMAND ----------

# MAGIC %run ./_includes/Setup-Exercise-02

# COMMAND ----------

# MAGIC %md Run the following cell to preview a list of the files you will be processing in this exercise.

# COMMAND ----------

files = dbutils.fs.ls(f"{working_dir}/raw/orders/batch") # List all the files
display(files)                                           # Display the list of files

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #2.A - Ingest Fixed-Width File</h2>
# MAGIC 
# MAGIC **In this step you will need to:**
# MAGIC 1. Use the variable **`batch_2017_path`**, and **`dbutils.fs.head`** to investigate the 2017 batch file, if needed.
# MAGIC 2. Configure a **`DataFrameReader`** to ingest the text file identified by **`batch_2017_path`** - this should provide one record per line, with a single column named **`value`**
# MAGIC 3. Using the information in **`fixed_width_column_defs`** (or the dictionary itself) use the **`value`** column to extract each new column of the appropriate length.<br/>
# MAGIC   * The dictionary's key is the column name
# MAGIC   * The first element in the dictionary's value is the starting position of that column's data
# MAGIC   * The second element in the dictionary's value is the length of that column's data
# MAGIC 4. Once you are done with the **`value`** column, remove it.
# MAGIC 5. For each new column created in step #3, remove any leading whitespace
# MAGIC   * The introduction of \[leading\] white space should be expected when extracting fixed-width values out of the **`value`** column.
# MAGIC 6. For each new column created in step #3, replace all empty strings with **`null`**.
# MAGIC   * After trimming white space, any column for which a value was not specified in the original dataset should result in an empty string.
# MAGIC 7. Add a new column, **`ingest_file_name`**, which is the name of the file from which the data was read from.
# MAGIC   * This should not be hard coded.
# MAGIC   * For the proper function, see the <a href="https://spark.apache.org/docs/latest/api/python/index.html" target="_blank">pyspark.sql.functions</a> module
# MAGIC 8. Add a new column, **`ingested_at`**, which is a timestamp of when the data was ingested as a DataFrame.
# MAGIC   * This should not be hard coded.
# MAGIC   * For the proper function, see the <a href="https://spark.apache.org/docs/latest/api/python/index.html" target="_blank">pyspark.sql.functions</a> module
# MAGIC 9. Write the corresponding **`DataFrame`** in the "delta" format to the location specified by **`batch_target_path`**
# MAGIC 
# MAGIC **Special Notes:**
# MAGIC * It is possible to use the dictionary **`fixed_width_column_defs`** and programatically extract <br/>
# MAGIC   each column but, it is also perfectly OK to hard code this step and extract one column at a time.
# MAGIC * The **`SparkSession`** is already provided to you as an instance of **`spark`**.
# MAGIC * The classes/methods that you will need for this exercise include:
# MAGIC   * **`pyspark.sql.DataFrameReader`** to ingest data
# MAGIC   * **`pyspark.sql.DataFrameWriter`** to ingest data
# MAGIC   * **`pyspark.sql.Column`** to transform data
# MAGIC   * Various functions from the **`pyspark.sql.functions`** module
# MAGIC   * Various transformations and actions from **`pyspark.sql.DataFrame`**
# MAGIC * The following methods can be used to investigate and manipulate the Databricks File System (DBFS)
# MAGIC   * **`dbutils.fs.ls(..)`** for listing files
# MAGIC   * **`dbutils.fs.rm(..)`** for removing files
# MAGIC   * **`dbutils.fs.head(..)`** to view the first N bytes of a file
# MAGIC 
# MAGIC **Additional Requirements:**
# MAGIC * The unified batch dataset must be written to disk in the "delta" format
# MAGIC * The schema for the unified batch dataset must be:
# MAGIC   * **`submitted_at`**:**`string`**
# MAGIC   * **`order_id`**:**`string`**
# MAGIC   * **`customer_id`**:**`string`**
# MAGIC   * **`sales_rep_id`**:**`string`**
# MAGIC   * **`sales_rep_ssn`**:**`string`**
# MAGIC   * **`sales_rep_first_name`**:**`string`**
# MAGIC   * **`sales_rep_last_name`**:**`string`**
# MAGIC   * **`sales_rep_address`**:**`string`**
# MAGIC   * **`sales_rep_city`**:**`string`**
# MAGIC   * **`sales_rep_state`**:**`string`**
# MAGIC   * **`sales_rep_zip`**:**`string`**
# MAGIC   * **`shipping_address_attention`**:**`string`**
# MAGIC   * **`shipping_address_address`**:**`string`**
# MAGIC   * **`shipping_address_city`**:**`string`**
# MAGIC   * **`shipping_address_state`**:**`string`**
# MAGIC   * **`shipping_address_zip`**:**`string`**
# MAGIC   * **`product_id`**:**`string`**
# MAGIC   * **`product_quantity`**:**`string`**
# MAGIC   * **`product_sold_price`**:**`string`**
# MAGIC   * **`ingest_file_name`**:**`string`**
# MAGIC   * **`ingested_at`**:**`timestamp`**

# COMMAND ----------

# MAGIC %md ### Fixed-Width Meta Data 
# MAGIC 
# MAGIC The following dictionary is provided for reference and/or implementation<br/>
# MAGIC (depending on which strategy you choose to employ).
# MAGIC 
# MAGIC Run the following cell to instantiate it.

# COMMAND ----------

fixed_width_column_defs = {
  "submitted_at": (1, 15),
  "order_id": (16, 40),
  "customer_id": (56, 40),
  "sales_rep_id": (96, 40),
  "sales_rep_ssn": (136, 15),
  "sales_rep_first_name": (151, 15),
  "sales_rep_last_name": (166, 15),
  "sales_rep_address": (181, 40),
  "sales_rep_city": (221, 20),
  "sales_rep_state": (241, 2),
  "sales_rep_zip": (243, 5),
  "shipping_address_attention": (248, 30),
  "shipping_address_address": (278, 40),
  "shipping_address_city": (318, 20),
  "shipping_address_state": (338, 2),
  "shipping_address_zip": (340, 5),
  "product_id": (345, 40),
  "product_quantity": (385, 5),
  "product_sold_price": (390, 20)
}

# COMMAND ----------

# MAGIC %md ### Implement Exercise #2.A
# MAGIC 
# MAGIC Implement your solution in the following cell:

# COMMAND ----------

dbutils.fs.head(batch_2017_path)

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

schema=StructType([StructField("submitted_at", StringType(), True),
                     StructField("order_id", StringType(), True),
                     StructField("customer_id", StringType(), True),
                     StructField("sales_rep_id", StringType(), True),
                     StructField("sales_rep_ssn", StringType(), True),
                     StructField("sales_rep_first_name", StringType(), True),
                     StructField("sales_rep_last_name", StringType(), True),
                     StructField("sales_rep_address", StringType(), True),
                     StructField("sales_rep_city", StringType(), True),
                     StructField("sales_rep_state", StringType(), True),
                     StructField("sales_rep_zip", StringType(), True),
                     StructField("shipping_address_attention", StringType(), True),
                     StructField("shipping_address_address", StringType(), True),
                     StructField("shipping_address_city", StringType(), True),
                     StructField("shipping_address_state", StringType(), True),
                     StructField("shipping_address_zip", StringType(), True),
                     StructField("product_id", StringType(), True),
                     StructField("product_quantity", StringType(), True),
                     StructField("product_sold_price", StringType(), True) ])

# COMMAND ----------

df=spark.read.option("header","True").option("inferschema","true").text(batch_2017_path)
display(df)

# COMMAND ----------

df=df.select(
          df.value.substr(1,15).alias("submitted_at"),
          df.value.substr(16,40).alias("order_id"),
          df.value.substr(56,40).alias("customer_id"),
          df.value.substr(96, 40).alias("sales_rep_id"),
          df.value.substr(136,15).alias("sales_rep_ssn"),
          df.value.substr(151,15).alias("sales_rep_first_name"),
          df.value.substr(166,15).alias("sales_rep_last_name"),
          df.value.substr(181,40).alias("sales_rep_address"),
          df.value.substr(221,20).alias("sales_rep_city"),
          df.value.substr(241, 2).alias("sales_rep_state"),
          df.value.substr(243, 5).alias("sales_rep_zip"),
          df.value.substr(248, 30).alias("shipping_address_attention"),
          df.value.substr(278, 40).alias("shipping_address_address"),
          df.value.substr(318, 20).alias("shipping_address_city"),
          df.value.substr(338, 2).alias("shipping_address_state"),
          df.value.substr(340, 5).alias("shipping_address_zip"),
          df.value.substr(345, 40).alias("product_id"),
          df.value.substr(385, 5).alias("product_quantity"),
          df.value.substr(390, 20).alias("product_sold_price"),
           )


# COMMAND ----------

df.display()

# COMMAND ----------

df=(df.withColumn("submitted_at",trim("submitted_at"))
  .withColumn("order_id",trim("order_id"))
  .withColumn("customer_id",trim("customer_id"))
  .withColumn("sales_rep_id",trim("sales_rep_id"))
  .withColumn("sales_rep_ssn",trim("sales_rep_ssn"))
  .withColumn("sales_rep_first_name",trim("sales_rep_first_name"))
  .withColumn("sales_rep_last_name",trim("sales_rep_last_name"))
  .withColumn("sales_rep_address",trim("sales_rep_address"))
  .withColumn("sales_rep_city",trim("sales_rep_city"))
  .withColumn("sales_rep_state",trim("sales_rep_state"))
  .withColumn("sales_rep_zip",trim("sales_rep_zip"))
  .withColumn("shipping_address_attention",trim("shipping_address_attention"))
  .withColumn("shipping_address_address",trim("shipping_address_address"))
  .withColumn("shipping_address_city",trim("shipping_address_city"))
  .withColumn("shipping_address_state",trim("shipping_address_state"))
  .withColumn("shipping_address_zip",trim("shipping_address_zip"))
  .withColumn("product_id",trim("product_id"))
  .withColumn("product_quantity",trim("product_quantity"))
  .withColumn("product_sold_price",trim("product_sold_price")))
  

# COMMAND ----------

df=df.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in df.columns])

# COMMAND ----------

df=df.select([when(col(c)=="null",None,).otherwise(col(c)).alias(c) for c in df.columns])

# COMMAND ----------

df.display()

# COMMAND ----------

df=df.withColumn("ingest_file_name",input_file_name())

# COMMAND ----------

df.display()

# COMMAND ----------

df=df.withColumn("ingested_at",lit(meta_batch_count_2017).cast("timestamp"))

# COMMAND ----------

df.display()

# COMMAND ----------

df.write.format("delta").mode("overwrite").save(batch_target_path)

# COMMAND ----------

# MAGIC %md ### Reality Check #2.A
# MAGIC Run the following command to ensure that you are on track:

# COMMAND ----------

reality_check_02_a()

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #2.B - Ingest Tab-Separted File</h2>
# MAGIC 
# MAGIC **In this step you will need to:**
# MAGIC 1. Use the variable **`batch_2018_path`**, and **`dbutils.fs.head`** to investigate the 2018 batch file, if needed.
# MAGIC 2. Configure a **`DataFrameReader`** to ingest the tab-separated file identified by **`batch_2018_path`**
# MAGIC 3. Add a new column, **`ingest_file_name`**, which is the name of the file from which the data was read from - note this should not be hard coded.
# MAGIC 4. Add a new column, **`ingested_at`**, which is a timestamp of when the data was ingested as a DataFrame - note this should not be hard coded.
# MAGIC 5. **Append** the corresponding **`DataFrame`** to the previously created datasets specified by **`batch_target_path`**
# MAGIC 
# MAGIC **Additional Requirements**
# MAGIC * Any **"null"** strings in the CSV file should be replaced with the SQL value **null**

# COMMAND ----------

# MAGIC %md ### Implement Exercise #2.b
# MAGIC 
# MAGIC Implement your solution in the following cell:

# COMMAND ----------

dbutils.fs.head(batch_2018_path) 

# COMMAND ----------

df1=spark.read.option("header",True).option("sep","\t").csv(batch_2018_path)
df1.display()

# COMMAND ----------

df1=df1.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in df.columns])

# COMMAND ----------

df1=df1.select([when(col(c)=="null",None,).otherwise(col(c)).alias(c) for c in df.columns])

# COMMAND ----------

df1=df1.withColumn("ingest_file_name",input_file_name())

# COMMAND ----------

df1=df1.withColumn("ingested_at",lit(meta_batch_count_2017+meta_batch_count_2018).cast("timestamp"))

# COMMAND ----------

df1.write.format("delta").mode("append").save(batch_target_path)

# COMMAND ----------

# MAGIC %md ### Reality Check #2.B
# MAGIC Run the following command to ensure that you are on track:

# COMMAND ----------

reality_check_02_b()

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #2.C - Ingest Comma-Separted File</h2>
# MAGIC 
# MAGIC **In this step you will need to:**
# MAGIC 1. Use the variable **`batch_2019_path`**, and **`dbutils.fs.head`** to investigate the 2019 batch file, if needed.
# MAGIC 2. Configure a **`DataFrameReader`** to ingest the comma-separated file identified by **`batch_2019_path`**
# MAGIC 3. Add a new column, **`ingest_file_name`**, which is the name of the file from which the data was read from - note this should not be hard coded.
# MAGIC 4. Add a new column, **`ingested_at`**, which is a timestamp of when the data was ingested as a DataFrame - note this should not be hard coded.
# MAGIC 5. **Append** the corresponding **`DataFrame`** to the previously created dataset specified by **`batch_target_path`**<br/>
# MAGIC    Note: The column names in this dataset must be updated to conform to the schema defined for Exercise #2.A - there are several strategies for this:
# MAGIC    * Provide a schema that alters the names upon ingestion
# MAGIC    * Manually rename one column at a time
# MAGIC    * Use **`fixed_width_column_defs`** programaticly rename one column at a time
# MAGIC    * Use transformations found in the **`DataFrame`** class to rename all columns in one operation
# MAGIC 
# MAGIC **Additional Requirements**
# MAGIC * Any **"null"** strings in the CSV file should be replaced with the SQL value **null**<br/>

# COMMAND ----------

# MAGIC %md ### Implement Exercise #2.C
# MAGIC 
# MAGIC Implement your solution in the following cell:

# COMMAND ----------

dff=spark.read.option("header","true").schema(schema).csv(batch_2019_path)
dff.display()

# COMMAND ----------

dff=dff.withColumn("ingest_file_name",input_file_name())

# COMMAND ----------

dff=dff.withColumn("ingested_at",lit(meta_batch_count_2017+meta_batch_count_2018+meta_batch_count_2019).cast("timestamp"))

# COMMAND ----------

dff.display()


# COMMAND ----------

dff=dff.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in df.columns])
dff=dff.select([when(col(c)=="null",None,).otherwise(col(c)).alias(c) for c in df.columns])

# COMMAND ----------

dff.count()

# COMMAND ----------

dff.write.format("delta").mode("append").save(batch_target_path)

# COMMAND ----------

# MAGIC %md ### Reality Check #2.C
# MAGIC Run the following command to ensure that you are on track:

# COMMAND ----------

reality_check_02_c()

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #2 - Final Check</h2>
# MAGIC 
# MAGIC Run the following command to make sure this exercise is complete:

# COMMAND ----------

reality_check_02_final()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>