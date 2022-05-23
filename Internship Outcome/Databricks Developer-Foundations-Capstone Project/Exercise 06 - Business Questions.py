# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md 
# MAGIC # Exercise #6 - Business Questions
# MAGIC 
# MAGIC In our last exercise, we are going to execute various joins across our four tables (**`orders`**, **`line_items`**, **`sales_reps`** and **`products`**) to answer basic business questions
# MAGIC 
# MAGIC This exercise is broken up into 3 steps:
# MAGIC * Exercise 6.A - Use Database
# MAGIC * Exercise 6.B - Question #1
# MAGIC * Exercise 6.C - Question #2
# MAGIC * Exercise 6.D - Question #3

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Setup Exercise #6</h2>
# MAGIC 
# MAGIC To get started, run the following cell to setup this exercise, declaring exercise-specific variables and functions.

# COMMAND ----------

# MAGIC %run ./_includes/Setup-Exercise-06

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #6.A - Use Database</h2>
# MAGIC 
# MAGIC Each notebook uses a different Spark session and will initially use the **`default`** database.
# MAGIC 
# MAGIC As in the previous exercise, we can avoid contention to commonly named tables by using our user-specific database.
# MAGIC 
# MAGIC **In this step you will need to:**
# MAGIC * Use the database identified by the variable **`user_db`** so that any tables created in this notebook are **NOT** added to the **`default`** database

# COMMAND ----------

# MAGIC %md ### Implement Exercise #6.A
# MAGIC 
# MAGIC Implement your solution in the following cell:

# COMMAND ----------

spark.sql(f"use {user_db}")

# COMMAND ----------

# MAGIC %md ### Reality Check #6.A
# MAGIC Run the following command to ensure that you are on track:

# COMMAND ----------

reality_check_06_a()

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #6.B - Question #1</h2>
# MAGIC ## How many orders were shipped to each state?
# MAGIC 
# MAGIC **In this step you will need to:**
# MAGIC * Aggregate the orders by **`shipping_address_state`**
# MAGIC * Count the number of records for each state
# MAGIC * Sort the results by **`count`**, in descending order
# MAGIC * Save the results to the temporary view **`question_1_results`**, identified by the variable **`question_1_results_table`**

# COMMAND ----------

# MAGIC %md ### Implement Exercise #6.B
# MAGIC 
# MAGIC Implement your solution in the following cell:

# COMMAND ----------

df=spark.table("orders")
df.display()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df=df.groupby("shipping_address_state").count().orderBy(col("count").desc())
df.display()

# COMMAND ----------

df.createOrReplaceTempView(question_1_results_table)

# COMMAND ----------

# MAGIC %md ### Reality Check #6.B
# MAGIC Run the following command to ensure that you are on track:

# COMMAND ----------

reality_check_06_b()

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #6.C - Question #2</h2>
# MAGIC ## What is the average, minimum and maximum sale price for green products sold to North Carolina where the Sales Rep submitted an invalid Social Security Number (SSN)?
# MAGIC 
# MAGIC **In this step you will need to:**
# MAGIC * Execute a join across all four tables:
# MAGIC   * **`orders`**, identified by the variable **`orders_table`**
# MAGIC   * **`line_items`**, identified by the variable **`line_items_table`**
# MAGIC   * **`products`**, identified by the variable **`products_table`**
# MAGIC   * **`sales_reps`**, identified by the variable **`sales_reps_table`**
# MAGIC * Limit the result to only green products (**`color`**).
# MAGIC * Limit the result to orders shipped to North Carolina (**`shipping_address_state`**)
# MAGIC * Limit the result to sales reps that initially submitted an improperly formatted SSN (**`_error_ssn_format`**)
# MAGIC * Calculate the average, minimum and maximum of **`product_sold_price`** - do not rename these columns after computing.
# MAGIC * Save the results to the temporary view **`question_2_results`**, identified by the variable **`question_2_results_table`**
# MAGIC * The temporary view should have the following three columns: **`avg(product_sold_price)`**, **`min(product_sold_price)`**, **`max(product_sold_price)`**
# MAGIC * Collect the results to the driver
# MAGIC * Assign to the following local variables, the average, minimum, and maximum values - these variables will be passed to the reality check for validation.
# MAGIC  * **`ex_avg`** - the local variable holding the average value
# MAGIC  * **`ex_min`** - the local variable holding the minimum value
# MAGIC  * **`ex_max`** - the local variable holding the maximum value

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

line_items_df=spark.table("line_items")
line_items_df.display()

# COMMAND ----------

order_df=spark.table("orders")
order_df.display()

# COMMAND ----------

products_df=spark.table("products")
products_df.display()

# COMMAND ----------

sales_reps_df=spark.table("sales_reps")
sales_reps_df.display()

# COMMAND ----------

order_line_df=order_df.join(line_items_df,"order_id")

# COMMAND ----------

order_line_product_df=order_line_df.join(products_df,"product_id")

# COMMAND ----------

final_df=order_line_product_df.join(sales_reps_df,"sales_rep_id")
final_df.display()

# COMMAND ----------

final_df1=final_df.filter(col("color")=="green").filter(col("shipping_address_state")=="NC").filter(col("_error_ssn_format")=="true")
final_df1.display()

# COMMAND ----------

final_df2=final_df1.select(avg("product_sold_price"),min("product_sold_price"),max("product_sold_price"))
final_df2.display()

# COMMAND ----------

final_df2.createOrReplaceTempView(question_2_results_table)

# COMMAND ----------

question_2_results_table

# COMMAND ----------

from pyspark.sql.functions import avg,min,max

# COMMAND ----------

spark.sql("select * from question_2_results").display()

# COMMAND ----------

results = spark.table(question_2_results_table).collect()

ex_avg =  results[0][0]
ex_min = results[0][1]
ex_max = results[0][2]

# COMMAND ----------

# MAGIC %md ### Implement Exercise #6.C
# MAGIC 
# MAGIC Implement your solution in the following cell:

# COMMAND ----------

# MAGIC %md ### Reality Check #6.C
# MAGIC Run the following command to ensure that you are on track:

# COMMAND ----------

reality_check_06_c(ex_avg, ex_min, ex_max)

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #6.D - Question #3</h2>
# MAGIC ## What is the first and last name of the top earning sales rep based on net sales?
# MAGIC 
# MAGIC For this scenario...
# MAGIC * The top earning sales rep will be identified as the individual producing the largest profit.
# MAGIC * Profit is defined as the difference between **`product_sold_price`** and **`price`** which is then<br/>
# MAGIC   multiplied by **`product_quantity`** as seen in **(product_sold_price - price) * product_quantity**
# MAGIC 
# MAGIC **In this step you will need to:**
# MAGIC * Execute a join across all four tables:
# MAGIC   * **`orders`**, identified by the variable **`orders_table`**
# MAGIC   * **`line_items`**, identified by the variable **`line_items_table`**
# MAGIC   * **`products`**, identified by the variable **`products_table`**
# MAGIC   * **`sales_reps`**, identified by the variable **`sales_reps_table`**
# MAGIC * Calculate the profit for each line item of an order as described above.
# MAGIC * Aggregate the results by the sales reps' first &amp; last name and then sum each reps' total profit.
# MAGIC * Reduce the dataset to a single row for the sales rep with the largest profit.
# MAGIC * Save the results to the temporary view **`question_3_results`**, identified by the variable **`question_3_results_table`**
# MAGIC * The temporary view should have the following three columns: 
# MAGIC   * **`sales_rep_first_name`** - the first column by which to aggregate by
# MAGIC   * **`sales_rep_last_name`** - the second column by which to aggregate by
# MAGIC   * **`sum(total_profit)`** - the summation of the column **`total_profit`**

# COMMAND ----------

# MAGIC %md ### Implement Exercise #6.D
# MAGIC 
# MAGIC Implement your solution in the following cell:

# COMMAND ----------

final_df.display()

# COMMAND ----------

final_part2_df=final_df.withColumn("total_profit",(col("product_sold_price")-col("price"))*col("product_quantity"))
final_part2_df.display()

# COMMAND ----------

final_part3_df=final_part2_df.groupby("sales_rep_first_name","sales_rep_last_name").sum("total_profit")
final_part3_df.display()

# COMMAND ----------

final_part3_df=final_part3_df.sort(col("sum(total_profit)").desc()).limit(1)

# COMMAND ----------

final_part3_df.display()

# COMMAND ----------

final_part3_df.createOrReplaceTempView(question_3_results_table)

# COMMAND ----------

# MAGIC %md ### Reality Check #6.D
# MAGIC Run the following command to ensure that you are on track:

# COMMAND ----------

reality_check_06_d()

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #6 - Final Check</h2>
# MAGIC 
# MAGIC Run the following command to make sure this exercise is complete:

# COMMAND ----------

reality_check_06_final()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>