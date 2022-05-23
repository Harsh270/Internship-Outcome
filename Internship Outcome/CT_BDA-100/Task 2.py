# Databricks notebook source
# File location and type
file_location = "/FileStore/tables/flightdatajson.csv"
file_type = "csv"

# CSV options
infer_schema = "True"
first_row_is_header = "True"
delimiter = "|"

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
.option("escape","\"")\
  .load(file_location)

display(df)

# COMMAND ----------



# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

travellerDetailSchema=ArrayType(spark.read.json(df.rdd.map(lambda row:row.travellerdetails)).schema)

# COMMAND ----------

travellerDetails=df.withColumn("travellerdetails",from_json("travellerdetails",travellerDetailSchema))

# COMMAND ----------

df1=travellerDetails.select(explode("travellerDetails")).select("col.*")

# COMMAND ----------

df1.display()

# COMMAND ----------

df.display()

# COMMAND ----------

bookjson_Schema=ArrayType(spark.read.json(df.rdd.map(lambda row2:row2.bookjson)).schema)

# COMMAND ----------

new_bookjson=df.withColumn("bookjson",from_json("bookjson",bookjson_Schema))

# COMMAND ----------

df2=new_bookjson.select(explode("bookjson")).select("col.*")

# COMMAND ----------

df2.display()

# COMMAND ----------

df2.printSchema()

# COMMAND ----------

dff=df2.withColumn("new_code",explode("ssrcode_baggage"))

# COMMAND ----------

dff1=dff.select("new_code").select("*")
        
        

# COMMAND ----------

dff1.display()

# COMMAND ----------

dff1.select(dict)

# COMMAND ----------

def replace(string):
    return string.replace("-"," ")

# COMMAND ----------

udf1=udf(replace)

# COMMAND ----------

dff1=dff1.drop("desc")

# COMMAND ----------

dff1.printSchema()

# COMMAND ----------

dff1.select(dff1.new_code.code.alias("Code"),dff1.new_code.fare.alias("Fare"),dff1.new_code.originalfare.alias("OriginalFare")).display()

# COMMAND ----------

df2_2=df2.withColumn("new_ssrcode_meal",explode("ssrcode_meal")).select("new_ssrcode_meal.*")

# COMMAND ----------

df2_2.display()

# COMMAND ----------

df2=df2.withColumn("fare",to_json(col("fare")))

# COMMAND ----------

schema_fare=ArrayType(spark.read.json(df2.rdd.map(lambda d:d.fare)).schema)

# COMMAND ----------

new_fare=df2.withColumn("fare",from_json("fare",schema_fare))

# COMMAND ----------

new_fare.display()

# COMMAND ----------

new_fare.withColumn("fare1",explode("fare")).select("fare1.*").display()

# COMMAND ----------

df2.display()

# COMMAND ----------

df2=df2.withColumn("fare_tax_1",to_json(col("fare_tax_1")))

# COMMAND ----------

faretax_schema=ArrayType(spark.read.json(df2.rdd.map(lambda l:l.fare_tax_1)).schema)

# COMMAND ----------

newtax=df2.withColumn("fare_tax_1",from_json("fare_tax_1",faretax_schema))

# COMMAND ----------

newtax.withColumn("new_faretax",explode("fare_tax_1")).select("new_faretax.*").display()

# COMMAND ----------

