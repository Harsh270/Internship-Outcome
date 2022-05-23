# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

data=[["2018-01-01","2020-10-23"]]
df=spark.createDataFrame(data,["Start","End"])

# COMMAND ----------

df = df.withColumn('Start', col('Start').cast('date')).withColumn('End', col('End').cast('date'))

# COMMAND ----------

df.display()

# COMMAND ----------

df1 = df.withColumn('Dates', explode(expr('sequence(Start, End, interval 1 day)'))).drop("Start","End")

# COMMAND ----------

df1=df1.withColumn("Id",monotonically_increasing_id())

# COMMAND ----------

df1=(df1.withColumn("year",split("Dates","-").getItem(0))
   .withColumn("month",split("Dates","-").getItem(1))
   .withColumn("Date",split("Dates","-").getItem(2)))

# COMMAND ----------

df1.display()

# COMMAND ----------

d=df1.count()

# COMMAND ----------

year=df1.select("Date").collect()[3][0]
year

# COMMAND ----------

def createfiles(df):
    for i in range(1,d):
        year=df.select("year").collect()[i][0]
        month=df.select("month").collect()[i][0]
        Date=df.select("Date").collect()[i][0]
        Dates=df.select("Dates").collect()[i][0]
        df.write.format("parquet").mode("overwrite").save("FileStore/Task5/"+str(year)+"/"+str(year)+str(month)+"/TSK_"+str(Dates)+".parquet")
    
#f'/FileStore/task5/{year}/{year}{month}/"TSK_"+{year+month+Date}.parquet'   

# COMMAND ----------

createfiles(df1)

# COMMAND ----------

display(dbutils.fs.ls("/FileStore/"))

# COMMAND ----------

dbutils.fs.rm("/FileStore/task5/", True)

# COMMAND ----------

CreateDir("2018-01-01","2020-10-23")

# COMMAND ----------



# COMMAND ----------

