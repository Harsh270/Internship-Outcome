# Databricks notebook source
pip install azure-eventhub

# COMMAND ----------

pip install azure-eventhub-checkpointstoreblob-aio

# COMMAND ----------

pip install nest-asyncio

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Receving event script

# COMMAND ----------

import asyncio
from azure.eventhub.aio import EventHubConsumerClient
from azure.eventhub.extensions.checkpointstoreblobaio import BlobCheckpointStore

import nest_asyncio
nest_asyncio.apply()


async def on_event(partition_context, event):
    # Print the event data.
    print("Received the event: \"{}\" from the partition with ID: \"{}\"".format(event.body_as_str(encoding='UTF-8'), partition_context.partition_id))

    # Update the checkpoint so that the program doesn't read the events
    # that it has already read when you run it next time.
    await partition_context.update_checkpoint(event)

async def main():
    # Create an Azure blob checkpoint store to store the checkpoints.
    checkpoint_store = BlobCheckpointStore.from_connection_string("DefaultEndpointsProtocol=https;AccountName=1testingstorageaccount;AccountKey=bktj+DAMH/nMaRXzWhXwgaU2baZzIXu5ZyrfITHjSvgwafv5IVVPxkbftwCCby2Xkzc54g69VGEN+AStIbHEog==;EndpointSuffix=core.windows.net", "bdatasks")

    # Create a consumer client for the event hub.
    client = EventHubConsumerClient.from_connection_string("Endpoint=sb://eventhubtask1.servicebus.windows.net/;SharedAccessKeyName=randomnogenerator;SharedAccessKey=c1EmKkJ1V7RpScrtBS24/z26U4gfXWiCH+kbFf2qmkc=;EntityPath=bda2task1", consumer_group="$Default", eventhub_name="bda2task1",checkpoint_store=checkpoint_store)
    async with client:
        # Call the receive method. Read from the beginning of the partition (starting_position: "-1") or @latest from last
        await client.receive(on_event=on_event,  starting_position="-1")

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    # Run the main method.
    loop.run_until_complete(main())

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Connection string 

# COMMAND ----------

connectionString ="Endpoint=sb://eventhubtask1.servicebus.windows.net/;SharedAccessKeyName=randomnogenerator;SharedAccessKey=c1EmKkJ1V7RpScrtBS24/z26U4gfXWiCH+kbFf2qmkc=;EntityPath=bda2task1"

ehConf = {}
ehConf['eventhubs.connectionString'] = connectionString

# For 2.3.15 version and above, the configuration dictionary requires that connection string be encrypted.
ehConf['eventhubs.connectionString'] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString)

# COMMAND ----------

from pyspark.sql.functions import col,when,lit,to_json,struct
from datetime import datetime as dt
import json
startOffset = "-1"

# End at the current time. This datetime formatting creates the correct string format from a python datetime object
endTime = dt.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
startingEventPosition = {
  "offset": startOffset,  
  "seqNo": -1,            #not in use
  "enqueuedTime": None,   #not in use
  "isInclusive": True
}
endingEventPosition = {
  "offset": None,           #not in use
  "seqNo": -1,              #not in use
  "enqueuedTime": endTime,
  "isInclusive": True
}
ehConf["eventhubs.startingPosition"] = json.dumps(startingEventPosition)
ehConf["eventhubs.endingPosition"] = json.dumps(endingEventPosition)
print(ehConf)

# COMMAND ----------

df = spark \
  .readStream \
  .format("eventhubs") \
  .options(**ehConf) \
  .load()


# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

schema=StructType([
    StructField("Random_Number", IntegerType(), False)
])

# COMMAND ----------

display(df.selectExpr("String(body)"))

# COMMAND ----------

df1=(df.selectExpr("String(body)")
  .withColumn("body",from_json("body",schema))
  .select(col("body.Random_Number").alias("Random_Number")))

# COMMAND ----------

df1.display()

# COMMAND ----------

df1=df1.withColumn("Risk",when(col("Random_Number")>80,lit("High")).otherwise(lit("Low")))

# COMMAND ----------

df1.display()

# COMMAND ----------

df2=df1.select(to_json(struct("*")).alias("body"))
df2.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Send data to another event hub 

# COMMAND ----------

connectionString = "Endpoint=sb://eventhubtask1.servicebus.windows.net/;SharedAccessKeyName=resultpolicytask1;SharedAccessKey=yzkq1MqrWrU9sg1bE6NukKOYV+moiRNQq2+cX9NEZ/4=;EntityPath=resulttask1"

result = {}
result['eventhubs.connectionString'] = connectionString

# For 2.3.15 version and above, the configuration dictionary requires that connection string be encrypted.
result['eventhubs.connectionString'] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString)


# COMMAND ----------

print(result)

# COMMAND ----------

(df2.writeStream
   .format("eventhubs")
   .options(**result)
   .outputMode("update")
   .trigger(processingTime="2 second")
   .option("checkpointlocation","/streamcheckpoint")
   .start())
    

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scala Code for the same 

# COMMAND ----------

import org.apache.spark.eventhubs.{ ConnectionStringBuilder, EventHubsConf, EventPosition }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger._
val eventHubsConfIncome = EventHubsConf("Endpoint=sb://testeventhubcelebal.servicebus.windows.net/;SharedAccessKeyName=my-receiver;SharedAccessKey=H5DJOodrFvWNcebeBsuMpoZWWp6TQSJERpBJH2tKVaA=;EntityPath=testeventhub").setStartingPosition(EventPosition.fromEndOfStream)

val eventHubsConfOutcome = EventHubsConf("Endpoint=sb://testeventhubcelebal.servicebus.windows.net/;SharedAccessKeyName=my-sender;SharedAccessKey=BVG682EmLxQWpBI+flRDE0epbXQ8NvQxh2SJnQL75Dc=;EntityPath=secondeventhub").setStartingPosition(EventPosition.fromEndOfStream)

spark.readStream
.format("eventhubs")
.options(eventHubsConfIncome.toMap)
.load()
.select(get_json_object(($"body").cast("string"), "$.random").alias("random"))
.select($"random".cast("integer"))
.withColumn("Risky",when($"random">=80,"High").otherwise("Low"))
.select(to_json(struct("*")).alias("body"))
.writeStream
.format("eventhubs")
.outputMode("update")
.trigger(ProcessingTime("2 second"))
.options(eventHubsConfOutcome.toMap)
.option("checkpointLocation","/checkpoint/")
.start()

# COMMAND ----------

df1.createOrReplaceTempView("table1")