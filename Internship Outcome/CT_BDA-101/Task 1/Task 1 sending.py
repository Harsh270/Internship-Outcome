# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

import json 

# COMMAND ----------

import random
import time

import json 
def try1():
    while True:
        x={"no_generator":random.randint(50,100)}
        time.sleep(2)
        json_object = json.dumps(x)
        print(json_object)

# COMMAND ----------

try1()

# COMMAND ----------

pip install azure-eventhub

# COMMAND ----------

pip install azure-eventhub-checkpointstoreblob-aio

# COMMAND ----------

import asyncio
import random
import time
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData
from time import time, sleep
import random
import nest_asyncio
nest_asyncio.apply()

async def run():
    # Create a producer client to send messages to the event hub.
    # Specify a connection string to your event hubs namespace and
    # the event hub name.
    producer = EventHubProducerClient.from_connection_string(conn_str="Endpoint=sb://eventhubtask1.servicebus.windows.net/;SharedAccessKeyName=randomnogenerator;SharedAccessKey=c1EmKkJ1V7RpScrtBS24/z26U4gfXWiCH+kbFf2qmkc=;EntityPath=bda2task1", eventhub_name="bda2task1")
    async with producer:
        # Create a batch.
        event_data_batch = await producer.create_batch() #partition_id parameter can also be passed with the partition key
        # Add events to the batch.
        while True:
            x={"Random_Number":random.randint(50,100)}
            sleep(2.0)
            json_object = json.dumps(x)
            event_data_batch.add(EventData(json_object))
            print("Events are send")
            # Send the batch of events to the event hub.
            await producer.send_batch(event_data_batch)
    

loop = asyncio.get_event_loop()
loop.run_until_complete(run()) 

# COMMAND ----------

l=[5,8,9,5,4,15,20,6,2]
min=l[0]
for i in l:
    if i>min:
        min=i
print(min)

# COMMAND ----------

