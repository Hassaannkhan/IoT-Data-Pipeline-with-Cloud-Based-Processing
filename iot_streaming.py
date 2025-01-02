# Databricks notebook source
iot_endp = ""
ehConfig = {   
    "eventhubs.connectionString": sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(iot_endp),
    "ehName": "iotdevice",
    "eventhubs.consumerGroup": "$Default"
}

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType , IntegerType

spark.conf.set("spark.databricks.delta.optimizeWrite.enabled","true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled","true")

json_schema = StructType([
    StructField("Consumer_ID", StringType(), True),
    StructField("Device_ID", StringType(), True),
    StructField("Timestamp", TimestampType(), True),
    StructField("Energy_Consumption_kWh", FloatType(), True),
    StructField("Peak_Demand_kW", FloatType(), True),
    StructField("Threshold_Limit_kWh", FloatType(), True),
    StructField("Voltage_Level", IntegerType(), True)
])


# COMMAND ----------

import pyspark.sql.functions as F

eh_stream = spark.readStream.format("eventhubs").options(**ehConfig).load().withColumn('body',F.from_json(F.col('body').cast('string'),json_schema))
eh_stream = eh_stream.select('body.*')
display(eh_stream)

# COMMAND ----------

