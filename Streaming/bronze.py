# Databricks notebook source
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS streaming.realtimestreaming.bronze
# MAGIC (
# MAGIC   Item_Identifier STRING,
# MAGIC   Item_Weight DOUBLE,
# MAGIC   Item_Fat_Content STRING,
# MAGIC   Item_Visibility DOUBLE,
# MAGIC   Item_Type STRING,
# MAGIC   Item_MRP DOUBLE,
# MAGIC   Outlet_Identifier STRING,
# MAGIC   Outlet_Establishment_Year INT,
# MAGIC   Outlet_Size STRING,
# MAGIC   Outlet_Location_Type STRING,
# MAGIC   Outlet_Type STRING,
# MAGIC   Item_Outlet_Sales DOUBLE
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------


bronze_schema = StructType([
    StructField("Item_Identifier", StringType(), True),
    StructField("Item_Weight", DoubleType(), True),
    StructField("Item_Fat_Content", StringType(), True),
    StructField("Item_Visibility", DoubleType(), True),
    StructField("Item_Type", StringType(), True),
    StructField("Item_MRP", DoubleType(), True),
    StructField("Outlet_Identifier", StringType(), True),
    StructField("Outlet_Establishment_Year", IntegerType(), True),
    StructField("Outlet_Size", StringType(), True),
    StructField("Outlet_Location_Type", StringType(), True),
    StructField("Outlet_Type", StringType(), True),
    StructField("Item_Outlet_Sales", DoubleType(), True)
])


df = spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format", "csv")\
    .option("header", "true")\
    .schema(bronze_schema)\
    .option("cloudFiles.schemaLocation","/Volumes/streaming/realtimestreaming/destinationdata/schema_checkpoint")\
    .load("/Volumes/streaming/realtimestreaming/sourcedata/data_sales/")


df_clean = df.filter(df.Item_Identifier != "<eof>") \
             .filter(df.Item_Identifier.isNotNull())


# COMMAND ----------

#bronze_path = "/Volumes/streaming/realtimestreaming/destinationdata/bronze"
query = (
  df_clean.writeStream
    .format("delta")\
    .option("checkpointLocation", "/Volumes/streaming/realtimestreaming/destinationdata/data_checkpoint")\
    .outputMode("append")\
    .trigger(availableNow=True)\
    .table("streaming.realtimestreaming.bronze")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from streaming.realtimestreaming.bronze
# MAGIC order by Item_Identifier
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- delete from streaming.realtimestreaming.bronze
# MAGIC -- where value is null
# MAGIC -- Truncate table streaming.realtimestreaming.gold

# COMMAND ----------


