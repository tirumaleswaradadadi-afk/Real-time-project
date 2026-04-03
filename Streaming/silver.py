# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS streaming.realtimestreaming.silver
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

# Read Bronze table as streaming source
bronze_stream = spark.readStream.table("streaming.realtimestreaming.bronze")

# Silver Transformations
silver_df = (
    bronze_stream.na.replace("", None)

        # Standardize Fat Content
        .withColumn(
            "Item_Fat_Content",
            when(col("Item_Fat_Content").isin("LF", "low fat", "Low Fat"), "Low Fat")
            .when(col("Item_Fat_Content").isin("reg", "Regular"), "Regular")
            .otherwise(col("Item_Fat_Content"))
        )

        # Clean Outlet Size (empty → null)
        .withColumn(
            "Outlet_Size",
            when(col("Outlet_Size") == "", None).otherwise(col("Outlet_Size"))
        )

        # Remove duplicate rows
        .dropDuplicates(["Item_Identifier", "Outlet_Identifier"])

        # Filter out rows where key columns are null
        .filter(col("Item_Identifier").isNotNull())
)

# COMMAND ----------

query = (
    silver_df.writeStream
        .format("delta")
        .option("checkpointLocation",
                "/Volumes/streaming/realtimestreaming/destinationdata/silverdata_checkpoint")
        .outputMode("append")
        .trigger(availableNow=True)   # CE requirement
        .table("streaming.realtimestreaming.silver")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from streaming.realtimestreaming.silver
# MAGIC order by Item_Identifier

# COMMAND ----------


