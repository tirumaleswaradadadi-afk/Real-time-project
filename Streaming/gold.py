# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS streaming.realtimestreaming.gold
# MAGIC (
# MAGIC   Item_Identifier STRING,
# MAGIC   Item_Type STRING,
# MAGIC   Outlet_Identifier STRING,
# MAGIC   Outlet_Type STRING,
# MAGIC   Outlet_Size STRING,
# MAGIC   Outlet_Location_Type STRING,
# MAGIC   Total_Sales DOUBLE,
# MAGIC   Total_Items BIGINT,
# MAGIC   Avg_Item_Price DOUBLE,
# MAGIC   Avg_Visibility DOUBLE,
# MAGIC   Last_Updated TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# %sql
# drop table streaming.realtimestreaming.gold

# COMMAND ----------

# Read streaming silver table
silver_stream = spark.readStream.table("streaming.realtimestreaming.silver")

# GOLD Transformations
gold_df = (
    silver_stream

        # -----------------------
        # Replace NULLs with meaningful values (dashboard friendly)
        # -----------------------
        .na.fill({
            "Item_Type": "Unknown",
            "Outlet_Size": "Unknown",
            "Outlet_Location_Type": "Unknown",
            "Outlet_Type": "Unknown",
            "Item_Fat_Content": "Unknown",
            "Item_Visibility": 0
        })
        
        # -----------------------
        # Business Aggregations
        # -----------------------
        .groupBy(
            "Item_Identifier",
            "Item_Type",
            "Outlet_Identifier",
            "Outlet_Type",
            "Outlet_Size",
            "Outlet_Location_Type"
        )
        .agg(
            sum("Item_Outlet_Sales").alias("Total_Sales"),
            count("*").alias("Total_Items"),
            avg("Item_MRP").alias("Avg_Item_Price"),
            avg("Item_Visibility").alias("Avg_Visibility")
        )

        # Add last updated timestamp
        .withColumn("Last_Updated", current_timestamp())

)

# COMMAND ----------

query = (
    gold_df.writeStream
        .format("delta")
        .option("checkpointLocation",
                "/Volumes/streaming/realtimestreaming/destinationdata/golddata_checkpoint")
        .outputMode("complete")   # ✅ Aggregations need COMPLETE mode
        .trigger(availableNow=True)
        .table("streaming.realtimestreaming.gold")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM streaming.realtimestreaming.gold
# MAGIC ORDER BY Total_Sales DESC
# MAGIC LIMIT 50;

# COMMAND ----------


