# Databricks notebook source
# MAGIC %run /Users/500067883@stu.upes.ac.in/maanorgdev/Utilities

# COMMAND ----------

df = spark.readStream.format("cloudfiles").option("cloudfiles.format",'csv')\
    .option("cloudfiles.schemaLocation","/dbfs/FileStore/tables/schema/Flight")\
    .load('/mnt/raw_datalake/Flight')

# COMMAND ----------

# dbutils.fs.rm('/dbfs/FileStore/tables/checkpointLocation/Airport',True)
display(df)

# COMMAND ----------

from pyspark.sql.functions import col

columns_to_cast = df.columns  # List of column names to cast

for column in columns_to_cast:
    if column not in ('UniqueCarrier', 'TailNum', 'Origin', 'Dest'):
        df = df.withColumn(column, col(column).cast("int"))


# COMMAND ----------

    df.writeStream.trigger(once=True)\
    .format("delta")\
    .option("checkpointLocation","/dbfs/FileStore/tables/checkpointLocation/Flight")\
    .start("/mnt/cleansed_datalake/flight")


# COMMAND ----------


f_cleansed_load('flight', '/mnt/cleansed_datalake/flight/', 'cleansed_maanorg')

# COMMAND ----------

# MAGIC %sql
# MAGIC select *  from cleansed_maanorg.flight

# COMMAND ----------


