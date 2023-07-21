# Databricks notebook source
# MAGIC %run /Users/500067883@stu.upes.ac.in/maanorgdev/Utilities

# COMMAND ----------

df = spark.readStream.format("cloudfiles").option("cloudfiles.format",'csv')\
    .option("cloudfiles.schemaLocation","/dbfs/FileStore/tables/schema/PLANE")\
    .load('/mnt/raw_datalake/PLANE/')

# COMMAND ----------

dbutils.fs.rm('/mnt/cleansed_datalake/plane/',True)

# COMMAND ----------

from pyspark.sql.functions import col

df = df.withColumn('year', col('year').cast("int"))


# COMMAND ----------

df_base = df.selectExpr(
    "tailnum as tailid",
     "type", "manufacturer",
      "to_date(issue_date) as issue_date",
       "model", "status", "aircraft_type",
        "engine_type",
         "year"
    )

df_base.writeStream.trigger(once=True)\
    .format("delta")\
    .option("checkpointLocation","/dbfs/FileStore/tables/checkpointLocation/PLANE")\
    .start("/mnt/cleansed_datalake/plane")

# COMMAND ----------

f_cleansed_load('plane', '/mnt/cleansed_datalake/plane/', 'cleansed_maanorg')

# COMMAND ----------

# MAGIC %sql
# MAGIC select *  from cleansed_maanorg.plane

# COMMAND ----------


