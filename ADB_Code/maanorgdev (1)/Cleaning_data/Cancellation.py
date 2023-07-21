# Databricks notebook source
# MAGIC %run /Users/500067883@stu.upes.ac.in/maanorgdev/Utilities

# COMMAND ----------

df = spark.readStream.format("cloudfiles").option("cloudfiles.format",'parquet')\
    .option("cloudfiles.schemaLocation","/dbfs/FileStore/tables/schema/Cancellation")\
    .load('/mnt/raw_datalake/Cancellation/')

# COMMAND ----------

# dbutils.fs.rm('/mnt/cleansed_datalake/cancellation',True)
# display(df)

# COMMAND ----------

# from pyspark.sql.functions import col

# df = df.withColumn('year', col('year').cast("int"))


# COMMAND ----------

df_base = df.selectExpr(
    "Code as code",
     "Description as description"
    
    )

df_base.writeStream.trigger(once=True)\
    .format("delta")\
    .option("checkpointLocation","/dbfs/FileStore/tables/checkpointLocation/Cancellation")\
    .start("/mnt/cleansed_datalake/cancellation")

# COMMAND ----------

f_cleansed_load('cancellation', '/mnt/cleansed_datalake/cancellation/', 'cleansed_maanorg')

# COMMAND ----------

# MAGIC %sql
# MAGIC select *  from cleansed_maanorg.cancellation

# COMMAND ----------


