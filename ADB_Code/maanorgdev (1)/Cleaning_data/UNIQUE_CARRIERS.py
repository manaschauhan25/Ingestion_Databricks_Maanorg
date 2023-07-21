# Databricks notebook source
# MAGIC %run /Users/500067883@stu.upes.ac.in/maanorgdev/Utilities

# COMMAND ----------

df = spark.readStream.format("cloudfiles").option("cloudfiles.format",'parquet')\
    .option("cloudfiles.schemaLocation","/dbfs/FileStore/tables/schema/UNIQUE_CARRIERS")\
    .load('/mnt/raw_datalake/UNIQUE_CARRIERS/')

# COMMAND ----------

# dbutils.fs.rm('/dbfs/FileStore/tables/checkpointLocation/CancelUNIQUE_CARRIERSlation',True)
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
    .option("checkpointLocation","/dbfs/FileStore/tables/checkpointLocation/UNIQUE_CARRIERS")\
    .start("/mnt/cleansed_datalake/UNIQUE_CARRIERS")

# COMMAND ----------

f_cleansed_load('UNIQUE_CARRIERS', '/mnt/cleansed_datalake/UNIQUE_CARRIERS/', 'cleansed_maanorg')

# COMMAND ----------

# MAGIC %sql
# MAGIC select *  from cleansed_maanorg.UNIQUE_CARRIERS

# COMMAND ----------


