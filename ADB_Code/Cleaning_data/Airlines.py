# Databricks notebook source
# MAGIC %run /Users/500067883@stu.upes.ac.in/maanorgdev/Utilities

# COMMAND ----------

df = spark.readStream.format("cloudfiles").option("cloudfiles.format",'json')\
    .option("cloudfiles.schemaLocation","/dbfs/FileStore/tables/schema/Airlines")\
    .load('/mnt/raw_datalake/Airlines/')

# COMMAND ----------

df= spark.read.json("dbfs:/mnt/raw_datalake/Airlines/Date-Part 2023-07-16/airlines.json")

# COMMAND ----------

from pyspark.sql.functions import explode

df1=df.select(explode("response"))
df_final=df1.select("col.*")

# COMMAND ----------

df_final.write.format("delta").mode("overwrite").save("/mnt/cleansed_datalake/airline")

# COMMAND ----------

f_cleansed_load('airline','/mnt/cleansed_datalake/airline/', 'cleansed_maanorg')

# COMMAND ----------

# MAGIC %sql
# MAGIC select *  from cleansed_maanorg.airline

# COMMAND ----------


