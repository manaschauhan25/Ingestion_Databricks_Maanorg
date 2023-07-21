# Databricks notebook source
# MAGIC %run /Users/500067883@stu.upes.ac.in/maanorgdev/Utilities

# COMMAND ----------

df = spark.readStream.format("cloudfiles").option("cloudfiles.format",'csv')\
    .option("cloudfiles.schemaLocation","/dbfs/FileStore/tables/schema/Flight")\
    .load('/mnt/raw_datalake/Flight')

# COMMAND ----------

from pyspark.sql.functions import col, lpad

df = df.withColumn("Month", lpad(col("Month"), 2, "0"))
df = df.withColumn("DayofMonth", lpad(col("DayofMonth"), 2, "0"))
df = df.withColumn("DepTime", lpad(col("DepTime"), 4, "0"))
df = df.withColumn("CRSDepTime", lpad(col("CRSDepTime"), 4, "0"))
df = df.withColumn("ArrTime", lpad(col("ArrTime"), 4, "0"))
df = df.withColumn("CRSArrTime", lpad(col("CRSArrTime"), 4, "0"))


# COMMAND ----------

# 
# dbutils.f
# s.rm('/dbfs/FileStore/tables/checkpointLocation/Airport',True)
display(df)

# COMMAND ----------

from pyspark.sql.functions import col, when

df = df.withColumn("DepTime", when(col("DepTime") == "2400", "00:00").otherwise(expr("substring(DepTime, 1, 2) || ':' || substring(DepTime, 3, 2)")))
df = df.withColumn("CRSDepTime", when(col("CRSDepTime") == "2400", "00:00").otherwise(expr("substring(CRSDepTime, 1, 2) || ':' || substring(CRSDepTime, 3, 2)")))
df = df.withColumn("ArrTime", when(col("ArrTime") == "2400", "00:00").otherwise(expr("substring(ArrTime, 1, 2) || ':' || substring(ArrTime, 3, 2)")))
df = df.withColumn("CRSArrTime", when(col("CRSArrTime") == "2400", "00:00").otherwise(expr("substring(CRSArrTime, 1, 2) || ':' || substring(CRSArrTime, 3, 2)")))


# COMMAND ----------

# df_ = df.selectExpr(
#     "to_date(concat_ws('-',Year,Month,DayOfMonth),'yyyy-MM-DD') as date")

# COMMAND ----------

# "from_unixtime(unix_timestamp(case when DepTime=2400 then 0 else DepTime End,'HHmm'),'HH:mm') as DepTime",
#     "from_unixtime(unix_timestamp(case when CRSDepTime=2400 then 0 else CRSDepTime End,'HHmm'),'HH:mm') as CRSDepTime",
#     "from_unixtime(unix_timestamp(case when ArrTime=2400 then 0 else ArrTime End,'HHmm'),'HH:mm') as ArrTime",
#     "from_unixtime(unix_timestamp(case when CRSArrTime=2400 then 0 else CRSArrTime End,'HHmm'),'HH:mm') as CRSArrTime",

# COMMAND ----------

from pyspark.sql.functions import concat_ws

df = df.selectExpr(
    "to_date(concat_ws('-',Year,Month,DayOfMonth),'yyyy-MM-DD') as date",
    "DepTime",
    "CRSDepTime",
    "ArrTime",
    "CRSArrTime",
    "UniqueCarrier",
    "FlightNum",
    "TailNum",
    "ActualElapsedTime",
    "CRSElapsedTime",
    "AirTime",
    "ArrDelay",
    "DepDelay",
    "Origin",
    "Dest",
    "Distance",
    "TaxiIn",
    "TaxiOut",
    "Cancelled",
    "CancellationCode",
    "Diverted",
    "CarrierDelay",
    "WeatherDelay",
    "NASDelay",
    "SecurityDelay",
    "LateAircraftDelay"
)

# COMMAND ----------

display(df)

# COMMAND ----------

columns_to_caste = ['ActualElapsedTime',
 'AirTime',
 'ArrDelay',
 'CRSElapsedTime',
 'CancellationCode',
 'Cancelled',
 'CarrierDelay',
 'DepDelay',
 'Distance',
 'Diverted',
 'FlightNum',
 'LateAircraftDelay',
 'NASDelay',
 'SecurityDelay',
 'TaxiIn',
 'TaxiOut',
 'WeatherDelay']

# COMMAND ----------

from pyspark.sql.functions import col

# columns_to_cast = df_.columns  # List of column names to cast
for column in columns_to_caste:
    df = df.withColumn(column, col(column).cast("int"))


# COMMAND ----------

set(columns_to_cast) - set(('date','UniqueCarrier', 'TailNum', 'Origin', 'Dest','DepTime','CRSDepTime','ArrTime','CRSArrTime'))

# COMMAND ----------

df.schema

# COMMAND ----------

# dbutils.fs.rm("/mnt/cleansed_datalake/flight",True)
# dbutils.fs.rm("/dbfs/FileStore/tables/checkpointLocation/Flight",True)

# COMMAND ----------

spark.conf.set("spark.legacy.timeParsePolicy","Legacy")

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


