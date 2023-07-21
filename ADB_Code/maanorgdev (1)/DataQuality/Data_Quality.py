# Databricks notebook source
# MAGIC %run /Users/500067883@stu.upes.ac.in/maanorgdev/Utilities

# COMMAND ----------

list_of_tables=[('UNIQUE_CARRIERS','STREAMING UPDATE',200),('airport','STREAMING UPDATE',100),('cancellation','STREAMING UPDATE',2),('flight','STREAMING UPDATE',300),('plane','STREAMING UPDATE',100),('airline','WRITE',10)]
for i in list_of_tables:
    count_check('cleansed_maanorg',i[0],i[1],i[2])
