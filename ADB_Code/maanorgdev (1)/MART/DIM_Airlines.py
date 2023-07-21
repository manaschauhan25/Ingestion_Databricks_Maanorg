# Databricks notebook source
# MAGIC %run /Users/500067883@stu.upes.ac.in/maanorgdev/Utilities

# COMMAND ----------

# MAGIC %sql
# MAGIC use datalake_mart;
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *  FROM cleansed_maanorg.airline

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC cleansed_maanorg.airline

# COMMAND ----------

# %py
# # a=spark.sql("DESCRIBE cleansed_maanorg.plane").show()
# a=spark.sql("DESCRIBE cleansed_maanorg.plane").select("col_name", "data_type")


# COMMAND ----------

# schema_table=a.collect()

# COMMAND ----------

# create_schema_of_existing_table('cleansed_maanorg','flight')

# COMMAND ----------

create_mart_table('cleansed_maanorg','airline','/mnt/mart_datalake/DIM_Airlines','datalake_mart','DIM_Airlines',None)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from datalake_mart.DIM_Airlines

# COMMAND ----------

# def f_column_names(db,table):
#     col_names_obj=spark.sql("DESCRIBE {0}.{1}".format(db,table)).select("col_name").collect()
#     columns = ""
#     for i in col_names_obj:
#         columns+=i.col_name+','
#     return columns[0:-1]

# COMMAND ----------


# f_column_names('cleansed_maanorg','plane')

# COMMAND ----------

# def insert_into_table(fromdb,fromtable,todb,totable):
#     columns=f_column_names(fromdb,fromtable)
#     spark.sql("""
#               INSERT OVERWRITE {2}.{3}
#               SELECT
#               {4}
#               from
#               {0}.{1}
#               """.format(fromdb,fromtable,todb,totable,columns))


# COMMAND ----------

insert_into_table('cleansed_maanorg','airline','datalake_mart','DIM_Airlines',None)

# COMMAND ----------


