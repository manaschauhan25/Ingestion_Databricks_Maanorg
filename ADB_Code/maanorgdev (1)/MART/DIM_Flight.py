# Databricks notebook source
# MAGIC %run /Users/500067883@stu.upes.ac.in/maanorgdev/Utilities

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *  FROM cleansed_maanorg.flight

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC cleansed_maanorg.flight

# COMMAND ----------

create_schema_of_existing_table('cleansed_maanorg','flight')

# COMMAND ----------

# %sql
# drop table datalake_mart.DIM_Flight

# COMMAND ----------

schema='date date,DepTime string,ArrTime string,UniqueCarrier string,FlightNum int,TailNum string,AirTime int,ArrDelay int,DepDelay int,Origin string,Distance int,Cancelled int,CancellationCode int'

# COMMAND ----------

# dbutils.fs.rm('/mnt/mart_datalake/DIM_Flight',True)

# COMMAND ----------

create_mart_table('cleansed_maanorg','flight','/mnt/mart_datalake/DIM_Flight','datalake_mart','DIM_Flight',schema)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from datalake_mart.DIM_Flight

# COMMAND ----------

column='date_year'

# COMMAND ----------

# MAGIC %sql
# MAGIC select years from (select distinct(year(date)) as date_year from datalake_mart.DIM_Flight) where date_year is not null

# COMMAND ----------

schema=create_schema_of_existing_table('cleansed_maanorg','flight')

# COMMAND ----------

schema='date date,date_year int,DepTime string,ArrTime string,UniqueCarrier string,FlightNum int,TailNum string,AirTime int,ArrDelay int,DepDelay int,Origin string,Distance int,Cancelled int,CancellationCode int'

# COMMAND ----------

def create_table_usingpartition(cleanseddb, cleansedtable, mart_location,martdb, marttable,schema,partition_col):
    try:
        if schema is None:
            schema= create_schema_of_existing_table(cleanseddb,cleansedtable)
        spark.sql(""" CREATE table If NOT EXISTS {3}.{1}
                  ({0})
                  using delta
                  location '{2}'
                  PARTITIONED BY ({4})
                  """.format(schema, marttable, mart_location, martdb,partition_col))
    except Exception as err:
        print("Error  "+ str(err))

# COMMAND ----------

create_table_usingpartition('cleansed_maanorg','flight','/mnt/mart_datalake/DIM_Flight','datalake_mart','DIM_Flight',schema,'date_year')

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC datalake_mart.DIM_Flight

# COMMAND ----------

for i in years:
    print(i.years)

# COMMAND ----------

columns=schema='date ,date_year ,DepTime ,ArrTime ,UniqueCarrier ,FlightNum ,TailNum ,AirTime ,ArrDelay ,DepDelay ,Origin ,Distance ,Cancelled ,CancellationCode '

# COMMAND ----------

def insert_using_partition(fromdb,fromtable,todb,totable,columns,partition_col_list,partition_col_name):
    if columns is None:
        columns=f_column_names(fromdb,fromtable)
    for i in partition_col_list:
        spark.sql("""
                INSERT OVERWRITE {2}.{3} PARTITION ({5}={6})
                SELECT
                {4}
                from
                {0}.{1} where year(date)={6}
                """.format(fromdb,fromtable,todb,totable,columns,partition_col_name,i.years))

# COMMAND ----------

insert_using_partition('cleansed_maanorg','flight','datalake_mart','DIM_Flight',columns,years,'date_year')

# COMMAND ----------

years= spark.sql("select years from (select distinct(year(date)) as years from datalake_mart.DIM_Flight) where years is not null order by years").collect()

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE table If NOT EXISTS {3}.{1}
# MAGIC                   ({0})
# MAGIC                   using delta
# MAGIC                   location '{2}'
# MAGIC                 

# COMMAND ----------

# def f_column_names(db,table):
#     col_names_obj=spark.sql("DESCRIBE {0}.{1}".format(db,table)).select("col_name").collect()
#     columns = ""
#     for i in col_names_obj:
#         columns+=i.col_name+','
#     return columns[0:-1]

# COMMAND ----------


# f_column_names('cleansed_maanorg','plane')
columns='date,DepTime,ArrTime,UniqueCarrier,FlightNum,TailNum ,AirTime ,ArrDelay ,DepDelay ,Origin ,Distance ,Cancelled ,CancellationCode'

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

insert_into_table('cleansed_maanorg','flight','datalake_mart','DIM_Flight',columns)

# COMMAND ----------


