# Databricks notebook source
# DBTITLE 1,Prepare Schema
def prep_schema(location):
    try:
        df = spark.read.format('delta').load(f'{location}').limit(1)
        schema_list=df.dtypes
        schema=''
        for i in schema_list:
            schema=schema+i[0] + ' ' +i[1]+','
        schema=schema[0:len(schema)-1]
        return schema
    except Exception as err:
        print("Exception ", str(err))

# COMMAND ----------

# DBTITLE 1,Create table
# MAGIC %py
# MAGIC def f_cleansed_load(tablename, location, database):
# MAGIC     try:
# MAGIC         schema= prep_schema(location)
# MAGIC         spark.sql(""" CREATE table If NOT EXISTS {3}.{1}
# MAGIC                   ({0})
# MAGIC                   using delta
# MAGIC                   location '{2}'
# MAGIC                   """.format(schema, tablename, location,database))
# MAGIC     except Exception as err:
# MAGIC         print("Error  "+ str(err))

# COMMAND ----------

# DBTITLE 1,Checks
def count_check(dbname, tablename, operation, num_diff):
    # print(dbname, tablename, operation, num_diff)
    curr_query=f"""select operationMetrics.numOutputRows from Table_count where version = (select max(version) from Table_count where trim(lower(operation))=lower('{operation}'))"""
    # print(curr_query)
    spark.sql(f"""DESC HISTORY {dbname}.{tablename}""").createOrReplaceTempView("Table_Count")
    count_curr = spark.sql(curr_query)
    count_prev = spark.sql("""select operationMetrics.numOutputRows from Table_count where version < (select max(version) from Table_count where trim(lower(operation))='{operation}')""")
    # print(count_prev.first())
    if (count_curr.first() is None):
        final_count_curr=0
    else:
        final_count_curr = int(count_curr.first().numOutputRows)


    if (count_prev.first() is None) or count_prev.first().numOutputRows is None:
        final_count_prev=0
    else:
        final_count_prev = int(count_prev.first().numOutputRows)

    if final_count_curr-final_count_prev>=num_diff:
        raise Exception("Huge Difference: ", tablename)
    
    # print(final_count_curr, final_count_prev)


# COMMAND ----------

# %sql
# DESC HISTORY cleansed_maanorg.plane

# COMMAND ----------

# %py
# spark.sql("""DESC HISTORY cleansed_maanorg.airport""").createOrReplaceTempView("Table_Count")

# COMMAND ----------

# spark.sql("select operationMetrics.numOutputRows from Table_count where version=0").first().numOutputRows is None

# COMMAND ----------

# %sql
# DESC HISTORY cleansed_maanorg.cancellation

# COMMAND ----------

# %py
# spark.sql("""DESC HISTORY cleansed_maanorg.airline""").createOrReplaceTempView("Table_Count1")

# COMMAND ----------

# spark.sql("""select operationMetrics.numOutputRows from Table_count1 where version < (select max(version) from Table_count1 where operation="WRITE")""").first() is None

# COMMAND ----------

# spark.sql("""select operationMetrics.numOutputRows from Table_count1 where version = (select max(version) from Table_count1 where operation="WRITE")""").first() 

# COMMAND ----------

# def count_check(dbname, tablename, operation, num_diff):
#     # print(dbname, tablename, operation, num_diff)
#     curr_query=f"""select operationMetrics.numOutputRows from Table_count where version = (select max(version) from Table_count where trim(lower(operation))=lower('{operation}'))"""
#     # print(curr_query)
#     spark.sql(f"""DESC HISTORY {dbname}.{tablename}""").createOrReplaceTempView("Table_Count")
#     count_curr = spark.sql(curr_query)
#     count_prev = spark.sql("""select operationMetrics.numOutputRows from Table_count where version < (select max(version) from Table_count where trim(lower(operation))='{operation}')""")
#     # print(count_prev.first())
#     if (count_curr.first() is None):
#         final_count_curr=0
#     else:
#         final_count_curr = int(count_curr.first().numOutputRows)


#     if (count_prev.first() is None) or count_prev.first().numOutputRows is None:
#         final_count_prev=0
#     else:
#         final_count_prev = int(count_prev.first().numOutputRows)

#     if final_count_curr-final_count_prev>=num_diff:
#         print("Huge Difference ", tablename)
    
#     # print(final_count_curr, final_count_prev)


# COMMAND ----------

# count_check('cleansed_maanorg','airline','WRITE',100)

# COMMAND ----------

# count_check('cleansed_maanorg','cancellation','STREAMING UPDATE',100)

# COMMAND ----------

# list_of_tables=[('UNIQUE_CARRIERS','STREAMING UPDATE',200),('airport','STREAMING UPDATE',100),('cancellation','STREAMING UPDATE',2),('flight','STREAMING UPDATE',300),('plane','STREAMING UPDATE',100),('airline','WRITE',10)]
# for i in list_of_tables:
#     count_check('cleansed_maanorg',i[0],i[1],i[2])

# COMMAND ----------


