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

# DBTITLE 1,Create cleansed table from raw table
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

# DBTITLE 1,Create mart table from cleansed table
def create_mart_table(cleanseddb, cleansedtable, mart_location,martdb, marttable,schema):
    try:
        if schema is None:
            schema= create_schema_of_existing_table(cleanseddb,cleansedtable)
        spark.sql(""" CREATE table If NOT EXISTS {3}.{1}
                  ({0})
                  using delta
                  location '{2}'
                  """.format(schema, marttable, mart_location, martdb))
    except Exception as err:
        print("Error  "+ str(err))

# COMMAND ----------

# DBTITLE 1,Get Schema of Existing Table
def create_schema_of_existing_table(database_name,table_name):
    schema_table=spark.sql("DESCRIBE {0}.{1}".format(database_name,table_name)).select("col_name", "data_type").collect()
    schema=""
    for i in schema_table:
        schema = schema+i.col_name+' '+i.data_type+','
    
    return schema[0:-1]

# COMMAND ----------

# DBTITLE 1,Insert data into table
def insert_into_table(fromdb,fromtable,todb,totable,columns):
    if columns is None:
        columns=f_column_names(fromdb,fromtable)
    spark.sql("""
              INSERT OVERWRITE {2}.{3}
              SELECT
              {4}
              from
              {0}.{1}
              """.format(fromdb,fromtable,todb,totable,columns))


# COMMAND ----------

# DBTITLE 1,Get All column lists
def f_column_names(db,table):
    col_names_obj=spark.sql("DESCRIBE {0}.{1}".format(db,table)).select("col_name").collect()
    columns = ""
    for i in col_names_obj:
        columns+=i.col_name+','
    return columns[0:-1]

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


