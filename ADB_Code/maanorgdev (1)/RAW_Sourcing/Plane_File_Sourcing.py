# Databricks notebook source
!pip install tabula-py --upgrade --quiet

# COMMAND ----------



# COMMAND ----------

list_files=dbutils.fs.ls('/mnt/source_blob')

# COMMAND ----------

files=[i.name for i in list_files if i.name.split('.')[1]=='pdf']

# COMMAND ----------

files

# COMMAND ----------

import tabula
from datetime import date
def load_to_adls(source, destination, filename, pages, outputformat):
    try:
        todays_date= date.today()
        sink_path= f"{destination}/{filename.split('.')[0]}/DatePart-{todays_date}"
        dbutils.fs.mkdirs(sink_path)
        tabula.convert_into(f"{source}/{filename}",f"/dbfs/{sink_path}/{filename.split('.')[0]}.{outputformat}",output_format=outputformat,pages=pages)
    except Exception as err:
        print("Error-->",str(err))


# COMMAND ----------

for i in files:
    load_to_adls('/dbfs/mnt/source_blob','/mnt/raw_datalake',i,'all','csv')

# COMMAND ----------


