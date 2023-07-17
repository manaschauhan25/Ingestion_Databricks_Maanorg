# Databricks notebook source
# DBTITLE 1,Blob Storage Mounting
# MAGIC %scala
# MAGIC val containerName1 = dbutils.secrets.get(scope="maanorg_kysecret", key="blobcontainer")
# MAGIC val sas = dbutils.secrets.get(scope="maanorg_kysecret", key="blobsassourcetoken")
# MAGIC val storageaccountName = dbutils.secrets.get(scope="maanorg_kysecret", key="blobstoragename")
# MAGIC val config = "fs.azure.sas." + containerName1 + "." + storageaccountName + ".blob.core.windows.net"
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# dbutils.secrets.get(scope="maanorg_kysecret", key="blobstoragename")

# COMMAND ----------

# %scala
# val config = "fs.azure.sas.source.maanorgblobsource.blob.core.windows.net"
# val sas="sp=rl&st=2023-07-16T21:44:32Z&se=2023-07-31T05:44:32Z&spr=https&sv=2022-11-02&sr=c&sig=H34t5V%2FN%2BoG86djDSNlCUkmDj8kTFaw2DNEqh7TDP7g%3D"

# COMMAND ----------

# MAGIC %scala
# MAGIC dbutils.fs.mount(
# MAGIC   source = dbutils.secrets.get(scope="maanorg_kysecret", key="blobsourcecontainerpath"),
# MAGIC   mountPoint = "/mnt/source_blob/",
# MAGIC   extraConfigs = Map(config -> sas))

# COMMAND ----------

 dbutils.fs.ls("/mnt/source_blob/")

# COMMAND ----------

# DBTITLE 1,ADLS Mount for RAW
# MAGIC %py
# MAGIC configs = { "fs.azure.account.auth.type": "OAuth",
# MAGIC         "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
# MAGIC         "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope="maanorg_kysecret", key="spnclientid"),
# MAGIC         "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="maanorg_kysecret", key="sp-secret"),
# MAGIC         "fs.azure.account.oauth2.client.endpoint": dbutils.secrets.get(scope="maanorg_kysecret", key="endpoints")
# MAGIC            }
# MAGIC mountPoint = "/mnt/raw_datalake/"
# MAGIC
# MAGIC if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
# MAGIC     dbutils.fs.mount(
# MAGIC         source = dbutils.secrets.get(scope="maanorg_kysecret", key="datalakerawpath"),
# MAGIC         mount_point= mountPoint,
# MAGIC         extra_configs= configs
# MAGIC     )

# COMMAND ----------

dbutils.fs.ls("/mnt/raw_datalake/")

# COMMAND ----------

# DBTITLE 1,ADLS Cleansed Mounting
# MAGIC %py
# MAGIC configs = { "fs.azure.account.auth.type": "OAuth",
# MAGIC         "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
# MAGIC         "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope="maanorg_kysecret", key="spnclientid"),
# MAGIC         "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="maanorg_kysecret", key="sp-secret"),
# MAGIC         "fs.azure.account.oauth2.client.endpoint": dbutils.secrets.get(scope="maanorg_kysecret", key="endpoints")
# MAGIC            }
# MAGIC mountPoint = "/mnt/cleansed_datalake/"
# MAGIC
# MAGIC if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
# MAGIC     dbutils.fs.mount(
# MAGIC         source = dbutils.secrets.get(scope="maanorg_kysecret", key="adlscleansedpath"),
# MAGIC         mount_point= mountPoint,
# MAGIC         extra_configs= configs
# MAGIC     )

# COMMAND ----------

dbutils.fs.ls("/mnt/cleansed_datalake/")

# COMMAND ----------


