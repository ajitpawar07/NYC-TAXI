# Databricks notebook source
# MAGIC %md
# MAGIC #Data Reading, Writing and creating DELTA Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ###Data Access

# COMMAND ----------

#spark.conf.set("fs.azure.account.auth.type.<storage-account>.dfs.core.windows.net", "OAuth")
#spark.conf.set("fs.azure.account.oauth.provider.type.<storage-account>.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
#spark.conf.set("fs.azure.account.oauth2.client.id.<storage-account>.dfs.core.windows.net", "<application-id>")
#spark.conf.set("fs.azure.account.oauth2.client.secret.<storage-account>.dfs.core.windows.net", service_credential)
#spark.conf.set("fs.azure.account.oauth2.client.endpoint.<storage-account>.dfs.core.windows.net", "https://login.microsoftonline.com/<directory-id>/oauth2/token")

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ####Data Reading

# COMMAND ----------

df_trip=spark.read.format("parquet")\
             .option("inferSchema",True)\
             .option("header",True)\
             .option("recursiveFileLookup",True)\
             .load("/FileStore/nycTaxi/")

# COMMAND ----------

df_trip.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Writing the data to gold layer as delta format

# COMMAND ----------

# MAGIC %md
# MAGIC ####Creating database

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE gold

# COMMAND ----------

df_trip.write.format("delta")\
             .mode("append")\
             .option("path","abfss://gold@nycTaxiadlsgen2.dfs.core.windows.net/trips2023data")\
             .saveAsTable(gold.trip_data)
 

# COMMAND ----------

# MAGIC %md
# MAGIC ####Learning Delta Lake

# COMMAND ----------

# MAGIC %md
# MAGIC ####Versioning

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold.trip_zone

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE gold.trip_zone
# MAGIC SET borough="EMR" WHERE LocationID=1;

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM gold.trip_zone
# MAGIC WHERE LocationID=1

# COMMAND ----------

# MAGIC %sql
# MAGIC --How to see versions
# MAGIC DESCRIBE HISTORY gold.trip_zone

# COMMAND ----------

# MAGIC %md
# MAGIC ###Time Travel

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE gold.trip_zone TO VERSION AS OF 0
# MAGIC
# MAGIC SELECT * FROM gold.trip_zone

# COMMAND ----------

# MAGIC %md
# MAGIC ##Delta Table

# COMMAND ----------


