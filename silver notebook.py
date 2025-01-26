# Databricks notebook source
# MAGIC %md
# MAGIC ###To access the data from the adls to databticks we requires 3 important credential
# MAGIC ####1)Application(client)ID
# MAGIC ####2)SecretID value (certificates and secrets)
# MAGIC ####3)Directory(tenant)ID

# COMMAND ----------

#spark.conf.set("fs.azure.account.auth.type.<storage-account>.dfs.core.windows.net", "OAuth")
#spark.conf.set("fs.azure.account.oauth.provider.type.<storage-account>.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
#spark.conf.set("fs.azure.account.oauth2.client.id.<storage-account>.dfs.core.windows.net", "<application-id>")
#spark.conf.set("fs.azure.account.oauth2.client.secret.<storage-account>.dfs.core.windows.net", service_credential)
#spark.conf.set("fs.azure.account.oauth2.client.endpoint.<storage-account>.dfs.core.windows.net", "https://login.microsoftonline.com/<directory-id>/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Data reading

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

df_trips=spark.read.format("parquet")\
              .option("inferschema",True)\
              .option("header",True)\
              .load("/FileStore/nycTaxi/green_tripdata_2024_01.parquet")

# COMMAND ----------

df_trips=spark.read.format("parquet")\
              .option("inferschema",True)\
              .option("header",True)\
              .option("recursiveFileLookup",True)\
              .load("/FileStore/nycTaxi/")

#It is always good to define your own schema when using Recursive File Lookup
#Recursive file lookup is used to ignore heirarcial file system we can directly write the first folder name and then directly mention the folder which we want to read.

# COMMAND ----------

df_trips.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Data Transformation

# COMMAND ----------

df_trips=df_trips.withColumnRenamed("store_and_fwd_flag","flag")

# COMMAND ----------

df_trips.display()

# COMMAND ----------

df_trips.printSchema()

# COMMAND ----------

df_trips=df_trips.withColumn("trip_date",to_date("lpep_pickup_datetime"))\
                 .withColumn("trip_month",month("lpep_pickup_datetime"))\
                 .withColumn("trip_year",year("lpep_pickup_datetime"))

# COMMAND ----------

df_trips.display()

# COMMAND ----------

df_trips=df_trips.select("VendorID","PULocationID","DOLocationID","fare_amount")

# COMMAND ----------

df_trips.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Data writing

# COMMAND ----------

df_trips.write.format("parquet")\
    .mode("append")\
    .option("path","silver@nycTaxiadlsgen2.dfs.core.windows.net/trips2023Data")\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #Analysis

# COMMAND ----------

df_trips.display()
