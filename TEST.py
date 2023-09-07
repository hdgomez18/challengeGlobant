# Databricks notebook source
# DBTITLE 1,Get parameters 🏍
dbutils.widgets.text("fecini","")
get_fecini = dbutils.widgets.get("fecini")

# COMMAND ----------

# DBTITLE 1,Parameters connections and functions 💫
from pyspark.sql import SparkSession, Window
from pyspark.sql import types as T, functions as F
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from delta.tables import *
from pyspark.sql.types import StringType,BooleanType,DateType,IntegerType,DoubleType
from pyspark import SparkContext
import json

    
STORAGE_KEY = dbutils.secrets.get(scope="",key="")
spark.conf.set("", STORAGE_KEY)
spark.conf.set("spark.sql.session.timeZone", "EST")
spark.conf.set("spark.databricks.io.cache.enabled", True)
#SQL Server secrets
USER_KEY = dbutils.secrets.get(scope="keyvaultu",key="keyvaultmsqluserdb")
PASS_KEY = dbutils.secrets.get(scope="keyvaultp",key="keyvaultmsqlpassdb")

# COMMAND ----------

# DBTITLE 1,Create path 🏎
dir_gen='abfss:'
dir_bronze ='Bronze/TEST//MAIN'
dir_silv='Silver/TEST/MAIN/'


dir_sil = dir_gen+dir_silv
dir_br= f'{dir_gen}/{dir_bronze}'

get_fin = get_fecini[0:4]+'-'+get_fecini[4:6]+'-01'

# COMMAND ----------

import requests
#exmple api get data
url = "https://weatherbit-v1-mashape.p.rapidapi.com/forecast/3hourly"

querystring = {"lat":"35.5","lon":"-78.5"}

headers = {
	"X-RapidAPI-Key": "2aabc617f9msh2b7418bfaf8c777p192912jsn887af4e296e0",
	"X-RapidAPI-Host": "weatherbit-v1-mashape.p.rapidapi.com"
}

response = requests.request("GET", url, headers=headers, params=querystring)

# COMMAND ----------

#convert response to json
js = response.json()
#access to data
data = js['data']
#save data as pandas
import pandas as pd
pdf = pd.DataFrame(data)
#pdf = pdf[['wind_cdir_full', 'datetime','temp']]
#save data as spark dataframe
df_data = spark.createDataFrame(pdf)
#show data
#df_data.display()
#select columns and change names
df_data = df_data.select(concat('wind_cdir_full').alias('source'),concat('timestamp_utc').alias('date'),concat('temp').alias('data'))
#show data after select
df_data.display()

# COMMAND ----------

#save data into datalake with delta format
df_data.write.format('delta').mode("overwrite").save(f'{dir_sil}/UTILITIES/datatest')

# COMMAND ----------

#ToDo
#save data into sql server
dbname = "test"
tablename = "hr_deparment"
user = USER_KEY
password = PASS_KEY
jdbcUrl = f"jdbc:sqlserver://database.windows.net;databaseName={dbname}"

df_data.write \
    .format("com.microsoft.sqlserver.jdbc.spark") \
    .mode("overwrite") \
    .option("url", jdbcUrl) \
    .option("dbtable", tablename) \
    .option("user", user) \
    .option("password", password) \
    .option("truncate","true") \
    .save()

# COMMAND ----------

df_data.printSchema()
