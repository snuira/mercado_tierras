# Databricks notebook source
# DBTITLE 1,Configure storage account key
spark.conf.set("fs.azure.account.key.stupramonitoreomercado.dfs.core.windows.net", "XXXXX")
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")
dbutils.fs.ls("abfss://raw@stupramonitoreomercado.dfs.core.windows.net/")
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false")
#spark.conf.set("spark.sql.shuffle.partitions",3000)
#spark.conf.set("spark.default.parallelism",3000)

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import udf
from pyspark.sql.window import Window
import pyspark.sql.functions as F

import numpy as np
import pandas as pd
import pandas_profiling
from pandas_profiling import ProfileReport
#pd.__version__  # '1.2.3'
pandas_profiling.__version__  # '2.11.0'


# COMMAND ----------

# DBTITLE 1,List filesystem
dbutils.fs.ls("abfss://raw@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/json_deptos/")

# COMMAND ----------

# DBTITLE 1,Convertir archivo json single line a multiline
import json
import pandas as pd

def parse_json_to_multiline_file(folder_name, file_name, dataset_name ):
  print(file_name)
  dbutils.fs.cp(folder_name + "/" + file_name , "file:/tmp/"+file_name)
  with open("/tmp/"+file_name) as json_file:
    data = json.load(json_file)
    rows = data[dataset_name]
    print("total filas: ")
    print(len(rows))
    output_file_name = 'multiline_'+file_name
    df = pd.DataFrame.from_records(rows)
    df.to_json('/tmp/'+output_file_name, orient="records",  lines=True , force_ascii=False ) 
    #with open('/tmp/'+output_file_name, 'w') as outfile:
    #  json.dump(rows, outfile, ensure_ascii=False)
    #  #df = pd.read_json (r'tmp_json.json')
    #  #df.to_csv('multiline_'+file_name, sep = '\t')
    #  #
    dbutils.fs.cp('file:/tmp/'+output_file_name, folder_name + "/" + output_file_name )

# COMMAND ----------


parse_json_to_multiline_file("abfss://raw@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/json_deptos/", "CONDICION_2017R1_R2.json", "SASTableData+QUERY_FOR_2017_UNIONCATASTROS_R1" )

parse_json_to_multiline_file("abfss://raw@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/json_deptos/", "PAIS_TRANSACCIONES.json", "SASTableData+QUERY_FOR_8507_SNR_RAD_DESD_0002" )

parse_json_to_multiline_file("abfss://raw@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/json_deptos/", "1_BOLIVAR_8507_SNR_RAD_DESD_2015.json", "SASTableData+BOLIVAR_8507_SNR_RAD_DESD_2015" )
parse_json_to_multiline_file("abfss://raw@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/json_deptos/", "2_CORDOBA_8507_SNR_RAD_DESD_2015.json", "SASTableData+CORDOBA_8507_SNR_RAD_DESD_2015" )
parse_json_to_multiline_file("abfss://raw@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/json_deptos/", "3_RESTO_8507_SNR_RAD_DESD_2015.json", "SASTableData+RESTO_8507_SNR_RAD_DESD_2015" )


# COMMAND ----------

dbutils.fs.head("abfss://raw@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/json_deptos/1_BOLIVAR_8507_SNR_RAD_DESD_2015.json")

# COMMAND ----------

# DBTITLE 1,Procesar transacciones Total
df = spark.read.json("abfss://raw@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/json_deptos/multiline_PAIS_TRANSACCIONES.json")

df.display()

# COMMAND ----------

df.count()

# COMMAND ----------

df = df.withColumnRenamed("AÑO RADICACION", "ANIO_RADICACION")
df.printSchema()

# COMMAND ----------

df_sin_duplicados = df.dropDuplicates()
df_sin_duplicados.count()

# COMMAND ----------

# contar por depto y muni
df.createOrReplaceTempView("transacciones")

df_result = spark.sql( '''   select COD_DANE, count(*)
from transacciones  group by COD_DANE order by COD_DANE
''')
df_result.display()


# COMMAND ----------

df.write.parquet("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/transacciones.parquet")
profile = ProfileReport(df.select("*").toPandas(), title="Transacciones inmobiliarias",  minimal=True)
#displayHTML(profile.to_html())
dbutils.fs.put("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/transacciones.parquet_profile.html", profile.to_html() )

# COMMAND ----------

# DBTITLE 1,Procesar intervinientes total
df_1 = spark.read.json("abfss://raw@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/json_deptos/multiline_1_BOLIVAR_8507_SNR_RAD_DESD_2015.json")
df_2 = spark.read.json("abfss://raw@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/json_deptos/multiline_2_CORDOBA_8507_SNR_RAD_DESD_2015.json")
df_3 = spark.read.json("abfss://raw@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/json_deptos/multiline_3_RESTO_8507_SNR_RAD_DESD_2015.json")

df_intervinientes = df_1.union(df_2).union(df_3)

df_intervinientes.display()

# COMMAND ----------

df_intervinientes.count()  # 10908666
#df_intervinientes_sin_duplicados = df_intervinientes.dropDuplicates()
#df_intervinientes_sin_duplicados.count()  #  10908666

# COMMAND ----------

df_intervinientes = df_intervinientes.withColumnRenamed("AÑO RADICACION", "ANIO_RADICACION")
df_intervinientes = df_intervinientes.drop("NATURALEZA_JURIDICA")
df_intervinientes.printSchema()

# COMMAND ----------

# contar por  y muni
df_intervinientes.createOrReplaceTempView("intervinientes")

df_result = spark.sql( '''   select COD_DANE, count(*)
from intervinientes  group by COD_DANE order by COD_DANE
''')
df_result.display()

# COMMAND ----------

df_intervinientes.write.parquet("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/intervinientes.parquet")
profile = ProfileReport(df_intervinientes.select("*").toPandas(), title="Transacciones inmobiliarias",  minimal=True)
#displayHTML(profile.to_html())
dbutils.fs.put("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/intervinientes.parquet_profile.html", profile.to_html() )

# COMMAND ----------


