# Databricks notebook source
# DBTITLE 1,Importar librerías
from pyspark.sql.types import *
from pyspark.sql.functions import udf
from pyspark.sql.window import Window
import pyspark.sql.functions as F


import numpy as np
import pandas as pd
import pandas_profiling
from pandas_profiling import ProfileReport
pd.__version__


# COMMAND ----------

# DBTITLE 1,Configure storage account key
spark.conf.set("fs.azure.account.key.stupramonitoreomercado.dfs.core.windows.net", "XXXXX")
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")
dbutils.fs.ls("abfss://raw@stupramonitoreomercado.dfs.core.windows.net/")
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false")
#spark.conf.set("spark.sql.shuffle.partitions",3000)
#spark.conf.set("spark.default.parallelism",3000)

# COMMAND ----------

# DBTITLE 1,Empresas SECOP, datos.gov.co (cálculo de frecuencia palabras)
# Archivo con nombres de empresas generado a partir de datos encontrados en datos.gov.co entre otros SECOP I
df_empresas  = spark.read.csv("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/DATOS_GOV_CO/empresas_all_sin_nit_para_conteo_palabras.tsv", sep=r'\t', header=True)
df_empresas.count() 

# COMMAND ----------

df_intervinientes_nit =  spark.sql("""
SELECT  PERSONA_CLEAN as empresa
FROM intervinientes_clean  where TIPO_DOCUMENTO ='N'  
""")
df_intervinientes_nit.count()

# COMMAND ----------

df_empresas_union = df_empresas.union(df_intervinientes_nit)
df_empresas_union.count()

# COMMAND ----------

#df_empresas_union.printSchema()
df_empresas_union = df_empresas_union.dropDuplicates()
df_empresas_union.count()
#df_empresas_union.show()

# COMMAND ----------

# DBTITLE 1,Palabras más comunes en nombres de empresas
df_nit_common = df_empresas_union.withColumn('word', F.explode(F.split(F.col('empresa'), ' ')))\
    .groupBy('word')\
    .count()\
    .sort('count', ascending=False)

df_nit_common.show()
df_nit_common.coalesce(1).write.json("abfss://sandbox@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/nit_palabras_comunes.json")
df_empresas_union.write.parquet("abfss://sandbox@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/nombres_empresas.parquet")
df_empresas_union.coalesce(1).write.json("abfss://sandbox@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/nombres_empresas.json")

# COMMAND ----------

# DBTITLE 1,Dane - Directorio Estadístico de Empresas
# URL https://www.dane.gov.co/index.php/servicios-al-ciudadano/servicios-informacion/directorio-estadistico/directorio-estadistico-de-empresas
'''
El Directorio Estadístico de Empresas contiene información sobre la identificación y ubicación de todas las empresas, con sus establecimientos y activas en el territorio nacional, de acuerdo a la actualización continua realizada mediante los registros administrativos obtenidos de las Entidades tanto públicas como privadas, las investigaciones DANE y los diferentes operativos de actualización (web, telefónicos, correo electrónico, recoleccíón en terreno, etc
'''

df_empresas  = spark.read.csv("abfss://raw@stupramonitoreomercado.dfs.core.windows.net/OTROS/DANE/DIR_EST_EMP_2020.csv", sep=r',', header=True)
df_empresas.count() 

# COMMAND ----------

display(df_empresas)

# COMMAND ----------


profile = ProfileReport(df_empresas.select("*").toPandas(), title="Pandas Profiling Report - DANE  Directorio Estadístico de Empresas")
#displayHTML(profile.to_html())
dbutils.fs.put("abfss://raw@stupramonitoreomercado.dfs.core.windows.net/OTROS/DANE/DIR_EST_EMP_2020_profile.html", profile.to_html() )

# COMMAND ----------

df_empresas = df_empresas.dropDuplicates()
df_empresas.write.parquet("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/DANE/DIR_EST_EMP_2020.parquet")


# COMMAND ----------

df_empresas.createOrReplaceTempView("directorio_empresas")
df_clean =   spark.sql("""
SELECT 
trim( regexp_replace( regexp_replace(  regexp_replace( upper(RAZON_SOCIAL),  ' +', ' ') ,  ',+', ','    ) , '[^a-zA-Z ,]+' , '' ) )  as RAZON_SOCIAL_CLEAN 
FROM directorio_empresas
""")
display(df_clean)


# COMMAND ----------

df_common = df_clean.withColumn('word', F.explode(F.split(F.col('RAZON_SOCIAL_CLEAN'), ' ')))\
    .groupBy('word')\
    .count()\
    .sort('count', ascending=False)

df_common.coalesce(1).write.json("abfss://sandbox@stupramonitoreomercado.dfs.core.windows.net/OTROS/DANE/directorio_empresas_razon_social_palabras_comunes.json")

# COMMAND ----------


