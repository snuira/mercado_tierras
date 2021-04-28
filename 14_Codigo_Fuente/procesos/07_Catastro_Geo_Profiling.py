# Databricks notebook source
# DBTITLE 1,Configure storage account key
spark.conf.set("fs.azure.account.key.stupramonitoreomercado.dfs.core.windows.net", "XXXXX")
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")
dbutils.fs.ls("abfss://raw@stupramonitoreomercado.dfs.core.windows.net/")
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false")
#spark.conf.set("spark.sql.shuffle.partitions",3000)
#spark.conf.set("spark.default.parallelism",3000)

# COMMAND ----------

#importar librerías para pyspark

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

# DBTITLE 1,Registrar Sedona
import os

import geopandas as gpd
from pyspark.sql import SparkSession

from sedona.register import SedonaRegistrator
from sedona.utils import SedonaKryoRegistrator, KryoSerializer

spark = SparkSession. \
    builder. \
    appName('appName'). \
    config("spark.serializer", KryoSerializer.getName). \
    config("spark.kryo.registrator", SedonaKryoRegistrator.getName). \
    config('spark.jars.packages', 'org.apache.sedona:sedona-python-adapter-3.0_2.12:1.0.0-incubating,org.datasyslab:geotools-wrapper:geotools-24.0'). \
    getOrCreate()

SedonaRegistrator.registerAll(spark)

# COMMAND ----------

# DBTITLE 1,List filesystem
dbutils.fs.ls("abfss://raw@stupramonitoreomercado.dfs.core.windows.net/OTROS/CATASTRO/WKT/")

# COMMAND ----------

dbutils.fs.head("abfss://raw@stupramonitoreomercado.dfs.core.windows.net/OTROS/CATASTRO/WKT/predial2015_4326_rg.prj")

# COMMAND ----------

# DBTITLE 1,2015
df_2015 = spark.\
    read.\
    option("delimiter", ",").\
    option("header", "true").\
    csv("abfss://raw@stupramonitoreomercado.dfs.core.windows.net/OTROS/CATASTRO/WKT/predial2015_4326_rg.csv")

df_2015 = df_2015.withColumn('ANIO', F.lit(2015))
df_2015 = df_2015.withColumn("id",   F.row_number().over(Window.orderBy("ANIO"))     )
df_2015 = df_2015.drop("OBJECTID", "DEPARTAMENTO", "MUNICIPIO", "Shape_Length", "Shape_Area", "Id_Anonimo", "COD_DPTO", 'Area_Carto_ha', 'AREATERRENO_m2')
df_2015.printSchema()
#catastro_2015.createOrReplaceTempView("catastro_2015")
df_2015.display()

# COMMAND ----------

#df_export = df_2015.select("id", "WKT", "COD_DANE","COD_PREDIAL_TOTAL","ANIO", "Tamanopredial16", "AREATERRENO_ha","areaconstruida","ClasificacionUAF", "avaluo" )
df_export = df_2015.select("*" )
df_export.createOrReplaceTempView("catastro")
df_geom = spark.sql(
    """   with a as (
          SELECT c.id,  st_geomFromWKT(c.WKT) as geom   
          from catastro as c ), 
          b as ( select a.id , st_isvalid(a.geom) as valid
          from a  )
          select valid, count(*)
          from b 
          group by valid 
    """)

""" 
with a as (   
          SELECT c.id,   st_geomFromWKT(c.WKT) as geom 
          from catastro as c
        ), b as (
          select id, st_centroid(geom) as centroid from a
        )
        select  c.* , st_x( b.centroid) as centroid_x,  st_y( b.centroid) as centroid_y
        from catastro as c inner join b on c.id = b.id 
    """
#df_geom.cache()
#df_geom.count()
#df_geom.printSchema()
df_geom.display()

# COMMAND ----------

df_geom.explain(mode=)

# COMMAND ----------

df_geom.write.parquet("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/CATASTRO/predial2015_4326.parquet")

# COMMAND ----------

# DBTITLE 1,2016
df_2016 = spark.\
    read.\
    option("delimiter", ",").\
    option("header", "true").\
    csv("abfss://raw@stupramonitoreomercado.dfs.core.windows.net/OTROS/CATASTRO/WKT/predial2016_4326.csv")

df_2016 = df_2016.withColumn('ANIO', F.lit(2016))
df_2016 = df_2016.drop("OBJECTID", "DEPARTAMENTO", "MUNICIPIO", "Shape_Length", "Shape_Area", "ID_Anonimo")
df_2016 = df_2016.withColumnRenamed("CODPREDIAL_TOTAL", "COD_PREDIAL_TOTAL")
df_2016 = df_2016.withColumnRenamed("tamanopredial16", "Tamanopredial16")

df_2016.printSchema()
df_2016.display()

# COMMAND ----------

df_2017 = spark.\
    read.\
    option("delimiter", ",").\
    option("header", "true").\
    csv("abfss://raw@stupramonitoreomercado.dfs.core.windows.net/OTROS/CATASTRO/WKT/predial2017_4326.csv")

df_2017 = df_2017.withColumn('ANIO', F.lit(2017))
df_2017 = df_2017.drop("OBJECTID", "DEPARTAMENTO", "MUNICIPIO", "Shape_Length", "Shape_Area", "ID_Anonimo")
df_2017 = df_2017.withColumnRenamed("CODPREDIAL_TOTAL", "COD_PREDIAL_TOTAL")
df_2017 = df_2017.withColumnRenamed("tamanopredial16", "Tamanopredial16")

df_2017.printSchema()
df_2017.display()

# COMMAND ----------

df_2018 = spark.\
    read.\
    option("delimiter", ",").\
    option("header", "true").\
    csv("abfss://raw@stupramonitoreomercado.dfs.core.windows.net/OTROS/CATASTRO/WKT/predial2018_4326.csv")

df_2018 = df_2018.withColumn('ANIO', F.lit(2018))
df_2018 = df_2018.drop("OBJECTID", "DEPARTAMENTO", "MUNICIPIO", "Shape_Length", "Shape_Area", "ID_Anonimo")
df_2018 = df_2018.withColumnRenamed("CODPREDIAL_TOTAL", "COD_PREDIAL_TOTAL")
df_2018 = df_2018.withColumnRenamed("tamanopredial16", "Tamanopredial16")

df_2018.printSchema()
df_2018.display()

# COMMAND ----------

df_2019 = spark.\
    read.\
    option("delimiter", ",").\
    option("header", "true").\
    csv("abfss://raw@stupramonitoreomercado.dfs.core.windows.net/OTROS/CATASTRO/WKT/predial2019_4326.csv")

df_2019 = df_2019.withColumn('ANIO', F.lit(2019))
df_2019 = df_2019.drop("OBJECTID", "DEPARTAMENTO", "MUNICIPIO", "Shape_Length", "Shape_Area", "ID_Anonimo")
df_2019 = df_2019.withColumnRenamed("CODIGO_PREDIAL_TOTAL", "COD_PREDIAL_TOTAL")
df_2019 = df_2019.withColumnRenamed("tamanopredial16", "Tamanopredial16")

df_2019.printSchema()
df_2019.display()

# COMMAND ----------

list_difference = [item for item in df_2015.columns if item not in df_2016.columns]
print(list_difference)
print ( " " )
list_difference = [item for item in df_2016.columns if item not in df_2015.columns]
print(list_difference)

# COMMAND ----------

df_join = df_2015.select("WKT", "COD_DANE","COD_PREDIAL_TOTAL","ANIO", "Tamanopredial16")\
      .union(df_2016.select("WKT", "COD_DANE","COD_PREDIAL_TOTAL","ANIO", "Tamanopredial16"))\
      .union(df_2017.select("WKT", "COD_DANE","COD_PREDIAL_TOTAL","ANIO", "Tamanopredial16"))\
      .union(df_2018.select("WKT", "COD_DANE","COD_PREDIAL_TOTAL","ANIO", "Tamanopredial16"))\
      .union(df_2019.select("WKT", "COD_DANE","COD_PREDIAL_TOTAL","ANIO", "Tamanopredial16"))


df_join.printSchema()

# COMMAND ----------

df_join.count()

# COMMAND ----------

# Nota: Al intentar hacer profiling con todo el conjunto de datos se generan problemas de memoria
df_join.write.parquet("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/catastro_geom.parquet")
#profile = ProfileReport(df_all.select("*").toPandas(), title="Transacciones inmobiliarias",  minimal=True)
#displayHTML(profile.to_html())
#dbutils.fs.put("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/multiline_R1_R2_all.parquet_profile.html", profile.to_html() )

# COMMAND ----------

# DBTITLE 1,Generar Centroide

df_all = spark.read.parquet("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/catastro_geom.parquet")
df_all = df_all.withColumn("id",   F.row_number().over(Window.orderBy("ANIO"))     )
df_all.createOrReplaceTempView("catastro")
df_all.cache()
df_all.count()
df_all.printSchema()
df_all.display()

# COMMAND ----------

# DBTITLE 1,Almacenar datos con el id generado
df_all.write.parquet("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/catastro_geom_with_id.parquet")

# COMMAND ----------

df_all = spark.read.parquet("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/catastro_geom_with_id.parquet")
df_all.createOrReplaceTempView("catastro")
#df_all.cache()
#df_all.count()
df_all.printSchema()
df_all.display()

# COMMAND ----------

df_geom = spark.sql(
    """ 
        SELECT c.id,   st_geomFromWKT(c.WKT) as geom 
        from catastro as c
    """)
df_geom.createOrReplaceTempView("catastro_geom")
#df_geom.cache()
#df_geom.count()
df_geom.printSchema()

# COMMAND ----------

# validar geometrías inválidas
spark.sql(
    """ 
        with a as ( SELECT c.id,  ST_IsValid(geom) as valid 
        from catastro_geom as c where geom is not null)
        select valid, count(*)
        from a
        group by valid   
    """).display()

# COMMAND ----------

df_geom_centroid = spark.sql(
    """ 
        SELECT c.id,  st_centroid(geom) as centroid 
        from catastro_geom as c
    """)
df_geom_centroid.createOrReplaceTempView("catastro_geom_centroid")
#df_geom_centroid.cache()
#df_geom_centroid.count()
df_geom_centroid.printSchema()

# COMMAND ----------


transformed_df = spark.sql(
    """ 
        select c.id, c.WKT, c.COD_DANE, c.COD_PREDIAL_TOTAL, c.ANIO, c.Tamanopredial16 ,  
        st_x( st_centroid(b.centroid)) as centroid_x,  st_y( st_centroid(b.centroid)) as centroid_y
        from catastro as c inner join catastro_geom_centroid as b  on (c.id = b.id) 
    """)
#transformed_df.cache()
transformed_df.printSchema()

# COMMAND ----------

transformed_df.cache()
transformed_df.count()
transformed_df.printSchema()

# COMMAND ----------

transformed_df.explain()

# COMMAND ----------

transformed_df.write.parquet("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/catastro_geom_centroid.parquet")

# COMMAND ----------

title = "Disclaimer: this profiling report was generated using a sample of 5% of the original dataset."
profile = ProfileReport(transformed_df.select("*").toPandas().sample(frac=0.05), title=title,  minimal=True)
#displayHTML(profile.to_html())
dbutils.fs.put("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/catastro_geom_centroid.parquet_profile_sample.html", profile.to_html() )
