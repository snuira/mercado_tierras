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

#Registrar Sedona 
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

# DBTITLE 1,Generar archivo final de transacciones
df = spark.read.parquet("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/transacciones_clean_rural_sin_linderos.parquet")

df.display()


# COMMAND ----------

df.count() # 1016642


# COMMAND ----------

df.printSchema()

# COMMAND ----------

df_cod_nat = spark.read.csv("abfss://raw@stupramonitoreomercado.dfs.core.windows.net/OTROS/UPRA/codigos_naturaleza.csv", header=True)
df_cod_nat.display() 

# COMMAND ----------

df_join_transacc = df.join(df_cod_nat, df.COD_NATURALEZA == df_cod_nat.cod_naturaleza_juridica,   how='left') 
df_join_transacc = df_join_transacc.drop("cod_naturaleza_juridica")
df_join_transacc.display()

# COMMAND ----------

df_join_transacc.count()

# COMMAND ----------

#  Filtrar todos los que no son valorización
df_join_transacc_filter = df_join_transacc.filter(df_join_transacc.COD_NATURALEZA != 212)
df_join_transacc_filter.count()  # 991084

# COMMAND ----------

df_conteo_sexo = spark.read.parquet("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/intervinientes_rurales_conteo_final.parquet")
df_conteo_sexo = df_conteo_sexo.withColumnRenamed("pk", "fk")
df_conteo_sexo.count()
df_conteo_sexo.display()

# COMMAND ----------

df_join_transacc_2 = df_join_transacc_filter.join(df_conteo_sexo, df_join_transacc.pk == df_conteo_sexo.fk,   how='left') 
df_join_transacc_2 = df_join_transacc_2.drop("fk")
df_join_transacc_2.display()

# COMMAND ----------

df_linderos = spark.read.parquet("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/transacciones_clean_rural_linderos_depuracion_area.parquet")
df_linderos = df_linderos.withColumnRenamed("pk", "fk")
df_linderos.createOrReplaceTempView("linderos")
df_linderos.count()
df_linderos.display()

# COMMAND ----------

df_linderos_count = spark.sql("""
select fk, count(*) as cantidad_linderos
from linderos group by fk
""")
df_linderos_count.display()

# COMMAND ----------

df_join_transacc_3 = df_join_transacc_2.join(df_linderos_count, df_join_transacc_2.pk == df_linderos_count.fk,   how='left') 
df_join_transacc_3 = df_join_transacc_3.drop("fk")
df_join_transacc_3.display()

# COMMAND ----------

df_join_transacc_3.count() # 991084

# COMMAND ----------

df_muni = spark.read.csv("abfss://raw@stupramonitoreomercado.dfs.core.windows.net/OTROS/DANE/DIVIPOLA_WKT/Municipios_4326.csv", header=True)
df_muni = df_muni.drop("WKT", "OBJECTID", "Shape_Leng", "Shape_Area")
df_muni = df_muni.withColumnRenamed("cod_dane", "codigo_dane")
df_muni = df_muni.withColumnRenamed("cod_depart", "codigo_departamento")
df_muni = df_muni.withColumnRenamed("departamen", "departamento")
df_muni.display() 

# COMMAND ----------

df_join_transacc_4 = df_join_transacc_3.join(df_muni, df_join_transacc_3.COD_DANE == df_muni.codigo_dane,   how='left') 
df_join_transacc_4 = df_join_transacc_4.drop("codigo_dane")
df_join_transacc_4.display()

# COMMAND ----------

df_join_transacc_4.count()

# COMMAND ----------

# DBTITLE 1,R1_R2
df_r1r2 = spark.read.parquet("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/multiline_R1_R2_all.parquet")
#df_r1r2 = df_r1r2.withColumnRenamed("pk", "fk")


df_r1r2.display()

# COMMAND ----------

df_r1r2.count()

# COMMAND ----------

df_r1r2_pk = df_r1r2.withColumn("id",    F.row_number().over(Window.orderBy("ANIO", "COD_MUN") )     )
df_r1r2_pk.cache()
df_r1r2_pk.display()

# COMMAND ----------

df_r1r2_pk.printSchema()

# COMMAND ----------

df_join_transacc_4.createOrReplaceTempView("transacciones_filtradas")
df_r1r2_pk.createOrReplaceTempView("r1r2")

# COMMAND ----------


# seleccionar el R1R2 del primer año para el cual se encuentre el cruce de la matrícula
df_r1r2_transacc = spark.sql("""
with ventana as  ( 
select t.pk,t.ANIO_RADICACION , r.id, r.COD_MUN , r.MATRICULA_DEPURADO , r.anio , 
row_number () over ( partition by r.COD_MUN , r.MATRICULA_DEPURADO order by r.anio) as row
from transacciones_filtradas as t , r1r2 as r
where   t.COD_DANE  = r.COD_MUN  and (t.MATRICULA_ORIP || '-' || t.MATRICULA_NUMERO ) = r.MATRICULA_DEPURADO
and  r.ANIO    >= cast(  t.ANIO_RADICACION as int  )  
order by t.pk, r.anio
)
select v.pk as pk_transaccion, r.*
from ventana as v inner join r1r2 as r on (v.id = r.id)
where v.row = 1
""")

#  
df_r1r2_transacc.display()
# para geo cruzar con codigopredialcarto   -> cod_predial_total   (hay menos por comisión / omisión )

# COMMAND ----------

df_r1r2_transacc.count() 
#  533062->sin importar año  84214->años iguales     toos los años 531191
# 339069  el año del r1r2 es >= que el año de la transacción
# 107486 transacciones únicas que cruzan con r1r2

# COMMAND ----------

df_r1r2_transacc.write.parquet("abfss://gold@stupramonitoreomercado.dfs.core.windows.net/UPRA/MERCADO_TIERRAS/transacciones_rurales_r1r2.parquet")

df_r1r2_transacc.coalesce(1).write.json("abfss://gold@stupramonitoreomercado.dfs.core.windows.net/UPRA/MERCADO_TIERRAS/transacciones_rurales_r1r2.json")

df_r1r2_transacc.coalesce(1).write.option("delimiter", "\t").option('header',True).mode('overwrite')\
  .csv("abfss://gold@stupramonitoreomercado.dfs.core.windows.net/UPRA/MERCADO_TIERRAS/transacciones_rurales_r1r2.tsv")

#title = "Transacciones Mercado tierras - Disclaimer: this profiling report was generated using a sample of 5% of the original dataset."
#profile = ProfileReport(df_join_transacc_4.select("*").toPandas().sample(frac=0.05), title=title,  minimal=False)

title = "Transacciones Mercado tierras - Catastro Registros 1 y 2  "
profile = ProfileReport(df_r1r2_transacc.select("*").toPandas(), title=title,  minimal=True)

#displayHTML(profile.to_html())
dbutils.fs.put("abfss://gold@stupramonitoreomercado.dfs.core.windows.net/UPRA/MERCADO_TIERRAS/transacciones_rurales_r1r2.parquet_profile.html", profile.to_html() )

# COMMAND ----------

df_join_transacc_4.count()

# COMMAND ----------

# DBTITLE 1,Guardar tabla final de transacciones
df_join_transacc_4.write.parquet("abfss://gold@stupramonitoreomercado.dfs.core.windows.net/UPRA/MERCADO_TIERRAS/transacciones_rurales.parquet")

df_join_transacc_4.coalesce(1).write.json("abfss://gold@stupramonitoreomercado.dfs.core.windows.net/UPRA/MERCADO_TIERRAS/transacciones_rurales.json")

df_join_transacc_4.coalesce(1).write.option("delimiter", "\t").option('header',True).mode('overwrite')\
  .csv("abfss://gold@stupramonitoreomercado.dfs.core.windows.net/UPRA/MERCADO_TIERRAS/transacciones_rurales.tsv")

#title = "Transacciones Mercado tierras - Disclaimer: this profiling report was generated using a sample of 5% of the original dataset."
#profile = ProfileReport(df_join_transacc_4.select("*").toPandas().sample(frac=0.05), title=title,  minimal=False)

title = "Transacciones Mercado tierras ."
profile = ProfileReport(df_join_transacc_4.select("*").toPandas(), title=title,  minimal=True)

#displayHTML(profile.to_html())
dbutils.fs.put("abfss://gold@stupramonitoreomercado.dfs.core.windows.net/UPRA/MERCADO_TIERRAS/transacciones_rurales.parquet_profile.html", profile.to_html() )

# COMMAND ----------

# DBTITLE 1,Transacciones - Catastro GEO
df_transacciones = spark.read.parquet("abfss://gold@stupramonitoreomercado.dfs.core.windows.net/UPRA/MERCADO_TIERRAS/transacciones_rurales.parquet")
df_transacciones.createOrReplaceTempView("transacciones_filtradas")
df_transacciones.count()




# COMMAND ----------

df_geo_2015 = spark.read.\
    option("delimiter", ",").\
    option("header", "true").\
    csv("abfss://raw@stupramonitoreomercado.dfs.core.windows.net/OTROS/CATASTRO/WKT/predial_2015_2019_poly_3116/predial_2015_rg_3116.csv")

df_geo_2016 = spark.read.\
    option("delimiter", ",").\
    option("header", "true").\
    csv("abfss://raw@stupramonitoreomercado.dfs.core.windows.net/OTROS/CATASTRO/WKT/predial_2015_2019_poly_3116/predial_2016_rg_3116.csv")

df_geo_2017 = spark.read.\
    option("delimiter", ",").\
    option("header", "true").\
    csv("abfss://raw@stupramonitoreomercado.dfs.core.windows.net/OTROS/CATASTRO/WKT/predial_2015_2019_poly_3116/predial_2017_rg_3116.csv")


df_geo_2018 = spark.read.\
    option("delimiter", ",").\
    option("header", "true").\
    csv("abfss://raw@stupramonitoreomercado.dfs.core.windows.net/OTROS/CATASTRO/WKT/predial_2015_2019_poly_3116/predial_2018_rg_3116.csv")


df_geo_2019 = spark.read.\
    option("delimiter", ",").\
    option("header", "true").\
    csv("abfss://raw@stupramonitoreomercado.dfs.core.windows.net/OTROS/CATASTRO/WKT/predial_2015_2019_poly_3116/predial_2019_rg_3116.csv")


# COMMAND ----------

print("2015 : " + str(  df_geo_2015.count() ) )
print("2016 : " + str(  df_geo_2016.count() ) )
print("2017 : " + str(  df_geo_2017.count() ) )
print("2018 : " + str(  df_geo_2018.count() ) )
print("2019 : " + str(  df_geo_2019.count() ) )



# COMMAND ----------

df_geo_2015 = df_geo_2015.withColumn('ANIO', F.lit(2015))
df_geo_2016 = df_geo_2016.withColumn('ANIO', F.lit(2016))
df_geo_2017 = df_geo_2017.withColumn('ANIO', F.lit(2017))
df_geo_2018 = df_geo_2018.withColumn('ANIO', F.lit(2018))
df_geo_2019 = df_geo_2019.withColumn('ANIO', F.lit(2019))

df_geo_union = df_geo_2015.union(  df_geo_2016 ).union(df_geo_2017).union(df_geo_2018).union(df_geo_2019)
df_geo_union.createOrReplaceTempView("catastro_geometrias")
df_geo_union.count() # total polígonos 5 años  18154322

# COMMAND ----------

#df_geo_union.display()
df_geo_union.printSchema()

# COMMAND ----------

# registros de r1r2 filtrados para las transacciones rurales
df_r1r2_filtrado = spark.read.parquet("abfss://gold@stupramonitoreomercado.dfs.core.windows.net/UPRA/MERCADO_TIERRAS/transacciones_rurales_r1r2.parquet")
df_r1r2_filtrado.createOrReplaceTempView("r1r2_filtrado")
df_r1r2_filtrado.count()

# COMMAND ----------

# gdal wkt trabaja como lon lat
# el st_transform de sedona trabaja con lat lon # By default, this function uses lat/lon order. You can use ST_FlipCoordinates to swap X and Y.
# https://sedona.apache.org/api/sql/GeoSparkSQL-Function/#st_transform

# query incluyendo operaciones espaciales
"""
with ventana as (
  select   g.*, r.pk_transaccion,
  row_number () over ( partition by g.CODIGO_PREDIAL_TOTAL order by g.anio) as row
  from r1r2_filtrado as r, catastro_geometrias as g
  where r.codigopredialcarto = g.CODIGO_PREDIAL_TOTAL
  and g.anio >= r.anio
  order by r.pk_transaccion, r.anio, g.anio
), geom as (
   select v.*, st_geomFromWKT(v.WKT)     as geometry 
   from ventana as v
   where v.row = 1
), centroids as ( 
  select g.*, st_transform( ST_FlipCoordinates  ( st_centroid(g.geometry) )  ,   'epsg:3116',  'epsg:4326' )     as centroid
  from geom as g
)
select c.* ,  st_x(c.centroid) as latitude , st_y(c.centroid) as longitude 
from centroids as c
"""


df_result = spark.sql("""
with ventana as (
  select   g.*, r.pk_transaccion,
  row_number () over ( partition by g.CODIGO_PREDIAL_TOTAL order by g.anio) as row
  from r1r2_filtrado as r, catastro_geometrias as g
  where r.codigopredialcarto = g.CODIGO_PREDIAL_TOTAL
  and g.anio >= r.anio
  order by r.pk_transaccion, r.anio, g.anio
)
   select v.*
   from ventana as v
   where v.row = 1
""")

df_result.display()

# COMMAND ----------

#df_result.explain()

# COMMAND ----------

df_result.count()  # 80119

# COMMAND ----------

#df_result = df_result.drop("OBJECTID", "row", "geometry", "centroid")
df_result = df_result.drop("OBJECTID", "row")
df_result.printSchema()

# COMMAND ----------

df_result.display()

# COMMAND ----------

df_result.createOrReplaceTempView("geometrias_filtrado")

# COMMAND ----------

# verificar un único polígono por cada transacción relacionada a través de r1r2
spark.sql("""
select pk_transaccion, count(*)
from geometrias_filtrado 
group by pk_transaccion
having count(*) >  1
""").display()

# COMMAND ----------

df_result.write.parquet("abfss://gold@stupramonitoreomercado.dfs.core.windows.net/UPRA/MERCADO_TIERRAS/transacciones_rurales_r1r2_geographic.parquet")

df_result.coalesce(1).write.json("abfss://gold@stupramonitoreomercado.dfs.core.windows.net/UPRA/MERCADO_TIERRAS/transacciones_rurales_r1r2_geographic.json")

df_result.coalesce(1).write.option('header',True).mode('overwrite')\
  .csv("abfss://gold@stupramonitoreomercado.dfs.core.windows.net/UPRA/MERCADO_TIERRAS/transacciones_rurales_r1r2_geographic.wkt.csv")


df_result.coalesce(1).write.option("delimiter", "\t").option('header',True).mode('overwrite')\
  .csv("abfss://gold@stupramonitoreomercado.dfs.core.windows.net/UPRA/MERCADO_TIERRAS/transacciones_rurales_r1r2_geographic.tsv")

#title = "Transacciones Mercado tierras - Disclaimer: this profiling report was generated using a sample of 5% of the original dataset."
#profile = ProfileReport(df_join_transacc_4.select("*").toPandas().sample(frac=0.05), title=title,  minimal=False)

title = "Transacciones Mercado tierras - Catastro Geográfico  "
profile = ProfileReport(df_result.select("*").toPandas(), title=title,  minimal=True)

#displayHTML(profile.to_html())
dbutils.fs.put("abfss://gold@stupramonitoreomercado.dfs.core.windows.net/UPRA/MERCADO_TIERRAS/transacciones_rurales_r1r2_geographic.parquet_profile.html", profile.to_html() )

# COMMAND ----------

# DBTITLE 1,Procesar datos precios mercado de tierras
dbutils.fs.cp('abfss://raw@stupramonitoreomercado.dfs.core.windows.net/OTROS/UPRA/Precios_Mercado_Tierras_shp/', 'file:/tmp/', True)
#dbutils.fs.cp('abfss://raw@stupramonitoreomercado.dfs.core.windows.net/OTROS/UPRA/Precios_MT_repair/Precios_MT_repair_shp.zip', 'file:/tmp/Precios_MT_repair_shp.zip')


# COMMAND ----------

# MAGIC %sh
# MAGIC cd /tmp
# MAGIC #unzip /tmp/Precios_MT_repair_shp.zip 
# MAGIC rm /tmp/Precios_MT.*
# MAGIC ls /tmp/

# COMMAND ----------

import geopandas as gpd

gdf = gpd.read_file("/tmp/Precios_MT_rg.shp")

# COMMAND ----------

ser_valid = gdf.is_valid

# COMMAND ----------

ser_valid.groupby(ser_valid).size()

# COMMAND ----------

# creación de spark dataframe a partir de un df de geopandas
df_precios_from_gpd = spark.createDataFrame(  gdf )
df_precios_from_gpd.display()

# COMMAND ----------

df_precios_from_gpd.createOrReplaceTempView("precios_from_gpd")
df_precios_from_gpd.printSchema()

# COMMAND ----------

spark.sql("""
with a as (
select p.OBJECTID, p.categoria_, st_area(geometry) as area
from precios_from_gpd as p 
)
select sum(area) as total
from a
""").display()

# COMMAND ----------

spark.sql("""
with a as (
select  st_area( ST_Buffer(geometry, 0) ) as area
from geometrias_filtrado as p 
)
select sum(area) as total
from a
""").display()

# COMMAND ----------

df_intersect = spark.sql("""
select g.pk_transaccion, p.OBJECTID, p.categoria_
from geometrias_filtrado as g , precios_from_gpd as p 
where ST_Intersects(g.geometry, p.geometry)
""")

df_intersect.display()

# COMMAND ----------

# precios mercado tierras  - carga desde WKT comprimido en bzip2

# carga de wkt con geometrías corregidas en qgis a partir de un shp 
df_precios_wkt = spark.read.\
    option("delimiter", ",").\
    option("header", "true").\
    csv("abfss://raw@stupramonitoreomercado.dfs.core.windows.net/OTROS/UPRA/Precios_MT_repair/Precios_MT_repair_wkt.csv.bz2")

df_precios_wkt.createOrReplaceTempView("precios_wkt")
df_precios_wkt.count() # 112350

#df_precios.printSchema()

# COMMAND ----------

df_precios_wkt.printSchema()

# COMMAND ----------

df_query = spark.sql("""
with pgeom as (
select p.*, st_geomFromWKT(p.WKT)     as geometry 
from precios_wkt as p
)
select g.pk_transaccion, p.OBJECTID, p.categoria_
from geometrias_filtrado as g , pgeom as p 
where ST_Intersects(g.geometry, p.geometry)
""")
df_query.display()

# COMMAND ----------

# st_transform( st_geomFromWKT(g.WKT), 'epsg:3116',  'epsg:4326' )  
#df_result = df_result.drop("row", "OBJECTID")
#df_result = df_result.withColumnRenamed("anio", "anio_geo")
#  

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Guardar tabla final intervinientes
df_intervinientes = spark.read.parquet("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/intervinientes_clean_sexo_nombres_rurales_total_clasificados.parquet")

df_intervinientes.display()


# COMMAND ----------

df_intervinientes.count()

# COMMAND ----------

df_join_transacc_4.createOrReplaceTempView("transacciones_filtradas")
df_intervinientes.createOrReplaceTempView("intervinientes")

# COMMAND ----------

df_result = spark.sql("""
select  i.*
from transacciones_filtradas as t  inner join intervinientes as i on (t.pk = i.pk)
""")
#df_result.display()
df_result.count()

# COMMAND ----------

df_result.write.parquet("abfss://gold@stupramonitoreomercado.dfs.core.windows.net/UPRA/MERCADO_TIERRAS/transacciones_rurales_intervinientes.parquet")

df_result.coalesce(1).write.json("abfss://gold@stupramonitoreomercado.dfs.core.windows.net/UPRA/MERCADO_TIERRAS/transacciones_rurales_intervinientes.json")

df_result.coalesce(1).write.option("delimiter", "\t").option('header',True).mode('overwrite')\
  .csv("abfss://gold@stupramonitoreomercado.dfs.core.windows.net/UPRA/MERCADO_TIERRAS/transacciones_rurales_intervinientes.tsv")

#title = "Transacciones Mercado tierras - Disclaimer: this profiling report was generated using a sample of 5% of the original dataset."
#profile = ProfileReport(df_join_transacc_4.select("*").toPandas().sample(frac=0.05), title=title,  minimal=False)

title = "Transacciones Mercado tierras - Catastro Geográfico  "
profile = ProfileReport(df_result.select("*").toPandas(), title=title,  minimal=True)

#displayHTML(profile.to_html())
dbutils.fs.put("abfss://gold@stupramonitoreomercado.dfs.core.windows.net/UPRA/MERCADO_TIERRAS/transacciones_rurales_intervinientes.parquet_profile.html", profile.to_html() )

# COMMAND ----------

# DBTITLE 1,Guardar tabla final linderos
df_transacciones = spark.read.parquet("abfss://gold@stupramonitoreomercado.dfs.core.windows.net/UPRA/MERCADO_TIERRAS/transacciones_rurales.parquet")
df_transacciones.count()

# COMMAND ----------

df_linderos = spark.read.parquet("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/transacciones_clean_rural_linderos_depuracion_area.parquet")
df_linderos.count()


# COMMAND ----------

df_transacciones.createOrReplaceTempView("transacciones_filtradas")
df_linderos.createOrReplaceTempView("linderos")

# COMMAND ----------

df_result = spark.sql("""
select  i.*
from transacciones_filtradas as t  inner join linderos as i on (t.pk = i.pk)
""")
#df_result.display()
df_result.count()

# COMMAND ----------

df_result.write.parquet("abfss://gold@stupramonitoreomercado.dfs.core.windows.net/UPRA/MERCADO_TIERRAS/transacciones_rurales_linderos.parquet")

df_result.coalesce(1).write.json("abfss://gold@stupramonitoreomercado.dfs.core.windows.net/UPRA/MERCADO_TIERRAS/transacciones_rurales_linderos.json")

df_result.coalesce(1).write.option("delimiter", "\t").option('header',True).mode('overwrite')\
  .csv("abfss://gold@stupramonitoreomercado.dfs.core.windows.net/UPRA/MERCADO_TIERRAS/transacciones_rurales_linderos.tsv")

#title = "Transacciones Mercado tierras - Disclaimer: this profiling report was generated using a sample of 5% of the original dataset."
#profile = ProfileReport(df_join_transacc_4.select("*").toPandas().sample(frac=0.05), title=title,  minimal=False)

title = "Transacciones Mercado tierras - Linderos"
profile = ProfileReport(df_result.select("*").toPandas(), title=title,  minimal=True)

#displayHTML(profile.to_html())
dbutils.fs.put("abfss://gold@stupramonitoreomercado.dfs.core.windows.net/UPRA/MERCADO_TIERRAS/transacciones_rurales_linderos.parquet_profile.html", profile.to_html() )

# COMMAND ----------


