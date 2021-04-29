# Databricks notebook source
# MAGIC %md # Limpieza y transformación de información de transacciones 

# COMMAND ----------

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

# DBTITLE 1,Leer datos desde el Data Lake
df = spark.read.parquet("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/transacciones.parquet")
#display(from_json(df2.select("col"))
df = df.withColumn("COD_NATURALEZA_INT", df.COD_NATURALEZA.cast('INTEGER'))
df = df.drop("COD_NATURALEZA")
df = df.withColumnRenamed("COD_NATURALEZA_INT", "COD_NATURALEZA")
df.cache()
display(df)

# COMMAND ----------

df.count()  #todos incluyendo urbanos y rurales  4894440

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# DBTITLE 1,Generación de llave única
df.createOrReplaceTempView("transacciones")
# divipola donde fue la transaccion + cod oficina orip + matricula inmobiliaria + num anotacion (hasta 5 digitos) +  cod nat juridica  
df2 = spark.sql("""
SELECT distinct  lpad( COD_DANE, 5, '0')  || '-' ||    
lpad(split(MATRICULA, '-')[0], 3, '0')    || '-' || lpad( split(MATRICULA, '-')[1], 6, '0' )
|| '-' ||  lpad( NRO_ANOTACION, 5, '0') || '-' ||  lpad(   COD_NATURALEZA, 3, '0')  || '-' || ANIO_RADICACION as pk, 
lpad( COD_DANE, 5, '0') as COD_DANE, lpad(ORIP, 3, '0')  as ORIP , 
lpad(split(MATRICULA, '-')[0], 3, '0')   AS MATRICULA_ORIP,  lpad( split(MATRICULA, '-')[1], 6, '0' ) AS MATRICULA_NUMERO, 
 lpad( NRO_ANOTACION, 5, '0') AS NRO_ANOTACION, COD_NATURALEZA, DEPARTAMENTO,  MUNICIPIO, trim(VEREDA) as VEREDA, TIPO_PREDIO, NATURALEZA_JURIDICA, DIRECCION, 
 trim( regexp_replace(  regexp_replace(  regexp_replace( upper(DIRECCION),  ' +', ' ') ,  ',+', ','  )   , '\"', ''))     as DIRECCION_CLEAN  ,
 SISTEMA, ANIO_RADICACION, LINDERO, 
 trim( regexp_replace(  regexp_replace(  regexp_replace( upper(LINDERO),  ' +', ' ') ,  ',+', ','  )     , '\"', ''))     as LINDERO_CLEAN  
 , VALOR, ESTADO_FOLIO
FROM transacciones
where ANIO_RADICACION > 2014 and ANIO_RADICACION < 2020  and TIPO_PREDIO <> 'URBANO'
""")

#df_key_transaccion = df_key_transaccion.withColumn('DIRECCION_CLEAN', F.regexp_replace('DIRECCION_CLEAN', r'\"', ''))   
# No se pueden adicionar porque se duplican registros: NRO_PROPIETARIOS   , ANIO_RADICACION, ESTADO_FOLIO

df2.display()

# COMMAND ----------

df2.count()
#     1323805 sin urbanos, con el campo valor y estado folio          

# COMMAND ----------

df2.createOrReplaceTempView("transacciones_filtered")

# COMMAND ----------

# validar pk duplicadas
spark.sql("""
with a as ( 
SELECT pk, count(*)
FROM transacciones_filtered
group by pk
having count(*)  > 1)
select t.pk, t.valor, t.tipo_predio, t.lindero_clean, t.direccion, t.*
from transacciones_filtered as t inner join a on (t.pk = a.pk) 
order by t.pk
""").display()


# COMMAND ----------

df2.dropDuplicates().count() # 1323805

# COMMAND ----------


df2.write.parquet("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/transacciones_clean.parquet")
profile = ProfileReport(df2.select("*").toPandas() , title="Transacciones inmobiliarias - Llave primaria", minimal=True)
#displayHTML(profile.to_html())
dbutils.fs.put("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/transacciones_clean.parquet_profile.html", profile.to_html() )

# COMMAND ----------

# DBTITLE 1,Encontrar rural Inferido
#df = spark.read.parquet("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/transacciones_principal.parquet")
df_sin_informacion = df2.filter(df.TIPO_PREDIO == "SIN INFORMACION")
df_sin_informacion.count() # 113741   174274

# COMMAND ----------

display(df_sin_informacion)

# COMMAND ----------

df_dir_common = df_sin_informacion.withColumn('word', F.explode(F.split(F.col('DIRECCION_CLEAN'), ' ')))\
    .groupBy('word')\
    .count()\
    .sort('count', ascending=False)

df_dir_common.display()

# COMMAND ----------

df_dir_common.coalesce(1).write.json("abfss://sandbox@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/direccion_sin_info_palabras_comunes.json")

# COMMAND ----------

df_rural_inferido = df_sin_informacion.filter(df_sin_informacion.DIRECCION_CLEAN.contains("CALLE") == False)
df_rural_inferido = df_rural_inferido.filter(df_rural_inferido.DIRECCION_CLEAN.contains("CARRERA") == False)
df_rural_inferido = df_rural_inferido.filter(df_rural_inferido.DIRECCION_CLEAN.contains("DIAGONAL") == False)
df_rural_inferido = df_rural_inferido.filter(df_rural_inferido.DIRECCION_CLEAN.contains("MANZANA") == False)
df_rural_inferido = df_rural_inferido.filter(df_rural_inferido.DIRECCION_CLEAN.contains("URBANIZACION") == False)
df_rural_inferido = df_rural_inferido.filter(df_rural_inferido.DIRECCION_CLEAN.contains("APARTAMENTO") == False)
df_rural_inferido = df_rural_inferido.filter(df_rural_inferido.DIRECCION_CLEAN.contains("EDIFICIO") == False)
df_rural_inferido = df_rural_inferido.filter(df_rural_inferido.DIRECCION_CLEAN.contains("BARRIO") == False)
df_rural_inferido = df_rural_inferido.filter(df_rural_inferido.DIRECCION_CLEAN.contains("CONJUNTO") == False)
df_rural_inferido = df_rural_inferido.filter(df_rural_inferido.DIRECCION_CLEAN.contains("HORIZONTAL") == False)
df_rural_inferido = df_rural_inferido.filter(df_rural_inferido.DIRECCION_CLEAN.contains("CEMENTERIO") == False)
df_rural_inferido = df_rural_inferido.filter(df_rural_inferido.DIRECCION_CLEAN.contains("TRANSVERSAL") == False)
df_rural_inferido = df_rural_inferido.filter(df_rural_inferido.DIRECCION_CLEAN.contains("P.H.") == False)
df_rural_inferido = df_rural_inferido.filter(df_rural_inferido.DIRECCION_CLEAN.contains("LOCAL") == False)
df_rural_inferido = df_rural_inferido.filter(df_rural_inferido.DIRECCION_CLEAN.contains("APTO") == False)
df_rural_inferido = df_rural_inferido.filter(df_rural_inferido.DIRECCION_CLEAN.contains("PARQUEADERO") == False)

df_rural_inferido.count()  # 31894   54668

# COMMAND ----------

df_rural = df2.filter(df2.TIPO_PREDIO == "RURAL")
df_rural.count()  # 970343

# COMMAND ----------

df_rural_todos = df_rural.union(df_rural_inferido)
df_rural_todos.count() #   1025011

# COMMAND ----------

df_rural_todos.write.parquet("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/transacciones_clean_rural.parquet")
profile = ProfileReport(df_rural_todos.select("*").toPandas() , title="Transacciones - Rurales", minimal=True)
#displayHTML(profile.to_html())
dbutils.fs.put("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/transacciones_clean_rural.parquet_profile.html", profile.to_html() )

# COMMAND ----------

# DBTITLE 1,Exportar archivo linderos
df_rural_todos.createOrReplaceTempView("rurales")
df_linderos =  spark.sql("""
select distinct  r.pk, r.vereda,  r.direccion, r.DIRECCION_CLEAN,  r.lindero,  r.LINDERO_CLEAN
from rurales as r 
order by r.pk  ASC
""")
df_linderos.count()

# COMMAND ----------

df_linderos = df_linderos.withColumn("id",   F.row_number().over(Window.orderBy("pk"))     )
df_linderos.display()

# COMMAND ----------

df_linderos.write.parquet("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/transacciones_clean_rural_linderos.parquet")
profile = ProfileReport(df_linderos.select("*").toPandas() , title="Transacciones - Rurales - Linderos", minimal=True)
#displayHTML(profile.to_html())
dbutils.fs.put("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/transacciones_clean_rural_linderos.parquet_profile.html", profile.to_html() )

# COMMAND ----------

df_rural_todos_sin_lindero =  df_rural_todos.drop("DEPARTAMENTO", "MUNICIPIO" , "NATURALEZA_JURIDICA", "VEREDA", "DIRECCION", "DIRECCION_CLEAN", "LINDERO", "LINDERO_CLEAN")
df_rural_todos_sin_lindero.count()
#df_rural_todos_sin_lindero.display()

# COMMAND ----------

df_rural_todos_sin_lindero = df_rural_todos_sin_lindero.dropDuplicates()
df_rural_todos_sin_lindero.count()

# COMMAND ----------

# DBTITLE 1,Llave primaria duplicada (múltiples años o múltiples direcciones)
df_rural_todos_sin_lindero.createOrReplaceTempView("rurales_sin_lindero")
spark.sql("""
SELECT pk , count(*)
FROM rurales_sin_lindero
group by pk  having count(*) >1 
order by  2 desc
""").display()
# 0

# COMMAND ----------

spark.sql("""
SELECT *
FROM rurales_sin_lindero
where pk = '11001-50C-192707-00011-205-2016'
""").display()

# COMMAND ----------

df_rural_todos_sin_lindero.write.parquet("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/transacciones_clean_rural_sin_linderos.parquet")
profile = ProfileReport(df_rural_todos_sin_lindero.select("*").toPandas() , title="Transacciones - Rurales - Sin columnas Linderos", minimal=True)
#displayHTML(profile.to_html())
dbutils.fs.put("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/transacciones_clean_rural_sin_linderos.parquet_profile.html", profile.to_html() )

# COMMAND ----------

# DBTITLE 1,Procesamiento de Linderos para predios rurales
df = spark.read.parquet("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/transacciones_clean_rural_linderos.parquet")
df.display()
df.count()


# COMMAND ----------

# DBTITLE 1,Limpieza del texto "LINDERO"
df = df.drop("LINDERO_CLEAN")
df = df.withColumn('LINDERO_CLEAN', F.upper('LINDERO'))  
df = df.withColumn('LINDERO_CLEAN', F.regexp_replace('LINDERO_CLEAN', r',+', ', '))
#df = df.withColumn('LINDERO_CLEAN', F.regexp_replace('LINDERO_CLEAN', r'\.+', '\. '))
df = df.withColumn('LINDERO_CLEAN', F.regexp_replace('LINDERO_CLEAN', r'_', ' '))  
df = df.withColumn('LINDERO_CLEAN', F.regexp_replace('LINDERO_CLEAN', r'HACTAREAS', ' HECTAREAS'))  
df = df.withColumn('LINDERO_CLEAN', F.regexp_replace('LINDERO_CLEAN', r'HETAREAS',  ' HECTAREAS'))  
df = df.withColumn('LINDERO_CLEAN', F.regexp_replace('LINDERO_CLEAN', r'HECATAREAS', ' HECTAREAS'))  
df = df.withColumn('LINDERO_CLEAN', F.regexp_replace('LINDERO_CLEAN', r'ECTAREAS',  ' HECTAREAS'))  
df = df.withColumn('LINDERO_CLEAN', F.regexp_replace('LINDERO_CLEAN', r'HESTAREAS', ' HECTAREAS'))  
df = df.withColumn('LINDERO_CLEAN', F.regexp_replace('LINDERO_CLEAN', r'HCTAREAS',  ' HECTAREAS')) 
df = df.withColumn('LINDERO_CLEAN', F.regexp_replace('LINDERO_CLEAN', r'HETCAREAS',  ' HECTAREAS')) 
df = df.withColumn('LINDERO_CLEAN', F.regexp_replace('LINDERO_CLEAN', r'EHCTAREAS',  ' HECTAREAS')) 
#df = df.withColumn('LINDERO_CLEAN', F.regexp_replace('LINDERO_CLEAN', r' HAS ',  ' HECTAREAS ')) 
#df = df.withColumn('LINDERO_CLEAN', F.regexp_replace('LINDERO_CLEAN', r' HAS, ',  ' HECTAREAS ')) 
#df = df.withColumn('LINDERO_CLEAN', F.regexp_replace('LINDERO_CLEAN', r' HAS, ',  ' HECTAREAS ')) 

df = df.withColumn('LINDERO_CLEAN', F.regexp_replace('LINDERO_CLEAN', r'HECT[^\w]+AREAS', ' HECTAREAS'))  
df = df.withColumn('LINDERO_CLEAN', F.regexp_replace('LINDERO_CLEAN', r'AREA[^\w]+', 'AREA '))
df = df.withColumn('LINDERO_CLEAN', F.regexp_replace('LINDERO_CLEAN', r'AREAS[^\w]+', 'AREAS '))
df = df.withColumn('LINDERO_CLEAN', F.regexp_replace('LINDERO_CLEAN', r'[^\w]+AREA', ' AREA'))
df = df.withColumn('LINDERO_CLEAN', F.regexp_replace('LINDERO_CLEAN', r'HECTAREA[^\w]+', 'HECTAREA '))
df = df.withColumn('LINDERO_CLEAN', F.regexp_replace('LINDERO_CLEAN', r'HECTAREAS[^\w]+', 'HECTAREAS '))
df = df.withColumn('LINDERO_CLEAN', F.regexp_replace('LINDERO_CLEAN', r'[^\w]+HECTAREA', ' HECTAREA'))
df = df.withColumn('LINDERO_CLEAN', F.regexp_replace('LINDERO_CLEAN', r'METROS2',  ' METROS CUADRADOS ')) 
df = df.withColumn('LINDERO_CLEAN', F.regexp_replace('LINDERO_CLEAN', r'M2',  ' METROS CUADRADOS ')) 
df = df.withColumn('LINDERO_CLEAN', F.regexp_replace('LINDERO_CLEAN', r'MTRS2',  ' METROS CUADRADOS ')) 
df = df.withColumn('LINDERO_CLEAN', F.regexp_replace('LINDERO_CLEAN', r'MTS.2',  ' METROS CUADRADOS ')) 
df = df.withColumn('LINDERO_CLEAN', F.regexp_replace('LINDERO_CLEAN', r'MTS',  ' METROS ')) 
# separa número de caracter. ejm  2HECTAREAS ->  2 HECTAREAS
df = df.withColumn('LINDERO_CLEAN', F.regexp_replace('LINDERO_CLEAN', r'(?<=\d)(?=[a-zA-Z])', ' '))
# separa caracter de  número . ejm  METROS371,39 ->  METROS 371,39
df = df.withColumn('LINDERO_CLEAN', F.regexp_replace('LINDERO_CLEAN', r'(?<=[a-zA-Z])(?=\d)', ' '))
# casos: HAS HAS,  HAS.  HAS) 
df = df.withColumn('LINDERO_CLEAN', F.regexp_replace('LINDERO_CLEAN', r' HAS(\W)',  ' HECTAREAS ')) 
# casos (11.HAS  3.HAS"    3.HAS.5.604. METROS 
df = df.withColumn('LINDERO_CLEAN', F.regexp_replace('LINDERO_CLEAN', r'(\W)HAS(\W)',  ' HECTAREAS ')) 

# METROS; METROS,   ,METROS.
df = df.withColumn('LINDERO_CLEAN', F.regexp_replace('LINDERO_CLEAN', r'(?<=[a-zA-Z])(?=\W)', ' '))
df = df.withColumn('LINDERO_CLEAN', F.regexp_replace('LINDERO_CLEAN', r'(?<=\W)(?=[a-zA-Z])', ' '))

df = df.withColumn('LINDERO_CLEAN', F.regexp_replace('LINDERO_CLEAN', r' HAS ',  ' HECTAREAS ')) 
# limpieza de espacios en blanco múltiples
df = df.withColumn('LINDERO_CLEAN', F.regexp_replace('LINDERO_CLEAN', r' +', ' '))
#  HECTAREASY    HECTAREASM   HECTAREASEXTENSION
df = df.withColumn('LINDERO_CLEAN', F.regexp_replace('LINDERO_CLEAN', r'(?<= HECTAREAS)(?=\w)', ' '))

# separar palabras:  METROSCUADRADOS  METROSDEL METROSHASTA  
df = df.withColumn('LINDERO_CLEAN', F.regexp_replace('LINDERO_CLEAN', r'(?<= METROS)(?=\w)', ' '))
# separar palabras:   CABIDADE  CABIDAAPROXIMADA 
df = df.withColumn('LINDERO_CLEAN', F.regexp_replace('LINDERO_CLEAN', r'(?<= CABIDA)(?=\w)', ' '))

# parte 2
df = df.withColumn('LINDERO_CLEAN', F.regexp_replace('LINDERO_CLEAN', r' MT 2 ',  ' METROS CUADRADOS')) 
df = df.withColumn('LINDERO_CLEAN', F.regexp_replace('LINDERO_CLEAN', r' MT ',  ' METROS ')) 
df = df.withColumn('LINDERO_CLEAN', F.regexp_replace('LINDERO_CLEAN', r' M ',  ' METROS ')) 
df = df.withColumn('LINDERO_CLEAN', F.regexp_replace('LINDERO_CLEAN', r' MTRS ',  ' METROS ')) 
df = df.withColumn('LINDERO_CLEAN', F.regexp_replace('LINDERO_CLEAN', r' HS ',  ' HECTAREAS ')) 
df = df.withColumn('LINDERO_CLEAN', F.regexp_replace('LINDERO_CLEAN', r' HA ',  ' HECTAREA ')) 
df = df.withColumn('LINDERO_CLEAN', F.regexp_replace('LINDERO_CLEAN', r' H HECTAREAS',  ' HECTAREAS ')) 
df = df.withColumn('LINDERO_CLEAN', F.regexp_replace('LINDERO_CLEAN', r' : ',  ' ')) 
df = df.withColumn('LINDERO_CLEAN', F.regexp_replace('LINDERO_CLEAN', r' FANEGADA ',  ' FANEGADAS ')) 
df = df.withColumn('LINDERO_CLEAN', F.regexp_replace('LINDERO_CLEAN', r' CUADRA ',  ' CUADRAS ')) 
df = df.withColumn('LINDERO_CLEAN', F.regexp_replace('LINDERO_CLEAN', r' PLAZA ',  ' PLAZAS ')) 


df.createOrReplaceTempView("linderos")

## TODO 
##  reeemplazar ... ,,,   
# https://regex101.com/r/wH1nK1/1
# https://docs.microsoft.com/en-us/dotnet/standard/base-types/regular-expression-language-quick-reference

# COMMAND ----------

spark.sql("""

SELECT pk,  lindero_clean, lindero
FROM linderos
where  lindero_clean like "% FANEGADA %"  limit 50   
""").display()

#lindero_clean like "% AREAS %" 

# COMMAND ----------


df_lin_common = df.withColumn('word', F.explode(F.split(F.col('LINDERO_CLEAN'), ' ')))\
    .groupBy('word')\
    .count()\
    .sort('count', ascending=False)

df_lin_common.coalesce(1).write.mode("overwrite").json("abfss://sandbox@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/rural_linderos_palabras_comunes.json")
df_lin_common.display()

# COMMAND ----------


df.write.parquet("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/transacciones_clean_rural_linderos_depuracion.parquet")
profile = ProfileReport(df.select("*").toPandas() , title="Transacciones - Rurales - Sin columnas Linderos - Campo lindero depurado", minimal=True)
#displayHTML(profile.to_html())
dbutils.fs.put("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/transacciones_clean_rural_linderos_depuracion.parquet_profile.html", profile.to_html() )

# COMMAND ----------

# DBTITLE 1,Generar listados y reportes - Cargar Linderos depurados desde el data lake
df_rural_todos = spark.read.parquet("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/transacciones_clean_rural_linderos_depuracion.parquet")
df_rural_todos.display()

# COMMAND ----------

# DBTITLE 1,Contar ocurrencias de "AREA" o "CABIDA" en linderos de los predios rurales
def count_words(nombre):
  count = 0
  if nombre is None:
    count = 0
  else:
    words = nombre.split(" ")
    area_list = list(filter(lambda x: x == "AREA", words) )
    count = len(area_list)
    
    # si no encuentra AREA buscar CABIDA
    if count == 0:
      cabida_list = list(filter(lambda x: x == "CABIDA", words) )
      count = len(cabida_list)
      
  return count

count_words_udf = udf(count_words, IntegerType() )

df_linderos_rurales_count = df_rural_todos.withColumn("lindero_cantidad_areas", count_words_udf(df_rural_todos.LINDERO_CLEAN)  )
#df_linderos_rurales_count.show()
display(df_linderos_rurales_count)

# COMMAND ----------

display(df_linderos_rurales_count.filter("lindero_cantidad_areas == 0"))

# COMMAND ----------

display(df_linderos_rurales_count.filter("lindero_cantidad_areas == 1"))

# COMMAND ----------

display(df_linderos_rurales_count.filter("lindero_cantidad_areas > 6"))

# COMMAND ----------

df_linderos_rurales_count.printSchema()

# COMMAND ----------

df_linderos_rurales_count.createOrReplaceTempView("lindero_r_conteo")

df_export = spark.sql(
  """ 
 select pk, lindero_cantidad_areas , LINDERO_CLEAN 
 from  lindero_r_conteo l
 where l.lindero_cantidad_areas > 1
 ORDER BY lindero_cantidad_areas ASC 
 """ ) 

df_export.count()

# COMMAND ----------

df_export.coalesce(1).write.option("delimiter", "\t").option('header',True).mode('overwrite')\
  .csv("abfss://sandbox@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/transacciones_linderos_rurales_todos_multiples_areas.tsv")

# COMMAND ----------

df_linderos_rurales_count.write.parquet("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/transacciones_clean_rural_linderos_depuracion_conteo.parquet")
profile = ProfileReport(df_linderos_rurales_count.select("*").toPandas() , title="Transacciones - Rurales - Sin columnas Linderos - Campo lindero depurado - Conteo", minimal=True)
#displayHTML(profile.to_html())
dbutils.fs.put("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/transacciones_clean_rural_linderos_depuracion_conteo.parquet_profile.html", profile.to_html() )

# COMMAND ----------

# DBTITLE 1,Exportar linderos sin Area
df_linderos_sin_area = df_linderos_rurales_count.filter("lindero_cantidad_areas == 0")
df_linderos_sin_area.count()

# COMMAND ----------

df_linderos_sin_area.coalesce(1).write.option("delimiter", "\t").option('header',True).mode('overwrite')\
  .csv("abfss://sandbox@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/transacciones_linderos_rurales_sin_areas.tsv")

# COMMAND ----------

df_lin_common = df_linderos_sin_area.withColumn('word', F.explode(F.split(F.col('LINDERO_CLEAN'), ' ')))\
    .groupBy('word')\
    .count()\
    .sort('count', ascending=False)

df_lin_common.coalesce(1).write.mode("overwrite").json("abfss://sandbox@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/transacciones_linderos_rurales_sin_areas_palabras_comunes.json")

# COMMAND ----------


