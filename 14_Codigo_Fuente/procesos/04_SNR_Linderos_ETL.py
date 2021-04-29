# Databricks notebook source
# MAGIC %md # Extraer información de linderos

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

spark.conf.set("fs.azure.account.key.stupramonitoreomercado.dfs.core.windows.net", "XXXXX")
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")
dbutils.fs.ls("abfss://raw@stupramonitoreomercado.dfs.core.windows.net/")
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false")
#spark.conf.set("spark.sql.shuffle.partitions",3000)
#spark.conf.set("spark.default.parallelism",3000)

# COMMAND ----------

df = spark.read.parquet("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/transacciones_clean_rural_linderos_depuracion.parquet")

'''
Se realizó un análisis de frecuencia de la palabra “AREA” en los linderos de las transacciones rurales encontrando lo siguiente: 

    En el 43.2%  (442621)	 no se encontró la palabra “AREA” o "Cabida"

    El 	49.2%  (504027) de los registros contienen la palabra “AREA” o "Cabida" una sola vez 

    Hay 7.6 %  en donde aparece más de una vez la palabra “AREA” 

El usuario temático realizó las siguientes observaciones: 

    Para el caso de aquellos linderos que no contienen la palabra AREA se debe realizar la búsqueda del término “CABIDA” con su respectivo valor numérico y unidad. 
    Para el de los registros contienen la palabra “AREA” una sola vez se debe realizar la búsqueda del valor numérico y unidad correspondiente. 

    No se deben tener en cuenta los registros que tengan más de una vez la palabra área. 
    
    No se deben tener en cuenta los registros que no tengan la palabra área o cabida  
'''

df.display()

# COMMAND ----------

df.count() # 1024775

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# DBTITLE 1,Conteo de palabras clave iniciales para extracción de linderos
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
      
    # si no encuentra CABIDA buscar SUPERFICIE
    if count == 0:
      cabida_list = list(filter(lambda x: x == "SUPERFICIE", words) )
      count = len(cabida_list)
      
    # si no encuentra SUPERFICIE buscar EXTENSION
    if count == 0:
      cabida_list = list(filter(lambda x: x == "EXTENSION", words) )
      count = len(cabida_list)
      
  return count

count_words_udf = udf(count_words, IntegerType() )

df = df.withColumn("CANTIDAD_PALABRAS_CLAVE_INICIO", count_words_udf(df.LINDERO_CLEAN)  )
#df_linderos_rurales_count.show()
df.createOrReplaceTempView("linderos")
spark.sql("""
select CANTIDAD_PALABRAS_CLAVE_INICIO, count(*)
from linderos
group by CANTIDAD_PALABRAS_CLAVE_INICIO
order by CANTIDAD_PALABRAS_CLAVE_INICIO
""").display()



# COMMAND ----------


# en cuenta únicamente los que tienen una palabra clave (una sola área)

df = df.withColumn('LINDERO_CLEAN', F.regexp_replace('LINDERO_CLEAN', r' HAC ',   ' HECTAREAS ')) 
df = df.withColumn('LINDERO_CLEAN', F.regexp_replace('LINDERO_CLEAN', r' HCTS ',  ' HECTAREAS ')) 
df = df.withColumn('LINDERO_CLEAN', F.regexp_replace('LINDERO_CLEAN', r' HECTAREA ',  ' HECTAREAS ')) 

df.createOrReplaceTempView("linderos")

#  regexp_extract(LINDERO_CLEAN, '(?<= AREA | CABIDA |LOTE DE TERRENO)(.*?)(?=((METRO(\s|S))|(HECTAREA(\s|S))))', 1)  
#  regexp_extract(LINDERO_CLEAN, '(?<=AREA|CABIDA|LOTE DE TERRENO)(((?!AREA|CABIDA|LOTE DE TERRENO).)*?)(?=((METRO(\s|S))|(HECTAREA(\s|S))))', 1)  as b, 
df_linderos_procesados = spark.sql("""
 select pk, id, 
 trim( regexp_replace(     
      regexp_extract(LINDERO_CLEAN, '(?<=AREA|CABIDA|SUPERFICIE|EXTENSION)(((?!AREA|CABIDA|SUPERFICIE|EXTENSION).)*?)(?=( (HECTAREA(\s|S)) | (METRO(\s|S))|( FANEGADAS )|( CUADRAS )|( PLAZAS ) |( VARAS )  ))', 1)  
  , '[a-zA-Z]', ' ') )  AS AREA,
      regexp_extract(LINDERO_CLEAN, '(?<=AREA|CABIDA|SUPERFICIE|EXTENSION)(.*?)(?=( (HECTAREA(\s|S))|(METRO(\s|S))|( FANEGADAS )|( CUADRAS )|( PLAZAS ) |( VARAS ) ))', 2) AS UNIDAD,
 LINDERO_CLEAN, lindero, vereda, direccion, DIRECCION_CLEAN, CANTIDAD_PALABRAS_CLAVE_INICIO
 from linderos
 where CANTIDAD_PALABRAS_CLAVE_INICIO = 1
""")
df_linderos_procesados = df_linderos_procesados.withColumn('AREA_CLEAN', F.regexp_replace('AREA', r'[\\(\\);\\"]' , ''))
df_linderos_procesados = df_linderos_procesados.withColumn('AREA_CLEAN', F.regexp_replace('AREA_CLEAN', r':' , ''))
df_linderos_procesados = df_linderos_procesados.withColumn('AREA_CLEAN', F.regexp_replace('AREA_CLEAN', r'\'' , ''))
df_linderos_procesados = df_linderos_procesados.withColumn('AREA_CLEAN', F.regexp_replace('AREA_CLEAN', r'(^| )-' , ''))
# remover , al comienzo o al final
df_linderos_procesados = df_linderos_procesados.withColumn('AREA_CLEAN', F.regexp_replace('AREA_CLEAN', r'(^|,+)(,+|\s|$)' , ''))
# ...
df_linderos_procesados = df_linderos_procesados.withColumn('AREA_CLEAN', F.regexp_replace('AREA_CLEAN', r'\s+\.+(\s+|$)' , ''))
# remover . al inicio
df_linderos_procesados = df_linderos_procesados.withColumn('AREA_CLEAN', F.regexp_replace('AREA_CLEAN', r'(^|\s|\.)\.+' , ''))
# remover . al final
df_linderos_procesados = df_linderos_procesados.withColumn('AREA_CLEAN', F.regexp_replace('AREA_CLEAN', r'\.+($|\s)' , ''))
# eliminar signo negativo al inicio de la cadena
df_linderos_procesados = df_linderos_procesados.withColumn('AREA_CLEAN', F.regexp_replace('AREA_CLEAN', r'(^| |)-+' , ''))

df_linderos_procesados = df_linderos_procesados.withColumn('AREA_CLEAN', F.trim('AREA_CLEAN'))
df_linderos_procesados = df_linderos_procesados.withColumn("AREA_CLEAN_FLOAT", df_linderos_procesados.AREA_CLEAN.cast('FLOAT'))
df_linderos_procesados = df_linderos_procesados.withColumn('UNIDAD', F.trim('UNIDAD'))

def area_to_ha(unidad, area ):
  if area is None or unidad == "":
    return None
  if ( unidad == "HECTAREAS" ):
    return area
  if ( unidad == "METROS" ):
    return area / 10000
  if ( unidad == "FANEGADAS" ):
    return area * 0.64
  if ( unidad == "CUADRAS" ):
    # La cuadra es la misma fanegada, es decir 6,400 m2.  http://abc.finkeros.com/a-que-equivale-un-cuadra/
    return area * 0.64 
  else:
    return None

area_to_ha_udf = udf(area_to_ha, FloatType() )

#  Notas: hay algunas personas que separan apellido y nombre usando una coma 
df_linderos_procesados = df_linderos_procesados.withColumn("AREA_HA", area_to_ha_udf(df_linderos_procesados.UNIDAD, df_linderos_procesados.AREA_CLEAN_FLOAT   )  )

df_linderos_procesados = df_linderos_procesados.cache()
df_linderos_procesados.createOrReplaceTempView("linderos_procesados")

df_linderos_procesados.select("pk", "id", "unidad" , "AREA_CLEAN_FLOAT" ,"AREA_HA", "AREA_CLEAN", "AREA", "LINDERO_CLEAN" ).display()
#df_linderos_procesados.display()



# COMMAND ----------

df_linderos_procesados.count() # 628586

# COMMAND ----------

spark.sql("""
SELECT pk, id, unidad , AREA_CLEAN_FLOAT , AREA_HA, AREA_CLEAN, AREA, LINDERO_CLEAN
FROM linderos_procesados
where   UNIDAD = 'METROS' and AREA_HA IS not NULL
-- and  
-- AREA_HA < 0 
-- id = 2757 (100 ..)       id = 1877 (superficie)   id = 2993  (extension)  1525 (HCTS)     id = 410847 (-5555)
limit 10  

""").display()

# COMMAND ----------

spark.sql("""
SELECT unidad, count(*)
FROM linderos_procesados
group by unidad 
""").display()

# COMMAND ----------

df_linderos_procesados.printSchema()

# COMMAND ----------

df_linderos_procesados.display()

# COMMAND ----------

#unir los procesados (1 palabra clave) con el resto
df_sin_procesar = df.filter(df.CANTIDAD_PALABRAS_CLAVE_INICIO != 1)
df_sin_procesar.count()
df_sin_procesar = df_sin_procesar.withColumn('AREA_CLEAN', F.lit(None).cast(StringType()) )
df_sin_procesar = df_sin_procesar.withColumn('AREA_CLEAN_FLOAT', F.lit(None).cast(FloatType()) )
df_sin_procesar = df_sin_procesar.withColumn('AREA_HA', F.lit(None).cast(FloatType())  )
df_sin_procesar = df_sin_procesar.withColumn('AREA', F.lit(None).cast(StringType()) )
df_sin_procesar = df_sin_procesar.withColumn('UNIDAD', F.lit(None).cast(StringType()) )
df_sin_procesar.printSchema()

# COMMAND ----------

#df_sin_procesar.display()
df_linderos_procesados.printSchema()

# COMMAND ----------

columns = df_linderos_procesados.columns
columns

# COMMAND ----------

df_union = df_linderos_procesados.select(*columns).union( df_sin_procesar.select(*columns))
df_union.count() # 1024775

# COMMAND ----------

df_union.printSchema()
df_union.display()

# COMMAND ----------

df_union.write.parquet("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/transacciones_clean_rural_linderos_depuracion_area.parquet")
profile = ProfileReport(df_union.select("*").toPandas() , title="Transacciones - Linderos depurados con extracción area", minimal=True)
#displayHTML(profile.to_html())
dbutils.fs.put("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/transacciones_clean_rural_linderos_depuracion_area.parquet_profile.html", profile.to_html() )

# COMMAND ----------


