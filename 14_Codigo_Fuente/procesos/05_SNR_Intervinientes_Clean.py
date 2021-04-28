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

# DBTITLE 1,Generación de llave primaria
df2 = spark.read.parquet("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/intervinientes.parquet")
df2 = df2.withColumn("COD_NATURALEZA_INT", df2.COD_NATURALEZA.cast('INTEGER'))
df2 = df2.drop("COD_NATURALEZA")
df2 = df2.withColumnRenamed("COD_NATURALEZA_INT", "COD_NATURALEZA")


df2.createOrReplaceTempView("intervinientes")

df_intervinientes = spark.sql("""
SELECT distinct  lpad( COD_DANE, 5, '0')  || '-' ||    
lpad(split(MATRICULA, '-')[0], 3, '0')    || '-' || lpad( split(MATRICULA, '-')[1], 6, '0' )
|| '-' ||  lpad( NRO_ANOTACION, 5, '0') || '-' ||  lpad(   COD_NATURALEZA, 3, '0') || '-' || ANIO_RADICACION   as pk, 
trim(NUMERO_DOCUMENTO) as NUMERO_DOCUMENTO, 
trim(PERSONA) as PERSONA, ROL, trim(TIPO_DOCUMENTO) as TIPO_DOCUMENTO
FROM intervinientes 
where ANIO_RADICACION > 2014 and ANIO_RADICACION < 2020
""")
df_intervinientes.count()
df_intervinientes.display()

# COMMAND ----------

df_intervinientes.count()

# COMMAND ----------

df_intervinientes = df_intervinientes.dropDuplicates()
df_intervinientes.count()

# COMMAND ----------

df_intervinientes.write.parquet("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/intervinientes_clean.parquet")
profile = ProfileReport(df_intervinientes.select("*").toPandas(), title="Intervinientes",  minimal=True)
#displayHTML(profile.to_html())
dbutils.fs.put("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/intervinientes_clean.parquet_profile.html", profile.to_html() )

# COMMAND ----------

# DBTITLE 1,Leer datos Crudos desde el Data Lake
df2 = spark.read.parquet("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/intervinientes_clean.parquet")
#display(from_json(df2.select("col"))
display(df2)

# COMMAND ----------

df2.printSchema()

# COMMAND ----------

df2.count()

# COMMAND ----------

#df2 = df2.withColumn("id", F.monotonically_increasing_id())
df2 = df2.withColumn("id",    F.row_number().over(Window.orderBy("pk"))     )
df2.cache()
df2.display()

# COMMAND ----------

df2.createOrReplaceTempView("intervinientes")

# COMMAND ----------

# listado de funciones   https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#module-pyspark.sql.functions
df_clean =   spark.sql("""
SELECT  *,
trim( 
  regexp_replace( regexp_replace(  regexp_replace( upper(PERSONA), ',+', ',' )  , '[^a-zA-Z ,]+' , '' ) , ' +', ' '  )    
  )  as PERSONA_CLEAN  , 
trim(NUMERO_DOCUMENTO) as NUMERO_DOCUMENTO_CLEAN 
FROM intervinientes
""")
df_clean = df_clean.withColumn('PERSONA_CLEAN', F.regexp_replace('PERSONA_CLEAN', r' DEL ', ' '))
df_clean = df_clean.withColumn('PERSONA_CLEAN', F.regexp_replace('PERSONA_CLEAN', r' DE LA ', ' '))
df_clean = df_clean.withColumn('PERSONA_CLEAN', F.regexp_replace('PERSONA_CLEAN', r' DE LAS ', ' '))
df_clean = df_clean.withColumn('PERSONA_CLEAN', F.regexp_replace('PERSONA_CLEAN', r' DE ', ' '))
df_clean = df_clean.withColumn('PERSONA_CLEAN', F.regexp_replace('PERSONA_CLEAN', r' LOS ', ' '))
df_clean = df_clean.withColumn('PERSONA_CLEAN', F.regexp_replace('PERSONA_CLEAN', r'^DEL ', ''))
df_clean = df_clean.withColumn('PERSONA_CLEAN', F.regexp_replace('PERSONA_CLEAN', r'^DE LA ', ''))
df_clean = df_clean.withColumn('PERSONA_CLEAN', F.regexp_replace('PERSONA_CLEAN', r'^DE LAS ', ''))
df_clean = df_clean.withColumn('PERSONA_CLEAN', F.regexp_replace('PERSONA_CLEAN', r'^DE ', ''))
df_clean = df_clean.withColumn('PERSONA_CLEAN', F.regexp_replace('PERSONA_CLEAN', r'^LOS ', ''))
df_clean = df_clean.withColumn('PERSONA_CLEAN', F.regexp_replace('PERSONA_CLEAN', r'^LA ', ''))
df_clean = df_clean.withColumn('PERSONA_CLEAN', F.regexp_replace('PERSONA_CLEAN', r' S A(\s|$)', ' SA'))
df_clean = df_clean.withColumn('PERSONA_CLEAN', F.regexp_replace('PERSONA_CLEAN', r' S A S(\s|$)', ' SAS'))
#remover coma entre letras (Ejm S,A  S,A,S )
df_clean = df_clean.withColumn('PERSONA_CLEAN', F.regexp_replace('PERSONA_CLEAN', r'(?![a-zA-Z]),(?=[a-zA-Z])', ''))
#Separar coma de palabra  (ejm: GARCIA, ALFONSO  -> GARCIA , ALFONSO            BANCOLOMBIA SA, BOGOTA )
df_clean = df_clean.withColumn('PERSONA_CLEAN', F.regexp_replace('PERSONA_CLEAN', r',', ' , '))
# REMOVER PALABRAS CON UN SOLO CARACTER  (EJM: Y )   (^|\s+)(\S(\s+|$))+     (^|\s).(\s|$)   
df_clean = df_clean.withColumn('PERSONA_CLEAN', F.regexp_replace('PERSONA_CLEAN', r'(^|\s+)(\w(\s+|$))+', ' '))
# remover coma si está al final
df_clean = df_clean.withColumn('PERSONA_CLEAN', F.regexp_replace('PERSONA_CLEAN', r'(\s),($)', ' '))
# remover doble espacio por espacio sencillo
df_clean = df_clean.withColumn('PERSONA_CLEAN', F.regexp_replace('PERSONA_CLEAN', r' +' , ' '))
# trim
df_clean = df_clean.withColumn('PERSONA_CLEAN', F.trim('PERSONA_CLEAN'))
# remover - 
df_clean = df_clean.withColumn('NUMERO_DOCUMENTO_CLEAN', F.regexp_replace('NUMERO_DOCUMENTO_CLEAN', r'^-', ''))
df_clean = df_clean.withColumn('TIPO_DOCUMENTO', F.trim('TIPO_DOCUMENTO'))

def count_chars(nombre):
  if nombre is None :
    return 0
  else:
    return len(nombre)

count_chars_udf = udf(count_chars, IntegerType() )

def count_words(nombre):
  return len(nombre.split(" ") )

count_words_udf = udf(count_words, IntegerType() )

def count_commas(nombre):
  #return len(nombre.split(",") )
  return nombre.count(",") 

count_commas_udf = udf(count_commas, IntegerType() )

def classify_type(tipo_documento):
  if ( tipo_documento == "E" or tipo_documento == "P" ):
    return "EXTRANJERO"
  else:
    return "NACIONAL"

classify_types_udf = udf(classify_type, StringType() )

#  Notas: hay algunas personas que separan apellido y nombre usando una coma 
df_clean = df_clean.withColumn("NUMERO_DOCUMENTO_CLEAN_LEN", count_chars_udf(df_clean.NUMERO_DOCUMENTO_CLEAN)  )
df_clean = df_clean.withColumn("persona_num_words", count_words_udf(df_clean.PERSONA_CLEAN)  )
df_clean = df_clean.withColumn("persona_num_commas", count_commas_udf(df_clean.PERSONA_CLEAN)  )
df_clean = df_clean.withColumn("TIPO", classify_types_udf(df_clean.TIPO_DOCUMENTO)  )
#df_clean.show(truncate=False) 

df_clean.createOrReplaceTempView("intervinientes_clean")
df_clean.cache()
display(df_clean)
#df_clean.show(truncate=False)


# COMMAND ----------

'''
df_validar = spark.sql("""
select TIPO_DOCUMENTO, NUMERO_DOCUMENTO_CLEAN_LEN, count(id) as conteo
from intervinientes_clean  where TIPO = "NACIONAL"
group by TIPO_DOCUMENTO, NUMERO_DOCUMENTO_CLEAN_LEN
order by TIPO_DOCUMENTO, NUMERO_DOCUMENTO_CLEAN_LEN
""")

df_validar = spark.sql("""
select TIPO_DOCUMENTO, PERSONA_CLEAN, NUMERO_DOCUMENTO_CLEAN,
NUMERO_DOCUMENTO_CLEAN_LEN,
persona_num_words,
persona_num_commas
from intervinientes_clean
where TIPO_DOCUMENTO = 'P' and  NUMERO_DOCUMENTO_CLEAN_LEN > -1
order by NUMERO_DOCUMENTO_CLEAN_LEN asc, NUMERO_DOCUMENTO_CLEAN asc 
""")
'''
#df_validar = spark.sql("""  select * from intervinientes_clean  where PERSONA LIKE "% S,A %"  """)
#df_validar = spark.sql("""  select * from intervinientes_clean  where PERSONA LIKE "%,"  """)
df_validar = spark.sql("""  select * from intervinientes_clean  where PERSONA LIKE "% Y %"  """)
#df_validar.show()
df_validar.display()

# COMMAND ----------

# DBTITLE 1,Clasificar por sexo según rango de cédulas
df_clasificar_sexo = spark.sql("""
select id , PERSONA_CLEAN, NUMERO_DOCUMENTO_CLEAN
from intervinientes_clean
where TIPO_DOCUMENTO = 'C' and NUMERO_DOCUMENTO_CLEAN_LEN = 8 
order by NUMERO_DOCUMENTO_CLEAN asc
""")
#df_validar.show()
df_clasificar_sexo = df_clasificar_sexo.withColumn("NUMERO_DOCUMENTO_CLEAN_INTEGER", df_clasificar_sexo.NUMERO_DOCUMENTO_CLEAN.cast('INTEGER'))
df_clasificar_sexo.display()

# COMMAND ----------

df_clasificar_sexo.printSchema()

# COMMAND ----------

# MASCULINO hombres eran del 001 hasta el 19.999.999, después solicitaron un  nuevo cupo que inicio en el  70.000.001 al 99.999.999.  
# FEMENINO 20000001 hasta el 69999999, 

def classify_sex(num_documento):
  if ( ( num_documento >= 20000000) &  ( num_documento <=  69999999 ) ):
    return "FEMENINO"
  else:
    return "MASCULINO"

classify_sex_udf = udf(classify_sex, StringType() )

#  Notas: hay algunas personas que separan apellido y nombre usando una coma 
df_clasificar_sexo = df_clasificar_sexo.withColumn("SEXO", classify_sex_udf(df_clasificar_sexo.NUMERO_DOCUMENTO_CLEAN_INTEGER)  )
df_clasificar_sexo = df_clasificar_sexo.withColumn('TIPO_CLASIFICACION', F.lit("RANGO_CEDULA"))
df_clasificar_sexo.display()

# COMMAND ----------

df_clasificar_sexo_for_join =  df_clasificar_sexo.select("id","SEXO", "TIPO_CLASIFICACION")
df_clasificar_sexo_for_join = df_clasificar_sexo_for_join.withColumnRenamed("id", "fk")
df_clasificar_sexo_for_join.display()

# COMMAND ----------

df_clean_join_sex = df_clean.join(df_clasificar_sexo_for_join, df_clean.id == df_clasificar_sexo_for_join.fk,   how='left') 
#df_clean_join_sex = df_clean_join_sex.drop("RANGOS_CEDULA_SEXO")
df_clean_join_sex = df_clean_join_sex.drop("fk")
display(df_clean_join_sex) 

# COMMAND ----------

df_clean_join_sex.printSchema()

# COMMAND ----------

# DBTITLE 1,REGISTROS MARCADOS COMO CÉDULAS QUE EN REALIDAD SON EMPRESAS
# filtrar empresas
# filtrar los que tienen TIPO_DOCUMENTO = 'C' and  NUMERO_DOCUMENTO_CLEAN_LEN = 9  y son empresas (palabras clave)
# los nombres con muchas palabras (ejm. +20 ) son de empresas o copy paste de otros campos
# A FAVOR SUYO SU CONYUGE O COMPAERO A PERMANENTE SUS HIJOS MENORES ACTUALES Y DE QUE LLEGARE A TENER TAMAYO
# los  TIPO_DOCUMENTO = ''  pueden tener personas o empresas.  no tienen número de documento



# clasificar por sexo los que no se pueden por rango:
# no tener en cuenta los tipo "E", 'P' (Extranjería o pasaporte)   EXTRAnJERO
# no tener en cuenta 'N' NIT ,   para la clasificación de sexo   Empresas
# si incluir para clasificar por sexo 'R', 'C' , 'T'


# COMMAND ----------

# DBTITLE 1,Listado de palabras más comunes presentes en nombres de empresas de Colombia
#df_palabras_comunes_empresas == df_pce
df_pce = spark.read.json("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/DATOS_GOV_CO/nit_palabras_comunes_filtradas.json")
df_pce = df_pce.select("word")
display(df_pce)

# COMMAND ----------

df_clean_join_sex.createOrReplaceTempView("intervinientes_sex")
df_pce.createOrReplaceTempView("pce")

# COMMAND ----------

df_empresas_inferidas =   spark.sql("""
with words as ( 
SELECT id,  explode(  split(PERSONA_CLEAN, ' ') )  as word
FROM intervinientes_sex
where TIPO = 'NACIONAL' and TIPO_DOCUMENTO <> 'N'  )
select distinct w.id as id_empresa
from words as w inner join pce p on p.word = w.word
""")
display(df_empresas_inferidas)

# COMMAND ----------

df_empresas_inferidas.count() # 1990109 1995318 1995850

# COMMAND ----------

df_clean_join_sex_empresas = df_clean_join_sex.join(df_empresas_inferidas, df_clean_join_sex.id == df_empresas_inferidas.id_empresa,   how='left') 
display(df_clean_join_sex_empresas)

# COMMAND ----------

def classify_tipo_interviniente(tipo, tipo_documento, id_empresa, persona_num_words ):
  if persona_num_words == 1:
    # si el nombre tiene únicamente una palabra se asume que el interviniente es una emprsa
    return "EMPRESA"
  elif ( tipo == "NACIONAL" ):
    if (tipo_documento == "N" ):
      return "EMPRESA"
    if id_empresa is  None :
      return "PERSONA"
    else:
      return "EMPRESA"
  else:
    return ""

classify_tipo_interviniente_udf = udf(classify_tipo_interviniente, StringType() )

#  Notas: hay algunas personas que separan apellido y nombre usando una coma 
df_clean_join_sex_empresas = df_clean_join_sex_empresas.withColumn("TIPO_INTERVINENTE",
                           classify_tipo_interviniente_udf(df_clean_join_sex_empresas.TIPO,       df_clean_join_sex_empresas.TIPO_DOCUMENTO  , 
                                                           df_clean_join_sex_empresas.id_empresa, df_clean_join_sex_empresas.persona_num_words  )  )


df_clean_join_sex_empresas = df_clean_join_sex_empresas.drop("id_empresa")
df_clean_join_sex_empresas.display()

# COMMAND ----------

# DBTITLE 1,Procesar nombres
df_clean_join_sex_empresas.createOrReplaceTempView("intervinientes_sex_empresas")

df_personas_procesar  = spark.sql("""
SELECT id, PERSONA , PERSONA_CLEAN, persona_num_words, persona_num_commas
FROM intervinientes_sex_empresas
where TIPO_INTERVINENTE = 'PERSONA' 
""")

df_personas_procesar.count()  #  4229394 4229350 4229338 4143250 7520118 7514982


# COMMAND ----------

df_personas_procesar.cache()
df_personas_procesar.createOrReplaceTempView("personas_procesar")
df_personas_procesar.display()

# COMMAND ----------

# DBTITLE 1,Caso persona_num_parts = 2 :    Apellido Nombre
df_apellido_nombre =  spark.sql("""
SELECT * 
FROM personas_procesar
where  persona_num_words = 2
""")

split_col = F.split(df_apellido_nombre['PERSONA_CLEAN'], ' ')
df_apellido_nombre = df_apellido_nombre.withColumn('APELLIDOS', split_col.getItem(0))
df_apellido_nombre = df_apellido_nombre.withColumn('NOMBRES', split_col.getItem(1))
df_apellido_nombre.display()


# COMMAND ----------

# DBTITLE 1,Caso persona_num_parts >= 3 :    Apellidos (2)  Nombre(s)
df_apellido_multi_nombre =  spark.sql("""
SELECT * 
FROM personas_procesar
where  persona_num_words > 2 and persona_num_commas = 0
""")

split_col = F.split(df_apellido_multi_nombre['PERSONA_CLEAN'], '^(?:\S+\s){1}(\S+)'   )
df_apellido_multi_nombre = df_apellido_multi_nombre.withColumn("APELLIDOS", F.regexp_extract(df_apellido_multi_nombre.PERSONA_CLEAN, "([\w]+\s){2}", 0))
df_apellido_multi_nombre = df_apellido_multi_nombre.withColumn('NOMBRES', split_col.getItem(1)  )
#df_apellido_multi_nombre.show(n= 10, truncate=False) 
df_apellido_multi_nombre.display()


# COMMAND ----------

# DBTITLE 1,Caso Apellidos (,) Nombres
# se ignoran los nombres con más de 3 ","

df_apellido_sep_nombre =   spark.sql("""
SELECT *
FROM personas_procesar
where  persona_num_commas = 1
""")
split_col = F.split(df_apellido_sep_nombre['PERSONA_CLEAN'], ',')
df_apellido_sep_nombre = df_apellido_sep_nombre.withColumn('APELLIDOS', split_col.getItem(0))
df_apellido_sep_nombre = df_apellido_sep_nombre.withColumn('NOMBRES', split_col.getItem(1))
df_apellido_sep_nombre.display()

# COMMAND ----------

# DBTITLE 1,Unir todos los dataframes que contienen apellidos Nombres
df_all_nombres = df_apellido_nombre.union(df_apellido_multi_nombre).union(df_apellido_sep_nombre)
#df_all_nombres.count() # 4286067
#df_all_nombres.show(n= 10, truncate=False) 
df_all_nombres.display()

# COMMAND ----------

df_all_nombres_for_join = df_all_nombres.select("id", "PERSONA", "PERSONA_CLEAN",  "APELLIDOS", "NOMBRES" )
df_all_nombres_for_join = df_all_nombres_for_join.withColumn('APELLIDOS', F.trim(  df_all_nombres_for_join.APELLIDOS  )   )
df_all_nombres_for_join = df_all_nombres_for_join.withColumn('NOMBRES', F.trim(  df_all_nombres_for_join.NOMBRES  )   )
# TODO remover palabras que tienen una sola letra
df_all_nombres_for_join = df_all_nombres_for_join.withColumn("PRIMER_NOMBRE", F.regexp_extract(df_all_nombres_for_join.NOMBRES, "(\w+)", 0))
df_all_nombres_for_join = df_all_nombres_for_join.withColumn("PRIMER_NOMBRE_ULT_LETR", F.regexp_extract(df_all_nombres_for_join.PRIMER_NOMBRE, "\w{1}$", 0))
df_all_nombres_for_join = df_all_nombres_for_join.withColumn("PRIMER_NOMBRE_ULT_3LETR", F.regexp_extract(df_all_nombres_for_join.PRIMER_NOMBRE, "\w{3}$", 0))
df_all_nombres_for_join = df_all_nombres_for_join.withColumn("PRIMER_NOMBRE_PRIM_3LETR", F.regexp_extract(df_all_nombres_for_join.PRIMER_NOMBRE, "^\w{3}", 0))
# TODO generar feature 3 ultimas letras del primer nombre
df_all_nombres_for_join = df_all_nombres_for_join.withColumnRenamed("id", "fk")
display(df_all_nombres_for_join)

# COMMAND ----------

# validar nombres con un solo caracter

df_all_nombres_for_join.createOrReplaceTempView("nombres_all")
spark.sql("""
SELECT length(primer_nombre) , *
FROM nombres_all
where  length(primer_nombre) = 1
""").display()


# COMMAND ----------

# DBTITLE 1,Join de dataframe principal con los nombres
df_all_nombres_for_join = df_all_nombres_for_join.drop("PERSONA", "PERSONA_CLEAN")
df_clean_join_sex_empresas_nombres = df_clean_join_sex_empresas.join(df_all_nombres_for_join, 
                                                                     df_clean_join_sex_empresas.id == df_all_nombres_for_join.fk,   how='left') 

df_clean_join_sex_empresas_nombres = df_clean_join_sex_empresas_nombres.drop("fk")
display(df_clean_join_sex_empresas_nombres) 

# COMMAND ----------

df_clean_join_sex_empresas_nombres.createOrReplaceTempView("intervinientes_validate")
spark.sql("""
select *
from intervinientes_validate 
where TIPO_INTERVINENTE = 'PERSONA' AND APELLIDOS IS NOT NULL AND PERSONA_NUM_COMMAS = 1
ORDER BY PERSONA_NUM_WORDS DESC
LIMIT 200
""").display()

# COMMAND ----------


df_clean_join_sex_empresas_nombres.printSchema()

# COMMAND ----------

df_clean_join_sex_empresas_nombres.write.parquet("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/intervinientes_clean_sexo_nombres.parquet")
profile = ProfileReport(df_clean_join_sex_empresas_nombres.select("*").toPandas(), title="Intervinientes",  minimal=True)
#displayHTML(profile.to_html())
dbutils.fs.put("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/intervinientes_clean_sexo_nombres.parquet_profile.html", profile.to_html() )

# COMMAND ----------

# DBTITLE 1,Filtrar nombres por clasificar únicamente para las transacciones rurales
df_transacciones= spark.read.parquet("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/transacciones_clean_rural_sin_linderos.parquet")
df_transacciones.createOrReplaceTempView("transacciones_rurales")
df_transacciones.display()

# COMMAND ----------

df_clean_join_sex_empresas_nombres.createOrReplaceTempView("intervinientes_validate")
df_interv_rurales = spark.sql("""
 select i.*
 from intervinientes_validate as i inner join  transacciones_rurales as t on (i.pk = t.pk)
""") 
df_interv_rurales.display()

# COMMAND ----------

df_interv_rurales.count()  # 159133   2407284

# COMMAND ----------

df_interv_rurales.write.parquet("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/intervinientes_clean_sexo_nombres_rurales.parquet")
profile = ProfileReport(df_interv_rurales.select("*").toPandas(), title="Intervinientes",  minimal=True)
#displayHTML(profile.to_html())
dbutils.fs.put("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/intervinientes_clean_sexo_nombres_rurales.parquet_profile.html", profile.to_html() )

# COMMAND ----------

df_interv_rurales= spark.read.parquet("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/intervinientes_clean_sexo_nombres_rurales.parquet")
df_interv_rurales.createOrReplaceTempView("intervinientes_rurales")
df_interv_rurales_por_clasificar = spark.sql("""
 select i.*
 from intervinientes_rurales i 
 where TIPO_INTERVINENTE = 'PERSONA' AND SEXO IS NULL
""") 
df_interv_rurales_por_clasificar.count() # 27872

# COMMAND ----------

df_interv_rurales_por_clasificar.display()

# COMMAND ----------

# DBTITLE 1,Clasificar Sexo por Diccionario
df_diccionario_nombres = spark.read.csv("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/nombres_Diccionario_Filtrado.csv", header=True)

df_diccionario_nombres.count()   # 2345  2279

# COMMAND ----------

df_diccionario_nombres.display()

# COMMAND ----------

df_interv_rurales_por_clasificar.createOrReplaceTempView("rurales_clasificar")
df_diccionario_nombres.createOrReplaceTempView("diccionario")

df_clasificados_diccionario = spark.sql("""
 select i.id AS fk, d.sexo AS SEXO_DICT, 'DICCIONARIO' AS TIPO_CLASIFICACION_DICT  
 from rurales_clasificar i , diccionario d
 where i.PRIMER_NOMBRE = d.PRIMER_NOMBRE
""") 
df_clasificados_diccionario.count() # 25710  24600  1272897

# COMMAND ----------

# DBTITLE 1,nombres repetidos hombre / mujer
spark.sql("""
 select d.primer_nombre, count(*) 
 from diccionario d
 group by d.primer_nombre
 having count(*) > 1
""").display ()

# COMMAND ----------

df_clasificados_diccionario.display()

# COMMAND ----------

df_interv_rurales_clasif_dict = df_interv_rurales.join(df_clasificados_diccionario, df_interv_rurales.id == df_clasificados_diccionario.fk,   how='left') 


df_interv_rurales_clasif_dict = df_interv_rurales_clasif_dict.withColumn("SEXO", 
                       F.when(F.col("TIPO_CLASIFICACION_DICT")=='DICCIONARIO', F.col("SEXO_DICT")).otherwise(F.col("SEXO")))
df_interv_rurales_clasif_dict = df_interv_rurales_clasif_dict.withColumn("TIPO_CLASIFICACION", 
                       F.when(F.col("TIPO_CLASIFICACION_DICT")=='DICCIONARIO', F.col("TIPO_CLASIFICACION_DICT")).otherwise(F.col("TIPO_CLASIFICACION")))

df_interv_rurales_clasif_dict = df_interv_rurales_clasif_dict.withColumn("SEXO", 
                       F.when(F.col("TIPO_INTERVINENTE")=='EMPRESA',  "EMPRESA" ).otherwise(F.col("SEXO")))
df_interv_rurales_clasif_dict = df_interv_rurales_clasif_dict.withColumn("TIPO_CLASIFICACION", 
                       F.when(F.col("TIPO_INTERVINENTE")=='EMPRESA', "EMPRESA" ).otherwise(F.col("TIPO_CLASIFICACION")))


df_interv_rurales_clasif_dict = df_interv_rurales_clasif_dict.withColumn("TIPO_INTERVINENTE", 
                       F.when(F.col("TIPO")=='EXTRANJERO', "EXTRANJERO" ).otherwise(F.col("TIPO_INTERVINENTE")))
df_interv_rurales_clasif_dict = df_interv_rurales_clasif_dict.withColumn("TIPO_CLASIFICACION", 
                       F.when(F.col("TIPO")=='EXTRANJERO', "EXTRANJERO" ).otherwise(F.col("TIPO_CLASIFICACION")))
df_interv_rurales_clasif_dict = df_interv_rurales_clasif_dict.withColumn("SEXO", 
                       F.when(F.col("TIPO")=='EXTRANJERO', "EXTRANJERO" ).otherwise(F.col("SEXO")))


df_interv_rurales_clasif_dict = df_interv_rurales_clasif_dict.drop("fk", "SEXO_DICT" ,"TIPO_CLASIFICACION_DICT" )

df_interv_rurales_clasif_dict.display()

# COMMAND ----------

df_interv_rurales_clasif_dict.createOrReplaceTempView("test")
spark.sql("""
 select *
 from test
 where TIPO_INTERVINENTE = ""
""").display() 


# COMMAND ----------

df_interv_rurales.count() # 2407284
#df_interv_rurales_clasif_dict.count() # 159133

#df_interv_rurales_clasif_dict.dropDuplicates().count() # 2407284

# COMMAND ----------

df_interv_rurales_clasif_dict.write.parquet("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/intervinientes_clean_sexo_nombres_rurales_diccionario.parquet")
profile = ProfileReport(df_interv_rurales_clasif_dict.select("*").toPandas(), title="Intervinientes clasificados por rango cédulas y diccionario de nombres",  minimal=True)
#displayHTML(profile.to_html())
dbutils.fs.put("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/intervinientes_clean_sexo_nombres_rurales_diccionario.parquet_profile.html", profile.to_html() )

# COMMAND ----------

# DBTITLE 1,Pendientes de clasificación por modelo
df_interv_rurales_clasif_dict.createOrReplaceTempView("intervinientes_rurales_clasificados")
df_interv_rurales_clasif_por_modelo = spark.sql("""
 select i.*
 from intervinientes_rurales_clasificados i 
 where TIPO_INTERVINENTE = 'PERSONA' AND SEXO IS NULL
""") 
df_interv_rurales_clasif_por_modelo.count() # 117495

# COMMAND ----------

df_interv_rurales_clasif_por_modelo.display()

# COMMAND ----------

df_interv_rurales_clasif_por_modelo.printSchema()

# COMMAND ----------

df_interv_rurales_clasif_por_modelo.write.parquet("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/intervinientes_clean_sexo_nombres_rurales_pendientes_modelo.parquet")

df_interv_rurales_clasif_por_modelo.coalesce(1).write.json("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/intervinientes_clean_sexo_nombres_rurales_pendientes_modelo.json")


profile = ProfileReport(df_interv_rurales_clasif_por_modelo.select("*").toPandas(), title="Intervinientes pendientes clasificar por modelo",  minimal=True)
#displayHTML(profile.to_html())
dbutils.fs.put("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/intervinientes_clean_sexo_nombres_rurales_pendientes_modelo.parquet_profile.html", profile.to_html() )

# COMMAND ----------

# DBTITLE 1,Análisis por similaridad
df_diccionario_nombres = spark.read.csv("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/nombres_Diccionario_Filtrado.csv", header=True)
df_diccionario_nombres = df_diccionario_nombres.withColumn("PRIMER_NOMBRE_ULT_LETR", F.regexp_extract(df_diccionario_nombres.PRIMER_NOMBRE, "\w{1}$", 0))
#df_diccionario_nombres.count()   # 2345  2279
df_diccionario_nombres.display()   # 2345  2279

# COMMAND ----------

df_por_clasificar = spark.read.parquet("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/intervinientes_clean_sexo_nombres_rurales_pendientes_modelo.parquet")
#display(from_json(df2.select("col"))
df_por_clasificar.display()

# COMMAND ----------

df_por_clasificar.count()

# COMMAND ----------

df_diccionario_nombres.createOrReplaceTempView("nombres")
df_por_clasificar.createOrReplaceTempView("clasificar")

df_similaridad = spark.sql("""
 select c.id, c.primer_nombre, n.primer_nombre as primer_nombre_clasificado, n.sexo 
 from clasificar as c , nombres as n
 where c.PRIMER_NOMBRE_ULT_LETR = n.PRIMER_NOMBRE_ULT_LETR
""")
df_similaridad = df_similaridad.withColumn("levenshtein", F.levenshtein(F.col("primer_nombre"), F.col("primer_nombre_clasificado")))
df_similaridad.display()



# COMMAND ----------

df_levenshtein1 =    df_similaridad.filter("levenshtein < 2")
df_levenshtein1.count()  # 88782
df_levenshtein1.display()

# COMMAND ----------

df_levenshtein1.createOrReplaceTempView("levenshtein1")
df_levenshtein1_unicos = spark.sql("""
 with a as ( select distinct id, sexo from levenshtein1)
 select a.id as id_unico, count(*) as conteo
 from a 
 group by id
 having count(*) =  1
""")
df_levenshtein1_unicos.count()
# distinct 50220
# unicos sin repetición 44142


# COMMAND ----------

df_levenshtein1_unicos.display()

# COMMAND ----------

df_levenshtein1_filtro = df_levenshtein1.join(df_levenshtein1_unicos, df_levenshtein1.id == df_levenshtein1_unicos.id_unico,   how='left') 
df_levenshtein1_filtro.count()  # 88782

df_levenshtein1_filtro =    df_levenshtein1_filtro.filter("id_unico is not null")
df_levenshtein1_filtro.count()  # 78945

# COMMAND ----------

#df_levenshtein1_filtro.display()
df_levenshtein1_filtro_join  = df_levenshtein1_filtro.select("id", "sexo" )
df_levenshtein1_filtro_join = df_levenshtein1_filtro_join.withColumnRenamed("id", "fk")
df_levenshtein1_filtro_join = df_levenshtein1_filtro_join.withColumnRenamed("sexo", "sexo_fk")
df_levenshtein1_filtro_join = df_levenshtein1_filtro_join.withColumn('TIPO_CLASIFICACION_fk', F.lit("SIMILARIDAD") )
df_levenshtein1_filtro_join.display()

# COMMAND ----------

df_por_clasificar_con_similaridad =  df_por_clasificar.join(df_levenshtein1_filtro_join, df_por_clasificar.id == df_levenshtein1_filtro_join.fk,   how='left') 

df_por_clasificar_con_similaridad = df_por_clasificar_con_similaridad.withColumn("SEXO", 
                       F.when(F.col("TIPO_CLASIFICACION_fk")=='SIMILARIDAD', F.col("sexo_fk")).otherwise(F.col("SEXO")))
df_por_clasificar_con_similaridad = df_por_clasificar_con_similaridad.withColumn("TIPO_CLASIFICACION", 
                       F.when(F.col("TIPO_CLASIFICACION_fk")=='SIMILARIDAD', F.col("TIPO_CLASIFICACION_fk")).otherwise(F.col("TIPO_CLASIFICACION")))

df_por_clasificar_con_similaridad = df_por_clasificar_con_similaridad.drop("fk", "TIPO_CLASIFICACION_fk", "sexo_fk")
df_por_clasificar_con_similaridad.display()

# COMMAND ----------

df_por_clasificar_con_similaridad = df_por_clasificar_con_similaridad.dropDuplicates()
df_por_clasificar_con_similaridad.count()

# COMMAND ----------

df_por_clasificar_con_similaridad.write.parquet("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/intervinientes_clean_sexo_nombres_rurales_pendientes_modelo_similaridad.parquet")

df_por_clasificar_con_similaridad.coalesce(1).write.json("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/intervinientes_clean_sexo_nombres_rurales_pendientes_modelo_similaridad.json")


profile = ProfileReport(df_por_clasificar_con_similaridad.select("*").toPandas(), title="Intervinientes pendientes clasificar por modelo incluyendo similaridad",  minimal=True)
#displayHTML(profile.to_html())
dbutils.fs.put("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/intervinientes_clean_sexo_nombres_rurales_pendientes_modelo_similaridad.parquet_profile.html", profile.to_html() )

# COMMAND ----------

# DBTITLE 1,Carga datos clasificados por modelo
df_modelo = spark.read.csv("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/prediccion_intervinientes_clean_sexo_nombres_rurales_pendientes_modelo_ml.csv", header=True)

df_modelo = df_modelo.withColumnRenamed("id", "fk")
df_modelo = df_modelo.withColumnRenamed("sexo", "sexo_modelo")
df_modelo = df_modelo.withColumnRenamed("tipo_clasificacion", "tipo_clasificacion_modelo")
df_modelo = df_modelo.drop("primer_nombre")
df_modelo.display()   


# COMMAND ----------

df_modelo.count()   # 73353

# COMMAND ----------

df_interv_pendientes = spark.read.parquet("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/intervinientes_clean_sexo_nombres_rurales_pendientes_modelo_similaridad.parquet")
#df_interv_rurales.createOrReplaceTempView("intervinientes_rurales")
df_interv_pendientes.display()

# COMMAND ----------

df_interv_pendientes.count()

# COMMAND ----------

df_interv_pendientes_join = df_interv_pendientes.join(df_modelo, df_interv_pendientes.id == df_modelo.fk,   how='left') 


df_interv_pendientes_join = df_interv_pendientes_join.withColumn("SEXO", 
                       F.when(F.col("tipo_clasificacion_modelo")=='MODELO_ML', F.col("sexo_modelo")).otherwise(F.col("SEXO")))
df_interv_pendientes_join = df_interv_pendientes_join.withColumn("TIPO_CLASIFICACION", 
                       F.when(F.col("tipo_clasificacion_modelo")=='MODELO_ML', F.col("tipo_clasificacion_modelo")).otherwise(F.col("TIPO_CLASIFICACION")))

df_interv_pendientes_join = df_interv_pendientes_join.drop("fk", "tipo_clasificacion_modelo", "sexo_modelo")
df_interv_pendientes_join.display()

# COMMAND ----------

df_interv_pendientes_join.count() # 117495

# COMMAND ----------

df_interv_pendientes_join_to_principal  = df_interv_pendientes_join.select("id" , "SEXO", "TIPO_CLASIFICACION")
df_interv_pendientes_join_to_principal = df_interv_pendientes_join_to_principal.withColumnRenamed("id", "fk")
df_interv_pendientes_join_to_principal = df_interv_pendientes_join_to_principal.withColumnRenamed("sexo", "sexo_fk")
df_interv_pendientes_join_to_principal = df_interv_pendientes_join_to_principal.withColumnRenamed("TIPO_CLASIFICACION", "TIPO_CLASIFICACION_fk")
df_interv_pendientes_join_to_principal.display()

# COMMAND ----------

df_interv_total = spark.read.parquet("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/intervinientes_clean_sexo_nombres_rurales_diccionario.parquet")
#df_interv_rurales.createOrReplaceTempView("intervinientes_rurales")
df_interv_total.display()




# COMMAND ----------

df_interv_total.count()

# COMMAND ----------

df_interv_total_clasificados = df_interv_total.join(df_interv_pendientes_join_to_principal, df_interv_total.id == df_interv_pendientes_join_to_principal.fk,   how='left') 
df_interv_total_clasificados.count()

# COMMAND ----------




df_interv_total_clasificados = df_interv_total_clasificados.withColumn("SEXO", 
                       F.when(F.col("TIPO_CLASIFICACION_fk")=="MODELO_ML", F.col("sexo_fk")).otherwise(F.col("SEXO")))
df_interv_total_clasificados = df_interv_total_clasificados.withColumn("TIPO_CLASIFICACION", 
                       F.when(F.col("TIPO_CLASIFICACION_fk")=="MODELO_ML", F.col("TIPO_CLASIFICACION_fk")).otherwise(F.col("TIPO_CLASIFICACION")))

df_interv_total_clasificados = df_interv_total_clasificados.withColumn("SEXO", 
                       F.when(F.col("TIPO_CLASIFICACION_fk")=="SIMILARIDAD", F.col("sexo_fk")).otherwise(F.col("SEXO")))
df_interv_total_clasificados = df_interv_total_clasificados.withColumn("TIPO_CLASIFICACION", 
                       F.when(F.col("TIPO_CLASIFICACION_fk")=="SIMILARIDAD", F.col("TIPO_CLASIFICACION_fk")).otherwise(F.col("TIPO_CLASIFICACION")))

df_interv_total_clasificados = df_interv_total_clasificados.drop("fk", "TIPO_CLASIFICACION_fk", "sexo_fk")
#df_interv_total_clasificados.display()
df_interv_total_clasificados.count()

# COMMAND ----------

df_interv_total_clasificados.count()
df_interv_total_clasificados.printSchema()

# COMMAND ----------

df_interv_total_clasificados.write.parquet("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/intervinientes_clean_sexo_nombres_rurales_total_clasificados.parquet")

df_interv_total_clasificados.coalesce(1).write.json("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/intervinientes_clean_sexo_nombres_rurales_total_clasificados.json")

profile = ProfileReport(df_interv_total_clasificados.select("*").toPandas(), title="Intervinientes clasificados por Sexo",  minimal=True)
#displayHTML(profile.to_html())
dbutils.fs.put("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/intervinientes_clean_sexo_nombres_rurales_total_clasificados.parquet_profile.html", profile.to_html() )

# COMMAND ----------

# DBTITLE 1,Generar conteos de intervinientes para el archivo de transacciones
df_intervinientes_clasificados = spark.read.parquet("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/intervinientes_clean_sexo_nombres_rurales_total_clasificados.parquet")
df_intervinientes_clasificados.createOrReplaceTempView("interv_clasif")
df_intervinientes_clasificados.display()

# COMMAND ----------

df_interv_total = spark.sql("""
 select pk , count(*) as total_intervinientes
 from interv_clasif
 group by pk
""")
df_interv_total.display() 

# COMMAND ----------

df_interv_tipo_pais = spark.sql("""
select * from (
   select pk as fk , TIPO
   from interv_clasif
)
PIVOT (
  count(TIPO) as tipo  FOR TIPO in (  'NACIONAL' as TIPO_PAIS_NACIONAL, 'EXTRANJERO' AS TIPO_PAIS_EXTRANJERO  )
)
""")
df_interv_tipo_pais = df_interv_tipo_pais.na.fill(0)
df_interv_tipo_pais.display() 

# COMMAND ----------

df_interv_rol = spark.sql("""
select * from (
   select pk as fk , ROL
   from interv_clasif
)
PIVOT (
  count(ROL) as rol  FOR ROL in (  'DE' as ROL_DE, 'A' AS ROL_A  )
)
""")
df_interv_rol = df_interv_rol.na.fill(0)
df_interv_rol.display() 

# COMMAND ----------

df_interv_tipo_persona = spark.sql("""
select * from (
   select pk as fk , TIPO_INTERVINENTE
   from interv_clasif
)
PIVOT (
  count(TIPO_INTERVINENTE) as TIPO_INTERVINENTE  FOR TIPO_INTERVINENTE in (  'PERSONA' as TIPO_INTERVINENTE_PERSONA, 'EMPRESA' AS TIPO_INTERVINENTE_EMPRESA  )
)
""")
df_interv_tipo_persona = df_interv_tipo_persona.na.fill(0)
df_interv_tipo_persona.display() 

# COMMAND ----------

df_interv_tipo_sexo = spark.sql("""
select * from (
   select pk as fk , SEXO
   from interv_clasif
)
PIVOT (
  count(SEXO) as SEXO  FOR SEXO in (  'MASCULINO' as SEXO_MASCULINO, 'FEMENINO' AS SEXO_FEMENINO  )
)
""")
df_interv_tipo_sexo = df_interv_tipo_sexo.na.fill(0)
df_interv_tipo_sexo.display() 

# COMMAND ----------

spark.sql("""
   select *
   from interv_clasif  where pk = '25377-50N-203218-00003-125-2016'
""").display()
# df_interv_rol.count() # 1016642
#  df_interv_total  df_interv_tipo_pais df_interv_rol df_interv_tipo_persona df_interv_tipo_sexo

# COMMAND ----------

df_all = df_interv_total.join(df_interv_tipo_pais, df_interv_total.pk == df_interv_tipo_pais.fk )\
                        .join(df_interv_rol, df_interv_total.pk == df_interv_rol.fk )\
                        .join(df_interv_tipo_persona, df_interv_total.pk == df_interv_tipo_persona.fk )\
                        .join(df_interv_tipo_sexo, df_interv_total.pk == df_interv_tipo_sexo.fk )

df_all = df_all.drop("fk")
df_all.display()

# COMMAND ----------

df_all.write.parquet("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/intervinientes_rurales_conteo_final.parquet")

df_all.coalesce(1).write.json("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/intervinientes_rurales_conteo_final.json")

profile = ProfileReport(df_all.select("*").toPandas(), title="Intervinientes clasificados por Sexo",  minimal=True)
#displayHTML(profile.to_html())
dbutils.fs.put("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/intervinientes_rurales_conteo_final.parquet_profile.html", profile.to_html() )

# COMMAND ----------



# COMMAND ----------

#df_all.count()

# COMMAND ----------

# DBTITLE 1,Reporte cantidad de intervinientes para transacciones rurales
df_transacciones= spark.read.parquet("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/transacciones_clean_rural_sin_linderos.parquet")
df_transacciones.createOrReplaceTempView("transacciones_rurales")
df_transacciones.count()

# COMMAND ----------

df_interv_rurales_clasif_dict.count()

# COMMAND ----------

# DBTITLE 1,Reporte conteo intervinientes por transacción
df_interv_rurales_clasif_dict.createOrReplaceTempView("intervinientes_rurales")
df_intervinientes_group_pk = spark.sql("""
 select pk as fk, count(*) as conteo
 from intervinientes_rurales
 group by pk
""")
df_intervinientes_group_pk.count()  # 62294

# COMMAND ----------

df_export = df_transacciones.join(df_intervinientes_group_pk, df_transacciones.pk == df_intervinientes_group_pk.fk,   how='left') 
df_export.count()

# COMMAND ----------

df_export = df_export.select("pk", "conteo" )
df_export = df_export.withColumn("conteo", 
                       F.when(F.col("conteo") > 0 , F.col("conteo") ).otherwise( 0  ))
df_export.display()

# COMMAND ----------

df_export.coalesce(1).write.json("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/99_export_transacciones_conteo_intervinientes.json")
profile = ProfileReport(df_export.select("*").toPandas(), title="Conteo de intervinientes por transacción",  minimal=True)
#displayHTML(profile.to_html())
dbutils.fs.put("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/99_export_transacciones_conteo_intervinientes.parquet_profile.html", profile.to_html() )

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Conteo de nombres por cantidad de palabras
df_interv.createOrReplaceTempView("intervinientes")
df_export = spark.sql("""
with t as (
  select count(*) as total from intervinientes where TIPO_INTERVINENTE = 'PERSONA' and SEXO = 'MASCULINO' 
), f as ( 
  SELECT SEXO, PRIMER_NOMBRE, count(*) AS frecuencia 
  FROM intervinientes as i  
  where i.TIPO_INTERVINENTE = 'PERSONA' and  i.SEXO = 'MASCULINO'
  group by SEXO, PRIMER_NOMBRE 
)
select  f.SEXO, f.PRIMER_NOMBRE, f.frecuencia, f.frecuencia / t.total as prob
from f , t
order by f.SEXO ASC , f.frecuencia DESC 



""")
display(df_export) 

# COMMAND ----------

# DBTITLE 1,Conteo de nombres por cantidad de palabras
df_export2 = spark.sql("""
with t as (
  select count(*) as total from intervinientes where TIPO_INTERVINENTE = 'PERSONA' and SEXO = 'FEMENINO' 
), f as ( 
  SELECT SEXO, PRIMER_NOMBRE, count(*) AS frecuencia 
  FROM intervinientes as i  
  where i.TIPO_INTERVINENTE = 'PERSONA' and  i.SEXO = 'FEMENINO'
  group by SEXO, PRIMER_NOMBRE 
)
select  f.SEXO, f.PRIMER_NOMBRE, f.frecuencia, f.frecuencia / t.total as prob
from f , t
order by f.SEXO ASC , f.frecuencia DESC 

""")
display(df_export2) 

# COMMAND ----------

df_export_all = df_export.union(df_export2)
df_export_all.coalesce(1).write.json("abfss://sandbox@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/sexo_nombre_prob.json")

# COMMAND ----------

# DBTITLE 1,nombres por clasificar 
df_export = spark.sql("""
SELECT PRIMER_NOMBRE, PRIMER_NOMBRE_ULT_LETR, PRIMER_NOMBRE_ULT_3LETR,PRIMER_NOMBRE_PRIM_3LETR, count(*) AS TOTAL 
FROM intervinientes  
where TIPO_INTERVINENTE = 'PERSONA' and SEXO IS NULL
group by PRIMER_NOMBRE, PRIMER_NOMBRE_ULT_LETR, PRIMER_NOMBRE_ULT_3LETR,PRIMER_NOMBRE_PRIM_3LETR 
order by  count(*) DESC , PRIMER_NOMBRE ASC 
""")
display(df_export) 



# COMMAND ----------

df_export.coalesce(1).write.json("abfss://sandbox@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/nombres_para_clasificar_total.json")

# COMMAND ----------

# DBTITLE 1,nombres comunes hombre mujer
# TODO: estadísticamente cuales nombres "comunes" se pueden ignorar?
#  cantidad de hombres == cantidad mujeres ? 

df_export = spark.sql("""
with h as ( 
  SELECT PRIMER_NOMBRE, count(*) AS TOTAL 
  FROM intervinientes  
  where TIPO_INTERVINENTE = 'PERSONA' and SEXO = 'MASCULINO'
  group by PRIMER_NOMBRE
  ) ,  f as (
  SELECT PRIMER_NOMBRE, count(*) AS TOTAL 
  FROM intervinientes  
  where TIPO_INTERVINENTE = 'PERSONA' and SEXO = 'FEMENINO'
  group by PRIMER_NOMBRE
)
select h.PRIMER_NOMBRE, h.TOTAL AS TOTAL_HOMBRES, f.TOTAL as TOTAL_MUJERES
from h inner join f on h.PRIMER_NOMBRE = f.PRIMER_NOMBRE
ORDER BY h.TOTAL DESC, f.TOTAL desc 
""")
display(df_export) 

# COMMAND ----------

df_export.coalesce(1).write.json("abfss://sandbox@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/nombres_comunes_masculino_femenino.json")

# COMMAND ----------

# DBTITLE 1,última letra
df_export = spark.sql("""

SELECT SEXO, PRIMER_NOMBRE_ULT_LETR, count(*) AS TOTAL 
FROM intervinientes  
where TIPO_INTERVINENTE = 'PERSONA' and SEXO IS NOT NULL
group by SEXO, PRIMER_NOMBRE_ULT_LETR
ORDER BY SEXO , COUNT(*) DESC
  """)

display(df_export) 

# COMMAND ----------

df_export.coalesce(1).write.json("abfss://sandbox@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/sexo_ultima_letra.json")

# COMMAND ----------

# DBTITLE 1,consultas de prueba
df_export = spark.sql("""

  SELECT tipo_documento, numero_documento, persona_clean, sexo
  FROM intervinientes as i  
  where i.TIPO_INTERVINENTE = 'PERSONA' and  i.SEXO = 'MASCULINO' and PRIMER_NOMBRE = 'ANA'
  ORDER by numero_documento, NOMBRES 

""")
display(df_export) 
