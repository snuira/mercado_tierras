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

'''
#spark.sparkContext.getConf().getAll()
from pyspark.sql import SparkSession

sparkSession = SparkSession. \
    builder. \
    getOrCreate(). \
    config("spark.serializer", KryoSerializer.getName). \
    config("spark.kryo.registrator", SedonaKryoRegistrator.getName). \
    config('spark.jars.packages',
           'org.apache.sedona:sedona-python-adapter-3.0_2.12:1.0.0-incubating,'
           'org.datasyslab:geotools-wrapper:geotools-24.0')
    '''

# COMMAND ----------

# DBTITLE 1,List filesystem
dbutils.fs.ls("abfss://raw@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/JSON/")

# COMMAND ----------

dbutils.fs.head("abfss://raw@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/JSON/multiline_R1_R2_2014.json")

# COMMAND ----------

'''
{"SASJSONExport":"1.0","SASTableData+RANGOS_AVALUO_2014":[{
"CATASTRO":"MEDELLIN2014","departamentonombre":"ANTIOQUIA","municipionombre":"MEDELLÍN",
"departamento":"05","municipio":"001","COD_MUN":"05001",
"codigopredial":"050010009908000010003000000000","LARGO":30,"CONDICION":"TERRENO",
"prediosconmejoras":"","codigopredialcarto":"90800010003","Nro_Predio":"5194026",
"CatastroPredio":"MEDELLIN20145194026",
"tipoderegistro":"R1","direccion":"BARRO BLANCO",
"destinoeconomico":"X","AREATERRENO_m2":8223.73,"AREATERRENO_ha":0.822373,
"Area_Cartografica_ha":0.822468595758,
"MinUAF":3,"MaxUAF":37,"clasificacionUAF":"MENOR A UAF",
"tamanopredial5":"Microfundio", "tamanopredial16":"A2. Mayor a 0.5 - hasta 1 ha","areaconstruida":0,
"RANGOS_AVALUO":"D1.Mayor que 60 hasta 70","vigencia":"ano 2014",
"vigencia_actual":2013,"matriculainmobiliaria":"5194026",
"MATRICULA_DEPURADO":"5194026","TIPO_MATRICULA":"SIN ESTRUCTURA"},
'''
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
    
  '''
  schema = StructType().add(
    StructField(dataset_name, ArrayType(
        StructType([
            StructField("CATASTRO", StringType()),   StructField("departamentonombre", StringType()), StructField("municipionombre", StringType()), 
            StructField("departamento", StringType()),   StructField("municipio", StringType()),   StructField("COD_MUN", StringType()),
            StructField("codigopredial", StringType()),   StructField("LARGO", StringType()),   StructField("CONDICION", StringType()),
            StructField("prediosconmejoras", StringType()),      StructField("codigopredialcarto", StringType()),   StructField("Nro_Predio", StringType()),
            StructField("CatastroPredio", StringType()),
            StructField("tipoderegistro", StringType()), StructField("direccion", StringType()),
            StructField("destinoeconomico", StringType()),   StructField("AREATERRENO_m2", StringType()),  StructField("AREATERRENO_ha", StringType()),
            StructField("Area_Cartografica_ha", StringType()),   
            StructField("MinUAF", StringType()),  StructField("MaxUAF", StringType()),   StructField("clasificacionUAF", StringType()),  
            StructField("tamanopredial5", StringType()),  StructField("tamanopredial16", StringType()),  StructField("areaconstruida", StringType()),  
            StructField("RANGOS_AVALUO", StringType()),  StructField("vigencia", StringType()),  
            StructField("vigencia_actual", StringType()),    StructField("matriculainmobiliaria", StringType()),
            StructField("MATRICULA_DEPURADO", StringType()),   StructField("TIPO_MATRICULA", StringType())
        ])
     )
  ))
  df = spark.read.option("multiline","true").json(file_path, schema=schema)
  df.printSchema()
  rows = df.select( F.explode(dataset_name).alias("row") )
  df_rows = rows.select( "row.*")
  #df_rows = df_rows.withColumn('ANIO', year)
  df_rows = df_rows.withColumn('ANIO', F.lit(year))
  df_rows.display()
  return df_rows
  '''

# COMMAND ----------

parse_json_to_multiline_file("abfss://raw@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/JSON", "R1_R2_2014.json", "SASTableData+RANGOS_AVALUO_2014" )

# COMMAND ----------

parse_json_to_multiline_file("abfss://raw@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/JSON", "R1_R2_2015.json", "SASTableData+RANGOS_AVALUOS_2015" )
parse_json_to_multiline_file("abfss://raw@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/JSON", "R1_R2_2016.json", "SASTableData+RANGOS_AVALUO_2016" )
parse_json_to_multiline_file("abfss://raw@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/JSON", "R1_R2_2017.json", "SASTableData+RANGOS_AVALUOS_2017" )
parse_json_to_multiline_file("abfss://raw@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/JSON", "R1_R2_2018.json", "SASTableData+RANGOS_AVALUOS_2018" )
parse_json_to_multiline_file("abfss://raw@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/JSON", "R1_R2_2019.json", "SASTableData+RANGOS_AVALUO_2019" )

# COMMAND ----------

# MAGIC %sh
# MAGIC pwd
# MAGIC ls -lah  /tmp
# MAGIC rm /tmp/*.json
# MAGIC #head -n 1 /tmp/multiline_R1_R2_2014.json

# COMMAND ----------

# DBTITLE 1,Lectura de archivos R1R2 json multiline
df_2014 = spark.read.json("abfss://raw@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/JSON/multiline_R1_R2_2014.json")
df_2014 = df_2014.withColumn('ANIO', F.lit(2014))
df_2014.display()

# COMMAND ----------

df_2015 = spark.read.json("abfss://raw@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/JSON/multiline_R1_R2_2015.json")
df_2016 = spark.read.json("abfss://raw@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/JSON/multiline_R1_R2_2016.json")
df_2017 = spark.read.json("abfss://raw@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/JSON/multiline_R1_R2_2017.json")
df_2018 = spark.read.json("abfss://raw@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/JSON/multiline_R1_R2_2018.json")
df_2019 = spark.read.json("abfss://raw@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/JSON/multiline_R1_R2_2019.json")


df_2015 = df_2015.withColumn('ANIO', F.lit(2015))
df_2016 = df_2016.withColumn('ANIO', F.lit(2016))
df_2017 = df_2017.withColumn('ANIO', F.lit(2017))
df_2018 = df_2018.withColumn('ANIO', F.lit(2018))
df_2019 = df_2019.withColumn('ANIO', F.lit(2019))


# COMMAND ----------

df_2014 = df_2014.drop("Area_Cartografica_ha")
df_2014.printSchema()

# COMMAND ----------

df_2017.printSchema()
len(df_2017.columns)

df_2017 = df_2017.withColumnRenamed("RANGOS_AVALUOS", "RANGOS_AVALUO")




# COMMAND ----------

list_difference = [item for item in df_2014.columns if item not in df_2017.columns]
print(list_difference)
list_difference = [item for item in df_2017.columns if item not in df_2014.columns]
print(list_difference)

# COMMAND ----------

#df_all = df_2014.union(df_2015).union(df_2016).union(df_2017).union(df_2018).union(df_2019)
df_all = df_2014.union(df_2015).union(df_2016).union(df_2018).union(df_2019)
df_all.printSchema()
df_all.display()

# COMMAND ----------

df_all.count()

# COMMAND ----------

len(df_all.columns)

# COMMAND ----------

# Nota: Al intentar hacer profiling con todo el conjunto de datos se generan problemas de memoria
df_all.write.parquet("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/multiline_R1_R2_all.parquet")
profile = ProfileReport(df_all.select("*").toPandas(), title="Transacciones inmobiliarias",  minimal=True)
#displayHTML(profile.to_html())
dbutils.fs.put("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/multiline_R1_R2_all.parquet_profile.html", profile.to_html() )

# COMMAND ----------

df_all = spark.read.parquet("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/multiline_R1_R2_all.parquet")

# COMMAND ----------

df_all.count()
df_all.printSchema()

# COMMAND ----------

title = "Disclaimer: this profiling report was generated using a sample of 5% of the original dataset."
profile = ProfileReport(df_all.select("*").toPandas().sample(frac=0.05), title=title,  minimal=True)
#displayHTML(profile.to_html())
dbutils.fs.put("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/multiline_R1_R2_all.parquet_profile_sample.html", profile.to_html() )

# COMMAND ----------

df_all.display()

# COMMAND ----------


