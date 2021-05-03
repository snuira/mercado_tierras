# -*- coding: utf-8 -*-
"""
Archivo Python adaptado desde el notebook de databricks dispuesto desde el servicio de Azure-Data Sandbox
Created on Mon Feb 15 22:14:16 2021
Modfied on Mon Apr 12 15:12:16 2021
@author: Carlos Delgado
"""

## Configrurando el storage account key


storage_account_name = "Storage Account"
storage_account_key = "Storage Account Key"
container = "Storage Account Source Container"
container_raw = "Storage Account Source Container Raw"


dbutils.fs.ls("abfss://raw@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/JSON/")
dbutils.fs.ls("abfss://sandbox@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/nombres_entrenamiento_espanol_filtrados/")

# Algoritmo adaptado basado en https://nlpforhackers.io/introduction-machine-learning/ y adaptado del repositorio https://github.com/Jcharis/Python-Machine-Learning/tree/master/Gender%20Classification%20With%20%20Machine%20Learning

#importando librerias iniciales de administracion de datos
import pandas as pd
import numpy as np

#importando libreria y paquedes de ML desde Scikit-learn
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction import DictVectorizer
from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeClassifier


from pyspark.sql.functions import *
from pyspark.sql.types import *

df_original = spark.read.csv("abfss://sandbox@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/nombres_entrenamiento_espanol_filtrados/nombres_entrenamiento_filtrados_v2.csv",header=True)
display(df_original)

#almacenando los nombres por genero a pandas dataframes
df_pd_nombres_mf_general = df_original.select("*").toPandas()
df_pd_nombres_mf_general.head()

# Limpieza de datos
# Verificando consistencia de columnnas 
print(df_pd_nombres_mf_general.columns)

# Verificando tipos de datos
print(df_pd_nombres_mf_general.dtypes)

# Verificando valores nulos
print(df_pd_nombres_mf_general.isnull().isnull().sum())

# Numero de nombres femeninos
print("Numero de nombres femeninos: %s" %(len(df_pd_nombres_mf_general[df_pd_nombres_mf_general.SEXO == 'FEMENINO'])))
# Numero de nombres masculinos
print("Numero de nombres masculinos: %s" %(len(df_pd_nombres_mf_general[df_pd_nombres_mf_general.SEXO == 'MASCULINO'])))

df_names = df_pd_nombres_mf_general

# Remplazando con ceros y unos.
df_names.SEXO.replace({'FEMENINO':0,'MASCULINO':1},inplace=True)
df_names.SEXO.unique()
df_names.dtypes

Xfeatures = df_pd_nombres_mf_general['PRIMER_NOMBRE']

#Extraccion de las características del df vectorizando
cv = CountVectorizer()
X = cv.fit_transform(Xfeatures.values.astype('U')) #Con el fin de no generar problemas en nombres con determinados carcateres 
cv.get_feature_names()

# Conformando el diccionario con la extraccion de las primeras y ultimas letras de cada uno de los nombres
def features(name):
    name = name.lower()
    return {
        'first-letter': name[0], # Primera letra
        'first2-letters': name[0:2], # Primeras 2 letras
        'first3-letters': name[0:3], # Primeras 3 letras
        'last-letter': name[-1], # Ultima letra
        'last2-letters': name[-2:], # Ultimas dos letras
        'last3-letters': name[-3:], # Ultimas tres letras
    }

# Vectorizando las características extraidas
features = np.vectorize(features)

#Ejemplo de extracción
print(features(["Anna", "Camilo", "Antonio","Margarita","Judith","Samuel"]))

#Extrayendo las características para el conjunto de datos vectorizado
df_X = features(df_names['PRIMER_NOMBRE'].values.astype('U'))
df_y = df_names['SEXO']

#Ejemplo de el diccionario de características transformado
arreglo = features(["Mike", "Julia"])
dv = DictVectorizer()
dv.fit(arreglo)
transformed = dv.transform(arreglo)
print(transformed)

dv.get_feature_names()

# Partiendo porcentaje de entrenamiento y testeo
dfX_train, dfX_test, dfy_train, dfy_test = train_test_split(df_X, df_y, test_size=0.2, random_state=42)
dfX_train
dv = DictVectorizer()
dv.fit_transform(dfX_train)

#Definicion del clasificador Decision Trees
dclf = DecisionTreeClassifier()
my_xfeatures = dv.transform(dfX_train)
dclf.fit(my_xfeatures, dfy_train)

#Creacion de la funcion para mayor facilidad
def prediccionGenero(a):
    test_name1 = [a]
    transform_dv =dv.transform(features(test_name1))
    vector = transform_dv.toarray()
    
    if dclf.predict(vector) == 0:
        return "FEMENINO"
    else:
        return("MASCULINO")

#Calculando precisión (Accuracy) del modelo teniendo en cuenta los datos de entrenamiento
print("Accuracy sobre los datos de entrenamiento: %s" %(dclf.score(dv.transform(dfX_train), dfy_train)))

#Calculando precisión (Accuracy) del modelo teniendo en cuenta los datos testeo
print("Accuracy sobre los datos de testeo: %s" %(dclf.score(dv.transform(dfX_test), dfy_test)))

dbutils.fs.mount(
  source = "wasbs://standarized@stupramonitoreomercado.blob.core.windows.net",
  mount_point = "/mnt/auxiliar_2",
  extra_configs = {"fs.azure.account.key.stupramonitoreomercado.blob.core.windows.net":"GlhaYCap6LQYbAwFb8PF3nzT3iPsjbTV6DmMB8rG08ms4R4KPcBn2Y3Y0p4OAtLCTqk8NS80y2tfBe0Ga2El7w=="}
)

import pickle
from sklearn.externals import joblib
import tempfile
import os
from joblib import dump, load

s = pickle.dumps(dclf)
classifier2 = pickle.loads(s)
tmpFile = tempfile.NamedTemporaryFile(delete=False)
dump(dclf, tmpFile)
tmpFile.flush()

#copiando el modelo en pkl desde el punto de montaje al datalake
dump(classifier2, '/tmp/modelo_gender_pred_dt.pkl') 
dbutils.fs.cp('file:/tmp/modelo_gender_pred_dt.pkl', '/mnt/auxiliar_2/OTROS/SNR/modelo_gender_pred_dt_dl.pkl')

dbutils.fs.cp('/mnt/auxiliar_2/OTROS/SNR/modelo_gender_pred_dt_dl.pkl', '/tmp/modelo_gender_pred_dt_saved.pkl', )
display(dbutils.fs.ls ("/tmp/"))
classifier_Final = joblib.load('/dbfs/tmp/modelo_gender_pred_dt_saved.pkl')

#leyendo el archivo desde el json del storage y alamcenandolo como pandas dataframe
dbutils.fs.ls("abfss://sandbox@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/")
df_from_json = spark.read.json("abfss://standarized@stupramonitoreomercado.dfs.core.windows.net/OTROS/SNR/intervinientes_clean_sexo_nombres_rurales_pendientes_modelo_similaridad.json")

display(df_from_json)

df_to_predict = df_from_json.select("*").toPandas()
df_to_predict.head()

#Prediccion de los nuevos nombres que se le presentan al modelo
final_gender = []

#for item in df_to_predict.NOMBRES:
for item in df_to_predict.PRIMER_NOMBRE:
    #print("Nombre: %s ---- Genero: %s" %(item,clf.predict((item, ))))
    if pd.isnull(item) == True or item == '':
        final_gender.append("")
    else:
        final_gender.append(prediccionGenero(item))

#Campo donde se almacena la predicción de los nombres 
df_to_predict['PREDICCION'] = final_gender
display(df_to_predict)

df_final_Selection = df_to_predict[['id','PRIMER_NOMBRE','SEXO','PREDICCION', 'TIPO_CLASIFICACION']]
df_no_similarity = df_final_Selection[df_final_Selection['TIPO_CLASIFICACION'].isnull()]
df_no_similarity['TIPO_CLASIFICACION'] = 'MODELO_ML'
df_no_similarity.SEXO = np.where(df_no_similarity.SEXO.isnull(), df_no_similarity.PREDICCION, df_no_similarity.SEXO)

final_table_to_export = df_no_similarity[['id','PRIMER_NOMBRE','SEXO','TIPO_CLASIFICACION']]
display(df_final_Selection)
display(final_table_to_export)
print("Numero de nombres que fueron predecidos: %s" %(len(final_table_to_export)))

#Guardando los reusltados de la predicción en CSV hacia el data lake
#df_to_predict.to_csv('/tmp/Prediccion_nombres_apellido_nombre.csv', index=False)
df_to_predict.to_csv('/tmp/prediccion_intervinientes_clean_sexo_nombres_rurales_pendientes_modelo_ml_v1.csv', index=False)
dbutils.fs.cp('file:/tmp/prediccion_intervinientes_clean_sexo_nombres_rurales_pendientes_modelo_ml_v1.csv', '/mnt/auxiliar_2/OTROS/SNR/prediccion_intervinientes_clean_sexo_nombres_rurales_pendientes_modelo_ml_v1.csv')