# -*- coding: utf-8 -*-
"""
Archivo Python adaptado desde el notebook de databricks dispuesto desde el servicio de Azure-Data Sandbox
Created on Mon Dec 14 08:18:00 2020
@author: Carlos Delgado
"""

## Configrurando el storage account key


storage_account_name = "Storage Account"
storage_account_key = "Storage Account Key"
container = "Storage Account Source Container"
container_raw = "Storage Account Source Container Raw"

spark.conf.set("fs.azure.account.key.{0}.dfs.core.windows.net".format(storage_account_name), storage_account_key)
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")
dbutils.fs.ls("abfss://{0}@{1}.dfs.core.windows.net/".format(container_raw, storage_account_name))
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false")


## Examinando el listado de archivos

dbutils.fs.ls("abfss://{0}@{1}.dfs.core.windows.net/OTROS/SNR/JSON/".format(container_raw, storage_account_name))
dbutils.fs.ls("abfss://{0}@{1}.dfs.core.windows.net/OTROS/KAGGLE/nombres_espanol_entrenamiento_test1/".format(container, storage_account_name))


## Importanto las principales librerias utilizadas en el algoritmo de predicción

# Algoritmo adaptado de https://www.kaggle.com/migalpha/gender-predictor-by-spanish-names
#importando librerias iniciales
import pandas as pd
import numpy as np

#importando libreria de AI Scikit-learn
from sklearn.model_selection import train_test_split


## Obtencion de los datos iniciales desde el csv con spark

from pyspark.sql.functions import *
from pyspark.sql.types import *

df_fem_original = spark.read.csv("abfss://{0}@{1}.dfs.core.windows.net/OTROS/KAGGLE/nombres_espanol_entrenamiento_test1/female_names.csv".format(container, storage_account_name),header=True)
df_mal_original = spark.read.csv("abfss://{0}@{1}.dfs.core.windows.net/OTROS/KAGGLE/nombres_espanol_entrenamiento_test1/male_names.csv".format(container, storage_account_name),header=True)
display(df_fem_original)
display(df_mal_original)



## Migrando de Spark dataframes a Pandas dtaframes

#almacenando los nombres por genero a pandas dataframes
fem_nombres = df_fem_original.select("*").toPandas()
mal_nombres = df_mal_original.select("*").toPandas()


## Instanciando variables

#asignación de indicadores para genero, 0 femenino, 1 masculino
fem_nombres['genero'] = 0
mal_nombres['genero'] = 1


#unificación de conjunto de datos eliminando potenciales duplicados
data = fem_nombres.append(mal_nombres, ignore_index=True)
data = data.drop_duplicates(subset='name', keep=False)

#parceando y limpiando atributos innecesarios

target = data['genero'].astype(str)

del(data['frequency'])
del(data['mean_age'])
del(data['genero'])

features = data['name'].astype(str)


## Configurando el modelo del modelo

#se dividen los datos 80% entrenamiento - 20% testeo

X_train, X_test, y_train, y_test = train_test_split(features, target, random_state=7, test_size=0.2)
print(X_train.shape, X_test.shape, y_train.shape, y_test.shape)

#libreria para extraer caracteristicas de los archivos de texto
from sklearn.feature_extraction.text import CountVectorizer

#TF-IDF Esta librería se encarga de transformar una matriz de conteo de ocurrencias (term-frequency)  a una matriz normalizada de ocurrencias ( term-frequency times inverse document-frequency)
#Mayor documentación tecnica en https://scikit-learn.org/stable/modules/generated/sklearn.feature_extraction.text.TfidfTransformer.html
from sklearn.feature_extraction.text import TfidfTransformer

#importando clasificador ML Naive Bayes NB
from sklearn.naive_bayes import MultinomialNB


#creando el pipeline
from sklearn.pipeline import Pipeline

text_classifier = Pipeline([('vect', CountVectorizer(ngram_range=(1,1))), ('tfidf',TfidfTransformer(norm='l2', sublinear_tf=True, use_idf=False)), ('clf', MultinomialNB(alpha=0.1))])


## Entrenamiento del modelo y calculo de accuracy

#entrenando el modelo
text_classifier = text_classifier.fit(X_train, y_train)

#calculando precisión del modelo utilizando el clasificador NB
from sklearn.metrics import accuracy_score
predicted = text_classifier.predict(X_test)

print(accuracy_score(y_test, predicted))



## Testeando el modelo

#importando GridSearchCV para seleccionar los mejores hiperparametros
from sklearn.model_selection import GridSearchCV
parameters = {
        #'vect__max_df': (0.5, 0.625, 0.75, 0.875, 1.0),  
        #'vect__max_features': (None, 5000, 10000, 20000),  
        #'vect__min_df': (1, 5, 10, 20, 50),  
        'vect__ngram_range': [(1, 1), (1, 2)], 
        'tfidf__use_idf': (True, False),
        'tfidf__sublinear_tf': (True, False),  
        #'vect__binary': (True, False),  
        'tfidf__norm': ('l1', 'l2'),  
        'clf__alpha': (1, 0.1, 0.01, 0.001, 0.0001, 0.00001)
}

gs_classifier = GridSearchCV(text_classifier, parameters, n_jobs=-1, cv=2)
gs_classifier = gs_classifier.fit(X_train, y_train)

print(gs_classifier.best_score_)
print(gs_classifier.best_params_)


## Generación del punto de montaje

#testeando punto de montaje, el montaje se esta ejecutando satisfactoriamente

dbutils.fs.mount(
  source = "wasbs://{0}@{1}.blob.core.windows.net".format(container, storage_account_name),
  mount_point = "/mnt/auxiliar",
  extra_configs = {"fs.azure.account.key.{0}.blob.core.windows.net".format(storage_account_name):storage_account_key}
)


## Salvando el modelo como archivo de pickle .pkl


import pickle
from sklearn.externals import joblib
import tempfile
import os
from joblib import dump, load

#model_path = "/mnt/auxiliar/modelo_gender_pred.pkl"
s = pickle.dumps(text_classifier)
classifier2 = pickle.loads(s)
tmpFile = tempfile.NamedTemporaryFile(delete=False)
dump(text_classifier, tmpFile)
tmpFile.flush()
#print(clf2)

#copiando el modelo en pkl desde el punto de montaje al datalake
dump(classifier2, '/tmp/modelo_gender_pred.pkl') 
dbutils.fs.cp('file:/tmp/modelo_gender_pred.pkl', '/mnt/auxiliar/OTROS/KAGGLE/nombres_espanol_entrenamiento_test1/modelo_gender_pred_data_lake.pkl')

dbutils.fs.cp('/mnt/auxiliar/OTROS/KAGGLE/nombres_espanol_entrenamiento_test1/modelo_gender_pred_data_lake.pkl', '/tmp/modelo_gender_saved.pkl', )
display(dbutils.fs.ls ("/tmp/"))
classifier_Final = joblib.load('/dbfs/tmp/modelo_gender_saved.pkl')


#testeando algunas predicciones desde el modelo almacenado

print(classifier_Final.predict(("Valentina", )))
print(classifier_Final.predict(("Miguel", )))
print(classifier_Final.predict(("Maria", )))
print(classifier_Final.predict(("Joe", )))
print(classifier_Final.predict(("Santiago", )))
print(classifier_Final.predict(("Magdalena", )))


## Prediccion - Leyendo el archivo para prediccion

#leyendo el archivo desde el json del storage y alamcenandolo como pandas dataframe
dbutils.fs.ls("abfss://{0}@{1}.dfs.core.windows.net/OTROS/SNR/".format(container, storage_account_name))
#df_from_json = spark.read.json("abfss://{0}@{1}.dfs.core.windows.net/OTROS/SNR/nombres_apellido_nombre.json")
df_from_json = spark.read.json("abfss://{0}@{1}.dfs.core.windows.net/OTROS/SNR/nombres_para_clasificar.json".format(container, storage_account_name))



display(df_from_json)

df_to_predict = df_from_json.select("*").toPandas()
df_to_predict.head()


## Ejecutando la prediccion de los genereros

#Prediccion de los nuevos nombres que se le presentan al modelo
final_gender = []

#for item in df_to_predict.NOMBRES:
for item in df_to_predict.PRIMER_NOMBRE:
    #print("Nombre: %s ---- Genero: %s" %(item,clf.predict((item, ))))
    if pd.isnull(item) == True:
        final_gender.append("")
    elif clf.predict((item, ))[0] == '0':
        #print("Mujer")
        final_gender.append("MUJER")
    else:
        #print("Hombre")
        final_gender.append("HOMBRE")

#Campo donde se almacena la predicción de los nombres        
df_to_predict['PREDICCION'] = final_gender

display(df_to_predict)


## Guardando los reusltados de la predicción en CSV hacia el data lake

#df_to_predict.to_csv('/tmp/Prediccion_nombres_apellido_nombre.csv', index=False)
df_to_predict.to_csv('/tmp/Prediccion_nombres_para_clasificar.csv', index=False)
dbutils.fs.cp('file:/tmp/Prediccion_nombres_para_clasificar.csv', '/mnt/auxiliar/OTROS/KAGGLE/nombres_espanol_entrenamiento_test1/Prediccion_final_nombres_nombres_para_clasificar.csv')




