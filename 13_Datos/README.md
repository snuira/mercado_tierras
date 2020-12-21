# Datos utilizados en la ejecución del modelo.

Dentro de esta carpeta se encuentran distribuidos los datos de la siguiente manera:

## 0_Entrenamiento: 

Esta carpeta contiene los datos que se han utilizado hasta el momento para realizar el entrenamiento del modelo de ML. Se encuentra a su vez subdividido por entidad de donde provienen los datos insumo de entrenamiento.

* Repositorio_Kaggle:
     
     female_names.csv: Este archivo contiene un listado de nombres femeninos con datos de frecuencia y promedio de edad.
     
     male_names.csv: Este archivo contiene un listado de nombres masculinos con datos de frecuencia y promedio de edad.

## 1_Para_Prediccion:

Esta carpeta contiene los datos en formato .json productos de los procesos de ETL con los datos del SNR para ser utilizados en la predicción del modelo de ML. También se encuentran organizados por entidad fuente.

## 2_Salida_Prediccion:

Esta carpeta contiene los archivos en formato .csv y xlsx resultado de los distintas predicciones con base en diferentes datos de entrenamiento y los datos a predecir.


## 4_Otros

* categorias_Tipo_Documento.xlsx  : Categorías de los tipos de documentos asociados a los intervinientes en las transacciones registrales
* empresas_nit9_sin_acentos.tsv:   Listado de nombres de empresas colombianas recopilados a partir de información publica dispuesta en datos.gov.co.
* nit_palabras_comunes.json : Listado de palabras con mayor frecuencia encontradas en nombres de empresas colombianas a partir de información recopilada desde  datos.gov.co.
* nit_palabras_comunes_filtradas.json  :  Selección de 340 palabras con mayor frecuencia encontradas en nombres de empresas colombianas a partir de información recopilada desde datos.gov.co.
* sexo_ultima_letra.json : Listado de frecuencias de la última letra en el primer nombre a partir de la información de los intervinientes.
