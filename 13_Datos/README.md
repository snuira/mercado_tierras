# Datos utilizados en la ejecución del modelo.

Dentro de esta carpeta se encuentran distribuidos los datos de la siguiente manera:

## 0_Entrenamiento: 

Esta carpeta contiene los datos que se han utilizado hasta el momento para realizar el entrenamiento del modelo de ML. Se encuentra a su vez subdividido por entidad de donde provienen los datos insumo de entrenamiento.

* Repositorio_Kaggle:
     
     female_names.csv: Listado de nombres femeninos con datos de frecuencia y promedio de edad.
     
     male_names.csv: Listado de nombres masculinos con datos de frecuencia y promedio de edad.

## 1_Para_Prediccion:

Esta carpeta contiene los datos en formato .json productos de los procesos de ETL con los datos del SNR para ser utilizados en la predicción del modelo de ML. También se encuentran organizados por entidad fuente.

* SNR

     nombres_patron_tipo_1.json: Listado de nombres masculinos y femeninos post ETL con genero asigando según rangos de cédula.
     
     nombres_patron_tipo_2.json: Listado de nombres masculinos y femeninos compuestos post ETL con genero asignado según rangos de cédula.
     
     nombres_patron_tipo_3.json: Listado de nombres masculinos y femeninos compuestos con patron separados por comas ","  post ETL con genero asignado según rangos de cédula.

## 2_Salida_Prediccion:

Esta carpeta contiene los archivos en formato .csv y xlsx resultado de los distintas predicciones con base en diferentes datos de entrenamiento y los datos a predecir.

* Prediccion_final_nombres_nombres_para_clasificar.csv: Archivo post-predicción con el atributo de género predicho según nombres no categorizados con genero previo.
* Prediccion_final_nombres_nombres_para_clasificar.csv: Archivo post-predicción con el atributo de género predicho con nombres simples.
* Prediccion_final_nombres_nombres_para_clasificar.csv: Archivo post-predicción con el atributo de género predicho con nombres compuestos.
* Prediccion_final_nombres_nombres_para_clasificar.csv: Archivo post-predicción con el atributo de género predicho con nombres compuestos separados por coma.

## 4_Otros

* categorias_Tipo_Documento.xlsx  : Categorías de los tipos de documentos asociados a los intervinientes en las transacciones registrales
* empresas_nit9_sin_acentos.tsv:   Listado de nombres de empresas colombianas recopilados a partir de información publica dispuesta en datos.gov.co.
* nit_palabras_comunes.json : Listado de palabras con mayor frecuencia encontradas en nombres de empresas colombianas a partir de información recopilada desde  datos.gov.co.
* nit_palabras_comunes_filtradas.json  :  Selección de 340 palabras con mayor frecuencia encontradas en nombres de empresas colombianas a partir de información recopilada desde datos.gov.co.
* sexo_ultima_letra.json : Listado de frecuencias de la última letra en el primer nombre a partir de la información de los intervinientes.
