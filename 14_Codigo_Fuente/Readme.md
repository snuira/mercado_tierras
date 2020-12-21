# Código fuente


* **etl:** Scripts para transformación y limpieza de datos 
  
  * etl_intervinientes_clean_v0.ipynb : Intervinientes (Limpieza y transformación)
  * etl_intervinientes_v0.ipynb  :  Intervinientes (Conversión de formato para data lake, perfilamiento)
  * etl_transacciones_clean_v0.ipynb : Transacciones inmobiliarias (Limpieza y transformación)
  * etl_transacciones_v0.ipynb :  Transacciones inmobiliarias (Conversión de formato para data lake, perfilamiento)
  * exploracion_nombres_empresas.ipynb : Exploración de palabras asociadas a nombres de empresas 
  * exploracion_nombres_masculinos_femeninos.ipynb: Exploración de nombres masculinos y femeninos
  * transacciones_json_sas_to_tsv.ipynb :  Conversión de archivo JSON-SAS a TSV
  * transacciones_revision_preliminar.ipynb : Revisión preliminar de archivo con información de transacciones inmobiliarias
  * adf: Copia de los archivos de configuración generados por Microsoft Azure Data Factory


* **modelo_ml_naive_bayes:** Modelo de clasificación utilizando Naive Bayes 
  
  *  Dentro de este folder se encuentra el archivo de Python [modelo_prediccion_genero_ml_nb.py](https://github.com/snuira/mercado_tierras/blob/main/14_Codigo_Fuente/modelo_ml_naive_bayes/modelo_prediccion_genero_ml_nb.py) el cual contiene el algoritmo para la predicción de genero basado en el nombre de una persona. El algoritmo está basado en un modelo de Machine Learning que implementa el teorema de Naive Bayes. Este modelo se encuentra configurado y parametrizado por medio de la librería scikit-learn. Este modelo se ha adaptado para ser ejecutado desde el servicio Databricks de Microsoft Azure.
