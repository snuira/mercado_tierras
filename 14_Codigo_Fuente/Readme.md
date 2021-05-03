# Código fuente


* **Carpeta etl:** Scripts para transformación y limpieza de datos 
  
  * etl_intervinientes_clean_v0.ipynb : Intervinientes (Limpieza y transformación)
  * etl_intervinientes_v0.ipynb  :  Intervinientes (Conversión de formato para data lake, perfilamiento)
  * etl_transacciones_clean_v0.ipynb : Transacciones inmobiliarias (Limpieza y transformación)
  * etl_transacciones_v0.ipynb :  Transacciones inmobiliarias (Conversión de formato para data lake, perfilamiento)
  * exploracion_nombres_empresas.ipynb : Exploración de palabras asociadas a nombres de empresas 
  * exploracion_nombres_masculinos_femeninos.ipynb: Exploración de nombres masculinos y femeninos
  * transacciones_json_sas_to_tsv.ipynb :  Conversión de archivo JSON-SAS a TSV
  * transacciones_revision_preliminar.ipynb : Revisión preliminar de archivo con información de transacciones inmobiliarias
  * adf: Copia de los archivos de configuración generados por Microsoft Azure Data Factory


* **Carpeta procesis:**


  * **01_SNUIRA_ETL_Empresas.py:** Limpieza y adecuación de datos con nombres de empresas provenientes de datos.gov.co, SECOP I, y Directorio estadístico de empresas DANE.
  * **02_SNR_Transacciones_Preproceso.py:** Preprocesamiento para la correcta adecuación de los datos referentes a Transacciones Inmobiliarias.
  * **03_SNR_Transacciones_Clean.py:** Limpieza y procesos de normalización de los datos de Transacciones Inmobiliarias para la definición de Transacciones Rurales únicas para cada una de las vigencias en estudio.
  * **04_SNR_Linderos_ETL.py:** Procesos de limpieza aplicados para el campo linderos de las Transacciones Rurales con el fin de realizar la extracción de palabras claves que informen acerca de la magnitud de área y unidades de los predios transados.
  * **05_SNR_Intervinientes_Clean.py:** Procesos de limpieza aplicados al campo nombre de aquellos intervinientes de Transacciones Rurales. Subsecuente invocación de métodos la para la predicción de genero basado en rangos de cédula, diccionario, similaridad y modelos ML.
  * **06_SNR_R2R2_Profiling.py:** Perfilamientos de datos de catastro alfanumérico R1R2 de las diferentes vigencias.
  * **07_Catastro_Geo_Profiling.py:** Limpieza y transformación de los datos de catastro geográfico a formatos leíbles desde Spark para la unión de las diferentes vigencias y generación del centroide a cada uno de los predios transados (geometría polígono).
  * **08_Modelo_Prediccion_Genero_ML_NB.py:** Experimentación de predicción de género de los intervinientes de Transacciones Rurales a partir del nombre, utilizando el modelo de ML Naive Bayes.
  * **09_Modelo_Prediccion_Genero_ML_DT.py:** Predicción de género de los intervinientes de Transacciones Rurales a partir del primer nombre utilizando el modelo de ML Decision Trees.
  * **10_Export_Data_Dashboard.py:** Adecuación del archivo interrelacionado de Transacciones Rruales con el catastro alfanumérico R1R2.