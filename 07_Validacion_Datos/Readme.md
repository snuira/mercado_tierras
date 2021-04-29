# Validación de datos

- [Validación de datos](#validación-de-datos)
  - [Disponibilidad y oportunidad de acceso a los datos](#disponibilidad-y-oportunidad-de-acceso-a-los-datos)
  - [Formato de transferencia de datos](#formato-de-transferencia-de-datos)
  - [Estructura de los datos](#estructura-de-los-datos)
  - [Consistencia](#consistencia)
  - [Registros repetidos (De-duplicación)](#registros-repetidos-de-duplicación)
  - [Contenido](#contenido)

Durante las fases del proyecto de descubrimiento y preparación de datos fue necesario realizar diferentes procesos de validación apoyados 
por herramientas tales como el perfilamiento de datos y la generación de reportes.

A continuación se enuncian los criterios de validación principales que se tuvieron en cuenta.

## Disponibilidad y oportunidad de acceso a los datos

Actualmente en la entidad se encuentran algunas dificultades respecto a la accesibilidad y disponibilidad de los datos a causa de aspectos como los siguientes:
* No existe un mecanismo  automatizado que facilite el acceso a la información por parte de los científicos de datos. 
* El acceso a los datos requiere la gestión de un proceso administrativo a través de la radicación manual de una solicitud para el acceso a la información. 
* El acceso al repositorio de datos institucional se encuentra restringido, razón por la cual es necesaria  intermediación humana para poder obtener el acceso a los datos.  
* En algunas ocasiones la atención de las solicitudes de información puede llevar demasiado tiempo (Ejm. semanas) debido a las múltiples actividades asignadas al recurso humano encargado de gestionar el acceso a los datos. 
* En ocasiones el tiempo de generación de las copia de datos se ve afectada por problemas de desempeño de la herramienta donde actualmente se almacenan muchos de los conjuntos de datos gestionados por la organización. (Nota: El almacenamiento de datos en la fuente original se realiza a través de SAS)   
* En ocasiones la frecuencia de actualización de los datos se ve afectada por procesos administrativos que se deben realizar ante las entidades externas encargadas de  generar la información. 


## Formato de transferencia de datos

* Algunas de las fuentes de datos provienen de archivos planos en formato CSV. Dicho formato tiene como inconveniente que genera problemas de interpretación para aquellos atributos en donde se almacena texto libre.
* Para transferir la información de forma correcta fue necesario solicitar al custodio de los datos generar la copia de los archivos en formato JSON.
* Los archivos de texto JSON generados de forma nativa por la herramienta SAS tienen como problema que contiene el texto en la primera línea del archivo físico.  Lo anterior causa problemas de lectura de los datos para aquellos ficheros que superan un tamaño mayor a los 2 GB.
* Fue necesario implementar rutinas que convirtieran los archivos "JSON single line" de SAS en archivos "JSON Multiline" o "JSON Lines" (https://jsonlines.org/ ) .
* Para optimizar el almacenamiento de datos en el Data Lake se utilizó el formato Parquet ( https://databricks.com/glossary/what-is-parquet# )   
* Para exportar los resultados generados por el proyecto se utilzaron los formatos JSON Multiline y Parquet.

## Estructura de los datos

* Se encontraron problemas en algunos de los conjuntos de datos a causa de la utilización de caracteres especiales (Ejm: espacios, tildes, ñ) para el nombramiento de atributos. Fue necesario renombrar dichos atributos sin utilizar caracteres especiales (Ejm: Renombrar "AÑO PUBLICACIÓN" como "ANIO_PUBLICACION" )
*  Algunos de los conjuntos de datos separados en diferentes archivos de múltiples vigencias se encontró que la estructura de los mismos variaba entre años. Para esos casos fue necesario reorganizar la estructura de los atributos con el fin de unificar un único el schema  consistente entre las múltiples particiones temporales.
* En algunos conjuntos de datos fue necesario remover atributos que se consideraban redundantes.  

## Consistencia 

* En algunos de los conjuntos de datos se encontraron problemas respecto al manejo de los dominios manejados. 
* En algunos de los archivos desnormalizados  se encontró que para mismo un código del dominio (Ejm. Código de municipio 11001) existían múltiples etiquetas (Ejm. "Bogotá",  "BOGOTA", "Santa fé de Bogotá", etc).  En esos casos fue necesario remover el etiquetado y reemplazarlo por su equivalente estandarizado a través de la codificación establecida por la fuente oficial (Ejm. DIVIPOLA)
* Para aquellos conjuntos de datos que no contenían un "primary key" fue necesario generar una que garantizara la identificación única de cada registro.
* Se validó el cruce ("join") entre conjuntos de datos con relaciones "Uno a muchos" o "Uno a Uno" de tal forma que garantizara la consistencia en la cantidad de registros que se "intersectan" en cada uno de los miembros de la relación.
* Se filtraron aquellos datos que no eran de interés para el análisis (Ejm. transacciones en áreas urbanas)
* En algunos casos fue necesario derivar nuevas entidades de datos a causa del comportamiento implícito de "uno a muchos"  encontrado en algunos atributos.   

## Registros repetidos (De-duplicación)

* Posterior a las validaciones y ajustes de consistencia de los datos, se procedió a la eliminación de registros duplicados (De-duplicación)

## Contenido

* Se realizó una validación del contenido de los datos en conjunto con el área temática con el fin de identificar posibles inconsistencias en la información.
* A partir de los hallazgos encontrados en el contenido de los diferentes registros se identificó la necesidad de realizar una limpieza de datos encaminada a corregir problemas relacionados con los tipos de dato (Ejm. Conversión de texto a número) y la estandarización del texto no estructurado (Ejm. Unificación en el nombramiento de unidadades de área:  "HECTAREA", "HA" "H.A.", "HECATAREAS", etc )  
* A partir de las tareas de procesamiento, limpieza y análisis de datos se derivaron nuevos atributos a permitieran dan soporte a la ejecución de las diferentes actividades del análisis de datos específico. 
* Se realizaron validaciones de los resultadods obtenidos en el proceso analítico en comparación con ejercicios similares realizados previamente por la entidad. 
