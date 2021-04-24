# Fases de Análisis

Las fases de análisis que se establecieron a lo largo del proyecto son:

1. [Transacciones](#Transacciones)
2. [Intervinientes](#Intervinientes)
3. [Linderos](#Linderos)
4. [Catastro R1R2](#R1R2)
5. [Componente Geográfico](#c_geo)

<a name="Transacciones"></a>
## 1.	Transacciones


![Transacciones](Diagrama_Analisis_Transacciones.png "Transacciones")

Dentro de los principales pasos del diagrama de flujo de la temática de Transacciones están:

*    Conversión del archivo JSON originalmente exportado desde el software SAS (Statistical Analysis Software) por parte del grupo de Análisis de Información desde un tipo Single-Line a Multi-Line. Esto con el fin de evitar que quede almacenado una gran cantidad de información en una misma línea de texto(~2GB). Subsecuentemente esto facilita la lectura y manipulación de los datos de una forma más optima y eficiente.

*    Adecuación de aquellos campos necesarios para el análisis, esto incluye una depuración de potenciales registros duplicados a lo largo de todo el conjunto de datos.

*    Creación del archivo .parquet en el datalake con todo el consolidado de las Transacciones desde la vigencia 2015 a la 2019. Esto ayuda a que se manipule de una forma mas eficiente los datos para la extracción, transformación y cargue de los mismos.

*    Conformación de una llave primaria. Debido a la necesidad de distinguir de forma única cada registro del archivo compilado de Transacciones es necesario crear una llave primaria genérica. Esto garantizará la veracidad en las estimaciones y de las posteriores relaciones entre catastro-registro. Esta se conforma de la siguiente forma: 

![Llave_Primaria](Llave_Primaria.PNG "Llave_Primaria")

*    Validación de registros duplicados teniendo en cuenta la llave primaria calculada en el paso anterior. Registros duplicados son desechados y se consolida una base de datos limpia.

*    Hallar potenciales Transacciones rurales que no están clasificadas desde los datos originales. Para ellos se aplican algoritmos que buscan dentro del campo “dirección” palabras claves (respaldadas temáticamente) almacenadas un diccionario que permitirán inferir registros de Transacciones que pueden ser consideradas Rurales.

*    Se generan y consolidan dos archivos de gran importancia para los siguientes procesos: Archivo de Transacciones Rurales y Linderos Rurales.

<a name="Intervinientes"></a>
## 2.	Intervinientes

![Intervinientes](Diagrama_Analisis_Intervinientes.png "Intervinientes")

Detallando el procesamiento desarrollado en la temática de Intervinientes se encuentran los siguientes pasos:

*    Generación de la llave primaría sobre el conjunto de datos original del consolidado de Intervinientes de las vigencias comprendidas entre el 2015 y 2019. La configuración de la llave obedece la misma forma que se detalla en la temática de Transacciones.

*    Aplicación de expresiones regulares al campo de “nombres” de intervinientes con el fin de efectuar una limpieza inicial.

*    Generar la clasificación de los intervinientes de acuerdo al tipo de documento que se encuentre registrado en el conjunto de datos. De esta forma se discriminan aquellos intervinientes Nacionales y Extranjeros. 

*    Limpiar y validar el campo donde se encuentra almacenado el número de documento de identidad. Este paso es necesario ya que desde los datos originales se encontraron muchas inconsistencias y errores que es necesario tratar de corregir en la medida de los posible.

*    Primera clasificación de genero de los intervinientes según el rango de cédula. Esta clasificación de genero se realizó únicamente para aquellos intervinientes en donde el número de cedula se encuentre entre los siguientes rangos:
		-	Hombres: Rangos del 1 al 19’000.000 y del 70’000.000 al 99’999.999
		-	Mujeres: Rango del 20’000.000 al 69’999.999
	 Es importante mencionar que este criterio en la numeración de hombres y mujeres fue establecido por la [registraduría](https://www.registraduria.gov.co/1-de-noviembre-de-2012-No-69-60.html) entre los años 1952 a 2003. Posterior a ello la asignación del documento de identidad obedece únicamente al NUIP donde no existen rangos de genero.

*    Extracción de nombres de empresas y empresas inferidas. Para estas últimas hubo la necesidad de configurar un diccionario con palabras que generalmente son asignadas a nombres de empresas.

*    Ejecución del procesamiento de nombres. Debido a que el campo de nombres no se encuentra estandarizado, es necesario tener en cuenta los diferentes casos de cómo están originalmente ubicados los nombres y apellidos en cada uno de los registros. Algunos ejemplos son:
		-	Apellido Nombre
		-	Apellido_1 Apelido_2 Nombre_1 Nombre_2
		-	Apellidos , Nombre
		-	Una sola palabra
		-	Múltiples Nombres
		-	Nombres de empresas marcados como personas
	 Una vez los nombres han sido limpiados se procede a extraer únicamente el primer nombre de cada interviniente ya que puede definir de forma certera el género de una persona.

*    Filtrado del conjunto de datos de intervinientes considerando únicamente aquellos que estén relacionados con Transacciones Rurales. Para ello se filtran considerando el conjunto de datos consolidado en el diagrama de flujo de Transacciones de la fase previa.

*    Aplicación de diferentes algoritmos y/o métodos secundarios para generar una clasificación por género de aquellos intervinientes que no pudieron ser clasificados con el método de rangos de cédula. Estos métodos secundarios están diseñados para que realicen la clasificación según el primer nombre del interviniente.

*    Es importante mencionar que para llevar a cabo la ejecución de esto métodos secundarios hubo la necesidad de construir un archivo de nombres comunes clasificados por género (hombre/mujer) para el contexto nacional (Colombia) que es validado manualmente para tener una mayor seguridad en la aplicación de métodos subsecuentes. 

	 - Método del Diccionario: Este método busca encontrar coincidencias en los primeros nombres de aquellos intervinientes que no fueron clasificados por el método de rango de cedula inicialmente, con el consolidado de nombres comunes que tuvo una validación manual. De esta forma nombres que casen de forma exacta serán clasificados por género.
	 - Método de Similaridad: Este método está orientado a clasificar el género de aquellos intervinientes que no fueron clasificados ni por el método de rangos de cédula, ni por el método de Diccionario mencionado anteriormente. En este orden ideas el método de Similaridad busca encontrar diferencias de un solo carácter entre el nombre a predecir y los nombres comunes que se encuentran clasificados previamente por el análisis de frecuencias. De esta forma se aplica el algoritmo de Levensthein para que encuentre coincidencias con una distancia de máximo 1 carácter y que la última letra del nombre a predecir coincida con el nombre común del diccionario.

	 Ejemplo:
		-	 NEDIS (nombre a clasificar) y YUDIS (nombre clasificado previamente como Femenino) tiene una distancia-Levensthein de 2 aunque coinciden la ultima letra de los dos nombres. En este caso no se puede clasificar el nombre.
		-	 NEDIS(nombre a clasificar) y LEDIS(nombre clasificado previamente como Femenino) tiene una distancia- Levensthein de 1, además de coincidr la ultima letra de los dos nombres. En este caso el nombre es clasificado como Femenino por similaridad.

*    Método de Machine – Learning: Este método está orientando a clasificar el genero de todos aquellos primeros nombres de intervinientes que no pudieron ser clasificados por los métodos previamente. Luego de hacer un testeo y una revisión de la aplicación de algoritmos similares que busca solucionar problemas de esta envergadura, se decidió aplicar el algoritmo de Arboles de Decisión para la clasificación de genero basado únicamente en el primer nombre del interviniente. 

*    Una vez clasificados todos los nombres de los intervinientes se realiza el conteo de los intervinientes que participaron transacciones rurales para finalmente generar un archivo consolidado que servirá para la generación de reportes y alimentar tableros de control.


<a name="Linderos"></a>
## 3.	Linderos

<a name="R1R2"></a>
## 4.	Catastro R1R2

<a name="c_geo"></a>
## 5.	Componente Geográfico