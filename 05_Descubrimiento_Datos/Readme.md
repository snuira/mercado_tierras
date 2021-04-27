# Descubrimiento de datos

- [Descubrimiento de datos](#descubrimiento-de-datos)
  - [Descripción de los datos](#descripción-de-los-datos)
    - [Transacciones inmobiliarias de la SNR](#transacciones-inmobiliarias-de-la-snr)
    - [Intervinientes en transacciones inmobiliarias de la SNR](#intervinientes-en-transacciones-inmobiliarias-de-la-snr)
    - [Información geográfica y alfanumérica del catastro colombiano (R1, R2)](#información-geográfica-y-alfanumérica-del-catastro-colombiano-r1-r2)
    - [Divipola -  DANE](#divipola----dane)
    - [Directorio Estadístico de Empresas -  DANE](#directorio-estadístico-de-empresas----dane)
    - [Base de datos personas inscritas al programa Tunja teletrabajo ( Alcaldía de Tunja )](#base-de-datos-personas-inscritas-al-programa-tunja-teletrabajo--alcaldía-de-tunja-)
    - [SECOP I y II](#secop-i-y-ii)
    - [Nombres de empresas - datos.gov.co](#nombres-de-empresas---datosgovco)
    - [DNP  - ConTexto](#dnp----contexto)
  - [Perfilamiento de datos](#perfilamiento-de-datos)


## Descripción de los datos

### Transacciones inmobiliarias de la SNR 

* Contenido:  Datos con información de transacciones registrales realizadas ante la SNR tanto en sectores rurales como urbanos 

* Tipo de conjunto de datos: Alfanumérico, Estructurada. 

* Cantidad de registros:  

  * Archivo versión 1: 4.194.049 
  * Archivo versión 2: 1.634.294 
  * Archivo versión 3: 4.894.440

* Tipos de datos:  

  * Archivo versión 1: Número de variables: 19 

  * Archivo versión 2:  Número de variables: 22 

* Atributos

  * COD_DANE: Código dane del municipio donde ocurrió la transacción	
  * ORIP : Código de la oficina de registro	
  * MATRICULA : Número de la matrícula del predio	
  * NRO_ANOTACION : Número de la anotación asociada a la transacción	
  * COD_NATURALEZA	: Código de la naturaleza de la transacción
  * ANIO_RADICACION	: Año de radicación de la transacción
  * NATURALEZA_JURIDICA	: Nombre de la naturaleza jurídica de la transacción	
  * DEPARTAMENTO : Nombre del departamento donde se encuentra el predio asociado a la transacción	
  * MUNICIPIO : Nombre del municipio donde se encuentra el predio asociado a la transacción		
  * VEREDA	 : Nombre de la vereda donde se encuentra el predio asociado a la transacción	
  * TIPO_PREDIO	: Tipo de predio (Rural, Urbano, Sin información)
  * DIRECCION	: Dirección del predio	
  * SISTEMA	 : Sistema de información desde donde provienen los datos
  * LINDERO : Información del lindero asociado a la transacción
  * VALOR: Valor de la transacción

* Velocidad de los datos: La información es recibida por la UPRA de forma anual 

* Veracidad: La información proviene directamente del custodio de la información registral del país (SNR). 

* Cantidad de filas repetidas: 0 (0%) 

* Valor: A través de este conjunto de datos puede estimarse el dinamismo del mercado inmobiliario en el sector rural. 



### Intervinientes en transacciones inmobiliarias de la SNR 

* Contenido: Intervinientes (personas o empresas) en las transaccciones inmobiliarias registradas por la SNR.  

* Tipo de conjunto de datos: Alfanumérico, Estructurada. 

* Cantidad de registros:  

    * Archivo versión 1: 5.985.303 

    * Archivo versión 2: 5.985.303 

* Tipos de datos:  

  * Archivo versión 1:  Número de variables: 7 

  * Archivo versión 2:  Número de variables: 12 

* Atributos:
  
  * COD_DANE: Código dane del municipio donde ocurrió la transacción	
  * ORIP : Código de la oficina de registro	
  * MATRICULA : Número de la matrícula del predio	
  * NRO_ANOTACION : Número de la anotación asociada a la transacción	
  * COD_NATURALEZA	: Código de la naturaleza de la transacción
  * ANIO_RADICACION	: Año de radicación de la transacción
  * NATURALEZA_JURIDICA	: Nombre de la naturaleza jurídica de la transacción
  * ROL	: Rol del interviniente en la transacción (DE / A)
  * PERSONA	: Nombre de la persona o empresa
  * TIPO_DOCUMENTO	:  Tipo de documento de la persona o empresa
  * NUMERO_DOCUMENTO : Número de documento de la persona o empresa 

* Velocidad de los datos: La información es recibida por la UPRA de forma anual 

* Veracidad: La información proviene directamente del custodio de la información registral del país (SNR). 

* Cantidad de filas repetidas: 12.411 (0.2%)  

* Valor: A través de este conjunto de datos puede estimarse la cantidad y tipo de intervinientes en las transacciones del mercado inmobiliario en el sector rural. 


### Información geográfica y alfanumérica del catastro colombiano (R1, R2) 

*    Contenido: Información de tipo físico de todos los predios de Colombia proveniente de los diferentes entes encargados de la gestión de catastro. 

### Divipola -  DANE

* Contenido: División político administrativa oficial del país

### Directorio Estadístico de Empresas -  DANE 

* Contenido : El Directorio Estadístico de Empresas contiene información sobre la identificación y ubicación de todas las empresas, con sus establecimientos y activas en el territorio nacional.   


###     Base de datos personas inscritas al programa Tunja teletrabajo ( Alcaldía de Tunja )

* Contenido:  En el portal de datos abiertos se encontró esta base de datos que contiene nombres de personas clasificados por género (másculino, femenino). Url: https://www.datos.gov.co/Ciencia-Tecnolog-a-e-Innovaci-n/BASE-DE-DATOS-PERSONAS-INSCRITAS-AL-PROGRAMA-TUNJA/cnce-pr7n  


### SECOP I y II

* Contenido: Se encuentra la información de los procesos contractuales ejecutados por entidades del estado.  De este conjunto de datos se tomaron los nombres de las empresas que intervinieron  en dichos contratos.


### Nombres de empresas - datos.gov.co

* Contenido: Nombres de empresas colombianas extraídas a partir de datos recopilados desde datos.gov.co : 
  * 1000 Empresas mas grandes del país	https://www.datos.gov.co/Econom-a-y-Finanzas/1000-Empresas-mas-grandes-del-pa-s/8hn7-rpp8	
  * Empresas Transportadoras en el Valle del Cauca	https://www.datos.gov.co/Transporte/Empresas-Transportadoras-en-el-Valle-del-Cauca/dis4-ttyv	
  * Registro Único de Prestadores de Servicios Públicos-RUPS	https://www.datos.gov.co/Hacienda-y-Cr-dito-P-blico/Registro-nico-de-Prestadores-de-Servicios-P-blicos/4qkq-csdn	
  * Empresas de Transporte Marítimo habilitadas y con permiso de operación	https://www.datos.gov.co/Seguridad-y-Defensa/
  * Empresas-de-Transporte-Mar-timo-habilitadas-y-con-/ex5b-wy32	
  * Empresas De Transporte Fluvial De Pasajeros	https://www.datos.gov.co/Transporte/Empresas-De-Transporte-Fluvial-De-Pasajeros/wwnc-h69c	



### DNP  - ConTexto

ConTexto: En el repositorio github de la librería ConTexto (https://github.com/ucd-dnp/ConTexto/tree/master/contexto/data/listas_stopwords) se encontraron los siguientes conjuntos de datos: 

*    stop_words_es.txt: Listado de stop words para el idioma castellano. 

*    apellidos.txt: Listado de apellidos comunes en español 

*    nombres_hombres.txt: Listado de nombres comunes de hombres en castellano 

*    nombres_mujeres.txt: Listado de nombres comunes de hombres en castellano 

**Nota:** La fuente principal de los nombres fue obtenida por el DNP a partir del listado encontrado en https://script.byu.edu/Plugins/FileManager/Files/Images/SiteAssets/Spanish-Nombres.pdf. Posteriormente los miembros del grupo de científicos de dadtos del DNP complementaron la información con nombres de otras fuentes   




## Perfilamiento de datos

* [05 - 01 - Perfilamiento de datos](05_01_Perfilamiento_Datos)  Carpeta que contiene los reportes html que contienen el perfilamiento general de los datos. 