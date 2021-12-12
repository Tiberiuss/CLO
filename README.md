# Análisis de las medidas del COVID-19
## Contenidos:
  - Descripcion del problema
  - ¿Por qué necesitamos Big Data?
  - Descripción de la solución 
  - Descripción de los datos
  - Descripción técnica del modelo
  - Links
  - Descripción técnica del software usado
  - Rendimiento de la aplicación
  - Conclusión

## Descripcion del problema
Los casos de COVID-19 van en aumento y los expertos empiezan a hablar de que esto se podría convertir en una sexta ola. Es por esto que nuestro proyecto se centrará en el análisis de las distintas medidas sanitarias que han tomado los países (como por ejemplo el uso de mascarillas o el cierre de colegios), de tal manera que podamos ver su efectividad y prepararnos para próximas olas.

## ¿Por qué necesitamos Big Data?
La necesidad de utilizar Big Data se debe al gran tamaño de los datos con los que vamos a trabajar y que además seguirá incrementándose a lo largo del tiempo puesto que los datos relativos a la pandemia se actualizan diariamente. Además de esto, tenemos una gran variedad de datos (salud, demografía, economía...).

## Descripción de la solución 
Para llegar a resolver el problema planteado realizaremos diversos graficos que muestren como ha afectado la pandemia (como por ejemplo las muertes o las hospitalizaciones) y como esto ha variado con las distintas medidas usadas. Al tener datos de todos los países del mundo, se tomarán varias muestras de paises de distintos continentes que nos ayudarán a sacar conclusiones como, porejemplo, si ciertas medidas sanitarias han sido beneficiosas o no. Esto mostrará como algunas medidas han sido claves a la hora del control de la pandemia y nos ayudará a tener una idea de cuales serán las medidas que habrá que tener más en cuenta en una próxima ola. A la hora de realizar la mayoría de estos gráficos hemos tenido como base un dataset que mediante un índice marcaba la exigencia de ciertas medidas tal como se explica a continuación:
- Stringency_index: es un índice que marca la exigencia total de un país teniendo en cuenta la exigencia que ha tenido con cada una de las medidas. Este indice nos ayuda a tener una idea general de lo estricto que ha sido el país.Para más información acerca de como se calcula pinche [aqui](https://github.com/OxCGRT/covid-policy-tracker/blob/master/documentation/index_methodology.md)
- Otros indices: Cada una de las medidas tomadas tiene un indice propio que indicará lo estricto que ha sido un país con respecto a una medida específica. Este índice nos ayuda a estudiar medidas concretas y ver si han servido o no. Para más información acerca de estos índices pinche [aqui](https://github.com/OxCGRT/covid-policy-tracker/blob/master/documentation/interpretation_guide.md)
### Limitaciones de la solución
Al tener un gran volumen de datos que hace referencia a todos los paises del mundo, nos hemos visto obligados a tomar una muestra de paises y sacar conclusiones a partir de estos, esto puede no ser representativo de la situación real. Es por esto, que para obtener una mejor solucion se debería hacer un estudio mucho más amplio, que abarque todos los países y todas las restricciones.

## Descripción de los datos
Todos los datasets usados los hemos obtenido [aqui](https://github.com/GoogleCloudPlatform/covid-19-open-data)
### Datasets útiles que utilizamos en el proyecto
- Demographics
- Economy
- Epidemiology
- Emergency Declaration
- Health
- Hospitalizations
- Mobility
- Vaccinations
- Government Response

En la siguiente imagen vemos un ejemplo de Government Response (medidas tomadas por el gobierno)

## Descripción técnica del modelo
### Pyspark
En cuanto a sofware nuestra principal herramienta ha sido pyspark, con este hemos podido transformar los datasets mencionados anteriormente (en formato csv) en informacion útil para llegar a una solución al problema planteado (usando principalmente la funcion de filtrado y de union de diferentes datasets). 

### Pandas y MatplotLib
Estas dos librerias de python nos han permitido transformar la información que habiamos obtenido mediante pyspark en distintos graficos (como por ejemplo un histograma). De esta forma se puedan ver de manera mucho mas clara las soluciones obtenidas, además de poder compararlas entre sí (algo que es esencial en nuestro proyecto).

### Google Cloud
Hemos usado GoogleCloud para ejecutar los scripts y poder comparar el tiempo que tardaba con lo que nos tardaba de forma local. De esta manera, aunque no estuvieramos trabajando con un volumen de datos tan grande se podría ver la mejora de rendimiento que nos proporciona esta herramienta.
## Links
  - [Scripts]()
  - [Graficos]()
  - [Dataset](https://github.com/GoogleCloudPlatform/covid-19-open-data)
## Descripción técnica del software usado (como ejecutarlo)
### Requisitos necesarios para ejecutar el software
Para ejecutar el software es necesarios tener instalado python con las librerias de pandas y matplotlib, pyspark y shell basado en unix.
### Como ejecutarlo (de manera local)
En la consola se escribirá "spark-submit nombreDelFichero.py", esto hará que spark ejecute el script y una vez vaya finalizando cada trabajo se nos abrirá otra pestaña que mostrará el gráfico obtenido (este grafico se puede descargar y una vez se haya hecho habrá que cerrar esta pestaña para que siga ejecutando el script).
### Como ejecutarlo (en Google Cloud)

## Rendimiento de la aplicación
## Conclusión
Las conclusiones que hemos obtenido varían dependiendo del script que se haya ejecutado por lo tanto se verá una conclusion por cada uno de los scripts y una conclusión que pueda englobar todos estos datos.
### Conclusiones indicesPorPaises.py
### Conclusiones economia.py
### Conclusiones muertes_Restricciones.py
### Conclusiones muertes_cierreColegios.py
### Conclusiones muertes_mascarillas.py
### Conclusiones muertes_cuarentena.py
