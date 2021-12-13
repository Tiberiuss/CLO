import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import StrMethodFormatter

import pyspark
from pyspark.sql.functions import to_date, col, xxhash64, year,month,date_format, max,when
from pyspark.sql import SparkSession
from pyspark import SparkFiles

'''
Este script compara las muertes por covid(por cada millón de habitantes) entre mujeres y hombres
'''


spark=SparkSession.builder.appName('muertesPorSexo').getOrCreate()

paisesLista = ['ES','BR', 'DE','AR']
listaProporcion = [0, 0, 0 ,0]

urlfile1="https://storage.googleapis.com/covid19-open-data/v3/by-sex.csv"
urlfile2="https://storage.googleapis.com/covid19-open-data/v3/demographics.csv"

spark.sparkContext.addFile(urlfile1)
spark.sparkContext.addFile(urlfile2)

spark.sparkContext.addFile(urlfile1)
df = spark.read.option('header','true').option('inferSchema','true').csv(SparkFiles.get("by-sex.csv"))
df = df.filter(df.location_key.isin(paisesLista)).select('location_key','cumulative_deceased_male','cumulative_deceased_female')

deseases = df.groupBy(df.location_key).agg({'cumulative_deceased_male': 'max','cumulative_deceased_female': 'max'})
deseases = deseases.withColumnRenamed("max(cumulative_deceased_female)","female")
deseases = deseases.withColumnRenamed("max(cumulative_deceased_male)","male")
deseases.show() # muertes totales

cont = 0
for x in paisesLista:
    df_spark_demographics = spark.read.option('header','true').option('inferSchema','true').csv(SparkFiles.get("demographics.csv")).filter(col("location_key") == x)
    poblacion = df_spark_demographics.first()["population"]
    proporcion = 1000000 / poblacion
    listaProporcion[cont] = proporcion
    cont += 1

deseases = deseases.withColumn('female', when(deseases.location_key.endswith(paisesLista[0]),col("female")*listaProporcion[0])\
    .when(deseases.location_key.endswith(paisesLista[1]),col("female")*listaProporcion[1])\
    .when(deseases.location_key.endswith(paisesLista[2]),col("female")*listaProporcion[2])\
    .when(deseases.location_key.endswith(paisesLista[3]),col("female")*listaProporcion[3]))

deseases = deseases.withColumn('male', when(deseases.location_key.endswith(paisesLista[0]),col("male")*listaProporcion[0])\
    .when(deseases.location_key.endswith(paisesLista[1]),col("male")*listaProporcion[1])\
    .when(deseases.location_key.endswith(paisesLista[2]),col("male")*listaProporcion[2])\
    .when(deseases.location_key.endswith(paisesLista[3]),col("male")*listaProporcion[3]))


df_total = deseases.toPandas()
df_total.plot.bar(x ='location_key', y=['female', 'male'])

plt.ylabel('deaths/million inhabitants')
plt.show() # muertes por cada millón de habitantes