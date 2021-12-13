import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import StrMethodFormatter

import pyspark
from pyspark.sql.functions import to_date, col, xxhash64, year,month,date_format, max, unix_timestamp, lit, when
from pyspark.sql import SparkSession
from pyspark import SparkFiles

'''
Este script muestra las muertes por covid(por cada millón de habitantes) por rango de edades
'''

spark=SparkSession.builder.appName('muertesPorEdad').getOrCreate()

paisesLista = ['ES','BR', 'MX','AR']
listaProporcion = [0, 0, 0 ,0]

urlfile1="https://storage.googleapis.com/covid19-open-data/v3/by-age.csv"
urlfile2="https://storage.googleapis.com/covid19-open-data/v3/demographics.csv"

spark.sparkContext.addFile(urlfile1)
spark.sparkContext.addFile(urlfile2)

df = spark.read.option('header','true').option('inferSchema','true').csv(SparkFiles.get("by-age.csv"))
df = df.filter(df.location_key.isin(paisesLista)).select('location_key','cumulative_deceased_age_0','cumulative_deceased_age_1','cumulative_deceased_age_2','cumulative_deceased_age_3'
,'cumulative_deceased_age_4','cumulative_deceased_age_5','cumulative_deceased_age_6','cumulative_deceased_age_7','cumulative_deceased_age_8','cumulative_deceased_age_9')


deseases_by_age = df.groupBy(df.location_key).agg({'cumulative_deceased_age_0': 'max','cumulative_deceased_age_1': 'max','cumulative_deceased_age_2': 'max',
'cumulative_deceased_age_3': 'max','cumulative_deceased_age_4': 'max','cumulative_deceased_age_5': 'max','cumulative_deceased_age_6': 'max','cumulative_deceased_age_7': 'max',
'cumulative_deceased_age_8': 'max','cumulative_deceased_age_9': 'max'})

deseases_by_age = deseases_by_age.withColumnRenamed("max(cumulative_deceased_age_0)","1-9")
deseases_by_age = deseases_by_age.withColumnRenamed("max(cumulative_deceased_age_1)","10-19")
deseases_by_age = deseases_by_age.withColumnRenamed("max(cumulative_deceased_age_2)","20-29")
deseases_by_age = deseases_by_age.withColumnRenamed("max(cumulative_deceased_age_3)","30-39")
deseases_by_age = deseases_by_age.withColumnRenamed("max(cumulative_deceased_age_4)","40-49")
deseases_by_age = deseases_by_age.withColumnRenamed("max(cumulative_deceased_age_5)","50-59")
deseases_by_age = deseases_by_age.withColumnRenamed("max(cumulative_deceased_age_6)","60-69")
deseases_by_age = deseases_by_age.withColumnRenamed("max(cumulative_deceased_age_7)","70-79")
deseases_by_age = deseases_by_age.withColumnRenamed("max(cumulative_deceased_age_8)","80-89")
deseases_by_age = deseases_by_age.withColumnRenamed("max(cumulative_deceased_age_9)","90-")


cont = 0
for x in paisesLista:
    df_spark_demographics = spark.read.option('header','true').option('inferSchema','true').csv(SparkFiles.get("demographics.csv")).filter(col("location_key") == x)
    poblacion = df_spark_demographics.first()["population"]
    proporcion = 1000000 / poblacion
    listaProporcion[cont] = proporcion
    cont += 1


deseases_by_age.show(vertical=True) # muertes por rango de edad totales

deseases_by_age = deseases_by_age.withColumn('1-9', when(deseases_by_age.location_key.endswith(paisesLista[0]),col("1-9")*listaProporcion[0])\
    .when(deseases_by_age.location_key.endswith(paisesLista[1]),col("1-9")*listaProporcion[1])\
    .when(deseases_by_age.location_key.endswith(paisesLista[2]),col("1-9")*listaProporcion[2])\
    .when(deseases_by_age.location_key.endswith(paisesLista[3]),col("1-9")*listaProporcion[3]))

deseases_by_age = deseases_by_age.withColumn('10-19', when(deseases_by_age.location_key.endswith(paisesLista[0]),col("10-19")*listaProporcion[0])\
    .when(deseases_by_age.location_key.endswith(paisesLista[1]),col("10-19")*listaProporcion[1])\
    .when(deseases_by_age.location_key.endswith(paisesLista[2]),col("10-19")*listaProporcion[2])\
    .when(deseases_by_age.location_key.endswith(paisesLista[3]),col("10-19")*listaProporcion[3]))

deseases_by_age = deseases_by_age.withColumn('20-29', when(deseases_by_age.location_key.endswith(paisesLista[0]),col("20-29")*listaProporcion[0])\
    .when(deseases_by_age.location_key.endswith(paisesLista[1]),col("20-29")*listaProporcion[1])\
    .when(deseases_by_age.location_key.endswith(paisesLista[2]),col("20-29")*listaProporcion[2])\
    .when(deseases_by_age.location_key.endswith(paisesLista[3]),col("20-29")*listaProporcion[3]))

deseases_by_age = deseases_by_age.withColumn('30-39', when(deseases_by_age.location_key.endswith(paisesLista[0]),col("30-39")*listaProporcion[0])\
    .when(deseases_by_age.location_key.endswith(paisesLista[1]),col("30-39")*listaProporcion[1])\
    .when(deseases_by_age.location_key.endswith(paisesLista[2]),col("30-39")*listaProporcion[2])\
    .when(deseases_by_age.location_key.endswith(paisesLista[3]),col("30-39")*listaProporcion[3]))

deseases_by_age = deseases_by_age.withColumn('40-49', when(deseases_by_age.location_key.endswith(paisesLista[0]),col("40-49")*listaProporcion[0])\
    .when(deseases_by_age.location_key.endswith(paisesLista[1]),col("40-49")*listaProporcion[1])\
    .when(deseases_by_age.location_key.endswith(paisesLista[2]),col("40-49")*listaProporcion[2])\
    .when(deseases_by_age.location_key.endswith(paisesLista[3]),col("40-49")*listaProporcion[3]))

deseases_by_age = deseases_by_age.withColumn('50-59', when(deseases_by_age.location_key.endswith(paisesLista[0]),col("50-59")*listaProporcion[0])\
    .when(deseases_by_age.location_key.endswith(paisesLista[1]),col("50-59")*listaProporcion[1])\
    .when(deseases_by_age.location_key.endswith(paisesLista[2]),col("50-59")*listaProporcion[2])\
    .when(deseases_by_age.location_key.endswith(paisesLista[3]),col("50-59")*listaProporcion[3]))

deseases_by_age = deseases_by_age.withColumn('60-69', when(deseases_by_age.location_key.endswith(paisesLista[0]),col("60-69")*listaProporcion[0])\
    .when(deseases_by_age.location_key.endswith(paisesLista[1]),col("60-69")*listaProporcion[1])\
    .when(deseases_by_age.location_key.endswith(paisesLista[2]),col("60-69")*listaProporcion[2])\
    .when(deseases_by_age.location_key.endswith(paisesLista[3]),col("60-69")*listaProporcion[3]))

deseases_by_age = deseases_by_age.withColumn('70-79', when(deseases_by_age.location_key.endswith(paisesLista[0]),col("70-79")*listaProporcion[0])\
    .when(deseases_by_age.location_key.endswith(paisesLista[1]),col("70-79")*listaProporcion[1])\
    .when(deseases_by_age.location_key.endswith(paisesLista[2]),col("70-79")*listaProporcion[2])\
    .when(deseases_by_age.location_key.endswith(paisesLista[3]),col("70-79")*listaProporcion[3]))

deseases_by_age = deseases_by_age.withColumn('80-89', when(deseases_by_age.location_key.endswith(paisesLista[0]),col("80-89")*listaProporcion[0])\
    .when(deseases_by_age.location_key.endswith(paisesLista[1]),col("80-89")*listaProporcion[1])\
    .when(deseases_by_age.location_key.endswith(paisesLista[2]),col("80-89")*listaProporcion[2])\
    .when(deseases_by_age.location_key.endswith(paisesLista[3]),col("80-89")*listaProporcion[3]))

deseases_by_age = deseases_by_age.withColumn('90-', when(deseases_by_age.location_key.endswith(paisesLista[0]),col("90-")*listaProporcion[0])\
    .when(deseases_by_age.location_key.endswith(paisesLista[1]),col("90-")*listaProporcion[1])\
    .when(deseases_by_age.location_key.endswith(paisesLista[2]),col("90-")*listaProporcion[2])\
    .when(deseases_by_age.location_key.endswith(paisesLista[3]),col("90-")*listaProporcion[3]))

deseases_by_age.show(vertical=True)

df_final=deseases_by_age.toPandas()
df_final.plot.bar(x ='location_key', y=['1-9', '10-19', '20-29', '30-39', '40-49', '50-59', '60-69', '70-79'
, '80-89', '90-'])

plt.ylabel('deaths/million inhabitants')
plt.legend(loc="upper center", bbox_to_anchor=(0.5, 1.15), ncol=5)
plt.show() #muertes por rango de edad por cada millón de habitantes