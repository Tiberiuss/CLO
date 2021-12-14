import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import StrMethodFormatter

import pyspark
from pyspark.sql.functions import to_date, col, xxhash64, year,month,date_format, max, unix_timestamp, lit, when
from pyspark.sql import SparkSession
from pyspark import SparkFiles

'''
Este script compara las muertes por covid y el pib per capita en USD para una lista de paises
'''


paisesLista = ['ES', 'BR', 'FR', 'AR', 'CH']
listaProporcion = [0, 0, 0 ,0 ,0]

spark = SparkSession.builder.appName('A').getOrCreate()

urlfile1="https://storage.googleapis.com/covid19-open-data/v3/epidemiology.csv"
urlfile2= "https://storage.googleapis.com/covid19-open-data/v3/economy.csv"
urlfile3="https://storage.googleapis.com/covid19-open-data/v3/demographics.csv"

spark.sparkContext.addFile(urlfile1)
spark.sparkContext.addFile(urlfile2)
spark.sparkContext.addFile(urlfile3)


df_spark_epidemiology = spark.read.option('header','true').option('inferSchema','true').csv("file://"+SparkFiles.get("epidemiology.csv"))
df_spark_epidemiology = df_spark_epidemiology.filter(df_spark_epidemiology.location_key.isin(paisesLista))

max_deceases = df_spark_epidemiology.groupBy('location_key').agg({'cumulative_deceased': 'max'})

df_spark_economy = spark.read.option('header','true').option('inferSchema','true').csv("file://"+SparkFiles.get("economy.csv"))
df_spark_economy = df_spark_economy.filter(df_spark_economy.location_key.isin(paisesLista))

dfJOIN = max_deceases.join(df_spark_economy, 'location_key')

cont = 0
for x in paisesLista:
    df_spark_demographics = spark.read.option('header','true').option('inferSchema','true').csv("file://"+SparkFiles.get("demographics.csv")).filter(col("location_key") == x)
    poblacion = df_spark_demographics.first()["population"]
    proporcion = 10000000 / poblacion
    listaProporcion[cont] = proporcion
    cont += 1


dfJOIN = dfJOIN.withColumn('max(cumulative_deceased)', when(dfJOIN.location_key.endswith(paisesLista[0]),col("max(cumulative_deceased)") * listaProporcion[0])\
    .when(dfJOIN.location_key.endswith(paisesLista[1]),col("max(cumulative_deceased)") * listaProporcion[1])\
    .when(dfJOIN.location_key.endswith(paisesLista[2]),col("max(cumulative_deceased)") * listaProporcion[2])\
    .when(dfJOIN.location_key.endswith(paisesLista[3]),col("max(cumulative_deceased)") * listaProporcion[3])\
    .when(dfJOIN.location_key.endswith(paisesLista[4]),col("max(cumulative_deceased)") * listaProporcion[4]))

df_panda = dfJOIN.toPandas()
df_panda.plot.bar(x ='location_key', y=['max(cumulative_deceased)', 'gdp_per_capita_usd'])
plt.show()
