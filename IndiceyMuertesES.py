import pandas as pd
#import plotly.graph_objs as go
#import plotly.express as px
import matplotlib.pyplot as plt
from matplotlib.ticker import StrMethodFormatter

import pyspark
from pyspark.sql.functions import to_date, col, xxhash64, year,month,date_format, max, unix_timestamp, lit
from pyspark.sql import SparkSession
from pyspark import SparkFiles

spark = SparkSession.builder.appName('Consulta2').getOrCreate()

urlfile1="https://storage.googleapis.com/covid19-open-data/v3/oxford-government-response.csv"
urlfile2= "https://storage.googleapis.com/covid19-open-data/v3/demographics.csv"
urlfile3="https://storage.googleapis.com/covid19-open-data/v3/epidemiology.csv"

spark.sparkContext.addFile(urlfile1)
spark.sparkContext.addFile(urlfile2)
spark.sparkContext.addFile(urlfile3)

df_spark_governmentResponse = spark.read.option('header','true').option('inferSchema','true').csv("file://"+SparkFiles.get("oxford-government-response.csv")).filter(col("stringency_index") != 0)
df_spark_governmentResponse = df_spark_governmentResponse.filter(col("location_key") == 'ES')

df_spark_epidemiology = spark.read.option('header','true').option('inferSchema','true').csv("file://"+SparkFiles.get("epidemiology.csv")).filter(col("location_key") == 'ES')

df_spark_demographics = spark.read.option('header','true').option('inferSchema','true').csv("file://"+SparkFiles.get("demographics.csv")).filter(col("location_key") == 'ES')
poblacion = df_spark_demographics.first()["population"]

dfJOIN = df_spark_epidemiology.join(df_spark_governmentResponse, 'date').orderBy(col("date").asc())
dfJOIN = dfJOIN.withColumn("new_deceased", col("new_deceased") * 10000000 / poblacion).withColumnRenamed("new_deceased", "new_deceased_1000000")

df_panda = dfJOIN.toPandas()
df_panda.plot(x ='date', y=["stringency_index", "new_deceased_1000000"], kind = 'line')
plt.show()