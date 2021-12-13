import pandas as pd
#import plotly.graph_objs as go
#import plotly.express as px
import matplotlib.pyplot as plt
from matplotlib.ticker import StrMethodFormatter

import pyspark
from pyspark.sql.functions import to_date, col, xxhash64, year,month,date_format, max
from pyspark.sql import SparkSession
from pyspark import SparkFiles

spark=SparkSession.builder.appName('Consulta1').getOrCreate()

urlfile1="https://storage.googleapis.com/covid19-open-data/v3/oxford-government-response.csv"

spark.sparkContext.addFile(urlfile1)
paisesLista = ['ES', 'BR', 'FR', 'AR', 'CH']
df = spark.read.option('header','true').option('inferSchema','true').csv("file://"+SparkFiles.get("oxford-government-response.csv")).filter(col("stringency_index") != 0)
df_spark = df.filter(df.location_key.isin(paisesLista))
#Media mundial stringency_index
mediaSI = df_spark.agg({"stringency_index": "avg"})
mediaSI.show()

#Media mundial de dias con restricciones
mediaDIAS = df_spark.groupBy("location_key").count().agg({"count": "avg"})
mediaDIAS.show()

#Media por paises stringency index
paisesSI = df_spark.groupBy("location_key").agg({"stringency_index": "avg"})
paisesSI.show()

#Dias totales con restricciones 
paisesDIAS = df_spark.groupBy("location_key").count().withColumn("count", col("count") / 7).withColumnRenamed("count", "weeks")
paisesDIAS.show()

#Join de los dias con restricciones y el indice por paises
dfJOIN = paisesSI.join(paisesDIAS, 'location_key')
dfJOIN.show()


#histograma del join
df_panda = dfJOIN.toPandas()

mediaIndex = mediaSI.first()["avg(stringency_index)"]
mediaTiempo = mediaDIAS.first()["avg(count)"]

df_panda.plot.bar(x = "location_key", y = ["avg(stringency_index)", "weeks"])

plt.axhline(mediaTiempo / 7, linestyle='dashed', linewidth=1, color = "orange", label = "world_avg(weeks)")
plt.axhline(mediaIndex, linestyle='dashed', linewidth=1, label = "world_avg(stringency_index)")
plt.legend(loc="upper center", bbox_to_anchor=(0.5, 1.15), ncol=2)
plt.show()
