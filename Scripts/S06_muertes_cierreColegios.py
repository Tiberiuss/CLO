import pandas as pd
#import plotly.graph_objs as go
#import plotly.express as px
import matplotlib.pyplot as plt
from matplotlib.ticker import StrMethodFormatter

import pyspark
from pyspark.sql.functions import to_date, col, xxhash64, year,month,date_format, max, unix_timestamp, lit
from pyspark.sql import SparkSession
from pyspark import SparkFiles

import sys

paisesLista = ['ES', 'BR', 'FR', 'AR', 'CH']

spark = SparkSession.builder.appName('A').getOrCreate()

inputUri=None
outputUri=None
if len(sys.argv) == 2:
    inputUri=sys.argv[1]
elif len(sys.argv) == 3:
    inputUri=sys.argv[1]
    outputUri=sys.argv[2]
else:
  raise Exception("Exactly 1 argument is required: <inputUri> [<outputUri>]")

df_spark_governmentResponse = spark.read.option('header','true').option('inferSchema','true').csv(f"{inputUri}/oxford-government-response.csv")\
    .filter(col("location_key").isin(paisesLista)).filter(col("school_closing") != 0).cache()
df_spark_epidemiology = spark.read.option('header','true').option('inferSchema','true').csv(f"{inputUri}/epidemiology.csv").cache()
df_spark_hospitalizations = spark.read.option('header','true').option('inferSchema','true').csv(f"{inputUri}/hospitalizations.csv").cache()
df_spark_demographics = spark.read.option('header','true').option('inferSchema','true').csv(f"{inputUri}/demographics.csv").cache()

for x in paisesLista:
    df_spark_governmentResponse_pais1 = df_spark_governmentResponse.filter(col("location_key") == x)
    df_spark_epidemiology1 = df_spark_epidemiology.filter(col("location_key") == x)
    df_spark_demographics1 = df_spark_demographics.filter(col("location_key") == x)
    df_spark_hospitalizations1 = df_spark_hospitalizations.filter(col("location_key") == x)
    
    poblacion = df_spark_demographics1.first()["population"]

    dfJOIN = df_spark_epidemiology1.join(df_spark_governmentResponse_pais1, 'date')
    dfJOIN = dfJOIN.join(df_spark_hospitalizations1, 'date').orderBy(col("date").asc())

    proporcion = 1000000 / poblacion
    percentage = 100 / 3

    dfJOIN = dfJOIN.withColumn("new_deceased", col("new_deceased") * proporcion).withColumnRenamed("new_deceased", "new_deceased/1000000")
    dfJOIN = dfJOIN.withColumn("school_closing", col("school_closing") * percentage).withColumnRenamed("school_closing", "school_closing_percentage")
    dfJOIN = dfJOIN.withColumn("new_hospitalized_patients", col("new_hospitalized_patients") * proporcion).withColumnRenamed("new_hospitalized_patients", "new_hospitalized_patients/100000")

    dfJOIN.show()
    df_panda = dfJOIN.toPandas()
    df_panda.plot(x ='date', y=["school_closing_percentage", "new_deceased/1000000", "new_hospitalized_patients/100000" ], kind = 'line')
    plt.savefig("graficos/muertes_cierreEscuelas" + x + ".jpeg")