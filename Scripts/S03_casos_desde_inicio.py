#!/usr/bin/env python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import sys

import pandas as pd
import plotly.express as px

inputUri=None
outputUri=None
if len(sys.argv) == 2:
    inputUri=sys.argv[1]
elif len(sys.argv) == 3:
    inputUri=sys.argv[1]
    outputUri=sys.argv[2]
else:
  raise Exception("Exactly 1 argument is required: <inputUri> [<outputUri>]")


spark=SparkSession.builder.appName('CLO').getOrCreate()

df_index = spark.read.option('header','true').option('inferSchema','true').csv(f"{inputUri}/index.csv")
df_epidemiology = spark.read.option('header','true').option('inferSchema','true').csv(f"{inputUri}/epidemiology.csv")
df_vaccinations = spark.read.option('header','true').option('inferSchema','true').csv(f"{inputUri}/vaccinations.csv")
df_demographics = spark.read.option('header','true').option('inferSchema','true').csv(f"{inputUri}/demographics.csv")

df_index = df_index.filter("aggregation_level=0").select(["location_key","country_name"])
df_demographics = df_demographics.select(["location_key","population"])
df_epidemiology = df_epidemiology.select(["location_key","date","cumulative_confirmed","new_confirmed"])
#aggregation_level 0: By countries
df_join = df_index.join(df_epidemiology,["location_key"]).sort("cumulative_confirmed")
df_join = df_join.join(df_demographics,["location_key"]).withColumn("new_confirmed_relative",10e6*(col("new_confirmed")/col("population")))

paises = ["ES","RU","AR","CN"]
df_join = df_join.filter(col("location_key").isin(paises))

if outputUri is not None:
    #Exportar resultado a csv
    ##df_join.coalesce(1).write.mode("overwrite").option("header","true").csv("out")   Esto es lo ideal para datasets largos
    with open(f'{outputUri}/S3.csv','w') as f:
        cols = df_join.columns
        print(*cols,sep=",",file=f)
        for row in df_join.collect():    
            print(*[row[i] for i in cols],sep=',',file=f)

fig = px.bar(df_join.toPandas(), color="new_confirmed",
                     x="date", y="new_confirmed", 
                      labels={
                     "date": "Date",
                     "new_confirmed_relative": "Confirmed cases",
                    },
                    title="Confirmed COVID-19 cases",
                    facet_col="country_name",facet_col_wrap=2)
fig.update_traces(dict(marker_line_width=0))

fig.show()
fig.write_image("Graficos/S3.svg",width=1920, height=1080)
