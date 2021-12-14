#!/usr/bin/env python
from pyspark.sql import SparkSession
from pyspark.sql.functions import last
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
df_government = spark.read.option('header','true').option('inferSchema','true').csv(f"{inputUri}/oxford-government-response.csv")

df_index = df_index.filter("aggregation_level==0").select(["location_key","country_name","iso_3166_1_alpha_3"])
df_government = df_government.select(["date","location_key","stringency_index"])
#aggregation_level 0: By countries
df_join = df_index.join(df_government,["location_key"])
df_join = df_join.sort(["date","stringency_index"]).groupBy(["location_key","iso_3166_1_alpha_3","country_name"]).agg(last("stringency_index",ignorenulls=True).alias("stringency_index"))

if outputUri is not None:
    #Exportar resultado a csv
    ##df_join.coalesce(1).write.mode("overwrite").option("header","true").csv("out")   Esto es lo ideal para datasets largos
    with open(f'{outputUri}/S1.csv','w') as f:
        cols = df_join.columns
        print(*cols,sep=",",file=f)
        for row in df_join.collect():    
            print(*[row[i] for i in cols],sep=',',file=f)

df_join = df_join.toPandas()
fig = px.choropleth(df_join, locations="iso_3166_1_alpha_3",
                    color="stringency_index",
                    hover_name="country_name",
                    color_continuous_scale="Hot_r")
fig.show()
fig.write_image("Graficos/S1.svg",width=1920, height=1080)