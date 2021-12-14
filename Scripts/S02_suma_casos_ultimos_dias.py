#!/usr/bin/env python
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, datediff, to_date, col, sum
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
df_demographics = spark.read.option('header','true').option('inferSchema','true').csv(f"{inputUri}/demographics.csv")

#aggregation_level 0: By countries
df_epidemiology = df_epidemiology.filter(datediff(current_date(), to_date(col("date"),"yyyy-MM-dd")) < 7)
df_join = df_index.filter("aggregation_level==0").join(df_epidemiology.groupBy("location_key").agg(sum("new_confirmed").alias("sum_confirmed")),["location_key"]).join(df_demographics,["location_key"])
df_join = df_join.withColumn("sum_confirmed_relative",10e6*(col("sum_confirmed")/col("population")))

df_join1 = df_join.sort('sum_confirmed_relative',ascending=False).limit(50)

if outputUri is not None:
    #Exportar resultado a csv
    ##df_join.coalesce(1).write.mode("overwrite").option("header","true").csv("out")   Esto es lo ideal para datasets largos
    with open(f'{outputUri}/S2.csv','w') as f:
        cols = df_join1.columns
        print(*cols,sep=",",file=f)
        for row in df_join.collect():    
            print(*[row[i] for i in cols],sep=',',file=f)
   

fig1 = px.bar(df_join1.toPandas(), color="country_name",
                x="country_name", y="sum_confirmed_relative",
                hover_name="location_key",title="Sum of confirmed COVID-19 cases per 1 million inhabitants of last 7 days")
fig1.layout.showlegend=False

fig1.show()
fig1.write_image("Graficos/S2.svg",width=1920, height=1080)