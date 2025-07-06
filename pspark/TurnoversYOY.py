from nba_api.stats.endpoints import commonallplayers,commonplayerinfo
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col, lit, udf, broadcast
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from rich.jupyter import display

import matplotlib.pyplot as plt
import pandas as pd

spark = SparkSession.builder \
    .appName("LoadPlayerDim")\
    .config('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1') \
    .config("spark.sql.extensions", 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions') \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config('spark.sql.catalog.local.warehouse', '$PWD/warehouse') \
    .config('spark.sql.defaultCatalog', 'local') \
    .config('spark.sql.catalog.local.type', 'hadoop') \
    .config("spark.sql.catalog.spark_catalog.type", "hive") \
    .getOrCreate()

def load():
    actionDF=spark.sql('Select * from local.db.dim_action')
    factDF=spark.sql('Select * from local.db.fact_playByPlay')
    gameLogDF=spark.sql('Select game_id as gameId,season_year from local.db.gamelogs')

    actionFactDF=factDF.join(broadcast(actionDF),['actionkey'])
    nDF=actionFactDF.filter(actionFactDF.actionType=='Turnover').join(broadcast(gameLogDF),'gameId').groupby('season_year','subtype').count()

    nDF.writeTo('local.db.YOY_TO').createOrReplace()




subtypes=spark.sql('Select distinct subtype from local.db.yoy_to').collect()
fig, ax = plt.subplots()
for i in subtypes:
     pandasDF=spark.sql(f"Select * from local.db.yoy_to where subtype='{i[0]}' order by 1 asc").toPandas()
     #print(pandasDF)
     pandasDF.plot(ax=ax,kind='line' , x='season_year',y='count',label=i[0])



plt.savefig("plots/TO_YOY.png")