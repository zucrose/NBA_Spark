from nba_api.stats.endpoints import commonallplayers,commonplayerinfo
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col, lit, udf, broadcast
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from rich.jupyter import display

import matplotlib.pyplot as plt
import pandas as pd


spark = SparkSession.builder \
    .appName("LoadTeamDimension")\
    .config('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1') \
    .config("spark.sql.extensions", 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions') \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config('spark.sql.catalog.local.warehouse', '$PWD/warehouse') \
    .config('spark.sql.defaultCatalog', 'local') \
    .config('spark.sql.catalog.local.type', 'hadoop') \
    .config("spark.sql.catalog.spark_catalog.type", "hive") \
    .getOrCreate()

actionDF=spark.sql('Select * from local.db.dim_action where subType="Driving Floating Jump Shot"')
pbpadf=spark.sql('Select * from local.db.fact_playbyplay') \
        .join(broadcast(actionDF),['actionKey']) \
        .groupby(['personid','shotResult']).count()
#pbpadf.show()
playerFloaterDF=spark.sql('Select person_id,display_first_last as name from local.db.dim_players') \
                 .withColumnRenamed('person_id','personid') \
                 .join(broadcast(pbpadf),['personId'])
madeFloatersDF=playerFloaterDF.filter('shotResult=="Made" and count>100')

totalFloatersDF=playerFloaterDF.groupby('personId').sum('count').join(madeFloatersDF,'personId')

withPercentDF=totalFloatersDF.withColumn('Percent',madeFloatersDF['count']/totalFloatersDF['sum(count)']) \
               .withColumnsRenamed({'sum(count)':'totalFloaters','count':'floatersMade'}).drop('shotResult')

pandasDF=withPercentDF.toPandas()

def funcPlot():
    pandasDF.plot.scatter(x='floatersMade',y='Percent')

    for index,row in pandasDF.iterrows():
        if row['floatersMade']>200 or row['Percent']>=0.5:
          plt.annotate(row['name'],(row['floatersMade'],row['Percent']))

    plt.savefig("plots/floaters.png")

funcPlot()


