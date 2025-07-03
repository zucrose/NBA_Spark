from nba_api.stats.endpoints import commonallplayers,commonplayerinfo
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col, lit, udf, broadcast
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from rich.jupyter import display

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





pbprawdf=spark.sql('Select * from local.db.playbyplay')
actiondf=spark.sql('Select * from local.db.dim_action')

print(actiondf.count())

pbpa=pbprawdf.join(broadcast(actiondf),['actionType','subtype','shotResult','isFieldGoal'])
pbpa=pbpa.drop('actionType','subtype','playerName','playerNameI','teamTricode','shotResult','isFieldGoal')
pbprawdf.show()



print(spark.conf.get('spark.sql.autoBroadcastJoinThreshold'))




