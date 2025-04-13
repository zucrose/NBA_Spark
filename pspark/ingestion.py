from nba_api.stats.endpoints import teamgamelogs
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master('spark://spark:7077') \
    .appName("NBAIngestion")\
.config('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1') \
    .config("spark.sql.extensions", 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions') \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config('spark.sql.catalog.local.warehouse', '$PWD/warehouse') \
    .config('spark.sql.defaultCatalog', 'local') \
    .config('spark.sql.catalog.local.type', 'hadoop') \
    .config("spark.sql.catalog.spark_catalog.type", "hive") \
    .getOrCreate()

def func():
    for i in range(1990,2025):
        nextyear=(i + 1) % 100
        if nextyear<10:
            nextyear='0'+str(nextyear)
        season=f'{i}-{nextyear}'
        gameLogs = teamgamelogs.TeamGameLogs(season_nullable=season)
        panda_df = gameLogs.get_data_frames()[0]
        #print(panda_df)
        spark_df = spark.createDataFrame(panda_df)
        if spark.catalog.tableExists("local.db.GameLogs"):
            spark_df.writeTo("local.db.GameLogs").append()
        else:
            spark_df.writeTo("local.db.GameLogs").createOrReplace()
        #spark.sql('Select * from local.db.GameLogs').show()
        spark.sql('Select count(*) from local.db.GameLogs').show()



func()