from nba_api.stats.endpoints import teamdetails
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col,lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

#.master('spark://spark:7077') \
spark = SparkSession.builder \
    .appName("LoadTeamDimension")\
    .config('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1') \
    .config("spark.sql.extensions", 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions') \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config('spark.sql.catalog.local.warehouse', '../$PWD/warehouse') \
    .config('spark.sql.defaultCatalog', 'local') \
    .config('spark.sql.catalog.local.type', 'hadoop') \
    .config("spark.sql.catalog.spark_catalog.type", "hive") \
    .getOrCreate()


team_names=spark.sql('select distinct team_id from local.db.gamelogs').collect()

spark.sql('DROP TABLE IF EXISTS local.db.dim_team ')
nba_teams_schema = StructType([
    StructField("TEAM_ID", IntegerType(), True),
    StructField("ABBREVIATION", StringType(), True),
    StructField("NICKNAME", StringType(), True),
    StructField("YEARFOUNDED", IntegerType(), True),
    StructField("CITY", StringType(), True),
    StructField("ARENA", StringType(), True),
    StructField("ARENACAPACITY", StringType(), True),
    StructField("OWNER", StringType(), True),
    StructField("GENERALMANAGER", StringType(), True),
    StructField("HEADCOACH", StringType(), True),
    StructField("DLEAGUEAFFILIATION", StringType(), True)
])


for i in team_names:
    pdf=teamdetails.TeamDetails(team_id=i['team_id']).get_data_frames()[0]
    print(pdf)
    sdf=spark.createDataFrame(pdf,schema=nba_teams_schema)
   
    if spark.catalog.tableExists('local.db.dim_team'):
         sdf.writeTo('local.db.dim_team').append()
    else:
          sdf.writeTo('local.db.dim_team').createOrReplace()




