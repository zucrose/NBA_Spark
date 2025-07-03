from nba_api.stats.endpoints import commonallplayers,commonplayerinfo
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from rich.jupyter import display

#.master('spark://spark:7077') \
spark = SparkSession.builder \
    .appName("LoadPlayerDim")\
    .config('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1') \
    .config("spark.sql.extensions", 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions') \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config('spark.sql.catalog.local.warehouse', '../$PWD/warehouse') \
    .config('spark.sql.defaultCatalog', 'local') \
    .config('spark.sql.catalog.local.type', 'hadoop') \
    .config("spark.sql.catalog.spark_catalog.type", "hive") \
    .getOrCreate()

nba_player_info_schema = StructType([
    StructField("PERSON_ID", IntegerType(), True),
    StructField("FIRST_NAME", StringType(), True),
    StructField("LAST_NAME", StringType(), True),
    StructField("DISPLAY_FIRST_LAST", StringType(), True),
    StructField("DISPLAY_LAST_COMMA_FIRST", StringType(), True),
    StructField("DISPLAY_FI_LAST", StringType(), True),
    StructField("PLAYER_SLUG", StringType(), True),
    StructField("BIRTHDATE", StringType(), True), # Often provided as a string, e.g., "YYYY-MM-DD"
    StructField("SCHOOL", StringType(), True),
    StructField("COUNTRY", StringType(), True),
    StructField("LAST_AFFILIATION", StringType(), True),
    StructField("HEIGHT", StringType(), True), # Can be 'X-X' format, so StringType
    StructField("WEIGHT", IntegerType(), True), # Typically an integer weight
    StructField("SEASON_EXP", IntegerType(), True), # Number of seasons experience
    StructField("JERSEY", StringType(), True), # Can be null or sometimes "N/A", so StringType
    StructField("POSITION", StringType(), True),
    StructField("ROSTERSTATUS", StringType(), True), # e.g., "Active", "Inactive"
    StructField("TEAM_ID", IntegerType(), True),
    StructField("TEAM_NAME", StringType(), True),
    StructField("TEAM_ABBREVIATION", StringType(), True),
    StructField("TEAM_CODE", StringType(), True),
    StructField("TEAM_CITY", StringType(), True),
    StructField("PLAYERCODE", StringType(), True),
    StructField("FROM_YEAR", IntegerType(), True), # Year player started in league
    StructField("TO_YEAR", IntegerType(), True),   # Year player last played in league
    StructField("DLEAGUE_FLAG", StringType(), True), # Often "Y" or "N", so StringType
    StructField("NBA_FLAG", StringType(), True),     # Often "Y" or "N", so StringType
    StructField("GAMES_PLAYED_FLAG", StringType(), True), # Often "Y" or "N", so StringType
    StructField("DRAFT_YEAR", StringType(), True), # Can be "Undrafted" or year, so StringType
    StructField("DRAFT_ROUND", StringType(), True), # Can be "Undrafted" or round number, so StringType
    StructField("DRAFT_NUMBER", StringType(), True) # Can be "Undrafted" or pick number, so StringType
])

def func(pid):
    try:
         x = commonplayerinfo.CommonPlayerInfo(player_id=pid).get_normalized_dict()
         res=x['CommonPlayerInfo'][0]
         print(pid)
         #res=pid
         return res
    except Exception as e:
        print(pid,e)
        return None



udf_func=udf(func,nba_player_info_schema)



def load_dim_players():
    playersApiRes=commonallplayers.CommonAllPlayers(season='2024-25',league_id='00',is_only_current_season='0')
    playersPDF=playersApiRes.get_data_frames()[0]

    df=spark.createDataFrame(playersPDF)
    playerDF=df.withColumn('Res',udf_func(col("PERSON_ID")))
    playerDF.writeTo('local.db.dim_players').createOrReplace()
    spark.sql('Select * from local.db.dim_players').show()


load_dim_players()

