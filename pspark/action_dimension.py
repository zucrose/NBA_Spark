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
    .config('spark.sql.catalog.local.warehouse', '$PWD/warehouse') \
    .config('spark.sql.defaultCatalog', 'local') \
    .config('spark.sql.catalog.local.type', 'hadoop') \
    .config("spark.sql.catalog.spark_catalog.type", "hive") \
    .getOrCreate()


df=spark.sql('select shotResult,isFieldGoal,actionType,subtype,row_number() over (order by actionType,subtype) as actionkey from local.db.playbyplay  group by actionType,subtype,shotResult,isFieldGoal')
df.writeTo('local.db.dim_action').createOrReplace()

