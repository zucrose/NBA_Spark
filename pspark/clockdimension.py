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

game_clock_schema = StructType([
    StructField("clock", StringType(), True),
    StructField("period", IntegerType(), True),
    StructField("minute", IntegerType(), True),
    StructField("second", IntegerType(), True),
    StructField("millisecond", IntegerType(), True)

    ])

data=[]

for period in range(1,10):
    maxMin=13 if period<5 else 6
    for minute in range(0,maxMin):
        minuteString=str(minute)
        if minute<10:
            minuteString='0'+str(minute)
        for second in range(0,60):
            secondString = str(second)
            if minute < 10:
                secondString = '0' + str(second)
            for milli in range(0,10):
                milliString=str(milli)+'0'
                key='PT'+minuteString+'M'+secondString+'.'+milliString+'S'
                data.append((key,period,minute,second,milli))



df=spark.createDataFrame(data,game_clock_schema)
df.show()
df.writeTo('local.db.dim_clock').createOrReplace()
spark.sql('select * from local.db.dim_clock').show()

