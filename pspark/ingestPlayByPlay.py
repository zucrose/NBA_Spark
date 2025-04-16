import time

from pyspark.sql import SparkSession
from nba_api.stats.endpoints import playbyplayv3
#.master('spark://spark:7077') \
spark = SparkSession.builder \
    .appName("NBAPlayByPlay")\
    .config('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1') \
    .config("spark.sql.extensions", 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions') \
    .config("spark.sql.catalog.hc", "org.apache.iceberg.spark.SparkSessionCatalog") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config('spark.sql.catalog.local.warehouse', '$PWD/warehouse') \
    .config('spark.sql.defaultCatalog', 'local') \
    .config('spark.sql.catalog.local.type', 'hadoop') \
    .config("spark.sql.catalog.hc.type", "hive") \
    .getOrCreate()

     #.drop('`_flags`', axis=1, errors='ignore'))
def rddfunc(row):
    flg=1
    while flg==1:
        try:
            xpdf = playbyplayv3.PlayByPlayV3(game_id=row[0]).get_data_frames()[0]

            #print(row[0])
            flg = 0
            #print(flags)
            return xpdf
        except Exception as e:
            flg = 1
            print(e)
            time.sleep(30)






def ingestplaybyplay():

    df=spark.sql("Select GAME_ID from local.db.GameLogs where PLAYOFFS='1' ")
    sdf=spark.createDataFrame(df.take(100))
    playbyplayRDD =sdf.rdd.map(rddfunc)

    ls=playbyplayRDD.collect()
    spark.sql('DROP TABLE IF EXISTS local.db.PlayByPlay ')
    for i in ls:
        trial = spark.createDataFrame(i)
        if spark.catalog.tableExists('local.db.PlayByPlay'):
            trial.writeTo('local.db.PlayByPlay').append()
        else:
            trial.writeTo('local.db.PlayByPlay').createOrReplace()

    try :
        print('hello')
        #playbyplayDF=playbyplayRDD.toDF()
        #playbyplayDF.show()
    except Exception as e:
      print(e)
    #playbyplayDF.show()

#ingestplaybyplay()

def func():
    print(playbyplayv3.PlayByPlayV3(game_id='0041700315').get_data_frames()[0])

def check():
    df = spark.sql('Select count(*),gameId from local.db.PlayByPlay group by gameId')
    df.show()
    df2 = spark.sql('Select count(*) from local.db.PlayByPlay ')
    df2.show()
#ingestplaybyplay()
check()
