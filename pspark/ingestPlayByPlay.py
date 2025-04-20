import time

from pyspark.sql import SparkSession
from pyspark.sql import Row
from nba_api.stats.endpoints import playbyplayv3
#.master('spark://spark:7077')\
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
            xpdf = playbyplayv3.PlayByPlayV3(game_id=row[0]).get_dict()
            dictlist=xpdf['game']['actions']
            ls=[]
            for i in dictlist:
                i['gameId']=xpdf['game']['gameId']
                ls.append(Row(**i))

            flg = 0
            return ls
        except Exception as e:
            flg = 1
            print(e)
            time.sleep(45)






def ingestplaybyplay():

    gamelist=spark.sql("Select GAME_ID from local.db.GameLogs order by GAME_ID ").collect()
    spark.sql('DROP TABLE IF EXISTS local.db.PlayByPlay ')
    for i in range(0,len(gamelist),100):
        sublist=gamelist[i:i+100]
        print(i,i+100)
        #print(sublist)
        sdf=spark.createDataFrame(sublist)
        sdf.show()
        playbyplayRDD =sdf.rdd.flatMap(rddfunc)
        ls=playbyplayRDD.collect()
        y=spark.createDataFrame(ls)

        if spark.catalog.tableExists('local.db.PlayByPlay'):
            y.writeTo('local.db.PlayByPlay').append()
        else:
            y.writeTo('local.db.PlayByPlay').createOrReplace()
        check()


def func():
    x=playbyplayv3.PlayByPlayV3(game_id='0041700315').get_dict()
    dictlist = x['game']['actions']
    for i in dictlist:
        i['gameId'] = x['game']['gameId']
    print(dictlist[0])
    #print(x.game)

def check():
    df = spark.sql('Select count(*),gameId from local.db.PlayByPlay group by gameId')
    df.show()
    df2 = spark.sql('Select count(*) from local.db.PlayByPlay ')
    df2.show()

#func()
ingestplaybyplay()
check()
