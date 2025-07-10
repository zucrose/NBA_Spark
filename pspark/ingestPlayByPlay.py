import time

from pyspark.sql import SparkSession
from pyspark.sql import Row
from nba_api.stats.endpoints import playbyplayv3
from pyspark.sql.functions import count

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
    ls = []
    try:
            xpdf = playbyplayv3.PlayByPlayV3(game_id=row[0]).get_dict()
            dictlist=xpdf['game']['actions']

            #print('I try ')
            for i in dictlist:
                i['gameId']=xpdf['game']['gameId']
                ls.append(Row(**i))

            flg = 0

    except Exception as e:
            flg = 0
            print(e)
            print(row[0])
            #time.sleep(45)
    return ls





def ingestplaybyplay():

    gamelist=spark.sql("Select GAME_ID from local.db.GameLogs group by GAME_ID order by GAME_ID ").collect()
    #spark.sql('DROP TABLE IF EXISTS local.db.PlayByPlay ')
    for i in range(0,len(gamelist),100):
        sublist=gamelist[i:i+100]
        print(i,i+100)
        #print(sublist)
        sdf=spark.createDataFrame(sublist)
        #sdf.show()
        playbyplayRDD =sdf.rdd.flatMap(rddfunc)
        ls=playbyplayRDD.collect()
        y=spark.createDataFrame(ls)

        if spark.catalog.tableExists('local.db.PlayByPlay'):
            y.writeTo('local.db.PlayByPlay').append()
        else:
            y.writeTo('local.db.PlayByPlay').createOrReplace()
        #check()

def recoverPlayByPlay():
    maxgidloaded=spark.sql("Select max(gameId) as mgid from local.db.PlayByPlay").collect()
    print(maxgidloaded[0]['mgid'])

    gamelist = spark.sql(f"Select GAME_ID from local.db.GameLogs where GAME_ID > {maxgidloaded[0]['mgid']} group by GAME_ID order by GAME_ID ").collect()
    # spark.sql('DROP TABLE IF EXISTS local.db.PlayByPlay ')
    print(len(gamelist))
    for i in range(0, len(gamelist), 100):
        sublist = gamelist[i:i + 100]
        print(i, i + 100)
         #print(sublist)
        sdf = spark.createDataFrame(sublist)
        #sdf.show()
        playbyplayRDD = sdf.rdd.flatMap(rddfunc)
        ls = playbyplayRDD.collect()
        y = spark.createDataFrame(ls)

        if spark.catalog.tableExists('local.db.PlayByPlay'):
            y.writeTo('local.db.PlayByPlay').append()
        else:
            y.writeTo('local.db.PlayByPlay').createOrReplace()



def ingestmissing():
    gidsloaded = spark.sql("Select gameId  from local.db.PlayByPlay group by gameId")
    allgids= spark.sql("Select GAME_ID from local.db.GameLogs group by GAME_ID order by GAME_ID ")
    missinggids=allgids.join(gidsloaded,allgids.GAME_ID == gidsloaded.gameId,'leftanti')
    missinggids.show()
    ls=missinggids.rdd.flatMap(rddfunc).collect()
    y = spark.createDataFrame(ls)

    if spark.catalog.tableExists('local.db.PlayByPlay'):
        y.writeTo('local.db.PlayByPlay').append()
    else:
        y.writeTo('local.db.PlayByPlay').createOrReplace()

def func():
    x=playbyplayv3.PlayByPlayV3(game_id='0041700315').get_dict()
    dictlist = x['game']['actions']
    for i in dictlist:
        i['gameId'] = x['game']['gameId']
    print(dictlist[0])

    #print(x.game)

def check():
    df = spark.sql('Select count(*) from local.db.PlayByPlay ')
    df.show()
    #gamelist = spark.sql("Select GAME_ID from local.db.GameLogs group by GAME_ID order by GAME_ID ").collect()
    #sublist=gamelist[0:50]
    #print(sublist)
    #sdf = spark.createDataFrame(sublist)
    #z=sdf.rdd.flatMap(rddfunc)
    #z=z.collect()
    #print(z)
    #gamelist = spark.sql("Select GAME_ID from local.db.GameLogs order by GAME_ID ").collect()
    #print(gamelist[0:50])
    #df2 = spark.sql('Select * from local.db.PlayByPlay ')
    #df2 = df2.distinct()
    #df2.writeTo('local.db.PlayByPlay').createOrReplace()
    #df2.select(count('*')).show()
    #df2.select().filter(df2.cnt>1).select(count(df2.cnt)).show()

    #df2.show()
    df3 = spark.sql('Select count(*) as x from local.db.PlayByPlay group by gameId')
    df3.select(count(df3.x)).show()
    #df4 = spark.sql('Select * from local.db.GameLogs order by GAME_ID').show()
    #df4.filter(df4.y>1).select(count(df4.y)).show()
    spark.sql('Select *  from local.db.PlayByPlay ').show()
#func()
#ingestplaybyplay()
#recoverPlayByPlay()
check()
#ingestmissing()
