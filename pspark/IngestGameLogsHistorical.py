from nba_api.stats.endpoints import teamgamelogs
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col,lit

#.master('spark://spark:7077') \
spark = SparkSession.builder \
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
    spark.sql('DROP TABLE IF EXISTS local.db.GameLogs')
    #spark.sql('CREATE TABLE IF NOT EXISTS local.db.GameLogs')
    for i in range(2000,2025):
        nextyear=(i + 1) % 100
        if nextyear<10:
            nextyear='0'+str(nextyear)
        season=f'{i}-{nextyear}'
        gameLogs = teamgamelogs.TeamGameLogs(season_nullable=season)
        playoffLogs=teamgamelogs.TeamGameLogs(season_nullable=season,season_type_nullable='Playoffs')
        panda_df = gameLogs.get_data_frames()[0]
        playoff_pdf=playoffLogs.get_data_frames()[0]
        if panda_df is None:
            print(season)
            continue
        #print(panda_df)

        try:
            if panda_df.AVAILABLE_FLAG.dtype == object :
                panda_df.drop('AVAILABLE_FLAG',axis=1)
                panda_df['AVAILABLE_FLAG']=0

            if playoff_pdf.AVAILABLE_FLAG.dtype == object:
                playoff_pdf.drop('AVAILABLE_FLAG', axis=1)
                playoff_pdf['AVAILABLE_FLAG'] = 0

            print(season)
            spark_df = spark.createDataFrame(panda_df)
            playoff_sdf=spark.createDataFrame(playoff_pdf)

            spark_df=spark_df.withColumn('AVAILABLE_FLAG',spark_df['AVAILABLE_FLAG'].cast(IntegerType()))\
            .withColumns({'REGULAR_SEASON':lit('1'),'PLAYOFFS':lit('0')})
            playoff_sdf = playoff_sdf.withColumn('AVAILABLE_FLAG', playoff_sdf['AVAILABLE_FLAG'].cast(IntegerType())) \
                .withColumns({'REGULAR_SEASON': lit('0'), 'PLAYOFFS': lit('1')})

            if spark.catalog.tableExists("local.db.GameLogs"):
                spark_df.writeTo("local.db.GameLogs").append()
            else:
                spark_df.writeTo("local.db.GameLogs").createOrReplace()
            playoff_sdf.writeTo("local.db.GameLogs").append()


        except Exception as e:
            print(f'exception:{str(e)}')
        #spark.sql('Select * from local.db.GameLogs').show()
        #if not spark.catalog.tableExists("local.db.GameLogs") :
            #spark.sql('Select count(*) from local.db.GameLogs').show()



def check():
    #spark.sql('DROP TABLE  IF EXISTS local.db.GameLogs PURGE')
    spark.sql('Select SEASON_YEAR,PLAYOFFS,count(*) from local.db.GameLogs Group BY SEASON_YEAR,PLAYOFFS').show()
func()
check()