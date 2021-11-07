import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
from pyspark.sql.types import DateType
from pyspark.sql.functions import avg

# add more functions as necessary

def main(inputs, output):
    # main logic starts here

    # input should be a csv
    games = spark.read.option("delimiter", ",").option("header", "true").csv(inputs)
    gmDetail = spark.read.option("delimiter", ",").option("header", "true").csv(inputs)

    games2 = games.withColumn('GAME_DATE', functions.to_date(games["GAME_DATE_EST"]))
    games2 = games2.filter((games2['GAME_DATE'] >= "2010-01-01") & (games2['GAME_DATE'] < "2015-01-01"))

    gamesDate = games2.groupBy('GAME_DATE', 'GAME_ID').count()
    gamesAvg = gamesDate.join(gmDetail, ['GAME_ID'], 'inner')
    gamesAvg = gamesAvg.groupBy(functions.year('GAME_DATE').alias("year"), 'TEAM_ID', 'PLAYER_ID').agg(avg('FG_PCT'), avg('FG3_PCT'), avg('FT_PCT'), avg('PLUS_MINUS'))

    return

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('Season 2020 NBA ETL process').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)