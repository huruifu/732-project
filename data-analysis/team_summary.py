import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types


schema = types.StructType([
    types.StructField('GAME_DATE_EST', types.StringType()),
    types.StructField('GAME_ID', types.StringType()),
    types.StructField('GAME_STATUS_TEXT', types.StringType()),
    types.StructField('HOME_TEAM_ID', types.StringType()),
    types.StructField('VISITOR_TEAM_ID', types.StringType()),
    types.StructField('SEASON', types.StringType()),
    types.StructField('TEAM_ID_home', types.StringType()),
    types.StructField('PTS_home', types.StringType()),
    types.StructField('FG_PCT_home', types.StringType()),
    types.StructField('FT_PCT_home', types.StringType()),
    types.StructField('FG3_PCT_home', types.StringType()),
    types.StructField('AST_home', types.StringType()),
    types.StructField('REB_home', types.StringType()),
    types.StructField('TEAM_ID_away', types.StringType()),
    types.StructField('PTS_away', types.StringType()),
    types.StructField('FG_PCT_away', types.StringType()),
    types.StructField('FT_PCT_away', types.StringType()),
    types.StructField('FG3_PCT_away', types.StringType()),
    types.StructField('AST_away', types.StringType()),
    types.StructField('REB_away', types.StringType()),
    types.StructField('HOME_TEAM_WINS', types.StringType())
])


def match_type(date, season):
    val = int(date[5:7] + date[8:10])
    if (season == '2020' and date[0:4] == '2021' and val >= 518) or \
            (season == '2019' and date[0:4] == '2020' and val >= 815) or \
            (season == '2018' and date[0:4] == '2019' and val >= 413) or \
            (season == '2017' and date[0:4] == '2018' and val >= 415) or \
            (season == '2016' and date[0:4] == '2017' and val >= 416) or \
            (season == '2015' and date[0:4] == '2016' and val >= 416) or \
            (season == '2014' and date[0:4] == '2015' and val >= 418) or \
            (season == '2013' and date[0:4] == '2014' and val >= 419) or \
            (season == '2012' and date[0:4] == '2013' and val >= 417) or \
            (season == '2011' and date[0:4] == '2012' and val >= 426) or \
            (season == '2010' and date[0:4] == '2011' and val >= 414):
        return 'playoff'
    else:
        return 'regular'


def main(inputs, output):
    game = spark.read.csv(inputs, header=True, schema=schema)\
                  .select('GAME_DATE_EST', 'HOME_TEAM_ID', 'VISITOR_TEAM_ID', 'SEASON', 'PTS_home',
                          'AST_home', 'REB_home', 'PTS_away', 'AST_away', 'REB_away')
    classify = functions.udf(match_type, types.StringType())
    # d1
    game_stats = game.withColumn('PTS_home', game['PTS_home'].cast(types.IntegerType()))\
                     .withColumn('AST_home', game['AST_home'].cast(types.IntegerType()))\
                     .withColumn('REB_home', game['REB_home'].cast(types.IntegerType()))\
                     .withColumn('PTS_away', game['PTS_away'].cast(types.IntegerType()))\
                     .withColumn('REB_away', game['REB_away'].cast(types.IntegerType()))\
                     .withColumn('AST_away', game['AST_away'].cast(types.IntegerType()))\
                     .withColumn('type', classify('GAME_DATE_EST', 'SEASON'))
    # Regular
    regular_game_stats = game_stats.where(game_stats.type == 'regular')

    # # PlayOff
    # playoff_game_stats = game_stats.where(game_stats.type == 'playoff')

    regular_team_home = regular_game_stats.groupBy('HOME_TEAM_ID', 'SEASON')\
                                          .agg(functions.avg('PTS_home').alias("avg_PTS_home"),
                                               functions.avg('REB_home').alias("avg_REB_home"),
                                               functions.avg('AST_home').alias("avg_AST_home"))
    regular_team_away = regular_game_stats.groupBy('VISITOR_TEAM_ID', 'SEASON')\
                                          .agg(functions.avg('PTS_away').alias("avg_PTS_away"),
                                               functions.avg('REB_away').alias("avg_REB_away"),
                                               functions.avg('AST_away').alias("avg_AST_away"))
    regular_team_home = regular_team_home.withColumnRenamed('HOME_TEAM_ID', 'TEAM_ID')
    regular_team_away = regular_team_away.withColumnRenamed('VISITOR_TEAM_ID', 'TEAM_ID')
    regular_team_summary = regular_team_home.join(functions.broadcast(regular_team_away), ['TEAM_ID', 'SEASON'])\
                                            .sort('SEASON', 'TEAM_ID').cache()

    # ---------- print ----------
    min_value = 2010
    max_value = 2020
    # max_value = int(regular_team_summary.agg({"SEASON": "max"}).collect()[0][0])
    # min_value = int(regular_team_summary.agg({"SEASON": "min"}).collect()[0][0])
    for i in range(min_value, max_value + 1):
        temp = regular_team_summary.where(regular_team_summary.SEASON == str(i))\
                                   .select('TEAM_ID', 'avg_PTS_home', 'avg_REB_home', 'avg_AST_home',
                                           'avg_PTS_away', 'avg_REB_away', 'avg_AST_away')\
                                   .coalesce(1)
        temp.write.csv(output + '/REGULAR_SEASON_' + str(i), header=True, mode='overwrite')


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('team summary').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)
