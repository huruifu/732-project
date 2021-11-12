from pyspark.sql import SparkSession, functions, types
import re
import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+


# add more functions as necessary
@functions.udf(returnType=types.StringType())
def unify_team_name(column):
    column = column.upper()
    if column == "PHO":
        column = "PHX"
    if column == "NOR":
        column = "NOP"
    if column == "SAN":
        column = "SAS"
    if column == "Gol":
        column = "GSW"
    if column == "BRO":
        column = "BKN"
    if column == "TOT":
        column = "TOR"
    if column == "CHH" or column == "CHO":
        column = "CHA"
    return column


def main():
    # main logic starts here
    player_info_before_2017_schema = types.StructType([
        types.StructField('Index', types.StringType()),
        types.StructField('Year', types.IntegerType()),
        types.StructField('Player', types.StringType()),
        types.StructField('Pos', types.StringType()),
        types.StructField('Age', types.IntegerType()),
        types.StructField('Tm', types.StringType())
    ])
    player_info_schema = types.StructType([
        types.StructField('FULL NAME', types.StringType()),
        types.StructField('TEAM', types.StringType()),
        types.StructField('POS', types.StringType())
    ])
    all_seasons_schema = types.StructType([
        types.StructField('index', types.StringType()),
        types.StructField('player_name', types.StringType()),
        types.StructField('team_abbreviation', types.StringType()),
        types.StructField('age', types.DoubleType()),
        types.StructField('player_height', types.DoubleType()),
        types.StructField('player_weight', types.DoubleType()),
        types.StructField('college', types.StringType()),
        types.StructField('country', types.StringType()),
        types.StructField('draft_year', types.StringType()),
        types.StructField('draft_round', types.StringType()),
        types.StructField('draft_number', types.StringType()),
        types.StructField('gp', types.DoubleType()),
        types.StructField('pts', types.DoubleType()),
        types.StructField('reb', types.DoubleType()),
        types.StructField('ast', types.DoubleType()),
        types.StructField('net_rating', types.DoubleType()),
        types.StructField('oreb_pct', types.DoubleType()),
        types.StructField('dreb_pct', types.DoubleType()),
        types.StructField('usg_pct', types.DoubleType()),
        types.StructField('ts_pct', types.DoubleType()),
        types.StructField('ast_pct', types.DoubleType()),
        types.StructField('season', types.StringType()),
    ])
    player_info_before_2017 = (spark.read.format("csv")
                               .option("header", "true")
                               .schema(player_info_before_2017_schema)
                               .load(player_info_before_2017_inputs)
                               .where(functions.col("Year") >= 2010)
                               .withColumnRenamed("Player", "playerName")
                               .withColumnRenamed("Pos", "player_position")
                               .select("playerName", "player_position"))
    player_info_2018 = (spark.read.format("csv")
                        .option("header", "true")
                        .schema(player_info_schema)
                        .load(player_info_2018_inputs)
                        .withColumnRenamed("FULL NAME", "playerName")
                        .withColumnRenamed("POS", "player_position")
                        .select("playerName", "player_position"))
    player_info_2019 = (spark.read.format("csv")
                        .option("header", "true")
                        .schema(player_info_schema)
                        .load(player_info_2019_inputs)
                        .withColumnRenamed("FULL NAME", "playerName")
                        .withColumnRenamed("POS", "player_position")
                        .select("playerName", "player_position"))
    player_info_2020 = (spark.read.format("csv")
                        .option("header", "true")
                        .schema(player_info_schema)
                        .load(player_info_2020_inputs)
                        .withColumnRenamed("FULL NAME", "playerName")
                        .withColumnRenamed("POS", "player_position")
                        .select("playerName", "player_position"))
    player = (spark.read.format("csv")
              .option("header", "true")
            #   .schema(player_schema)
              .load(player_inputs)
              .withColumnRenamed("season", "year")
              .withColumnRenamed("PLAYER_NAME", "playerName")
              .dropDuplicates(["playerName"]))
    player_info = (player_info_before_2017
                   .union(player_info_2018)
                   .union(player_info_2019)
                   .union(player_info_2020)
                   .dropDuplicates(["playerName"])
                   .orderBy("playerName"))
    all_seasons = (spark.read.format("csv")
                   .option("header", "true")
                   .schema(all_seasons_schema)
                   .load(all_seasons_inputs)
                   .withColumn('season', functions.substring(functions.col('season'), 1, 4).cast(types.IntegerType()))
                   .where(functions.col('season') >= 2010)
                   .select('player_name', 'team_abbreviation', 'age', 'player_height', 'player_weight', 'season', 'draft_year', 'draft_round', 'draft_number')
                   .orderBy(functions.col('player_name'), functions.col('season'), functions.col('team_abbreviation')))
    player_info = (all_seasons
                   .join(player_info,
                         functions.regexp_replace(all_seasons["player_name"], r'\.', "") == functions.regexp_replace(player_info["playerName"], r'\.', ""),
                         "left")
                   .select('player_name',  "player_position", 'team_abbreviation', 'age', 'player_height', 'player_weight', 'season', 'draft_year', 'draft_round', 'draft_number')
                   .orderBy(functions.col('player_name'), functions.col('season'), functions.col('team_abbreviation')))
    player_info = (player_info
                   .join(player,
                         functions.regexp_replace(player_info["player_name"], r'\.', "") == functions.regexp_replace(player["playerName"], r'\.', ""),
                         "left")
                   .select('player_name',  "player_position", 'team_abbreviation', 'age', 'player_height', 'player_weight', 'season', 'draft_year', 'draft_round', 'draft_number', "PLAYER_ID")
                   .orderBy(functions.col('player_name'), functions.col('season'), functions.col('team_abbreviation')))
    player_info.cache()
    numRows = player_info.groupBy("player_name").count().count()
    print(numRows) #1397
    player_info.coalesce(1).write.option("header", "true").csv(output, mode='overwrite')
    return


if __name__ == '__main__':
    player_info_before_2017_inputs = sys.argv[1]
    player_info_2018_inputs = sys.argv[2]
    player_info_2019_inputs = sys.argv[3]
    player_info_2020_inputs = sys.argv[4]
    all_seasons_inputs = sys.argv[5]
    player_inputs = sys.argv[6]
    output = sys.argv[7]
    spark = SparkSession.builder.appName(
        'Player Position Join process').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main()
