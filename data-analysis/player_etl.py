from pyspark.sql import SparkSession, functions, types
import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+


# add more functions as necessary


def main(all_seasons_inputs, injuries_inputs, output):
    # main logic starts here
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
    injuries_schema = types.StructType([
        types.StructField('Date', types.StringType()),
        types.StructField('Team', types.StringType()),
        types.StructField('Acquired', types.StringType()),
        types.StructField('Relinquished', types.StringType()),
        types.StructField('Notes', types.StringType())
    ])
    all_seasons = (spark.read.format("s3selectCSV")
                   .option("header", "true")
                   .schema(all_seasons_schema)
                   .load(all_seasons_inputs)
                   .withColumn('season', functions.substring(functions.col('season'), 1, 4).cast(types.IntegerType()))
                   .where(functions.col('season') >= 2010)
                   .select('player_name', 'team_abbreviation', 'age', 'player_height', 'player_weight', 'season')
                   .orderBy(functions.col('player_name'), functions.col('season'), functions.col('team_abbreviation')))
    injuries = (spark.read.format("s3selectCSV")
                .option("header", "true")
                .schema(all_seasons_schema)
                .load(injuries_inputs))
    
    all_seasons.coalesce(1).write.csv(output, mode='overwrite')
    return

# commands on AWS ECR
# Player General Information ETL process
# --conf spark.yarn.maxAppAttempts=1
# s3://c732-sfu-rha83-a5/player_etl.py
# s3://c732-sfu-rha83-a5/NBA_Data/all_seasons.csv s3://c732-sfu-rha83-a5/output/NBA/player_info/


if __name__ == '__main__':
    all_seasons_inputs = sys.argv[1]
    injuries_inputs = sys.argv[2]
    output = sys.argv[3]
    spark = SparkSession.builder.appName(
        'Player General Information ETL process').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(all_seasons_inputs, injuries_inputs, output)
