import sys

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types


# add more functions as necessary

def main(inputs, output):
    df = spark.read.option("header", "true").csv(inputs)
    # drop the column LEAGUE_ID, useless, all 0
    df = df.drop(df['LEAGUE_ID'])
    df_year = df.withColumn('YEAR', functions.substring('SEASON_ID', 2, 5)).drop('SEASON_ID')
    df_year = df_year.filter(df_year['YEAR'] >= 2010)
    # keep the newest date of the team record
    df_newest = df_year.groupBy(df_year['TEAM'], df_year['YEAR']).agg(functions.max(df_year['STANDINGSDATE'])).toDF(
        'TEAM_1', 'YEAR_1', 'DATE_1')
    df_all = df_newest.join(df_year).filter(
        (df_year['TEAM'] == df_newest['TEAM_1']) & (df_year['STANDINGSDATE'] == df_newest['DATE_1']) & (
                    df_year['YEAR'] == df_newest['YEAR_1']))
    df_done = df_all.drop('TEAM_1', 'DATE_1', 'YEAR_1')
    # df_done.show()
    # replace the season_id which contains 1 and 2 before the year to be year
    # df_done = df_done.withColumn('YEAR', functions.substring('SEASON_ID', 2, 5)).drop('SEASON_ID')
    df_done.write.partitionBy('YEAR', 'CONFERENCE').mode("overwrite").csv(output)
    # spark.read.option("header","true").csv("c/file/YEAR=/CONFERENCE=")


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('NBA ETL process process ').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)
