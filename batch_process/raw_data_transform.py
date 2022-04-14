#!/usr/bin/python

import os
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import *
from pyspark.sql.functions import trim, col, lower, when, split, expr, to_timestamp, date_format, lit


def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

spark = SparkSession \
    .builder \
    .appName("Chess data") \
    .getOrCreate()
quiet_logs(spark)

HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]

df = spark.read.format("csv").option("header", "true").load(HDFS_NAMENODE + "/raw/chess_games.csv")
#Total count : 6256184

# Removed : 4668 rows with null values
df = df.na.drop()

#Lower and trim all string columns, cast value to timestamp type
df = df.withColumn("Event", lower(trim(col("Event"))))\
    .withColumn('Opening',  lower(trim(col("Opening"))))\
    .withColumn('Termination',  lower(trim(col("Termination"))))\


#Remove rows with Correspondence value for Evenet column. Removed : 22211 rows
df = df.filter(df.Event != "correspondence")
#
# #[Row(Event='bullet tournament'), Row(Event='classical tournament'), Row(Event='blitz'), Row(Event='blitz tournament'), Row(Event='classical'), Row(Event='bullet')]
# # 3 distinct values
# #Blitz tournament -> blitz...
# df = df.withColumn("Event", when(col("Event").contains("tournament"), split(col("Event"), " ").getItem(0))
#       .otherwise(df.Event))
#
# df = df.withColumn("PlayersRank", when((col("WhiteElo") >= 1800) & (col("BlackElo") >= 1800), "expert").otherwise("beginner"))
# df = df.withColumn("GameID", expr("uuid()"))
#
# #Define time parts
# df = df.withColumn("DayPart", when((to_timestamp(col("UTCTime"), 'H:mm:ss')  >= to_timestamp(lit('21:00:00'), 'H:mm:ss')), 'night')\
#                    .when((to_timestamp(col("UTCTime"), 'H:mm:ss') >= to_timestamp(lit('12:00:00'), 'H:mm:ss'))
#                         & (to_timestamp(col("UTCTime"), 'H:mm:ss')  < to_timestamp(lit('21:00:00'), 'H:mm:ss')), 'afternoon') \
#                    .when((to_timestamp(col("UTCTime"), 'H:mm:ss')  >= to_timestamp(lit('06:00:00'), 'H:mm:ss'))
#                          & (to_timestamp(col("UTCTime"), 'H:mm:ss')  < to_timestamp(lit('12:00:00'), 'H:mm:ss')), 'morning') \
#                    .when((to_timestamp(col("UTCTime"), 'H:mm:ss') >= to_timestamp(lit('00:00:00'), 'H:mm:ss'))
#                          & (to_timestamp(col("UTCTime"), 'H:mm:ss')  < to_timestamp(lit('06:00:00'), 'H:mm:ss')), 'night') \
#                    .otherwise("None"))
#
#
# #============================================ Dataframes ready to be written to hdfs
# # 493 distinct ECO but more distinct Opening names
# openings_df = df.select(col("ECO"), col("Opening")).distinct()
# openings_df = openings_df.dropDuplicates(['ECO'])
# # store game moves in different data_frame
# game_moves = df.select(col("GameID"), col("AN"))
#
# blitz_games= df.filter(df.Event == "blitz").drop("Opening").drop("AN")
# bullet_games= df.filter(df.Event == "bullet").drop("Opening").drop("AN")
# classical_games= df.filter(df.Event == "classical").drop("Opening").drop("AN")

# white_players = df.select("WhiteElo", "White").withColumnRenamed("WhiteElo", "ELO").distinct().withColumnRenamed("White", "Name")
# black_players = df.select("BlackElo", "Black").withColumnRenamed("BlackElo", "ELO").distinct().withColumnRenamed("Black", "Name")
# players = white_players.union(black_players)

print("====== WRITING TO HDFS..")

openings_df.write.format("csv").mode('overwrite').option('header','true').save(HDFS_NAMENODE + "/transformed/openings.csv")
game_moves.write.format("csv").mode('overwrite').option('header','true').save(HDFS_NAMENODE + "/transformed/game_moves.csv")
blitz_games.write.format("csv").mode('overwrite').option('header','true').save(HDFS_NAMENODE + "/transformed/blitz_games.csv")
bullet_games.write.format("csv").mode('overwrite').option('header','true').save(HDFS_NAMENODE + "/transformed/bullet_games.csv")
classical_games.write.format("csv").mode('overwrite').option('header','true').save(HDFS_NAMENODE + "/transformed/classical_games.csv")
players.write.format("csv").mode('overwrite').option('header','true').save(HDFS_NAMENODE + "/transformed/players.csv")

print("====== WRITING DONE")
