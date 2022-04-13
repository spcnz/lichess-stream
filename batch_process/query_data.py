#!/usr/bin/python

import os
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.types import *
from pyspark.sql.window import Window



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

blitz_games = spark.read.format("csv").option("header", "true").load(HDFS_NAMENODE + "/transformed/blitz_games.csv")
bullet_games = spark.read.format("csv").option("header", "true").load(HDFS_NAMENODE + "/transformed/bullet_games.csv")
classical_games = spark.read.format("csv").option("header", "true").load(HDFS_NAMENODE + "/transformed/classical_games.csv")
openings = spark.read.format("csv").option("header", "true").load(HDFS_NAMENODE + "/transformed/openings.csv")


#
# ## ====== Success rate by opening kind and game type
# blitz_openings_white_succes_rate = blitz_games.filter(blitz_games.Result == "1-0")\
#     .groupBy("ECO").count().withColumnRenamed("count", "white_wins")
# blitz_openings_black_succes_rate = blitz_games.filter(blitz_games.Result == "0-1")\
#     .groupBy("ECO").count().withColumnRenamed("count", "black_wins")
# blitz_openings_draw = blitz_games.filter(blitz_games.Result == "1/2-1/2")\
#     .groupBy("ECO").count().withColumnRenamed("count", "draw_count")
#
# openings_played_blitz = blitz_games.groupBy("ECO").count().withColumnRenamed("count", "played_times")
# openings_played_blitz = openings_played_blitz\
#     .join(blitz_openings_black_succes_rate, ["ECO"])\
#     .join(blitz_openings_draw, ["ECO"])\
#     .join(blitz_openings_white_succes_rate, ["ECO"])\
#     .join(openings, ["ECO"])\
#     .withColumn("game_type", lit("blitz"))
# openings_played_blitz = openings_played_blitz\
#     .withColumn("white_success_rate", openings_played_blitz.white_wins / openings_played_blitz.played_times)\
#     .withColumn("black_success_rate", openings_played_blitz.black_wins / openings_played_blitz.played_times)\
#     .orderBy("white_success_rate")
# #------------------------------bullet
#
# bullet_openings_white_succes_rate = bullet_games.filter(bullet_games.Result == "1-0")\
#     .groupBy("ECO").count().withColumnRenamed("count", "white_wins")
# bullet_openings_black_succes_rate = bullet_games.filter(bullet_games.Result == "0-1")\
#     .groupBy("ECO").count().withColumnRenamed("count", "black_wins")
# bullet_openings_draw = bullet_games.filter(bullet_games.Result == "1/2-1/2")\
#     .groupBy("ECO").count().withColumnRenamed("count", "draw_count")
#
# openings_played_bullet = bullet_games.groupBy("ECO").count().withColumnRenamed("count", "played_times")
#
# openings_played_bullet = openings_played_bullet\
#     .join(bullet_openings_black_succes_rate, ["ECO"])\
#     .join(bullet_openings_draw, ["ECO"])\
#     .join(bullet_openings_white_succes_rate, ["ECO"])\
#     .join(openings, ["ECO"])\
#     .withColumn("game_type", lit("bullet"))
# openings_played_bullet = openings_played_bullet\
#     .withColumn("white_success_rate", openings_played_bullet.white_wins / openings_played_bullet.played_times)\
#     .withColumn("black_success_rate", openings_played_bullet.black_wins / openings_played_bullet.played_times)\
#     .orderBy("white_success_rate")
#
# #------------------------------classical
#
# classical_openings_white_succes_rate = classical_games.filter(classical_games.Result == "1-0")\
#     .groupBy("ECO").count().withColumnRenamed("count", "white_wins")
# classical_openings_black_succes_rate = classical_games.filter(classical_games.Result == "0-1")\
#     .groupBy("ECO").count().withColumnRenamed("count", "black_wins")
# classical_openings_draw = classical_games.filter(classical_games.Result == "1/2-1/2")\
#     .groupBy("ECO").count().withColumnRenamed("count", "draw_count")
#
# openings_played_classical = classical_games.groupBy("ECO").count().withColumnRenamed("count", "played_times")
#
# openings_played_classical = openings_played_classical\
#     .join(classical_openings_black_succes_rate, ["ECO"])\
#     .join(classical_openings_draw, ["ECO"])\
#     .join(classical_openings_white_succes_rate, ["ECO"])\
#     .join(openings, ["ECO"])\
#     .withColumn("game_type", lit("classical"))
# openings_played_classical = openings_played_classical\
#     .withColumn("white_success_rate", openings_played_classical.white_wins / openings_played_classical.played_times)\
#     .withColumn("black_success_rate", openings_played_classical.black_wins / openings_played_classical.played_times)\
#     .orderBy("white_success_rate")
#
# openings_played_blitz.show()
# openings_played_bullet.show()
# openings_played_classical.show()

#================= Openings by popularity
window = Window.partitionBy(["DayPart"]).orderBy("ECO")
day_part_games_played_blitz = blitz_games \
    .withColumn("times_played", count('ECO').over(window)) \
    .withColumn("row", row_number().over(window)) \
    .filter(col("row") == 1)
day_part_games_played_blitz.show()
day_part_games_played_bullet = bullet_games \
    .withColumn("times_played", count('ECO').over(window)) \
    .withColumn("row", row_number().over(window)) \
    .filter(col("row") == 1)

day_part_games_played = day_part_games_played_blitz.select("DayPart", "times_played")\
    .join(day_part_games_played_bullet.select("DayPart", "times_played"), ["DayPart"])
day_part_games_played.show()

day_part_games_played.withColumn("total_played", day_part_games_played.times_played + day_part_games_played.times_played.)
day_part_games_played.show()

# openings_played.show()
# window = Window.partitionBy(["PlayersRank","ECO"])
# blitz_games.withColumn("times_played", count('ECO').over(window)).show()

# blitz_games.show()
# #PlayersRank values: expert, beginner
# window = Window.partitionBy("PlayersRank").orderBy("DayPart")
# blitz_games.withColumn("row",row_number().over(window))\
#     .withColumn("dist", cume_dist().over(window)).show()

#
# df = blitz_games.select("WhiteElo", "ECO", "Result")
# training, test = df.randomSplit(weights=[0.8,0.2], seed=200)
# training = training.withColumn("Result", when(col("Result") == "1-0", '1').otherwise("0"))\
#     .withColumn("WhiteElo", col("WhiteElo").cast(IntegerType()))
#

"""
1. Otvaranja rangirana po popularnosti za obe grupe igraca (iskusni i pocetnici)
2.  Periodi dana kada se igra najvise partija
3.  Succes rate otvaranja po tipu igre i u odnosu na boju igraca? 10 najuspenijih otvaranja za neki tip igre [done]
4. Najpopularniji tipovi igre za pocetnike i iskusnije igrace (10 najpopularnijih npr)
6. 
7.
8.


"""


#
# sparkMongo = SparkSession.builder.appName("currated-application") \
#     .config("spark.mongodb.output.uri", "mongodb://mongodb:27017/chess_data") \
#     .getOrCreate()
# quiet_logs(sparkMongo)
#
# df_read = sparkMongo.read.format("csv").option("header", "true").load(HDFS_NAMENODE + "/transformed/AA.csv")
# df_read.write.format("mongo").mode("overwrite").option("database",
# "chess_data").option("collection", "chess_stats").save()
