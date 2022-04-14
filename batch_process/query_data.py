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
players = spark.read.format("csv").option("header", "true").load(HDFS_NAMENODE + "/transformed/players.csv")


## ====== Success rate by opening kind and game type
blitz_openings_white_succes_rate = blitz_games.filter(blitz_games.Result == "1-0")\
    .groupBy("ECO").count().withColumnRenamed("count", "white_wins")
blitz_openings_black_succes_rate = blitz_games.filter(blitz_games.Result == "0-1")\
    .groupBy("ECO").count().withColumnRenamed("count", "black_wins")
blitz_openings_draw = blitz_games.filter(blitz_games.Result == "1/2-1/2")\
    .groupBy("ECO").count().withColumnRenamed("count", "draw_count")
blitz_openings_time_forfeit = blitz_games.filter(blitz_games.Termination == "time forfeit")\
    .groupBy("ECO").count().withColumnRenamed("count", "time_forfeit")

openings_played_blitz = blitz_games.groupBy("ECO").count().withColumnRenamed("count", "played_times")
openings_played_blitz = openings_played_blitz\
    .join(blitz_openings_black_succes_rate, ["ECO"])\
    .join(blitz_openings_draw, ["ECO"])\
    .join(blitz_openings_white_succes_rate, ["ECO"]) \
    .join(blitz_openings_time_forfeit, ["ECO"]) \
    .join(openings, ["ECO"])\
    .withColumn("game_type", lit("blitz"))
openings_played_blitz = openings_played_blitz\
    .withColumn("white_success_rate", openings_played_blitz.white_wins / openings_played_blitz.played_times)\
    .withColumn("black_success_rate", openings_played_blitz.black_wins / openings_played_blitz.played_times) \
    .withColumn("time_forfeit_rate", openings_played_blitz.time_forfeit / openings_played_blitz.played_times) \
    .orderBy("white_success_rate")

#------------------------------bullet

bullet_openings_white_succes_rate = bullet_games.filter(bullet_games.Result == "1-0")\
    .groupBy("ECO").count().withColumnRenamed("count", "white_wins")
bullet_openings_black_succes_rate = bullet_games.filter(bullet_games.Result == "0-1")\
    .groupBy("ECO").count().withColumnRenamed("count", "black_wins")
bullet_openings_draw = bullet_games.filter(bullet_games.Result == "1/2-1/2")\
    .groupBy("ECO").count().withColumnRenamed("count", "draw_count")
bullet_openings_time_forfeit = bullet_games.filter(bullet_games.Termination == "time forfeit")\
    .groupBy("ECO").count().withColumnRenamed("count", "time_forfeit")

openings_played_bullet = bullet_games.groupBy("ECO").count().withColumnRenamed("count", "played_times")

openings_played_bullet = openings_played_bullet\
    .join(bullet_openings_black_succes_rate, ["ECO"])\
    .join(bullet_openings_draw, ["ECO"])\
    .join(bullet_openings_white_succes_rate, ["ECO"]) \
    .join(bullet_openings_time_forfeit, ["ECO"]) \
    .join(openings, ["ECO"])\
    .withColumn("game_type", lit("bullet"))
openings_played_bullet = openings_played_bullet\
    .withColumn("white_success_rate", openings_played_bullet.white_wins / openings_played_bullet.played_times)\
    .withColumn("black_success_rate", openings_played_bullet.black_wins / openings_played_bullet.played_times) \
    .withColumn("time_forfeit_rate", openings_played_bullet.time_forfeit / openings_played_bullet.played_times) \
    .orderBy("white_success_rate")


#------------------------------classical

classical_openings_white_succes_rate = classical_games.filter(classical_games.Result == "1-0")\
    .groupBy("ECO").count().withColumnRenamed("count", "white_wins")
classical_openings_black_succes_rate = classical_games.filter(classical_games.Result == "0-1")\
    .groupBy("ECO").count().withColumnRenamed("count", "black_wins")
classical_openings_draw = classical_games.filter(classical_games.Result == "1/2-1/2")\
    .groupBy("ECO").count().withColumnRenamed("count", "draw_count")
classical_openings_time_forfeit = classical_games.filter(classical_games.Termination == "time forfeit")\
    .groupBy("ECO").count().withColumnRenamed("count", "time_forfeit")

openings_played_classical = classical_games.groupBy("ECO").count().withColumnRenamed("count", "played_times")

openings_played_classical = openings_played_classical\
    .join(classical_openings_black_succes_rate, ["ECO"])\
    .join(classical_openings_draw, ["ECO"])\
    .join(classical_openings_white_succes_rate, ["ECO"]) \
    .join(classical_openings_time_forfeit, ["ECO"]) \
    .join(openings, ["ECO"])\
    .withColumn("game_type", lit("classical"))
openings_played_classical = openings_played_classical\
    .withColumn("white_success_rate", openings_played_classical.white_wins / openings_played_classical.played_times)\
    .withColumn("black_success_rate", openings_played_classical.black_wins / openings_played_classical.played_times) \
    .withColumn("time_forfeit_rate", openings_played_classical.time_forfeit / openings_played_classical.played_times) \
    .orderBy("white_success_rate")

openings_played_classical.printSchema()
#================= Part of day by popularity
window = Window.partitionBy(["DayPart"]).orderBy("ECO")
day_part_games_played_blitz = blitz_games \
    .withColumn("times_played_blitz", count('ECO').over(window)) \
    .withColumn("row", row_number().over(window)) \
    .filter(col("row") == 1)
day_part_games_played_bullet = bullet_games \
    .withColumn("times_played_bullet", count('ECO').over(window)) \
    .withColumn("row", row_number().over(window)) \
    .filter(col("row") == 1)
day_part_games_played_classical = classical_games \
    .withColumn("times_played_classical", count('ECO').over(window)) \
    .withColumn("row", row_number().over(window)) \
    .filter(col("row") == 1)

day_part_games_played = day_part_games_played_blitz.select("DayPart", "times_played_blitz")\
    .join(day_part_games_played_bullet.select("DayPart", "times_played_bullet"), ["DayPart"]) \
    .join(day_part_games_played_classical.select("DayPart", "times_played_classical"), ["DayPart"])


day_part_games_played = day_part_games_played.withColumn("total_played", day_part_games_played.times_played_blitz + day_part_games_played.times_played_bullet + day_part_games_played.times_played_classical)

#================= Opening by popularity for player type

openings_played_global = openings_played_blitz.withColumnRenamed("played_times", "played_blitz").select("ECO", "played_blitz")\
    .join(openings_played_classical.withColumnRenamed("played_times", "played_cls").select("ECO", "played_cls"), ["ECO"]) \
    .join(openings_played_bullet.withColumnRenamed("played_times", "played_bullet").select("ECO", "played_bullet"), ["ECO"])

openings_played_global = openings_played_global.withColumn("total_played", openings_played_global.played_blitz + openings_played_global.played_cls + openings_played_global.played_bullet)
openings_played_global = openings_played_global.withColumn("total_played", col("total_played").cast(IntegerType()))

all_games = blitz_games.union(bullet_games).union(classical_games)

all_games = all_games.join(openings_played_global.select("ECO","total_played"), "ECO")

ranked_openings = all_games.select("ECO", "PlayersRank", "total_played").distinct()
window = Window.partitionBy(["PlayersRank"]).orderBy("total_played")
ranked_openings = ranked_openings.withColumn("rank", rank().over(window))
ranked_openings = ranked_openings.join(openings, ["ECO"])
ranked_openings.show()

#================= For each player
white_players = players.join(blitz_games.withColumnRenamed("WhiteName", "Name"), ["Name"])
white_players = white_players.filter(white_players.Result == "1-0").groupBy("Name").count().withColumnRenamed("count", "won_games")
white_players = white_players.filter(white_players.Result == "0-1").groupBy("Name").count().withColumnRenamed("count", "lost_games")

white_players.show()

print("====== WRITING TO HDFS..")
#
day_part_games_played.write.format("csv").mode('overwrite').option('header','true').save(HDFS_NAMENODE + "/curated/day_part_games.csv")
openings_played_blitz.write.format("csv").mode('overwrite').option('header','true').save(HDFS_NAMENODE + "/curated/openings_played_blitz.csv")
openings_played_bullet.write.format("csv").mode('overwrite').option('header','true').save(HDFS_NAMENODE + "/curated/openings_played_bullet.csv")
openings_played_classical.write.format("csv").mode('overwrite').option('header','true').save(HDFS_NAMENODE + "/curated/openings_played_classical.csv")
ranked_openings.write.format("csv").mode('overwrite').option('header','true').save(HDFS_NAMENODE + "/curated/ranked_openings.csv")

print("====== WRITING DONE")




"""
1. Koja otvaranja su najuspesnija za belog i crnog igraca u zavisnosti od tipa igre? Prikaz : 10 najboljih za belog u Blitz i 10 najgorih za belog u Blitz 
2. Periodi dana kada se igra najvise partija (po tipu i ukupno)
3. Otvaranja rangirana po popularnosti za obe grupe igraca (iskusni i pocetnici)
4. Otvaranja rangirana po riziku isteka vremena za svaki tip igre
5. Odnos pobeda belog i crnog za Bullet (da li utice vreme?)
6. 10 najboljih igraca za Blitz tip igre
7. Raspodela ELO bodova 



"""
