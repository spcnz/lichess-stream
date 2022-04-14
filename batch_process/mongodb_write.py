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

HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]

sparkMongo = SparkSession.builder.appName("currated-application") \
    .config("spark.mongodb.output.uri", "mongodb://mongodb:27017/chess_data") \
    .getOrCreate()
quiet_logs(sparkMongo)

print("====WRITING TO MONGODB")
df_read = sparkMongo.read.format("csv").option("header", "true").load(HDFS_NAMENODE + "/curated/day_part_games.csv")\
            .withColumn("times_played_bullet", col("times_played_bullet").cast(IntegerType()))\
            .withColumn("times_played_blitz", col("times_played_blitz").cast(IntegerType()))\
            .withColumn("times_played_classical", col("times_played_classical").cast(IntegerType())) \
            .withColumn("total_played", col("total_played").cast(IntegerType()))
df_read.write.format("mongo").mode("overwrite").option("database",
"mongo_chess").option("collection", "day_part_games_stats").save()
#
df_read = sparkMongo.read.format("csv").option("header", "true").load(HDFS_NAMENODE + "/curated/openings_played_blitz.csv")\
            .withColumn("white_success_rate", col("white_success_rate").cast(DoubleType())) \
            .withColumn("black_success_rate", col("black_success_rate").cast(DoubleType())) \
            .withColumn("played_times", col("played_times").cast(IntegerType())) \
            .withColumn("draw_count", col("draw_count").cast(IntegerType())) \
            .withColumn("white_wins", col("white_wins").cast(IntegerType())) \
            .withColumn("draw_count", col("draw_count").cast(IntegerType())) \
            .withColumn("black_wins", col("black_wins").cast(IntegerType()))\
            .withColumn("time_forfeit", col("time_forfeit").cast(IntegerType()))
df_read.write.format("mongo").mode("overwrite").option("database",
"mongo_chess").option("collection", "openings_played_blitz").save()
#
df_read = sparkMongo.read.format("csv").option("header", "true").load(HDFS_NAMENODE + "/curated/openings_played_bullet.csv")\
            .withColumn("white_success_rate", col("white_success_rate").cast(DoubleType())) \
            .withColumn("black_success_rate", col("black_success_rate").cast(DoubleType())) \
            .withColumn("played_times", col("played_times").cast(IntegerType())) \
            .withColumn("draw_count", col("draw_count").cast(IntegerType())) \
            .withColumn("white_wins", col("white_wins").cast(IntegerType())) \
            .withColumn("draw_count", col("draw_count").cast(IntegerType())) \
            .withColumn("black_wins", col("black_wins").cast(IntegerType()))\
            .withColumn("time_forfeit", col("time_forfeit").cast(IntegerType()))

df_read.write.format("mongo").mode("overwrite").option("database",
"mongo_chess").option("collection", "openings_played_bullet").save()
#
df_read = sparkMongo.read.format("csv").option("header", "true").load(HDFS_NAMENODE + "/curated/openings_played_classical.csv")\
            .withColumn("white_success_rate", col("white_success_rate").cast(DoubleType())) \
            .withColumn("black_success_rate", col("black_success_rate").cast(DoubleType())) \
            .withColumn("played_times", col("played_times").cast(IntegerType())) \
            .withColumn("draw_count", col("draw_count").cast(IntegerType())) \
            .withColumn("white_wins", col("white_wins").cast(IntegerType())) \
            .withColumn("draw_count", col("draw_count").cast(IntegerType())) \
            .withColumn("black_wins", col("black_wins").cast(IntegerType()))\
            .withColumn("time_forfeit", col("time_forfeit").cast(IntegerType()))
df_read.write.format("mongo").mode("overwrite").option("database",
"mongo_chess").option("collection", "openings_played_classical").save()

df_read = sparkMongo.read.format("csv").option("header", "true").load(HDFS_NAMENODE + "/curated/ranked_openings.csv") \
    .withColumn("rank", col("rank").cast(IntegerType()))\
    .withColumn("total_played", col("total_played").cast(DoubleType()))
df_read.write.format("mongo").mode("overwrite").option("database",
"mongo_chess").option("collection", "ranked_openings").save()
print("====DONE !")