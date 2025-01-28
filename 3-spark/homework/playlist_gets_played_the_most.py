from pyspark.sql import SparkSession
from pyspark.sql.functions import count

def read_dataframe(spark, path):
    return spark.read.option("header","true").csv(path)


def agg_dataframe(df):
    agg_df = df.groupBy("playlist_id").agg(count("match_id").alias("playlist_total_games"))
    return agg_df


def main():
    spark = SparkSession.builder \
      .master("local") \
      .appName("playlists") \
      .getOrCreate()
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    matches_df = read_dataframe(spark, "/home/iceberg/data/matches.csv")
    output_df = agg_dataframe(matches_df)
    output_df.write.mode("overwrite").saveAsTable("bootcamp.playlist_aggregated_staging")


