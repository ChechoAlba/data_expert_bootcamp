from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast

def read_dataframe(spark, path):
    return spark.read.option("header","true").csv(path)


def medals_dataframe(spark, path):
    df = read_dataframe(spark, path)
    return df.filter(col("classification") == "KillingSpree").select(col("medal_id"), col("classification"))


def matches_dataframe(spark, path):
    df = read_dataframe(spark, path)
    return df.select(col("match_id"), col("mapid"))


def agg_dataframe(medals_matches_players_df, medals_df, matches_df):
    agg_df = ( 
        medals_matches_players_df.join(broadcast(medals_df), on="medal_id").join(matches_df, on="match_id")
        .select(col("match_id"),col("medal_id"),col("classification"),col("player_gamertag"),col("count"),col("mapid"))
        .groupBy(col("player_gamertag"),col("mapid")).agg(sum("count").alias("total_medals"))
    )
    return agg_df


def main():
    spark = SparkSession.builder \
      .master("local") \
      .appName("medals") \
      .getOrCreate()
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    medals_df = medals_dataframe(spark, "/home/iceberg/data/medals.csv")
    medals_matches_players_df = read_dataframe(spark, "/home/iceberg/data/medals_matches_players.csv")
    matches_df = matches_dataframe(spark, "/home/iceberg/data/matches.csv")
    output_df = agg_dataframe(medals_matches_players_df, medals_df, matches_df)
    output_df.write.mode("overwrite").saveAsTable("bootcamp.medals_aggregated_staging")


