from pyspark.sql import SparkSession
from pyspark.sql.functions import col,count

def read_dataframe(spark, path):
    df = (spark.read.option("header","true").csv(path)
            .select(col("match_id"), 
                    col("player_gamertag"), 
                    col("player_total_kills").cast('int').alias("player_total_kills")
                    )
            )
    return df


def agg_dataframe(df):
    agg_df = (df.groupBy("player_gamertag")
              .agg(sum("player_total_kills").alias("player_total_kills"),
                   count("match_id").alias("player_total_games")
                   )
                .withColumn("avg_kills_per_game", col("player_total_kills") / col ("player_total_games"))
                )
    return agg_df


def main():
    spark = SparkSession.builder \
      .master("local") \
      .appName("players") \
      .getOrCreate()
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    match_details_df = read_dataframe(spark, "/home/iceberg/data/match_details.csv")
    output_df = agg_dataframe(match_details_df)
    output_df.write.mode("overwrite").saveAsTable("bootcamp.players_aggregated_staging")


