from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col, broadcast

def read_dataframe(spark, path):
    return spark.read.option("header","true").csv(path)


def agg_dataframe(matches_df, maps_df):
    agg_df = ( matches_df.groupBy("mapid").agg(count("match_id").alias("map_total_games"))
              .join(broadcast(maps_df), on="mapid").select(col("mapid"), col("name"), col("map_total_games"))
    )
    return agg_df


def main():
    spark = SparkSession.builder \
      .master("local") \
      .appName("maps") \
      .getOrCreate()
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    matches_df = read_dataframe(spark, "/home/iceberg/data/matches.csv")
    maps_df = read_dataframe(spark, "/home/iceberg/data/maps.csv")
    output_df = agg_dataframe(matches_df, maps_df)
    output_df.write.mode("overwrite").saveAsTable("bootcamp.maps_aggregated_staging")


