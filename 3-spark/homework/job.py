from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark = (SparkSession.builder 
      .master("local") 
      .appName("spark-homework") 
      .getOrCreate())

#- Disabled automatic broadcast join with `spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")`
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

#Read all the csv and loaded a dataframe for each file
matches_df = spark.read.option("header","true").csv("/home/iceberg/data/matches.csv")
match_details_df = spark.read.option("header","true").csv("/home/iceberg/data/match_details.csv")
maps_df = spark.read.option("header","true").csv("/home/iceberg/data/maps.csv")
medals_df = spark.read.option("header","true").csv("/home/iceberg/data/medals.csv")
medals_matches_players_df = spark.read.option("header","true").csv("/home/iceberg/data/medals_matches_players.csv")
#- Explicitly broadcast JOINs `medals` and `maps`
match_maps_df = matches_df.join(F.broadcast(maps_df), on="mapid").take(5)
medals_matches_detailed_df = medals_matches_players_df.join(F.broadcast(medals_df), on="medal_id").take(5)
#- Bucket join `match_details`, `matches`, and `medal_matches_players` on `match_id` with `16` buckets
numBuckets = 16
match_details_df.write.bucketBy(numBuckets, "match_id").saveAsTable("bootcamp.match_details_bucketed")
matches_df.write.bucketBy(numBuckets, "match_id").saveAsTable("bootcamp.matches_bucketed")
medals_matches_players_df.write.bucketBy(numBuckets, "match_id").saveAsTable("bootcamp.medals_match_players_bucketed")

match_details_bucketed_df = spark.table("bootcamp.match_details_bucketed")
matches_bucketed_df = spark.table("bootcamp.matches_bucketed")
medals_matches_players_bucketed_df = spark.table("bootcamp.medals_match_players_bucketed")
agg_df = match_details_bucketed_df.join(matches_bucketed_df, on="match_id").join(medals_matches_players_bucketed_df, on="match_id")
agg_df.explain()
#   - Aggregate the joined data frame to figure out questions like:
#     - Which player averages the most kills per game?
df_agg_1 = (agg_df.groupBy("match_details_bucketed.player_gamertag")
            .agg(F.sum("player_total_kills").alias("player_total_kills"),
                 F.count("match_id").alias("player_total_games"))
                 .withColumn("avg_kills_per_game", F.col("player_total_kills") / F.col ("player_total_games"))
                 )
df_agg_1.write.mode("overwrite").saveAsTable("bootcamp.players_aggregated_staging")
#     - Which playlist gets played the most?
df_agg_2 = agg_df.groupBy("playlist_id").agg(F.count("match_id").alias("playlist_total_games"))
df_agg_2.write.mode("overwrite").saveAsTable("bootcamp.playlist_aggregated_staging")
#     - Which map gets played the most?
df_agg_3 = agg_df.groupBy("mapid").agg(F.count("match_id").alias("map_total_games"))
df_agg_3.write.mode("overwrite").saveAsTable("bootcamp.maps_aggregated_staging")
#     - Which map do players get the most Killing Spree medals on?
df_agg_4 = (
    agg_df.join(F.broadcast(medals_df), on="medal_id").filter(F.col("classification") == "KillingSpree")
        .select(F.col("match_id"),F.col("medal_id"),F.col("classification"),F.col("match_details_bucketed.player_gamertag"),F.col("count"),F.col("mapid"))
        .groupBy(F.col("match_details_bucketed.player_gamertag"),F.col("mapid")).agg(F.sum("count").alias("total_medals"))
)
df_agg_4.write.mode("overwrite").saveAsTable("bootcamp.medals_aggregated_staging")
#- With the aggregated data set
#- Try different `.sortWithinPartitions` to see which has the smallest data size (hint: playlists and maps are both very low cardinality)
sorted = agg_df.select(F.col("match_details_bucketed.player_gamertag").alias("mdb_player_gamertag")).repartition(10, F.col("playlist_id")).sortWithinPartitions(F.col("playlist_id"))
sortedTwo = agg_df.select(F.col("match_details_bucketed.player_gamertag").alias("mdb_player_gamertag")).repartition(10, F.col("playlist_id")).sortWithinPartitions(F.col("mapid"))

sorted.write.mode("overwrite").saveAsTable("bootcamp.sorted")
sortedTwo.write.mode("overwrite").saveAsTable("bootcamp.sortedTwo")

query = """

SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'sorted' 
FROM demo.bootcamp.sorted.files

UNION ALL
SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'sortedTwo' 
FROM demo.bootcamp.sortedTwo.files

"""
spark.sql(query)

