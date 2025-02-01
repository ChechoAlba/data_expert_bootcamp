from pyspark.sql import SparkSession

query = """

with with_previous as (
	select actor,
		is_active,
		quality_class,
		year,
		LAG("quality_class",1) over (partition by actor order by "year") as previous_quality_class,
		LAG("is_active",1) over (partition by actor order by "year") as previous_is_active
	from actors
), with_indicators as (
	select *,
	case 
		when quality_class <> previous_quality_class then 1
		when is_active <> previous_is_active then 1
		else 0
	end as change_indicator
	from with_previous
), with_streaks as (
	select *,
	SUM(change_indicator) over (partition by actor order by year) as streak_identifier
	from with_indicators
),
     aggregated AS (
         SELECT
            actor,
            quality_class,
            is_active,
            MIN(year) as start_year,
	        MAX(year) as end_year
         FROM with_streaks
         GROUP BY actor,
            streak_identifier,
            is_active,
            quality_class
     )

     SELECT actor, quality_class, start_date, end_date
     FROM aggregated

"""


def do_actor_scd_transformation(spark, dataframe):
    dataframe.createOrReplaceTempView("actors")
    return spark.sql(query)


def main():
    spark = SparkSession.builder \
      .master("local") \
      .appName("actors_scd") \
      .getOrCreate()
    output_df = do_actor_scd_transformation(spark, spark.table("actors"))
    output_df.write.mode("overwrite").insertInto("actors_scd")

