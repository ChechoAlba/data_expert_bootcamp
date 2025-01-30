from pyspark.sql import SparkSession





def do_monthly_user_site_visits_transformation(spark, dataframe, ds):
    query = f"""
    SELECT
           month_start,
           SUM(COALESCE(unique_visitors[0], 0)) as num_visits_first_day,
           SUM(COALESCE(unique_visitors[1], 0)) AS num_visits_second_day,
           SUM(COALESCE(unique_visitors[2], 0)) as num_visits_third_day
    FROM monthly_user_site_visits
    WHERE date_partition = '{ds}'
    GROUP BY month_start
    """
    dataframe.createOrReplaceTempView("monthly_user_site_visits")
    return spark.sql(query)


def main():
    ds = '2023-01-01'
    spark = SparkSession.builder \
      .master("local") \
      .appName("players_scd") \
      .getOrCreate()
    output_df = do_monthly_user_site_visits_transformation(spark, spark.table("monthly_user_site_visits"), ds)
    output_df.write.mode("overwrite").insertInto("monthly_user_site_visits_agg")