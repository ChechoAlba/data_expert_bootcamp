from chispa.dataframe_comparer import *

from ..jobs.monthly_user_site_visits_job import do_monthly_user_site_visits_transformation
from collections import namedtuple

MonthlySiteVisit = namedtuple("MonthlySiteVisit",  "month_start unique_visitors date_partition")
MonthlySitevisitsAgg = namedtuple("MonthlySitevisitsAgg",  "month_start num_visits_first_day num_visits_second_day num_visits_third_day")


def test_monthly_site_visits(spark):
    ds = "2023-03-01"
    new_month_start = "2023-04-01"
    input_data = [
        # Make sure basic case is handled gracefully
        MonthlySiteVisit(
            month_start=ds,
            unique_visitors=[0, 1, 3],
            date_partition=ds
        ),
        MonthlySiteVisit(
            month_start=ds,
            unique_visitors=[1, 2, 3],
            date_partition=ds
        ),
        #  Make sure empty array is handled gracefully
        MonthlySiteVisit(
            month_start=new_month_start,
            unique_visitors=[],
            date_partition=ds
        ),
        # Make sure other partitions get filtered
        MonthlySiteVisit(
            month_start=new_month_start,
            unique_visitors=[],
            date_partition=""
        )
    ]

    source_df = spark.createDataFrame(input_data)
    actual_df = do_monthly_user_site_visits_transformation(spark, source_df, ds)

    expected_values = [
        MonthlySitevisitsAgg(
            month_start=ds,
            num_visits_first_day=1,
            num_visits_second_day=3,
            num_visits_third_day=6
        ),
        MonthlySitevisitsAgg(
            month_start=new_month_start,
            num_visits_first_day=0,
            num_visits_second_day=0,
            num_visits_third_day=0
        )
    ]
    expected_df = spark.createDataFrame(expected_values)
    assert_df_equality(actual_df, expected_df)

