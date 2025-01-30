from chispa.dataframe_comparer import *
from ..jobs.actors_scd_job import do_actor_scd_transformation
from collections import namedtuple
PlayerSeason = namedtuple("ActorSeason", "actor is_active year is_active")
ActorScd = namedtuple("ActorScd", "actor quality_class start_date end_date")


def test_scd_generation(spark):
    source_data = [
        PlayerSeason("Michael Keaton", True, 2001, 'Good'),
        PlayerSeason("Michael Keaton", True, 2002, 'Good'),
        PlayerSeason("Michael Keaton", True, 2003, 'Bad'),
        PlayerSeason("Someone Else", True, 2003, 'Bad')
    ]
    source_df = spark.createDataFrame(source_data)

    actual_df = do_actor_scd_transformation(spark, source_df)
    expected_data = [
        ActorScd("Michael Keaton", 'Good', 2001, 2002),
        ActorScd("Michael Keaton", 'Bad', 2003, 2003),
        ActorScd("Someone Else", 'Bad', 2003, 2003)
    ]
    expected_df = spark.createDataFrame(expected_data)
    assert_df_equality(actual_df, expected_df)