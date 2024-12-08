import pytest
from pyspark.errors.exceptions.captured import AnalysisException
from pyspark.sql.functions import col

from modules.transformations import populate_df, replace_lat_lng_with_geohash


def test_null_handling(input_restaurants_df, expected_restaurants_df):
    # input_restaurants_df.show()
    assert input_restaurants_df.filter(
        col("lat").isNull() | col("lng").isNull()
    ).count() == 3

    result_df = populate_df(input_restaurants_df)
    result_df.show()
    assert result_df.filter(
        col("lat").isNull() | col("lng").isNull()
    ).count() == 0

    assert result_df.collect() == expected_restaurants_df.collect()


def test_geohash_handling_restaurants(expected_restaurants_df):
    with pytest.raises(AnalysisException):
        cnt = expected_restaurants_df.filter(col("geohash").isNull()).count()

    result_df = replace_lat_lng_with_geohash(expected_restaurants_df)
    assert result_df.filter(col("geohash").isNull()).count() == 0


def test_geohash_handling_weather(input_weather_df):
    with pytest.raises(AnalysisException):
        cnt = input_weather_df.filter(col("geohash").isNull()).count()

    result_df = replace_lat_lng_with_geohash(input_weather_df)
    assert result_df.filter(col("geohash").isNull()).count() == 0


def test_joining(expected_restaurants_df, input_weather_df):
    restaurants_df = replace_lat_lng_with_geohash(expected_restaurants_df)
    weather_df = replace_lat_lng_with_geohash(input_weather_df)

    restaurants_df = restaurants_df.dropDuplicates()
    weather_df = weather_df.dropDuplicates()

    joined_df = restaurants_df.join(
        weather_df,
        on="geohash",
        how="left"
    )

    joined_df = joined_df.dropDuplicates()
    assert joined_df.count() == 7
