import pytest


@pytest.fixture
def input_restaurants_df(spark):
    input_data = [
        {
            "id": 197568495625, "franchise_id": 10, "franchise_name": "The Golden Spoon", "restaurant_franchise_id": 24784, "country": "US", "city": "Decatur",
            "lat": "34.578",
            "lng": "-87.021",
        },
        {
            "id": 2, "franchise_id": 23, "franchise_name": "The Burger Ming", "restaurant_franchise_id": 69, "country": "US", "city": "Charlotte",
            "lat": None,
            "lng": None,
        },
        {
            "id": 17179869242, "franchise_id": 59, "franchise_name": "Azalea Cafe", "restaurant_franchise_id": 10902, "country": "FR", "city": "Paris",
            "lat": "48.861",
            "lng": None,
        },
        {
            "id": 214748364826, "franchise_id": 27, "franchise_name": "The Corner Cafe", "restaurant_franchise_id": 92040, "country": "US", "city": "Rapid City",
            "lat": None,
            "lng": "-103.250",
        },
    ]
    input_df = spark.createDataFrame(input_data)

    return input_df


@pytest.fixture
def expected_restaurants_df(spark):
    expected_data = [
        {
            "id": 197568495625, "franchise_id": 10, "franchise_name": "The Golden Spoon", "restaurant_franchise_id": 24784, "country": "US", "city": "Decatur",
            "lat": "34.578",
            "lng": "-87.021",
        },
        {
            "id": 2, "franchise_id": 23, "franchise_name": "The Burger Ming", "restaurant_franchise_id": 69, "country": "US", "city": "Charlotte",
            "lat": "35.2272086",
            "lng": "-80.8430827",
        },
        {
            "id": 17179869242, "franchise_id": 59, "franchise_name": "Azalea Cafe", "restaurant_franchise_id": 10902, "country": "FR", "city": "Paris",
            "lat": "48.861",
            "lng": "2.3483915",
        },
        {
            "id": 214748364826, "franchise_id": 27, "franchise_name": "The Corner Cafe", "restaurant_franchise_id": 92040, "country": "US", "city": "Rapid City",
            "lat": "44.0806041",
            "lng": "-103.250",
        },
    ]
    expected_df = spark.createDataFrame(expected_data)

    return expected_df


@pytest.fixture
def geohash_restaurants_df(spark):
    geohash_data = [
        {
            "id": 197568495625, "franchise_id": 10,
            "franchise_name": "The Golden Spoon",
            "restaurant_franchise_id": 24784, "country": "US",
            "city": "Decatur",
            "geohash": "dn4h",
        },
        {
            "id": 2, "franchise_id": 23, "franchise_name": "The Burger Ming",
            "restaurant_franchise_id": 69, "country": "US",
            "city": "Charlotte",
            "geohash": "dnq8",
        },
        {
            "id": 17179869242, "franchise_id": 59,
            "franchise_name": "Azalea Cafe", "restaurant_franchise_id": 10902,
            "country": "FR", "city": "Paris",
            "geohash": "u09t",
        },
        {
            "id": 214748364826, "franchise_id": 27,
            "franchise_name": "The Corner Cafe",
            "restaurant_franchise_id": 92040, "country": "US",
            "city": "Rapid City",
            "geohash": "9xyd",
        },
    ]
    geohash_df = spark.createDataFrame(geohash_data)

    return geohash_df


@pytest.fixture
def input_weather_df(spark):
    data = [
        {
            "lat": "34.478", "lng": "-87.032", "avg_tmpr_f": 80.7, "avg_tmpr_c": 27.1, "wthr_date": "2017-08-29", "year": 2017, "month": 8, "day": 29,
        },
        {
            "lat": "34.478", "lng": "-87.032", "avg_tmpr_f": 81.6, "avg_tmpr_c": 27.6, "wthr_date": "2017-08-30", "year": 2017, "month": 8, "day": 30,
        },
        {
            "lat": "34.478", "lng": "-87.032", "avg_tmpr_f": 81.6, "avg_tmpr_c": 27.6, "wthr_date": "2017-08-30", "year": 2017, "month": 8, "day": 30,
        },
        {
            "lat": "48.861", "lng": "2.368", "avg_tmpr_f": 81.6, "avg_tmpr_c": 27.6, "wthr_date": "2017-08-29", "year": 2017, "month": 8, "day": 29,
        },
        {
            "lat": "48.861", "lng": "2.368", "avg_tmpr_f": 80.9, "avg_tmpr_c": 27.2, "wthr_date": "2017-08-30", "year": 2017, "month": 8, "day": 30,
        },
        {
            "lat": "44.080", "lng": "-103.250", "avg_tmpr_f": 77.8, "avg_tmpr_c": 25.4, "wthr_date": "2017-08-29", "year": 2017, "month": 8, "day": 29,
        },
        {
            "lat": "43.980", "lng": "-103.250", "avg_tmpr_f": 77.8, "avg_tmpr_c": 25.4, "wthr_date": "2017-08-29", "year": 2017, "month": 8, "day": 29,
        },
        {
            "lat": "43.980", "lng": "-103.250", "avg_tmpr_f": 75.2, "avg_tmpr_c": 24.0, "wthr_date": "2017-08-30", "year": 2017, "month": 8, "day": 30,
        },
    ]
    weather_df = spark.createDataFrame(data)

    return weather_df
