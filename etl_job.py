import os
from pyspark.sql import SparkSession

from modules.transformations import (
    populate_df,
    replace_lat_lng_with_geohash,
)
from env import BASE_DIR


def process_restaurants_df():
    input_folder = os.path.join(BASE_DIR, 'restaurant_csv')
    # Extracting the data
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .load(input_folder)
    )

    df.show()

    # Transforming the data
    df = populate_df(df)

    df = replace_lat_lng_with_geohash(df)
    df.show()
    return df


def process_weather_df():
    input_folder = os.path.join(BASE_DIR, 'weather')

    # Extracting the data
    df = (
        spark.read.format("parquet")
        .load(input_folder)
    )

    df.show()

    # Transforming the data
    df = replace_lat_lng_with_geohash(df)
    df.show()

    return df


def write_data(df):
    output_folder = os.path.join(BASE_DIR, 'output')

    # Loading the data
    df.write \
    .mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .parquet(output_folder)

    return df


if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("Local ETL Job")
        .getOrCreate()
    )

    restaurants_df = process_restaurants_df()
    weather_df = process_weather_df()

    restaurants_df = restaurants_df.dropDuplicates()
    weather_df = weather_df.dropDuplicates()

    joined_df = restaurants_df.join(
        weather_df,
        on="geohash",
        how="left"
    )

    joined_df = joined_df.dropDuplicates()
    joined_df.show()

    df = write_data(joined_df)
    df.show()

    spark.stop()
