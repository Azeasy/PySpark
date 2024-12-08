from pyspark.sql import SparkSession

from modules.transformations import (
    populate_df,
    extend_df_with_geohash,
)


def process_restaraunts_df():
    input_folder = "/Users/azeasy/Desktop/EPAM/task_2_app/restaurant_csv"
    # Extracting the data
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .load(input_folder)
    )

    df.show()

    # Transforming the data
    df = populate_df(df)

    df = extend_df_with_geohash(df).drop("lat").drop("lng")
    df.show()
    return df


def process_weather_df():
    input_folder = "/Users/azeasy/Desktop/EPAM/task_2_app/weather"
    # Extracting the data
    df = (
        spark.read.format("parquet")
        .load(input_folder)
    )

    df.show()

    # Transforming the data
    df = extend_df_with_geohash(df).drop("lat").drop("lng")
    df.show()

    return df


def write_data(df):
    output_folder = "/Users/azeasy/Desktop/EPAM/task_2_app/output"
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

    restaraunts_df = process_restaraunts_df()
    weather_df = process_weather_df()

    restaraunts_df = restaraunts_df.dropDuplicates()
    weather_df = weather_df.dropDuplicates()

    joined_df = restaraunts_df.join(
        weather_df,
        on="geohash",
        how="left"
    )

    joined_df = joined_df.dropDuplicates()
    joined_df.show()

    df = write_data(joined_df)
    df.show()

    spark.stop()
