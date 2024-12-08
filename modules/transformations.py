from pyspark.sql.functions import col, when, udf
from utils import fetch_lat, fetch_lng, get_geohash
from pyspark.sql.types import StringType


def populate_df(df):    
    # Filtering the data
    filtered_df = df.filter(col("lat").isNull() | col("lng").isNull())
    other_df = df.filter(col("lat").isNotNull() & col("lng").isNotNull())

    # Defining UDF
    fetch_lat_udf = udf(fetch_lat, StringType())
    fetch_lng_udf = udf(fetch_lng, StringType())

    transformed_df = filtered_df.withColumn(
        "lat",
        when(col("lat").isNull(), fetch_lat_udf(col("city"))).otherwise(col("lat"))
    ).withColumn(
        "lng",
        when(col("lng").isNull(), fetch_lng_udf(col("city"))).otherwise(col("lng"))
    )

    # transformed_df.show()

    final_df = other_df.union(transformed_df)

    # I've only found one row with null lat or lng,
    # and there exists this same city in the database
    # but the task is to make API request to fill in lat and lng,
    # so I'll use this record to compare our data to API
    target_df = df.filter(col("city") == 'Dillon')

    target_df.show()
    return final_df


def extend_df_with_geohash(df):
    # Defining UDF
    get_geohash_udf = udf(get_geohash, StringType())

    extended_df = df.withColumn(
        "geohash",
        when(col("lng").isNotNull(), get_geohash_udf(col("lat"), col("lng"))).otherwise("")
    )
    return extended_df