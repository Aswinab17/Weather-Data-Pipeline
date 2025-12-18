from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, to_timestamp, date_format
)

def clean_weather_df(df: DataFrame) -> DataFrame:
    df = df.withColumn("Event_Timestamp", to_timestamp(col("Formatted Date")))
    df = df.withColumn("Event_Date", date_format("Event_Timestamp", "yyyy-MM-dd"))

    df = df.fillna({"Precip Type": "None"})

    numeric_cols = [
        "Temperature (C)",
        "Apparent Temperature (C)",
        "Humidity",
        "Wind Speed (km/h)",
        "Visibility (km)",
        "Pressure (millibars)"
    ]

    for c in numeric_cols:
        if c in df.columns:
            df = df.withColumn(c, col(c).cast("double"))

    return df


def add_features(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        "Feels_Like_Diff",
        col("Temperature (C)") - col("Apparent Temperature (C)")
    )
    df = df.withColumn(
        "Is_Rainy",
        when(col("Precip Type").isin("Rain", "Snow"), True).otherwise(False)
    )
    return df
