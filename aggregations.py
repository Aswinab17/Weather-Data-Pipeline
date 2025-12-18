from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    avg, max as spark_max, year, month, col
)
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import udf


def register_temp_view(df: DataFrame, view="weather"):
    df.createOrReplaceTempView(view)


def daily_aggregates_sql(spark, view="weather"):
    return spark.sql(f"""
        SELECT
            Event_Date,
            AVG(`Temperature (C)`) AS avg_temp_c,
            MAX(`Wind Speed (km/h)`) AS max_wind
        FROM {view}
        GROUP BY Event_Date
        ORDER BY Event_Date
    """)


def monthly_aggregates_sql(spark, view="weather"):
    return spark.sql(f"""
        SELECT
            YEAR(Event_Timestamp) AS year,
            MONTH(Event_Timestamp) AS month,
            COUNT(CASE WHEN Is_Rainy THEN 1 END) AS rainy_days,
            AVG(Humidity) AS avg_humidity
        FROM {view}
        GROUP BY year, month
        ORDER BY year, month
    """)


def _severity(wind, precip):
    if wind is None: wind = 0
    factor = 1.5 if precip in ("Rain", "Snow") else 1.0
    return wind * factor

weather_severity_udf = udf(_severity, DoubleType())


def add_severity_index(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "Weather_Severity_Index",
        weather_severity_udf(col("Wind Speed (km/h)"), col("Precip Type"))
    )
