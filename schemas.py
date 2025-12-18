from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)

weather_schema = StructType([
    StructField("Formatted Date", StringType(), True),
    StructField("Summary", StringType(), True),
    StructField("Precip Type", StringType(), True),

    StructField("Temperature (C)", DoubleType(), True),
    StructField("Apparent Temperature (C)", DoubleType(), True),
    StructField("Humidity", DoubleType(), True),
    StructField("Wind Speed (km/h)", DoubleType(), True),
    StructField("Wind Bearing (degrees)", IntegerType(), True),
    StructField("Visibility (km)", DoubleType(), True),
    StructField("Loud Cover", IntegerType(), True),
    StructField("Pressure (millibars)", DoubleType(), True),
    StructField("Daily Summary", StringType(), True),

    StructField("City", StringType(), True),
    StructField("Station_Id", StringType(), True),
])
