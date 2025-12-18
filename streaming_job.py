from pyspark.sql.functions import (
    col, when, to_timestamp, date_format,
    window, avg, approx_count_distinct
)
from .config import INPUT_DIR, CHECKPOINT_DIR, GOLD_DIR, get_spark
from .schemas import weather_schema


def main():
    spark = get_spark("Weather-Streaming")
    print("=== Streaming Job ===")
    print(f"Watching folder: {INPUT_DIR}")

    stream_df = (
        spark.readStream
        .option("header", "true")
        .schema(weather_schema)
        .csv(INPUT_DIR)
    )

    stream_df = (
        stream_df
        .withColumn("Event_Timestamp", to_timestamp("Formatted Date"))
        .withColumn("Event_Date", date_format("Event_Timestamp", "yyyy-MM-dd"))
        .withColumn("Temperature (C)", col("Temperature (C)").cast("double"))
        .withColumn("Humidity", col("Humidity").cast("double"))
        .withColumn("Wind Speed (km/h)", col("Wind Speed (km/h)").cast("double"))
        .withColumn("Precip Type", when(col("Precip Type").isNull(), "None").otherwise(col("Precip Type")))
        .withColumn("Is_Rainy", when(col("Precip Type").isin("Rain", "Snow"), True).otherwise(False))
    )

    # Extreme events -> console
    extreme_df = stream_df.filter(
        (col("Wind Speed (km/h)") > 50) | (col("Precip Type").isin("Rain", "Snow"))
    )

    extreme_query = (
        extreme_df.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/extreme")
        .start()
    )

    # Daily avg temperature -> parquet (window + append)
    daily_avg_temp = (
        stream_df
        .withWatermark("Event_Timestamp", "1 day")
        .groupBy(window(col("Event_Timestamp"), "1 day").alias("day_window"))
        .agg(avg("Temperature (C)").alias("avg_temp"))
    )

    daily_avg_query = (
        daily_avg_temp.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", f"{GOLD_DIR}/daily_avg_temp")
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/daily_avg_temp")
        .start()
    )

    # Rainy days per city per week -> console
    rainy_df = (
        stream_df
        .filter(col("Is_Rainy") == True)
        .withWatermark("Event_Timestamp", "7 days")
        .groupBy(
            window(col("Event_Timestamp"), "7 days").alias("week_window"),
            col("City")
        )
        .agg(approx_count_distinct("Event_Date").alias("rainy_days"))
    )

    rainy_query = (
        rainy_df.writeStream
        .outputMode("complete")
        .format("console")
        .option("truncate", "false")
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/rainy_days")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
