import os
from .config import BRONZE_DIR, SILVER_DIR, get_spark
from .processing.transformations import clean_weather_df, add_features
from .processing.aggregations import (
    register_temp_view, daily_aggregates_sql, monthly_aggregates_sql, add_severity_index
)


def main():
    spark = get_spark("Weather-Batch-ETL")

    if not os.path.exists(BRONZE_DIR):
        print(" Bronze folder missing. Run ingestion first.")
        return

    df = spark.read.parquet(BRONZE_DIR)

    cleaned = clean_weather_df(df)
    enriched = add_features(cleaned)
    enriched = add_severity_index(enriched)

    enriched.write.mode("overwrite").parquet(SILVER_DIR)
    print(f" Silver written to {SILVER_DIR}")

    register_temp_view(enriched, "weather")

    print("=== Daily Aggregates ===")
    daily_aggregates_sql(spark).show()

    print("=== Monthly Aggregates ===")
    monthly_aggregates_sql(spark).show()

    spark.stop()


if __name__ == "__main__":
    main()
