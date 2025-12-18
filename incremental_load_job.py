import os
from .config import INPUT_DIR, BRONZE_DIR, SILVER_DIR, get_spark
from .schemas import weather_schema
from .processing.transformations import clean_weather_df, add_features
from .processing.aggregations import add_severity_index


def main():
    spark = get_spark("Incremental-Load")

    new_df = (
        spark.read
        .option("header", "true")
        .schema(weather_schema)
        .csv(INPUT_DIR)
    )

    new_clean = clean_weather_df(new_df)
    new_enriched = add_features(new_clean)
    new_enriched = add_severity_index(new_enriched)

    # merge to silver
    if os.path.exists(SILVER_DIR):
        old = spark.read.parquet(SILVER_DIR)
        merged = old.unionByName(new_enriched, allowMissingColumns=True).dropDuplicates(["Event_Timestamp"])
    else:
        merged = new_enriched

    merged.write.mode("overwrite").parquet(SILVER_DIR)

    # merge to bronze
    if os.path.exists(BRONZE_DIR):
        old_bronze = spark.read.parquet(BRONZE_DIR)
        merged_bronze = old_bronze.unionByName(new_df).dropDuplicates(["Formatted Date"])
    else:
        merged_bronze = new_df

    merged_bronze.write.mode("overwrite").parquet(BRONZE_DIR)

    print(" Incremental update completed.")
    spark.stop()


if __name__ == "__main__":
    main()
