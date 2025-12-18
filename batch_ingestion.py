import os
from pyspark.sql.functions import avg
from ..config import INPUT_DIR, BRONZE_DIR, get_spark
from ..schemas import weather_schema


def main():
    print("=== Weather Batch Ingestion ===")
    print(f"INPUT_DIR  : {INPUT_DIR}")
    print(f"BRONZE_DIR : {BRONZE_DIR}")

    if not os.path.exists(INPUT_DIR):
        print(f" INPUT_DIR does not exist: {INPUT_DIR}")
        return

    spark = get_spark("Weather-Batch-Ingestion")

    df = (
        spark.read
        .option("header", "true")
        .schema(weather_schema)
        .csv(INPUT_DIR)
    )

    print("\n=== Schema ===")
    df.printSchema()

    print("\n=== Sample Data ===")
    df.show(10, truncate=False)

    print(f"\nTotal records: {df.count()}")

    avg_temp = df.select(avg("Temperature (C)").alias("avg_temp")).collect()[0]["avg_temp"]
    print(f"Average Temperature (C): {avg_temp}")

    (
        df.write
        .mode("overwrite")
        .parquet(BRONZE_DIR)
    )

    print(f"\n Bronze data written to: {BRONZE_DIR}")

    spark.stop()


if __name__ == "__main__":
    main()
