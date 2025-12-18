import os
from .spark_utils import setup_pyspark_env

setup_pyspark_env()

from pyspark.sql import SparkSession

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(BASE_DIR)
DATA_DIR = os.path.join(ROOT_DIR, "data")

INPUT_DIR = os.path.join(DATA_DIR, "weather_input")
BRONZE_DIR = os.path.join(DATA_DIR, "bronze")
SILVER_DIR = os.path.join(DATA_DIR, "silver")
GOLD_DIR = os.path.join(DATA_DIR, "gold")
CHECKPOINT_DIR = os.path.join(DATA_DIR, "checkpoints")


def get_spark(app_name="WeatherPipeline"):
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
