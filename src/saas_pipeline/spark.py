import os
import sys

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


def get_spark() -> SparkSession:
    builder = (
        SparkSession.builder.appName("saas-data-platform")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.pyspark.python", sys.executable)
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def register_table(spark: SparkSession, schema: str, table: str, path: str) -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema}")
    abs_path = os.path.abspath(path).replace("\\", "/")
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {schema}.{table} USING DELTA LOCATION '{abs_path}'"
    )
