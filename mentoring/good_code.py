import logging

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)

VALID_DELIVERY_TYPES = ["ZPRE", "ZVE1", "Z04", "Z05"]


def normalize_units(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "cantidad_st",
        F.when(F.upper(F.col("unidad")) == "CS", F.col("cantidad") * 20)
        .otherwise(F.col("cantidad")),
    )


def calculate_revenue(df: DataFrame) -> DataFrame:
    return df.withColumn("total_revenue", F.col("cantidad_st") * F.col("precio"))


def filter_valid_deliveries(df: DataFrame) -> DataFrame:
    return df.filter(F.col("tipo_entrega").isin(VALID_DELIVERY_TYPES))


def validate_and_quarantine(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    invalid = df.filter(
        F.col("cantidad").isNull()
        | (F.col("cantidad") <= 0)
        | F.col("precio").isNull()
    )
    valid = df.filter(
        F.col("cantidad").isNotNull()
        & (F.col("cantidad") > 0)
        & F.col("precio").isNotNull()
    )
    return valid, invalid


def process_deliveries(
    spark: SparkSession,
    input_path: str,
    output_path: str,
    tenant_id: str,
) -> DataFrame:
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)
    df = df.filter(F.lower(F.col("pais")) == tenant_id)

    valid, quarantined = validate_and_quarantine(df)

    if quarantined.count() > 0:
        quarantined.write.format("delta").mode("append").save(
            f"{output_path}/quarantine/{tenant_id}/deliveries"
        )
        logger.warning(f"Quarantined {quarantined.count()} rows for tenant {tenant_id}")

    result = (
        valid.transform(filter_valid_deliveries)
        .transform(normalize_units)
        .transform(calculate_revenue)
    )

    (
        result.write.format("delta")
        .mode("overwrite")
        .partitionBy("fecha_proceso")
        .save(f"{output_path}/{tenant_id}/deliveries")
    )

    logger.info(f"Wrote {result.count()} rows for tenant {tenant_id}")
    return result
