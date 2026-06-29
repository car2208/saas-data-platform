import logging
from datetime import datetime

import pyspark.sql.functions as F
from omegaconf import DictConfig
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

logger = logging.getLogger("saas_pipeline.quality")

QUALITY_LOG_SCHEMA = StructType(
    [
        StructField("_run_id", StringType()),
        StructField("_batch_id", StringType()),
        StructField("tenant_id", StringType()),
        StructField("layer", StringType()),
        StructField("table_name", StringType()),
        StructField("check_name", StringType()),
        StructField("check_severity", StringType()),
        StructField("records_checked", LongType()),
        StructField("records_failed", LongType()),
        StructField("check_passed", BooleanType()),
        StructField("executed_at", TimestampType()),
    ]
)


def _log_check(
    spark: SparkSession,
    quality_logs_path: str,
    run_id: str,
    tenant_id: str,
    layer: str,
    table_name: str,
    check_name: str,
    severity: str,
    total: int,
    failed: int,
) -> None:
    passed = failed == 0
    row = [
        (
            run_id,
            f"{run_id}_{tenant_id}",
            tenant_id,
            layer,
            table_name,
            check_name,
            severity,
            total,
            failed,
            passed,
            datetime.utcnow(),
        )
    ]
    from saas_pipeline.spark import register_table

    df = spark.createDataFrame(row, QUALITY_LOG_SCHEMA)
    df.write.format("delta").mode("append").save(quality_logs_path)
    register_table(spark, "shared", "quality_logs", quality_logs_path)

    status = "PASSED" if passed else "FAILED"
    logger.info(f"[Quality] {check_name}: {status} ({failed}/{total} failed)")


def check_not_null(df: DataFrame, column: str) -> int:
    return df.filter(F.col(column).isNull()).count()


def check_valid_values(df: DataFrame, column: str, valid_values: list) -> int:
    return df.filter(~F.col(column).isin(valid_values)).count()


def check_duplicate_keys(df: DataFrame, keys: list) -> int:
    return df.count() - df.dropDuplicates(keys).count()


FACT_MERGE_KEYS = [
    "_tenant_id",
    "fecha_proceso",
    "transporte",
    "ruta",
    "material",
    "tipo_entrega",
]


def run_quality_checks(
    spark: SparkSession, config: DictConfig, tenant_id: str, run_id: str
) -> bool:
    silver_base = config.paths.silver
    quality_logs_path = config.paths.quality_logs
    fact_path = f"{silver_base}/{tenant_id}/fact_deliveries"

    df = spark.read.format("delta").load(fact_path)
    has_critical_failure = False
    total = df.count()

    checks = [
        (
            "not_null_cantidad_normalizada_st",
            "critical",
            check_not_null(df, "cantidad_normalizada_st"),
        ),
        ("not_null_precio", "critical", check_not_null(df, "precio")),
        (
            "valid_tipo_entrega",
            "warning",
            check_valid_values(df, "tipo_entrega", ["ZPRE", "ZVE1", "Z04", "Z05"]),
        ),
        ("enrichment_completeness", "warning", check_not_null(df, "material_descripcion")),
        ("no_duplicate_business_keys", "critical", check_duplicate_keys(df, FACT_MERGE_KEYS)),
    ]

    for check_name, severity, failed in checks:
        _log_check(
            spark,
            quality_logs_path,
            run_id,
            tenant_id,
            "silver",
            "fact_deliveries",
            check_name,
            severity,
            total,
            failed,
        )
        if failed > 0 and severity == "critical":
            has_critical_failure = True

    if has_critical_failure and config.quality.get("fail_on_critical", False):
        raise RuntimeError(
            f"Critical quality checks failed for tenant {tenant_id}. Aborting before Gold."
        )

    return not has_critical_failure
