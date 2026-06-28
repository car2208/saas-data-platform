import logging
from datetime import datetime

import pyspark.sql.functions as F
from omegaconf import DictConfig
from pyspark.sql import SparkSession
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


def run_quality_checks(
    spark: SparkSession, config: DictConfig, tenant_id: str, run_id: str
) -> bool:
    silver_base = config.paths.silver
    quality_logs_path = config.paths.quality_logs
    fact_path = f"{silver_base}/{tenant_id}/fact_deliveries"

    df = spark.read.format("delta").load(fact_path)
    has_critical_failure = False
    total = df.count()

    # Check 1: No null quantities after processing (critical)
    null_qty = df.filter(F.col("cantidad_normalizada_st").isNull()).count()
    _log_check(
        spark,
        quality_logs_path,
        run_id,
        tenant_id,
        "silver",
        "fact_deliveries",
        "not_null_cantidad_normalizada_st",
        "critical",
        total,
        null_qty,
    )
    if null_qty > 0:
        has_critical_failure = True

    # Check 2: Revenue calculable - precio not null (critical)
    null_precio = df.filter(F.col("precio").isNull()).count()
    _log_check(
        spark,
        quality_logs_path,
        run_id,
        tenant_id,
        "silver",
        "fact_deliveries",
        "not_null_precio",
        "critical",
        total,
        null_precio,
    )
    if null_precio > 0:
        has_critical_failure = True

    # Check 3: All tipo_entrega values are valid (warning)
    invalid_tipo = df.filter(~F.col("tipo_entrega").isin(["ZPRE", "ZVE1", "Z04", "Z05"])).count()
    _log_check(
        spark,
        quality_logs_path,
        run_id,
        tenant_id,
        "silver",
        "fact_deliveries",
        "valid_tipo_entrega",
        "warning",
        total,
        invalid_tipo,
    )

    # Check 4: Temporal join enrichment - no null material_descripcion (warning)
    null_desc = df.filter(F.col("material_descripcion").isNull()).count()
    _log_check(
        spark,
        quality_logs_path,
        run_id,
        tenant_id,
        "silver",
        "fact_deliveries",
        "enrichment_completeness",
        "warning",
        total,
        null_desc,
    )

    # Check 5: No duplicate business keys (critical)
    merge_keys = ["_tenant_id", "fecha_proceso", "transporte", "ruta", "material", "tipo_entrega"]
    dup_count = total - df.dropDuplicates(merge_keys).count()
    _log_check(
        spark,
        quality_logs_path,
        run_id,
        tenant_id,
        "silver",
        "fact_deliveries",
        "no_duplicate_business_keys",
        "critical",
        total,
        dup_count,
    )
    if dup_count > 0:
        has_critical_failure = True

    if has_critical_failure and config.quality.get("fail_on_critical", False):
        raise RuntimeError(
            f"Critical quality checks failed for tenant {tenant_id}. Aborting before Gold."
        )

    return not has_critical_failure
