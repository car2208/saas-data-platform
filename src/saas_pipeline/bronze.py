import logging

import pyspark.sql.functions as F
from omegaconf import DictConfig
from pyspark.sql import SparkSession

logger = logging.getLogger("saas_pipeline.bronze")


def run_bronze(spark: SparkSession, config: DictConfig, tenant_id: str, run_id: str) -> None:
    logger.info(f"[Bronze] tenant={tenant_id} start")

    raw_path = config.paths.raw
    bronze_base = config.paths.bronze
    start_date = config.execution.start_date.replace("-", "")
    end_date = config.execution.end_date.replace("-", "")

    _ingest_deliveries(spark, raw_path, bronze_base, tenant_id, run_id, start_date, end_date)
    _ingest_materials(spark, raw_path, bronze_base, tenant_id, run_id)
    logger.info(f"[Bronze] tenant={tenant_id} done")


def _ingest_deliveries(
    spark: SparkSession,
    raw_path: str,
    bronze_base: str,
    tenant_id: str,
    run_id: str,
    start_date: str,
    end_date: str,
) -> None:
    csv_path = f"{raw_path}/global_mobility_data_entrega_productos.csv"
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_path)

    # Filter by tenant (pais column comes uppercase, normalize to lowercase)
    df = df.filter(F.lower(F.col("pais")) == tenant_id)

    # Filter by date range
    df = df.filter((F.col("fecha_proceso") >= start_date) & (F.col("fecha_proceso") <= end_date))

    # Add technical columns
    batch_id = f"{run_id}_{tenant_id}"
    df = (
        df.withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_tenant_id", F.lit(tenant_id))
        .withColumn("_batch_id", F.lit(batch_id))
    )

    # Normalize tenant to lowercase in pais column
    df = df.withColumn("pais", F.lower(F.col("pais")))

    # Deduplicate exact rows (all original columns, excluding technical)
    original_cols = [c for c in df.columns if not c.startswith("_")]
    df = df.dropDuplicates(original_cols)

    output_path = f"{bronze_base}/{tenant_id}/deliveries"

    # Get distinct fecha_proceso values for partition overwrite
    fechas = [row.fecha_proceso for row in df.select("fecha_proceso").distinct().collect()]

    if not fechas:
        logger.warning(f"[Bronze] No data for tenant={tenant_id} in date range")
        return

    # Idempotent write: overwrite by partition (replaceWhere)
    fecha_list = ",".join([f"'{f}'" for f in fechas])
    replace_condition = f"fecha_proceso IN ({fecha_list})"

    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", replace_condition)
        .partitionBy("fecha_proceso")
        .save(output_path)
    )

    from saas_pipeline.spark import register_table

    register_table(spark, f"bronze_{tenant_id}", "deliveries", output_path)
    logger.info(f"[Bronze] deliveries: wrote {df.count()} rows to {output_path}")


def _ingest_materials(
    spark: SparkSession,
    raw_path: str,
    bronze_base: str,
    tenant_id: str,
    run_id: str,
) -> None:
    csv_path = f"{raw_path}/materials_catalog.csv"
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_path)

    batch_id = f"{run_id}_{tenant_id}"
    df = (
        df.withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_tenant_id", F.lit(tenant_id))
        .withColumn("_batch_id", F.lit(batch_id))
    )

    original_cols = [c for c in df.columns if not c.startswith("_")]
    df = df.dropDuplicates(original_cols)

    output_path = f"{bronze_base}/{tenant_id}/materials"

    df.write.format("delta").mode("overwrite").save(output_path)

    from saas_pipeline.spark import register_table

    register_table(spark, f"bronze_{tenant_id}", "materials", output_path)
    logger.info(f"[Bronze] materials: wrote {df.count()} rows to {output_path}")
