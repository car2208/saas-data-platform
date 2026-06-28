import logging

from omegaconf import DictConfig
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

logger = logging.getLogger("saas_pipeline.gold")


def run_gold(spark: SparkSession, config: DictConfig, tenant_id: str, run_id: str) -> None:
    logger.info(f"[Gold] tenant={tenant_id} start")

    silver_base = config.paths.silver
    gold_base = config.paths.gold

    _build_daily_metrics_by_delivery_type(spark, silver_base, gold_base, tenant_id)

    logger.info(f"[Gold] tenant={tenant_id} done")


def _build_daily_metrics_by_delivery_type(
    spark: SparkSession,
    silver_base: str,
    gold_base: str,
    tenant_id: str,
) -> None:
    fact_path = f"{silver_base}/{tenant_id}/fact_deliveries"
    df = spark.read.format("delta").load(fact_path)

    metrics = (
        df.groupBy("_tenant_id", "fecha_proceso", "tipo_entrega")
        .agg(
            F.sum("cantidad_normalizada_st").alias("total_units"),
            F.sum(F.col("cantidad_normalizada_st") * F.col("precio")).alias("total_revenue"),
            F.countDistinct("ruta").alias("active_routes"),
            F.countDistinct("transporte").alias("active_transports"),
        )
    )

    output_path = f"{gold_base}/{tenant_id}/daily_metrics_by_delivery_type"

    metrics.write.format("delta").mode("overwrite").partitionBy("fecha_proceso").save(output_path)

    from saas_pipeline.spark import register_table

    register_table(spark, f"gold_{tenant_id}", "daily_metrics_by_delivery_type", output_path)
    logger.info(f"[Gold] daily_metrics_by_delivery_type written to {output_path}")
