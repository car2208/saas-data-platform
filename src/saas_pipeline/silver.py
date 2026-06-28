import logging

from omegaconf import DictConfig
from pyspark.sql import SparkSession

logger = logging.getLogger("saas_pipeline.silver")


def run_silver(spark: SparkSession, config: DictConfig, tenant_id: str, run_id: str) -> None:
    logger.info(f"[Silver] tenant={tenant_id} - not implemented yet")