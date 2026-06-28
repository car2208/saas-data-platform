import logging

from omegaconf import DictConfig
from pyspark.sql import SparkSession

logger = logging.getLogger("saas_pipeline.gold")


def run_gold(spark: SparkSession, config: DictConfig, tenant_id: str, run_id: str) -> None:
    logger.info(f"[Gold] tenant={tenant_id} - not implemented yet")