import logging
import sys
import uuid

import click

from saas_pipeline.config import (
    load_config,
    resolve_tenants_to_process,
    validate_config,
)

logger = logging.getLogger("saas_pipeline")


def _setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )


@click.command()
@click.option("--env", default="dev", type=click.Choice(["dev", "qa", "main"]), help="Ambiente")
@click.option("--tenant", default=None, help="Tenant a procesar (código o 'all')")
@click.option("--start-date", default=None, help="Fecha inicio YYYY-MM-DD")
@click.option("--end-date", default=None, help="Fecha fin YYYY-MM-DD")
@click.option("--layer", default="all", type=click.Choice(["all", "bronze", "silver", "gold"]))
def main(env, tenant, start_date, end_date, layer):
    config = load_config(env)

    if tenant:
        config = config.copy()
        config.execution.tenant = tenant
    if start_date:
        config.execution.start_date = start_date
    if end_date:
        config.execution.end_date = end_date

    validate_config(config)
    _setup_logging(config.get("logging", {}).get("level", "INFO"))

    run_id = str(uuid.uuid4())
    tenants = resolve_tenants_to_process(config.execution.tenant)
    logger.info(f"Run {run_id} | env={env} | tenants={tenants} | layer={layer}")

    from saas_pipeline.bronze import run_bronze
    from saas_pipeline.gold import run_gold
    from saas_pipeline.quality import run_quality_checks
    from saas_pipeline.silver import run_silver
    from saas_pipeline.spark import get_spark

    spark = get_spark()
    failed_tenants = []

    for tenant_id in tenants:
        logger.info(f"Processing tenant: {tenant_id}")
        try:
            if layer in ("all", "bronze"):
                run_bronze(spark, config, tenant_id, run_id)

            if layer in ("all", "silver"):
                run_silver(spark, config, tenant_id, run_id)
                run_quality_checks(spark, config, tenant_id, run_id)

            if layer in ("all", "gold"):
                run_gold(spark, config, tenant_id, run_id)

        except Exception as e:
            logger.error(f"Tenant {tenant_id} failed: {e}")
            if config.execution.get("fail_fast", False):
                logger.error("fail_fast=true, aborting.")
                sys.exit(1)
            failed_tenants.append(tenant_id)

    if failed_tenants:
        logger.error(f"Failed tenants: {failed_tenants}")
        sys.exit(1)

    logger.info(f"Run {run_id} completed successfully.")


if __name__ == "__main__":
    main()
