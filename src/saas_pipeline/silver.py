import logging

from delta.tables import DeltaTable
from omegaconf import DictConfig
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F

logger = logging.getLogger("saas_pipeline.silver")

VALID_DELIVERY_TYPES = ["ZPRE", "ZVE1", "Z04", "Z05"]

FACT_MERGE_KEYS = [
    "_tenant_id",
    "fecha_proceso",
    "transporte",
    "ruta",
    "material",
    "tipo_entrega",
]


def run_silver(spark: SparkSession, config: DictConfig, tenant_id: str, run_id: str) -> None:
    logger.info(f"[Silver] tenant={tenant_id} start")

    bronze_base = config.paths.bronze
    silver_base = config.paths.silver
    quarantine_root = config.paths.quarantine_root

    _process_dim_materials(spark, bronze_base, silver_base, tenant_id, run_id)
    _process_fact_deliveries(spark, bronze_base, silver_base, quarantine_root, tenant_id, run_id)

    logger.info(f"[Silver] tenant={tenant_id} done")


def _process_dim_materials(
    spark: SparkSession,
    bronze_base: str,
    silver_base: str,
    tenant_id: str,
    run_id: str,
) -> None:
    bronze_path = f"{bronze_base}/{tenant_id}/materials"
    df = spark.read.format("delta").load(bronze_path)

    df = (
        df.withColumn("valid_from", F.to_date(F.col("valid_from")))
        .withColumn("valid_to", F.to_date(F.col("valid_to")))
        .withColumn("is_current", F.col("is_current").cast("boolean"))
        .withColumn("precio_base", F.col("precio_base").cast("decimal(18,2)"))
    )

    output_path = f"{silver_base}/{tenant_id}/dim_materials"

    if DeltaTable.isDeltaTable(spark, output_path):
        delta_table = DeltaTable.forPath(spark, output_path)
        (
            delta_table.alias("target")
            .merge(
                df.alias("source"),
                "target.material = source.material AND target.valid_from = source.valid_from",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        df.write.format("delta").mode("overwrite").save(output_path)

    from saas_pipeline.spark import register_table

    register_table(spark, f"silver_{tenant_id}", "dim_materials", output_path)
    logger.info(f"[Silver] dim_materials written to {output_path}")


def _process_fact_deliveries(
    spark: SparkSession,
    bronze_base: str,
    silver_base: str,
    quarantine_root: str,
    tenant_id: str,
    run_id: str,
) -> None:
    bronze_path = f"{bronze_base}/{tenant_id}/deliveries"
    df = spark.read.format("delta").load(bronze_path)

    quarantine_path = f"{quarantine_root}/silver_quarantine/{tenant_id}/fact_deliveries"

    # --- Anomaly detection ---
    quarantine_frames = []

    # 1. fecha_proceso null or invalid
    fecha_parsed = F.to_date(F.col("fecha_proceso").cast("string"), "yyyyMMdd")
    invalid_fecha = df.filter(F.col("fecha_proceso").isNull() | fecha_parsed.isNull())
    if invalid_fecha.count() > 0:
        quarantine_frames.append(
            invalid_fecha.withColumn("_quarantine_reason", F.lit("fecha_proceso_invalid"))
        )

    df = df.filter(F.col("fecha_proceso").isNotNull() & fecha_parsed.isNotNull())

    # 2. cantidad null, negative or zero
    invalid_cantidad = df.filter(F.col("cantidad").isNull() | (F.col("cantidad") <= 0))
    if invalid_cantidad.count() > 0:
        quarantine_frames.append(
            invalid_cantidad.withColumn("_quarantine_reason", F.lit("cantidad_invalid"))
        )
    df = df.filter(F.col("cantidad").isNotNull() & (F.col("cantidad") > 0))

    # 3. precio null
    invalid_precio = df.filter(F.col("precio").isNull())
    if invalid_precio.count() > 0:
        quarantine_frames.append(
            invalid_precio.withColumn("_quarantine_reason", F.lit("precio_null"))
        )
    df = df.filter(F.col("precio").isNotNull())

    # 4. material not in catalog
    dim_path = f"{silver_base}/{tenant_id}/dim_materials"
    dim_materials = spark.read.format("delta").load(dim_path).select("material").distinct()
    orphan = df.join(dim_materials, "material", "left_anti")
    if orphan.count() > 0:
        quarantine_frames.append(
            orphan.withColumn("_quarantine_reason", F.lit("material_not_in_catalog"))
        )
    df = df.join(dim_materials, "material", "left_semi")

    # Write quarantined rows
    if quarantine_frames:
        all_quarantine = quarantine_frames[0]
        for qf in quarantine_frames[1:]:
            all_quarantine = all_quarantine.unionByName(qf, allowMissingColumns=True)
        all_quarantine.write.format("delta").mode("append").save(quarantine_path)
        logger.info(f"[Silver] Quarantined {all_quarantine.count()} rows")

    # 5. Discard invalid tipo_entrega
    df = df.filter(F.upper(F.col("tipo_entrega")).isin(VALID_DELIVERY_TYPES))
    df = df.withColumn("tipo_entrega", F.upper(F.col("tipo_entrega")))

    # --- Transformations ---

    # Unit normalization: CS -> ST (1 CS = 20 ST)
    df = df.withColumn(
        "cantidad_normalizada_st",
        F.when(F.upper(F.col("unidad")) == "CS", F.col("cantidad") * 20)
        .otherwise(F.col("cantidad")),
    )

    # Delivery type flags
    df = (
        df.withColumn("is_routine_delivery", F.col("tipo_entrega").isin(["ZPRE", "ZVE1"]))
        .withColumn("is_bonus_delivery", F.col("tipo_entrega").isin(["Z04", "Z05"]))
    )

    # Temporal join with dim_materials (not just is_current)
    fecha_col = F.to_date(F.col("fecha_proceso").cast("string"), "yyyyMMdd")
    df = df.withColumn("_fecha_date", fecha_col)

    dim_full = spark.read.format("delta").load(dim_path)
    enriched = (
        df.alias("f")
        .join(
            dim_full.alias("d"),
            (F.col("f.material") == F.col("d.material"))
            & (F.col("f._fecha_date") >= F.col("d.valid_from"))
            & (F.col("f._fecha_date") <= F.col("d.valid_to")),
            "left",
        )
        .select(
            F.col("f.*"),
            F.col("d.descripcion").alias("material_descripcion"),
            F.col("d.categoria").alias("material_categoria"),
            F.col("d.precio_base").alias("material_precio_base"),
        )
        .drop("_fecha_date")
    )

    # --- MERGE INTO Silver ---
    output_path = f"{silver_base}/{tenant_id}/fact_deliveries"

    if DeltaTable.isDeltaTable(spark, output_path):
        merge_condition = " AND ".join(
            [f"target.{k} = source.{k}" for k in FACT_MERGE_KEYS]
        )
        delta_table = DeltaTable.forPath(spark, output_path)
        (
            delta_table.alias("target")
            .merge(enriched.alias("source"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        enriched.write.format("delta").mode("overwrite").partitionBy("fecha_proceso").save(
            output_path
        )

    from saas_pipeline.spark import register_table

    register_table(spark, f"silver_{tenant_id}", "fact_deliveries", output_path)
    logger.info(f"[Silver] fact_deliveries written to {output_path}")
