from pyspark.sql import Row
import pyspark.sql.functions as F


def test_duplicate_business_keys_detection(spark):
    data = [
        Row(_tenant_id="sv", fecha_proceso="20250301", transporte=1, ruta=1, material="M1", tipo_entrega="ZPRE"),
        Row(_tenant_id="sv", fecha_proceso="20250301", transporte=1, ruta=1, material="M1", tipo_entrega="ZPRE"),
        Row(_tenant_id="sv", fecha_proceso="20250301", transporte=2, ruta=1, material="M1", tipo_entrega="ZPRE"),
    ]
    keys = ["_tenant_id", "fecha_proceso", "transporte", "ruta", "material", "tipo_entrega"]
    df = spark.createDataFrame(data)
    total = df.count()
    deduped = df.dropDuplicates(keys).count()
    dup_count = total - deduped
    assert dup_count == 1


def test_orphan_material_detection(spark):
    deliveries = [
        Row(material="M001"),
        Row(material="M002"),
        Row(material="M999"),
    ]
    catalog = [Row(material="M001"), Row(material="M002")]

    df_del = spark.createDataFrame(deliveries)
    df_cat = spark.createDataFrame(catalog)

    orphans = df_del.join(df_cat, "material", "left_anti")
    assert orphans.count() == 1
    assert orphans.collect()[0].material == "M999"
