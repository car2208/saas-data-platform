from pyspark.sql import Row

from saas_pipeline.quality import check_duplicate_keys, check_not_null, check_valid_values


def test_duplicate_business_keys_detection(spark):
    data = [
        Row(_tenant_id="sv", fecha_proceso="20250301", transporte=1, ruta=1, material="M1", tipo_entrega="ZPRE"),
        Row(_tenant_id="sv", fecha_proceso="20250301", transporte=1, ruta=1, material="M1", tipo_entrega="ZPRE"),
        Row(_tenant_id="sv", fecha_proceso="20250301", transporte=2, ruta=1, material="M1", tipo_entrega="ZPRE"),
    ]
    keys = ["_tenant_id", "fecha_proceso", "transporte", "ruta", "material", "tipo_entrega"]
    df = spark.createDataFrame(data)
    assert check_duplicate_keys(df, keys) == 1


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


def test_check_not_null(spark):
    data = [Row(precio=10.0), Row(precio=None), Row(precio=5.0)]
    df = spark.createDataFrame(data)
    assert check_not_null(df, "precio") == 1


def test_check_valid_values(spark):
    data = [
        Row(tipo_entrega="ZPRE"),
        Row(tipo_entrega="COBR"),
        Row(tipo_entrega="Z04"),
        Row(tipo_entrega="Z99"),
    ]
    df = spark.createDataFrame(data)
    assert check_valid_values(df, "tipo_entrega", ["ZPRE", "ZVE1", "Z04", "Z05"]) == 2
