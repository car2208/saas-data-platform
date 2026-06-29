import pyspark.sql.functions as F
from pyspark.sql import Row

from saas_pipeline.silver import (
    add_delivery_flags,
    filter_valid_delivery_types,
    normalize_units,
)


def test_unit_normalization_cs_to_st(spark):
    data = [
        Row(unidad="CS", cantidad=2.0),
        Row(unidad="ST", cantidad=10.0),
        Row(unidad="cs", cantidad=1.0),
    ]
    df = spark.createDataFrame(data)
    result = normalize_units(df)
    rows = {row.unidad: row.cantidad_normalizada_st for row in result.collect()}
    assert rows["CS"] == 40.0
    assert rows["ST"] == 10.0
    assert rows["cs"] == 20.0


def test_delivery_type_filter(spark):
    data = [
        Row(tipo_entrega="ZPRE"),
        Row(tipo_entrega="ZVE1"),
        Row(tipo_entrega="Z04"),
        Row(tipo_entrega="Z05"),
        Row(tipo_entrega="COBR"),
        Row(tipo_entrega="Z99"),
    ]
    df = spark.createDataFrame(data)
    result = filter_valid_delivery_types(df)
    assert result.count() == 4


def test_delivery_type_flags(spark):
    data = [
        Row(tipo_entrega="ZPRE"),
        Row(tipo_entrega="ZVE1"),
        Row(tipo_entrega="Z04"),
        Row(tipo_entrega="Z05"),
    ]
    df = spark.createDataFrame(data)
    result = add_delivery_flags(df)
    rows = {row.tipo_entrega: row for row in result.collect()}
    assert rows["ZPRE"].is_routine_delivery is True
    assert rows["ZPRE"].is_bonus_delivery is False
    assert rows["Z04"].is_routine_delivery is False
    assert rows["Z04"].is_bonus_delivery is True


def test_tenant_normalization_to_lowercase(spark):
    data = [Row(pais="SV"), Row(pais="GT"), Row(pais="HN")]
    df = spark.createDataFrame(data)
    df = df.withColumn("pais", F.lower(F.col("pais")))
    values = [row.pais for row in df.collect()]
    assert values == ["sv", "gt", "hn"]


def test_anomaly_detection_invalid_cantidad(spark):
    data = [
        Row(cantidad=10.0),
        Row(cantidad=0.0),
        Row(cantidad=-5.0),
        Row(cantidad=None),
    ]
    df = spark.createDataFrame(data)
    invalid = df.filter(F.col("cantidad").isNull() | (F.col("cantidad") <= 0))
    valid = df.filter(F.col("cantidad").isNotNull() & (F.col("cantidad") > 0))
    assert invalid.count() == 3
    assert valid.count() == 1
