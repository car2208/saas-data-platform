import pyspark.sql.functions as F
from pyspark.sql import Row


def test_unit_normalization_cs_to_st(spark):
    data = [
        Row(unidad="CS", cantidad=2.0),
        Row(unidad="ST", cantidad=10.0),
        Row(unidad="cs", cantidad=1.0),
    ]
    df = spark.createDataFrame(data)
    df = df.withColumn(
        "cantidad_normalizada_st",
        F.when(F.upper(F.col("unidad")) == "CS", F.col("cantidad") * 20)
        .otherwise(F.col("cantidad")),
    )
    results = {row.unidad: row.cantidad_normalizada_st for row in df.collect()}
    assert results["CS"] == 40.0
    assert results["ST"] == 10.0
    assert results["cs"] == 20.0


def test_delivery_type_filter(spark):
    data = [
        Row(tipo_entrega="ZPRE"),
        Row(tipo_entrega="ZVE1"),
        Row(tipo_entrega="Z04"),
        Row(tipo_entrega="Z05"),
        Row(tipo_entrega="COBR"),
        Row(tipo_entrega="Z99"),
    ]
    valid_types = ["ZPRE", "ZVE1", "Z04", "Z05"]
    df = spark.createDataFrame(data)
    filtered = df.filter(F.upper(F.col("tipo_entrega")).isin(valid_types))
    assert filtered.count() == 4


def test_delivery_type_flags(spark):
    data = [
        Row(tipo_entrega="ZPRE"),
        Row(tipo_entrega="ZVE1"),
        Row(tipo_entrega="Z04"),
        Row(tipo_entrega="Z05"),
    ]
    df = spark.createDataFrame(data)
    df = (
        df.withColumn("is_routine_delivery", F.col("tipo_entrega").isin(["ZPRE", "ZVE1"]))
        .withColumn("is_bonus_delivery", F.col("tipo_entrega").isin(["Z04", "Z05"]))
    )
    rows = {row.tipo_entrega: row for row in df.collect()}
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
