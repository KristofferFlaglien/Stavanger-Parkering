import pytest
from pyspark.sql import SparkSession
from etl_pipeline import sjekk_duplikater, valider_manglende, valider_gyldige_verdier, konverter_timestamp

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("TestETL").getOrCreate()

def test_sjekk_duplikater(spark):
    data = [("A", "01.01.2024", "10:00", 5), ("A", "01.01.2024", "10:00", 5)]
    df = spark.createDataFrame(data, ["Sted", "Dato", "Klokkeslett", "Antall_ledige_plasser"])
    df_result = sjekk_duplikater(df)
    assert df_result.count() == 1

def test_valider_manglende_feiler(spark):
    data = [("A", None, "10:00", 5)]
    df = spark.createDataFrame(data, ["Sted", "Dato", "Klokkeslett", "Antall_ledige_plasser"])
    with pytest.raises(ValueError):
        valider_manglende(df)

def test_valider_gyldige_verdier_feiler(spark):
    data = [("A", "01.01.2024", "10:00", -1)]
    df = spark.createDataFrame(data, ["Sted", "Dato", "Klokkeslett", "Antall_ledige_plasser"])
    with pytest.raises(ValueError):
        valider_gyldige_verdier(df)

def test_konverter_timestamp(spark):
    data = [("01.01.2024", "10:00")]
    df = spark.createDataFrame(data, ["Dato", "Klokkeslett"])
    df_result = konverter_timestamp(df)
    assert "timestamp" in df_result.columns
