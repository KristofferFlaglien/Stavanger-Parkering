# etl_pipeline.py

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat_ws, to_timestamp
from typing import Tuple

def sjekk_duplikater(df: DataFrame) -> DataFrame:
    df_with_id = df.withColumn("unique_id", concat_ws("_", col("Sted"), col("Dato"), col("Klokkeslett")))
    df_deduped = df_with_id.dropDuplicates(["Sted", "Dato", "Klokkeslett"]).drop("unique_id")
    return df_deduped

def valider_manglende(df: DataFrame) -> None:
    for col_name in ["Sted", "Dato", "Klokkeslett", "Antall_ledige_plasser"]:
        if df.filter(col(col_name).isNull()).count() > 0:
            raise ValueError(f"Manglende verdier i {col_name}")

def valider_gyldige_verdier(df: DataFrame) -> None:
    negative = df.filter(col("Antall_ledige_plasser") < 0).count()
    if negative > 0:
        raise ValueError("Negative verdier i Antall_ledige_plasser")

def konverter_timestamp(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "timestamp",
        to_timestamp(concat_ws(" ", col("Dato"), col("Klokkeslett")), "dd.MM.yyyy HH:mm")
    ).drop("Dato", "Klokkeslett")
