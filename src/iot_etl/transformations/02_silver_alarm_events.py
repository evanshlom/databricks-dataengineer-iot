# Databricks notebook source
from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="silver_alarm_events",
    comment="Cleaned alarm events enriched with asset metadata",
)
def silver_alarm_events():
    alarms = (
        spark.read.table("bronze_alarm_events")
        .filter(F.col("asset_id").isNotNull() & F.col("timestamp").isNotNull())
        .select(
            F.col("alarm_id"),
            F.col("asset_id"),
            F.col("timestamp").cast("timestamp"),
            F.col("trigger_type"),
            F.col("severity"),
            F.col("process_value_c").cast("double"),
            F.col("dispatched").cast("boolean"),
            F.col("resolution_minutes").cast("integer"),
            F.col("false_alarm").cast("boolean"),
        )
    )
    assets = (
        spark.read.table("bronze_assets")
        .select("asset_id", "asset_type", "asset_subtype", "zone")
    )
    return alarms.join(assets, on="asset_id", how="left")
