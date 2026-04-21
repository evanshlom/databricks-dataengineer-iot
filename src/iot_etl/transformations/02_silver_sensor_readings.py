# Databricks notebook source
from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="silver_sensor_readings",
    comment="Cleaned sensor readings enriched with asset metadata",
)
def silver_sensor_readings():
    readings = (
        spark.read.table("bronze_sensor_readings")
        .filter(F.col("asset_id").isNotNull() & F.col("timestamp").isNotNull())
        .select(
            F.col("asset_id"),
            F.col("timestamp").cast("timestamp"),
            F.col("temperature_c").cast("double"),
            F.col("pressure_bar").cast("double"),
            F.col("flow_rate_lpm").cast("double"),
            F.col("vibration_mm_s").cast("double"),
            F.col("power_draw_kw").cast("double"),
            F.col("anomaly_3sig").cast("boolean"),
            F.col("spc_r1_spike").cast("boolean"),
            F.col("spc_r2_cluster").cast("boolean"),
            F.col("spc_r3_shift").cast("boolean"),
            F.col("spc_r4_bias").cast("boolean"),
        )
    )
    assets = (
        spark.read.table("bronze_assets")
        .select("asset_id", "asset_type", "asset_subtype", "zone",
                "manufacturer", "rated_capacity_kw", "temp_setpoint_c")
    )
    return readings.join(assets, on="asset_id", how="left")
