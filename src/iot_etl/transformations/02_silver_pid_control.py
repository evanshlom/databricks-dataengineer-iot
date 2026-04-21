# Databricks notebook source
from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="silver_pid_control",
    comment="Cleaned PID control responses enriched with asset metadata",
)
def silver_pid_control():
    pid = (
        spark.read.table("bronze_pid_control")
        .filter(F.col("asset_id").isNotNull() & F.col("timestamp").isNotNull())
        .select(
            F.col("asset_id"),
            F.col("timestamp").cast("timestamp"),
            F.col("setpoint_temp_c").cast("double"),
            F.col("process_temp_c").cast("double"),
            F.col("error_c").cast("double"),
            F.col("pid_output").cast("double"),
            F.col("p_term").cast("double"),
            F.col("i_term").cast("double"),
            F.col("d_term").cast("double"),
            F.col("valve_position_pct").cast("double"),
            F.col("actuator_power_pct").cast("double"),
            F.col("cooling_flow_cmd_lpm").cast("double"),
        )
    )
    assets = (
        spark.read.table("bronze_assets")
        .select("asset_id", "asset_type", "asset_subtype", "zone")
    )
    return pid.join(assets, on="asset_id", how="left")
