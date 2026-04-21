# Databricks notebook source
from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="gold_asset_health",
    comment="Asset health summary: sensor stats + PID performance + alarm counts",
)
def gold_asset_health():
    sensor_stats = (
        spark.read.table("silver_sensor_readings")
        .groupBy("asset_id", "asset_type", "asset_subtype", "zone",
                 "manufacturer", "rated_capacity_kw", "temp_setpoint_c")
        .agg(
            F.count("*").alias("reading_count"),
            F.round(F.avg("temperature_c"), 1).alias("avg_temp_c"),
            F.round(F.max("temperature_c"), 1).alias("max_temp_c"),
            F.round(F.min("temperature_c"), 1).alias("min_temp_c"),
            F.round(F.stddev("temperature_c"), 2).alias("std_temp_c"),
            F.round(F.avg("pressure_bar"), 2).alias("avg_pressure_bar"),
            F.round(F.avg("vibration_mm_s"), 2).alias("avg_vibration_mm_s"),
            F.round(F.max("vibration_mm_s"), 2).alias("max_vibration_mm_s"),
            F.round(F.avg("power_draw_kw"), 1).alias("avg_power_kw"),
            F.sum(F.when(F.col("spc_r1_spike"), 1).otherwise(0)).alias("r1_spike_count"),
            F.sum(F.when(F.col("spc_r2_cluster"), 1).otherwise(0)).alias("r2_cluster_count"),
            F.sum(F.when(F.col("spc_r3_shift"), 1).otherwise(0)).alias("r3_shift_count"),
            F.sum(F.when(F.col("spc_r4_bias"), 1).otherwise(0)).alias("r4_bias_count"),
            F.max("timestamp").alias("latest_reading"),
        )
    )

    pid_stats = (
        spark.read.table("silver_pid_control")
        .groupBy("asset_id")
        .agg(
            F.round(F.avg(F.abs(F.col("error_c"))), 2).alias("avg_abs_error_c"),
            F.round(F.max(F.abs(F.col("error_c"))), 2).alias("max_abs_error_c"),
            F.round(F.avg("valve_position_pct"), 1).alias("avg_valve_pct"),
            F.round(F.avg("actuator_power_pct"), 1).alias("avg_actuator_pct"),
        )
    )

    alarm_stats = (
        spark.read.table("silver_alarm_events")
        .groupBy("asset_id")
        .agg(
            F.count("*").alias("total_alarms"),
            F.sum(F.when(F.col("severity") == "high", 1).otherwise(0)).alias("high_alarms"),
            F.sum(F.when(F.col("severity") == "medium", 1).otherwise(0)).alias("medium_alarms"),
            F.sum(F.when(F.col("severity") == "low", 1).otherwise(0)).alias("low_alarms"),
            F.sum(F.when(F.col("dispatched") == True, 1).otherwise(0)).alias("dispatched_count"),
            F.sum(F.when(F.col("false_alarm") == True, 1).otherwise(0)).alias("false_alarm_count"),
            F.round(F.avg("resolution_minutes"), 0).alias("avg_resolution_min"),
            F.max("timestamp").alias("latest_alarm"),
        )
    )

    return (
        sensor_stats
        .join(pid_stats, on="asset_id", how="left")
        .join(alarm_stats, on="asset_id", how="left")
        .fillna(0, subset=["total_alarms", "high_alarms", "medium_alarms",
                           "low_alarms", "dispatched_count", "false_alarm_count"])
    )
