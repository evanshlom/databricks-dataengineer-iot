# Databricks notebook source
from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="iot_silver",
    comment="Cleaned IoT data with valid coordinates and standardized fields",
)
def iot_silver():
    return (
        spark.read.table("iot_bronze")
        .filter(F.col("latitude").isNotNull() & F.col("longitude").isNotNull())
        .select(
            F.col("device_id").cast("long"),
            F.col("device_name"),
            F.col("ip"),
            F.col("cca2").alias("country_code"),
            F.col("cca3").alias("country_code_3"),
            F.col("cn").alias("country_name"),
            F.col("latitude").cast("double"),
            F.col("longitude").cast("double"),
            F.col("scale").alias("temp_scale"),
            F.col("temp").cast("double").alias("temperature"),
            F.col("humidity").cast("double"),
            F.col("battery_level").cast("long"),
            F.col("c02_level").cast("long").alias("co2_level"),
            F.col("lcd").alias("lcd_status"),
            F.col("timestamp").cast("long").alias("event_ts"),
        )
    )
