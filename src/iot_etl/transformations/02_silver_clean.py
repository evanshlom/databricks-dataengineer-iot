# Databricks notebook source
from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="sensor_silver",
    comment="Cleaned Sensor.Community data with parsed sensor values and location",
)
def sensor_silver():
    return (
        spark.read.table("sensor_bronze")
        .filter(
            F.col("sensor_id").isNotNull()
            & F.col("latitude").isNotNull()
            & F.col("longitude").isNotNull()
        )
        .select(
            F.col("sensor_id").cast("long"),
            F.col("sensor_type"),
            F.col("location_id").cast("long"),
            F.col("latitude").cast("double"),
            F.col("longitude").cast("double"),
            F.col("country"),
            F.col("timestamp").cast("timestamp"),
            F.col("P1").cast("double").alias("pm10"),
            F.col("P2").cast("double").alias("pm25"),
            F.col("temperature").cast("double"),
            F.col("humidity").cast("double"),
            F.col("pressure").cast("double"),
            F.col("fetch_ts").cast("timestamp"),
        )
    )