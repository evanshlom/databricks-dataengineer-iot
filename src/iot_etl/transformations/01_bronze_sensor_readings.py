# Databricks notebook source
from pyspark import pipelines as dp


@dp.table(
    name="bronze_sensor_readings",
    comment="Raw IoT sensor telemetry with SPC flags from steel plant equipment",
)
def bronze_sensor_readings():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("/Volumes/catalog1/iot_hello_world/raw_landing/sensor_readings")
    )
