# Databricks notebook source
from pyspark import pipelines as dp


@dp.table(
    name="sensor_bronze",
    comment="Raw Sensor.Community air quality data ingested via Auto Loader",
)
def sensor_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load("/Volumes/catalog1/iot_hello_world/raw_landing/sensor_data")
    )