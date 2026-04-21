# Databricks notebook source
from pyspark import pipelines as dp


@dp.table(
    name="bronze_alarm_events",
    comment="Raw SPC-derived alarm events with debounce from steel plant",
)
def bronze_alarm_events():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("/Volumes/catalog1/iot_hello_world/raw_landing/alarm_events")
    )
