# Databricks notebook source
from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="sensor_gold_country_stats",
    comment="Aggregated air quality stats by country from Sensor.Community",
)
def sensor_gold_country_stats():
    return (
        spark.read.table("sensor_silver")
        .groupBy("country")
        .agg(
            F.countDistinct("sensor_id").alias("sensor_count"),
            F.count("*").alias("reading_count"),
            F.round(F.avg("pm10"), 2).alias("avg_pm10"),
            F.round(F.avg("pm25"), 2).alias("avg_pm25"),
            F.round(F.avg("temperature"), 2).alias("avg_temp"),
            F.round(F.avg("humidity"), 2).alias("avg_humidity"),
            F.round(F.avg("pressure"), 2).alias("avg_pressure"),
            F.max("timestamp").alias("latest_reading"),
        )
    )