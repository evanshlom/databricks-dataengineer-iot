# Databricks notebook source
from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="iot_gold_country_stats",
    comment="Aggregated IoT device stats by country",
)
def iot_gold_country_stats():
    return (
        spark.read.table("iot_silver")
        .groupBy("country_name", "country_code")
        .agg(
            F.count("*").alias("device_count"),
            F.round(F.avg("temperature"), 2).alias("avg_temp"),
            F.round(F.avg("humidity"), 2).alias("avg_humidity"),
            F.round(F.avg("co2_level"), 2).alias("avg_co2"),
            F.round(F.avg("battery_level"), 2).alias("avg_battery"),
        )
    )
