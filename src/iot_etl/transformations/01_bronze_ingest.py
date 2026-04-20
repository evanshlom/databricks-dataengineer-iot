# Databricks notebook source
from pyspark import pipelines as dp


@dp.materialized_view(
    name="iot_bronze",
    comment="Raw IoT device data from sample dataset",
)
def iot_bronze():
    return spark.read.json("/databricks-datasets/iot/iot_devices.json")
