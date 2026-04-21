# Databricks notebook source
from pyspark import pipelines as dp


@dp.table(
    name="bronze_assets",
    comment="Raw equipment registry from steel plant",
)
def bronze_assets():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("/Volumes/catalog1/iot_hello_world/raw_landing/assets")
    )
