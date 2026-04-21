# Databricks notebook source
from pyspark import pipelines as dp


@dp.table(
    name="bronze_work_orders",
    comment="Raw maintenance work orders - bronze only, user processes silver/gold",
)
def bronze_work_orders():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("/Volumes/catalog1/iot_hello_world/raw_landing/work_orders")
    )
