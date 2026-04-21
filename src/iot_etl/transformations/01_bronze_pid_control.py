# Databricks notebook source
from pyspark import pipelines as dp


@dp.table(
    name="bronze_pid_control",
    comment="Raw PID controller output responding to sensor readings",
)
def bronze_pid_control():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("/Volumes/catalog1/iot_hello_world/raw_landing/pid_control")
    )
