"""
Cluster-scale statistics using PySpark.
"""
from pyspark.sql import SparkSession
from aistk.backends.spark_backend import compute_stats_spark

def main():
    spark = SparkSession.builder.appName("aistk-examples").getOrCreate()
    sdf = spark.read.parquet("lake/clean/2024")
    out = compute_stats_spark(sdf, level="mmsi")
    out.show(20, truncate=False)
    spark.stop()

if __name__ == "__main__":
    main()
