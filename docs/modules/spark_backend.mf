# backends/spark_backend.py — Spark Backend

## Purpose
Provide **cluster-scale** AIS statistics computation using PySpark.

## Responsibilities
- Compute per-row haversine distance with Spark UDF.
- Use Spark SQL window functions for lag/lead and first/last points.
- Aggregate per-MMSI metrics: points, distance, straight-line distance, tortuosity, turn index, average/max SOG.

## Usage Example
```python
from pyspark.sql import SparkSession
from aisdataset.backends.spark_backend import compute_stats_spark

spark = SparkSession.builder.appName("aistk").getOrCreate()
sdf = spark.read.parquet("lake/clean/2024")
out = compute_stats_spark(sdf)
out.show()
spark.stop()
```
