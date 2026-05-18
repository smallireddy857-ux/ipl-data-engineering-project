from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import *

# Initialize Spark
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Read Silver Data

deliveries_df = spark.read.parquet(
    "s3://ipl-data-silver/ipl-data/processed/deliveries/"
)

# Clean Data

deliveries_df = deliveries_df.dropDuplicates()

deliveries_df = deliveries_df.withColumn(
    "batsman_runs",
    col("batsman_runs").cast("int")
)

# Match-wise runs

match_runs = deliveries_df.groupBy(
    "matchid",
    "batsman"
).agg(
    sum("batsman_runs").alias("runs_in_match")
)

# Consistency calculation

consistency_df = match_runs.groupBy(
    "batsman"
).agg(
    round(avg("runs_in_match"), 2).alias("average_runs"),
    round(stddev("runs_in_match"), 2).alias("consistency_score")
)

# Write Gold Table

consistency_df.write.mode("overwrite").parquet(
    "s3://ipl-data-gold/ipl-data/gold_consistency_score/"
)

print("gold_consistency_score created successfully")