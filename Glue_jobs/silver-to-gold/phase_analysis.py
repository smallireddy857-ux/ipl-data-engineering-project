from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from awsglue.job import Job

# -----------------------------
# INITIALIZE SPARK
# -----------------------------

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# -----------------------------
# READ SILVER DATA
# -----------------------------

deliveries_df = spark.read.parquet(
    "s3://ipl-data-silver/ipl-data/processed/deliveries/"
)

matches_df = spark.read.parquet(
    "s3://ipl-data-silver/ipl-data/processed/matches/"
)

# -----------------------------
# REMOVE DUPLICATES
# -----------------------------

deliveries_df = deliveries_df.dropDuplicates()

# VERY IMPORTANT
matches_df = matches_df.dropDuplicates(["matchid"])

# -----------------------------
# CHECK DUPLICATE MATCHIDS
# -----------------------------

duplicate_matchids = matches_df.groupBy("matchid") \
    .count() \
    .filter(col("count") > 1)

print("Duplicate MatchIDs After Cleaning:")
duplicate_matchids.show()

# -----------------------------
# CAST NUMERIC COLUMNS
# -----------------------------

deliveries_df = deliveries_df.withColumn(
    "batsman_runs",
    col("batsman_runs").cast("int")
)

deliveries_df = deliveries_df.withColumn(
    "ball",
    col("ball").cast("int")
)

deliveries_df = deliveries_df.withColumn(
    "over",
    col("over").cast("int")
)

# -----------------------------
# JOIN MATCHES + DELIVERIES
# -----------------------------

phase_df = deliveries_df.join(
    matches_df.select("matchid", "season"),
    on="matchid",
    how="inner"
)

# -----------------------------
# CREATE PHASE COLUMN
# -----------------------------

phase_df = phase_df.withColumn(
    "phase",
    when(col("over") <= 6, "Powerplay")
    .when((col("over") >= 7) & (col("over") <= 15), "Middle Overs")
    .otherwise("Death Overs")
)

# -----------------------------
# PHASE ANALYSIS
# -----------------------------

gold_phase_analysis = phase_df.groupBy(
    "season",
    "batting_team",
    "batsman",
    "phase"
).agg(
    sum("batsman_runs").alias("total_runs"),
    count("ball").alias("balls_faced")
)

# -----------------------------
# STRIKE RATE
# -----------------------------

gold_phase_analysis = gold_phase_analysis.withColumn(
    "strike_rate",
    round(
        (col("total_runs") * 100.0) / col("balls_faced"),
        2
    )
)

# -----------------------------
# WRITE TO GOLD
# -----------------------------

gold_phase_analysis.write.mode("overwrite").parquet(
    "s3://ipl-data-gold/ipl-data/gold_phase_analysis/"
)

print("gold_phase_analysis created successfully")