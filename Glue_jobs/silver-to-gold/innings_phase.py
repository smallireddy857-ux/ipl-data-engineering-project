from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import *

# -----------------------------------
# INITIALIZE SPARK
# -----------------------------------

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# -----------------------------------
# READ SILVER DATA
# -----------------------------------

deliveries_df = spark.read.parquet(
    "s3://ipl-data-silver/ipl-data/processed/deliveries/"
)

matches_df = spark.read.parquet(
    "s3://ipl-data-silver/ipl-data/processed/matches/"
)

# -----------------------------------
# REMOVE DUPLICATES
# -----------------------------------

deliveries_df = deliveries_df.dropDuplicates()

matches_df = matches_df.dropDuplicates(["matchid"])

# -----------------------------------
# CAST REQUIRED COLUMNS
# -----------------------------------

deliveries_df = deliveries_df.withColumn(
    "over",
    col("over").cast("int")
)

deliveries_df = deliveries_df.withColumn(
    "ball",
    col("ball").cast("int")
)

deliveries_df = deliveries_df.withColumn(
    "batsman_runs",
    col("batsman_runs").cast("int")
)

# -----------------------------------
# DEFINE MATCH PHASES
# -----------------------------------

deliveries_df = deliveries_df.withColumn(
    "phase",
    when(col("over").between(1, 6), "Powerplay")
    .when(col("over").between(7, 15), "Middle Overs")
    .otherwise("Death Overs")
)

# -----------------------------------
# JOIN MATCHES
# -----------------------------------

innings_df = deliveries_df.join(
    matches_df.select("matchid", "season"),
    on="matchid",
    how="inner"
)

# -----------------------------------
# INNINGS PHASE ANALYSIS
# -----------------------------------

innings_phase_analysis = innings_df.groupBy(
    "matchid",
    "inning",
    "season",
    "batting_team",
    "batsman",
    "phase"
).agg(
    sum("batsman_runs").alias("total_runs"),
    count("*").alias("balls_faced")
)

# -----------------------------------
# STRIKE RATE
# -----------------------------------

innings_phase_analysis = innings_phase_analysis.withColumn(
    "strike_rate",
    round(
        (col("total_runs") * 100.0) / col("balls_faced"),
        2
    )
)

# -----------------------------------
# WRITE GOLD TABLE
# -----------------------------------

innings_phase_analysis.write.mode("overwrite").parquet(
    "s3://ipl-data-gold/ipl-data/gold_innings_phase_analysis/"
)

print("gold_innings_phase_analysis created successfully")