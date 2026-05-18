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
    "batsman_runs",
    col("batsman_runs").cast("int")
)

matches_df = matches_df.withColumn(
    "season",
    col("season").cast("int")
)

# -----------------------------------
# JOIN MATCHES
# -----------------------------------

mvp_df = deliveries_df.join(
    matches_df.select("matchid", "season"),
    on="matchid",
    how="inner"
)

# -----------------------------------
# CREATE FOURS COLUMN
# -----------------------------------

mvp_df = mvp_df.withColumn(
    "fours",
    when(col("batsman_runs") == 4, 1)
    .otherwise(0)
)

# -----------------------------------
# CREATE SIXES COLUMN
# -----------------------------------

mvp_df = mvp_df.withColumn(
    "sixes",
    when(col("batsman_runs") == 6, 1)
    .otherwise(0)
)

# -----------------------------------
# MATCH LEVEL AGGREGATION
# -----------------------------------

match_stats = mvp_df.groupBy(
    "season",
    "matchid",
    "batsman"
).agg(

    sum("batsman_runs").alias("total_runs"),

    sum("fours").alias("total_fours"),

    sum("sixes").alias("total_sixes")

)

# -----------------------------------
# FIFTY BONUS
# -----------------------------------

match_stats = match_stats.withColumn(
    "fifty_bonus",
    when(
        (col("total_runs") >= 50)
        &
        (col("total_runs") < 100),
        10
    ).otherwise(0)
)

# -----------------------------------
# HUNDRED BONUS
# -----------------------------------

match_stats = match_stats.withColumn(
    "hundred_bonus",
    when(
        col("total_runs") >= 100,
        20
    ).otherwise(0)
)

# -----------------------------------
# MVP POINTS CALCULATION
# -----------------------------------

match_stats = match_stats.withColumn(
    "mvp_points",
    (
        col("total_fours") * 2.5
        +
        col("total_sixes") * 3.5
        +
        col("fifty_bonus")
        +
        col("hundred_bonus")
    )
)

# =====================================================
# SEASON MVP POINTS
# =====================================================

season_mvp = match_stats.groupBy(
    "season",
    "batsman"
).agg(

    round(
        sum("mvp_points"),
        2
    ).alias("season_mvp_points")

)

season_mvp.write.mode("overwrite").parquet(
    "s3://ipl-data-gold/ipl-data/gold_mvp_points_season/"
)

print("gold_mvp_points_season created successfully")

# =====================================================
# CAREER MVP POINTS
# =====================================================

career_mvp = match_stats.groupBy(
    "batsman"
).agg(

    round(
        sum("mvp_points"),
        2
    ).alias("career_mvp_points")

)

career_mvp.write.mode("overwrite").parquet(
    "s3://ipl-data-gold/ipl-data/gold_mvp_points_career/"
)

print("gold_mvp_points_career created successfully")