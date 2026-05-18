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

deliveries_df = deliveries_df.withColumn(
    "ball",
    col("ball").cast("int")
)

matches_df = matches_df.withColumn(
    "season",
    col("season").cast("int")
)

# -----------------------------------
# JOIN MATCHES
# -----------------------------------

season_df = deliveries_df.join(
    matches_df.select(
        "matchid",
        "season",
        "player_of_match"
    ),
    on="matchid",
    how="inner"
)

# -----------------------------------
# PLAYER SEASON STATS
# -----------------------------------

player_season_stats = season_df.groupBy(
    "season",
    "batsman"
).agg(

    sum("batsman_runs").alias("season_runs"),

    count("*").alias("balls_faced"),

    countDistinct("matchid").alias("matches_played"),

    sum(
        when(col("batsman_runs") == 4, 1)
        .otherwise(0)
    ).alias("fours"),

    sum(
        when(col("batsman_runs") == 6, 1)
        .otherwise(0)
    ).alias("sixes"),

    sum(
        when(
            col("player_of_match") == col("batsman"),
            1
        ).otherwise(0)
    ).alias("player_of_match_awards")

)

# -----------------------------------
# STRIKE RATE
# -----------------------------------

player_season_stats = player_season_stats.withColumn(
    "strike_rate",
    round(
        (col("season_runs") * 100.0) /
        col("balls_faced"),
        2
    )
)

# -----------------------------------
# WRITE GOLD TABLE
# -----------------------------------

player_season_stats.write.mode("overwrite").parquet(
    "s3://ipl-data-gold/ipl-data/gold_player_season_stats/"
)

print("gold_player_season_stats created successfully")