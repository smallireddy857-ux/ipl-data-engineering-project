from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import *

# =====================================================
# INITIALIZE SPARK
# =====================================================

sc = SparkContext.getOrCreate()

glueContext = GlueContext(sc)

spark = glueContext.spark_session

# =====================================================
# READ SILVER DATA
# =====================================================

deliveries_df = spark.read.parquet(
    "s3://ipl-data-silver/ipl-data/processed/deliveries/"
)

matches_df = spark.read.parquet(
    "s3://ipl-data-silver/ipl-data/processed/matches/"
)

# =====================================================
# REMOVE DUPLICATES
# =====================================================

deliveries_df = deliveries_df.dropDuplicates()

matches_df = matches_df.dropDuplicates(["matchid"])

# =====================================================
# CAST REQUIRED COLUMNS
# =====================================================

deliveries_df = deliveries_df.withColumn(
    "batsman_runs",
    col("batsman_runs").cast("int")
)

matches_df = matches_df.withColumn(
    "season",
    col("season").cast("int")
)

# =====================================================
# JOIN MATCH DATA
# =====================================================

base_df = deliveries_df.join(

    matches_df.select(
        "matchid",
        "season"
    ),

    on = "matchid",

    how = "inner"

)

# =====================================================
# SELECT REQUIRED COLUMNS
# =====================================================

base_df = base_df.select(

    "season",

    "matchid",

    "batsman",

    col("batting_team").alias("batsman_team"),

    col("bowling_team").alias("opposition_team"),

    "batsman_runs",

    "ball",

    "player_dismissed",

    "iswide",

    "isnoball"

)

# =====================================================
# CREATE FOURS COLUMN
# =====================================================

base_df = base_df.withColumn(

    "fours",

    when(
        col("batsman_runs") == 4,
        1
    ).otherwise(0)

)

# =====================================================
# CREATE SIXES COLUMN
# =====================================================

base_df = base_df.withColumn(

    "sixes",

    when(
        col("batsman_runs") == 6,
        1
    ).otherwise(0)

)

# =====================================================
# CREATE LEGAL BALL COLUMN
# =====================================================

base_df = base_df.withColumn(

    "legal_ball",

    when(
        (col("iswide").cast("double") > 0)
        |
        (col("isnoball").cast("double") > 0),
        0
    ).otherwise(1)

)

# =====================================================
# CREATE DISMISSAL FLAG
# =====================================================

base_df = base_df.withColumn(

    "dismissal_flag",

    when(
        col("player_dismissed").isNotNull(),
        1
    ).otherwise(0)

)

# =====================================================
# MATCH LEVEL AGGREGATION
# =====================================================

match_stats = base_df.groupBy(

    "season",

    "matchid",

    "batsman",

    "batsman_team",

    "opposition_team"

).agg(

    sum("batsman_runs").alias("total_runs"),

    sum("legal_ball").alias("balls_faced"),

    sum("fours").alias("total_fours"),

    sum("sixes").alias("total_sixes"),

    sum("dismissal_flag").alias("dismissals")

)

# =====================================================
# MATCH COUNT COLUMN
# =====================================================

match_stats = match_stats.withColumn(

    "matches",

    lit(1)

)

# =====================================================
# CREATE FIFTIES COLUMN
# =====================================================

match_stats = match_stats.withColumn(

    "fifties",

    when(
        (col("total_runs") >= 50)
        &
        (col("total_runs") < 100),
        1
    ).otherwise(0)

)

# =====================================================
# CREATE HUNDREDS COLUMN
# =====================================================

match_stats = match_stats.withColumn(

    "hundreds",

    when(
        col("total_runs") >= 100,
        1
    ).otherwise(0)

)

# =====================================================
# CAREER VS TEAM TABLE
# =====================================================

career_vs_team = match_stats.groupBy(

    "batsman",

    "batsman_team",

    "opposition_team"

).agg(

    sum("total_runs").alias("career_runs"),

    sum("balls_faced").alias("career_balls"),

    sum("total_fours").alias("career_fours"),

    sum("total_sixes").alias("career_sixes"),

    sum("dismissals").alias("dismissals"),

    sum("matches").alias("matches"),

    sum("fifties").alias("fifties"),

    sum("hundreds").alias("hundreds")

)

# =====================================================
# ADD STRIKE RATE
# =====================================================

career_vs_team = career_vs_team.withColumn(

    "strike_rate",

    round(

        (col("career_runs") / col("career_balls")) * 100,

        2

    )

)

# =====================================================
# ADD BATTING AVERAGE
# =====================================================

career_vs_team = career_vs_team.withColumn(

    "batting_average",

    when(

        col("dismissals") > 0,

        round(
            col("career_runs") / col("dismissals"),
            2
        )

    ).otherwise(None)

)

# =====================================================
# WRITE CAREER TABLE
# =====================================================

career_vs_team.write.mode("overwrite").parquet(

    "s3://ipl-data-gold/ipl-data/gold_batsman_vs_team/"

)

print("gold_batsman_vs_team created successfully")

# =====================================================
# SEASON VS TEAM TABLE
# =====================================================

season_vs_team = match_stats.groupBy(

    "season",

    "batsman",

    "batsman_team",

    "opposition_team"

).agg(

    sum("total_runs").alias("season_runs"),

    sum("balls_faced").alias("season_balls"),

    sum("total_fours").alias("season_fours"),

    sum("total_sixes").alias("season_sixes"),

    sum("dismissals").alias("dismissals"),

    sum("matches").alias("matches"),

    sum("fifties").alias("fifties"),

    sum("hundreds").alias("hundreds")

)

# =====================================================
# ADD STRIKE RATE
# =====================================================

season_vs_team = season_vs_team.withColumn(

    "strike_rate",

    round(

        (col("season_runs") / col("season_balls")) * 100,

        2

    )

)

# =====================================================
# ADD BATTING AVERAGE
# =====================================================

season_vs_team = season_vs_team.withColumn(

    "batting_average",

    when(

        col("dismissals") > 0,

        round(
            col("season_runs") / col("dismissals"),
            2
        )

    ).otherwise(None)

)

# =====================================================
# WRITE SEASON TABLE
# =====================================================

season_vs_team.write.mode("overwrite").parquet(

    "s3://ipl-data-gold/ipl-data/gold_batsman_vs_team_season/"

)

print("gold_batsman_vs_team_season created successfully")