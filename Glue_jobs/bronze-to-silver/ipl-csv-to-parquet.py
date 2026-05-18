from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import *

# =====================================================
# INITIALIZE SPARK
# =====================================================

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# =====================================================
# READ DELIVERIES CSV
# =====================================================

deliveries_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(
        "s3://ipl-data-bronze/ipl-data/raw/deliveries/"
    )

# =====================================================
# CLEAN DELIVERIES
# =====================================================

deliveries_df = deliveries_df.dropDuplicates()

# =====================================================
# WRITE DELIVERIES PARQUET
# =====================================================

deliveries_df.write.mode("overwrite").parquet(
    "s3://ipl-data-silver/ipl-data/processed/deliveries/"
)

print("silver_deliveries created successfully")

# =====================================================
# READ MATCHES CSV
# =====================================================

matches_df = spark.read \
    .option("header", "true") \
    .option("multiLine", "true") \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .option("inferSchema", "true") \
    .csv(
        "s3://ipl-data-bronze/ipl-data/raw/matches/"
    )

# =====================================================
# CLEAN MATCHES
# =====================================================

# Remove duplicate rows
matches_df = matches_df.dropDuplicates()

# Remove rows with null matchid or season
matches_df = matches_df.filter(
    col("matchid").isNotNull()
    &
    col("season").isNotNull()
)

# =====================================================
# STANDARDIZE SEASON FORMAT
# =====================================================

# Convert:
# 2007/08 -> 2008
# 2010/11 -> 2011

matches_df = matches_df.withColumn(
    "season",
    when(
        col("season").contains("/"),
        concat(
            lit("20"),
            split(col("season"), "/")[1]
        )
    ).otherwise(col("season"))
)

# Convert season to integer
matches_df = matches_df.withColumn(
    "season",
    col("season").cast("int")
)

# =====================================================
# DATA QUALITY CHECKS
# =====================================================

# -----------------------------
# INVALID SEASON CHECK
# -----------------------------

invalid_season_df = matches_df.filter(
    ~col("season").cast("string").rlike("^[0-9]{4}$")
)

invalid_season_count = invalid_season_df.count()

print(
    f"Invalid season records count: {invalid_season_count}"
)

# -----------------------------
# FUTURE SEASON CHECK
# -----------------------------

future_season_count = matches_df.filter(
    col("season") > 2030
).count()

print(
    f"Future season invalid count: {future_season_count}"
)

# -----------------------------
# EMPTY VENUE CHECK
# -----------------------------

empty_venue_count = matches_df.filter(
    col("venue").isNull()
).count()

print(
    f"Empty venue count: {empty_venue_count}"
)

print("Data quality checks completed")

# =====================================================
# WRITE MATCHES PARQUET
# =====================================================

matches_df.write.mode("overwrite").parquet(
    "s3://ipl-data-silver/ipl-data/processed/matches/"
)

print("silver_matches created successfully")