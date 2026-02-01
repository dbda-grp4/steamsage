import sys
from pyspark.context import SparkContext
from pyspark.sql.functions import (
    col,
    when,
    from_unixtime,
    to_date,
    year,
    month,
    date_format,
    countDistinct,
    count
)
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions


# --------------------------------------------------
# Glue job arguments
# --------------------------------------------------
args = getResolvedOptions(
    sys.argv,
    [
        'JOB_NAME',
        'RAW_BASE',
        'SILVER_BASE'
    ]
)

RAW_BASE = args['RAW_BASE']
SILVER_BASE = args['SILVER_BASE']

print(f"Starting Glue job: {args['JOB_NAME']}")
print(f"RAW_BASE: {RAW_BASE}")
print(f"SILVER_BASE: {SILVER_BASE}")


# --------------------------------------------------
# Glue Context
# --------------------------------------------------
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session


# --------------------------------------------------
# Input / Output paths
# --------------------------------------------------
reviews_input = f"{RAW_BASE}/reviews.csv"

review_out_parquet = f"{SILVER_BASE}/fact_review_level/parquet/"


# --------------------------------------------------
# Read RAW reviews CSV
# --------------------------------------------------
reviews_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("multiLine", "true")    # IMPORTANT
    .option("quote", "\"")
    .option("escape", "\"")
    .option("mode", "PERMISSIVE")
    .option("encoding", "UTF-8")
    .load(reviews_input)
)


# --------------------------------------------------
# Select BI-relevant columns
# --------------------------------------------------
bi_reviews_df = reviews_df.select(
    "recommendationid",
    "appid",

    "votes_up",
    "votes_funny",
    "comment_count",
    "weighted_vote_score",

    "author_playtime_at_review",
    "author_playtime_forever",
    "author_playtime_last_two_weeks",
    "author_num_games_owned",
    "author_num_reviews",

    "steam_purchase",
    "received_for_free",
    "written_during_early_access",

    "language",
    "timestamp_created"
)


# --------------------------------------------------
# Data integrity check
# --------------------------------------------------
bi_reviews_df.select(
    countDistinct("recommendationid").alias("distinct_reviews"),
    count("*").alias("total_rows")
).show()


# --------------------------------------------------
# Apply capping (pre-computed thresholds)
# --------------------------------------------------
bi_reviews_capped_df = (
    bi_reviews_df
    .withColumn(
        "author_playtime_forever_capped",
        when(col("author_playtime_forever") > 26026, 26026)
        .otherwise(col("author_playtime_forever"))
    )
    .withColumn(
        "author_playtime_at_review_capped",
        when(col("author_playtime_at_review") > 22677, 22677)
        .otherwise(col("author_playtime_at_review"))
    )
    .withColumn(
        "author_playtime_last_two_weeks_capped",
        when(col("author_playtime_last_two_weeks") > 2673, 2673)
        .otherwise(col("author_playtime_last_two_weeks"))
    )
    .withColumn(
        "votes_up_capped",
        when(col("votes_up") > 49, 49)
        .otherwise(col("votes_up"))
    )
    .withColumn(
        "votes_funny_capped",
        when(col("votes_funny") > 10, 10)
        .otherwise(col("votes_funny"))
    )
    .withColumn(
        "comment_count_capped",
        when(col("comment_count") > 4, 4)
        .otherwise(col("comment_count"))
    )
)


# --------------------------------------------------
# Time enrichment
# --------------------------------------------------
review_fact_df = (
    bi_reviews_capped_df
    .withColumn("review_timestamp", from_unixtime(col("timestamp_created")))
    .withColumn("review_date", to_date(col("review_timestamp")))
    .withColumn("review_year", year(col("review_timestamp")))
    .withColumn("review_month", month(col("review_timestamp")))
    .withColumn("review_year_month", date_format(col("review_timestamp"), "yyyy-MM"))
)


# --------------------------------------------------
# Write SILVER output (Parquet only)
# --------------------------------------------------
review_fact_df.write.mode("overwrite").parquet(review_out_parquet)

print("Reviews job completed successfully.")
