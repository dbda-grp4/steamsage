from pyspark.sql.functions import (
    countDistinct, count,
    from_unixtime, to_date, year,
    when, col, trim
)
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# =============================================================================
# Spark init
# =============================================================================
sc = SparkContext.getOrCreate()
spark = SparkSession.builder.getOrCreate()

# =============================================================================
# Paths
# =============================================================================
review_score_path = "s3://steam-dataset-2025-bucket/silver/reviews/reviews_scored_final.csv"

RAW_BASE = "s3://steam-dataset-2025-bucket/Steam/steam_dataset_2025_csv_package_v1/steam_dataset_2025_csv"
SILVER_BASE = "s3://steam-dataset-2025-bucket/silver/reviews/"

reviews_input = f"{RAW_BASE}/reviews.csv"

review_out_parquet = f"{SILVER_BASE}/fact_review_level/parquet/"
review_out_csv     = f"{SILVER_BASE}/fact_review_level/csv/"

# =============================================================================
# Read RAW reviews
# =============================================================================
reviews_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("multiLine", "true")
    .option("quote", "\"")
    .option("escape", "\"")
    .option("mode", "PERMISSIVE")
    .option("encoding", "UTF-8")
    .load(reviews_input)
)

# =============================================================================
# Select required columns
# =============================================================================
bi_reviews_df = reviews_df.select(
    "recommendationid",
    "appid",
    "votes_up",
    "votes_funny",
    "weighted_vote_score",
    "author_playtime_at_review",
    "author_num_reviews",
    "language",
    "timestamp_created"
)

# =============================================================================
# Convert playtime (minutes → hours → capped)
# =============================================================================
PLAYTIME_HOURS_P95 = 67.6

bi_reviews_df = (
    bi_reviews_df
    .withColumn("author_playtime_at_review_hours", col("author_playtime_at_review") / 60.0)
    .withColumn(
        "author_playtime",
        when(col("author_playtime_at_review_hours") > PLAYTIME_HOURS_P95, PLAYTIME_HOURS_P95)
        .otherwise(col("author_playtime_at_review_hours"))
    )
)

# =============================================================================
# Engagement metric
# =============================================================================
bi_reviews_df = bi_reviews_df.withColumn(
    "review_reactions",
    col("votes_up") + col("votes_funny")
)

# =============================================================================
# Time dimensions
# =============================================================================
bi_reviews_df = (
    bi_reviews_df
    .withColumn("review_timestamp", from_unixtime(col("timestamp_created")))
    .withColumn("review_date", to_date(col("review_timestamp")))
    .withColumn("review_year", year(col("review_timestamp")))
)

# =============================================================================
# Drop intermediates
# =============================================================================
bi_reviews_df = bi_reviews_df.drop(
    "votes_up",
    "votes_funny",
    "author_playtime_at_review",
    "author_playtime_at_review_hours",
    "timestamp_created",
    "review_timestamp"
)

# =============================================================================
# Read SENTIMENT FILE  (🚨 RENAME IMMEDIATELY 🚨)
# =============================================================================
review_score_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(review_score_path)
    .withColumnRenamed("category", "review_category")   # 🔴 FIX IS HERE
)

# =============================================================================
# Join sentiment data
# =============================================================================
review_fact_df = (
    bi_reviews_df
    .join(
        review_score_df,
        on="recommendationid",
        how="left"
    )
)

# =============================================================================
# Clean blanks → NULL
# =============================================================================
for c in ["language", "review_category"]:
    review_fact_df = review_fact_df.withColumn(
        c,
        when(trim(col(c)) == "", None).otherwise(col(c))
    )

# =============================================================================
# Final projection (NO category reference anymore)
# =============================================================================
review_fact_df = review_fact_df.select(
    "recommendationid",
    "appid",
    "review_date",
    "review_year",
    "review_reactions",
    "weighted_vote_score",
    "author_playtime",
    "author_num_reviews",
    "language",
    "review_category",
    "numeric_score"
)


# =============================================================================
# Write outputs
# =============================================================================
review_fact_df.write.mode("overwrite").parquet(review_out_parquet)

review_fact_df.coalesce(1) \
    .write.mode("overwrite") \
    .option("header", "true") \
    .csv(review_out_csv)
