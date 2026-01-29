# Generated from: Omsai Steam reviews EDA.ipynb
# Converted at: 2026-01-28T04:20:36.377Z
# Next step (optional): refactor into modules & generate tests with RunCell
# Quick start: pip install runcell

import logging

# Disable debug logging to prevent verbose thread monitor output
logging.getLogger().setLevel(logging.ERROR)
logging.getLogger('ThreadMonitor').setLevel(logging.ERROR)

# Configure AWS credentials for S3 access
spark.conf.set("fs.s3a.access.key", "ASIAXXXXX")
spark.conf.set("fs.s3a.secret.key", "XXXXXXXX/XXXX/XXXX")
spark.conf.set("fs.s3a.session.token", "XXx//////////xxxxxxxxx/xxxxx+tC//xxxxx//////////xxxxx/xxxx+V4DHoWpjIDxnmh2edubIZUVqszqXGtut7YwyLcC/FeqoMo854iyv4tyuKPEdDLKQNHqH0C1hw4jlnST6MIUTe4m89dfSkLfyA9FYr9evA25CVlmkRDg+89vLyXHmaBUSDJs1vjLNpboZxtuGAwXENtlAMpaEz8A3oMWsVg6yFfrQyBImC1mysotVU4moh4Wb/7kLow8KFNCU+/AywY6nAEHdmz/FLTKqm/4yvuZn4gLMF8+h6Mz667pOvQQ6yY2ZwcDgkJo845fbI2cu2DUGOq12VOa+ZWFZJ0MgZljrGutlOZ+OeajEfxkYjTf8kh7QgvOXcyEi5bqlWfYR1EdrlNdStEn24VlqCXwKgFr7eBa8lvHIWQNtpQHMfhQdNHgkuq2v7+BhCN+jSbF3s/n8lENSv9WX8irpVZwOGQ=")
spark.conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
spark.conf.set("fs.s3a.endpoint", "s3.amazonaws.com")



reviews_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("multiLine", "true")        # CRITICAL
    .option("quote", "\"")              # handle quoted text
    .option("escape", "\"")             # handle escaped quotes
    .option("mode", "PERMISSIVE")       # don't break on bad rows
    .option("encoding", "UTF-8")
    .load("s3://steam-dataset-2025-bucket/Steam/steam_dataset_2025_csv_package_v1/steam_dataset_2025_csv/reviews.csv")
)


display(reviews_df.limit(10))

reviews_df.printSchema()


display(
    reviews_df.select(
        "recommendationid",
        "appid",
        "review_text",
        "votes_up",
        "language"
    ).limit(10)
)


print(reviews_df.count())


bi_reviews_df = reviews_df.select(
    "appid",
    "recommendationid",
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


bi_reviews_df.printSchema()


display(bi_reviews_df.limit(10))


total_rows = bi_reviews_df.count()
total_rows


from pyspark.sql.functions import col, count, when

null_summary = bi_reviews_df.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in bi_reviews_df.columns
])

display(null_summary)


null_percentage = bi_reviews_df.select([
    (count(when(col(c).isNull(), c)) / total_rows * 100).alias(c)
    for c in bi_reviews_df.columns
])

display(null_percentage)


zero_summary = bi_reviews_df.select([
    count(when(col(c) == 0, c)).alias(c)
    for c in [
        "votes_up",
        "votes_funny",
        "comment_count",
        "author_playtime_at_review",
        "author_playtime_last_two_weeks",
        "author_num_games_owned",
        "author_num_reviews"
    ]
])

display(zero_summary)


quantiles = bi_reviews_df.select(
    "author_playtime_forever",
    "author_playtime_at_review",
    "author_playtime_last_two_weeks",
    "votes_up",
    "votes_funny",
    "comment_count"
).summary("min", "50%", "90%", "95%", "99%", "max")

display(quantiles)


caps = bi_reviews_df.approxQuantile(
    [
        "author_playtime_forever",
        "author_playtime_at_review",
        "author_playtime_last_two_weeks",
        "votes_up",
        "votes_funny",
        "comment_count"
    ],
    [0.99],
    0.01
)

caps


from pyspark.sql.functions import when, col

bi_reviews_capped_df = (
    bi_reviews_df
    # Playtime caps
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
    # Engagement caps
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


display(
    bi_reviews_capped_df.select(
        "author_playtime_forever",
        "author_playtime_forever_capped",
        "votes_funny",
        "votes_funny_capped",
        "comment_count",
        "comment_count_capped"
    ).limit(20)
)


from pyspark.sql.functions import max

display(
    bi_reviews_capped_df.select(max("author_playtime_forever_capped").alias("max_author_playtime_forever"))
)

from pyspark.sql.functions import (
    from_unixtime,
    to_date,
    year,
    month,
    date_format
)

bi_reviews_time_df = (
    bi_reviews_capped_df
    # Convert unix timestamp → timestamp
    .withColumn(
        "review_timestamp",
        from_unixtime(col("timestamp_created"))
    )
    # Date column
    .withColumn(
        "review_date",
        to_date(col("review_timestamp"))
    )
    # Time dimensions
    .withColumn(
        "review_year",
        year(col("review_timestamp"))
    )
    .withColumn(
        "review_month",
        month(col("review_timestamp"))
    )
    .withColumn(
        "review_year_month",
        date_format(col("review_timestamp"), "yyyy-MM")
    )
)


display(
    bi_reviews_time_df.select(
        "timestamp_created",
        "review_timestamp",
        "review_date",
        "review_year",
        "review_month",
        "review_year_month"
    ).limit(10)
)


from pyspark.sql.functions import (
    count,
    avg,
    sum as spark_sum
)

app_review_agg_df = (
    bi_reviews_time_df
    .groupBy("appid")
    .agg(
        count("*").alias("total_reviews"),

        avg("votes_up_capped").alias("avg_votes_up"),
        avg("votes_funny_capped").alias("avg_votes_funny"),
        avg("comment_count_capped").alias("avg_comment_count"),
        avg("weighted_vote_score").alias("avg_weighted_vote_score"),

        avg("author_playtime_forever_capped").alias("avg_playtime_forever"),

        (spark_sum(col("steam_purchase").cast("int")) / count("*")).alias("pct_steam_purchase"),
        (spark_sum(col("received_for_free").cast("int")) / count("*")).alias("pct_received_for_free"),
        (spark_sum(col("written_during_early_access").cast("int")) / count("*")).alias("pct_early_access")
    )
)


display(app_review_agg_df.limit(10))


from pyspark.sql.functions import count, avg, sum as spark_sum

app_month_review_agg_df = (
    bi_reviews_time_df
    .groupBy("appid", "review_year_month")
    .agg(
        count("*").alias("monthly_reviews"),

        avg("votes_up_capped").alias("avg_votes_up"),
        avg("votes_funny_capped").alias("avg_votes_funny"),
        avg("comment_count_capped").alias("avg_comment_count"),
        avg("weighted_vote_score").alias("avg_weighted_vote_score"),

        avg("author_playtime_forever_capped").alias("avg_playtime_forever"),

        (spark_sum(col("steam_purchase").cast("int")) / count("*")).alias("pct_steam_purchase"),
        (spark_sum(col("received_for_free").cast("int")) / count("*")).alias("pct_received_for_free"),
        (spark_sum(col("written_during_early_access").cast("int")) / count("*")).alias("pct_early_access")
    )
)


display(
    app_month_review_agg_df
    .orderBy("appid", "review_year_month")
    .limit(20)
)


app_review_agg_df.write.mode("overwrite").parquet(
    "s3://steam-dataset-2025-bucket/powerbi/fact_app_level/parquet/"
)


app_review_agg_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(
    "s3://steam-dataset-2025-bucket/powerbi/fact_app_level/csv/"
)


app_month_review_agg_df.write.mode("overwrite").parquet(
    "s3://steam-dataset-2025-bucket/powerbi/fact_app_month/parquet/"
)


app_month_review_agg_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(
    "s3://steam-dataset-2025-bucket/powerbi/fact_app_month/csv/"
)


bi_reviews_time_df.write.mode("overwrite").parquet(
    "s3://steam-dataset-2025-bucket/powerbi/fact_review_level/parquet/"
)


bi_reviews_time_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(
    "s3://steam-dataset-2025-bucket/powerbi/fact_review_level/csv/"
)


from pyspark.sql.functions import lit

app_level_combined = app_review_agg_df \
    .withColumn("review_year_month", lit(None).cast("string")) \
    .withColumn("dataset_type", lit("app_level"))

app_month_combined = app_month_review_agg_df \
    .withColumn("dataset_type", lit("app_month"))

review_level_combined = bi_reviews_time_df \
    .select("appid", "review_year_month") \
    .withColumn("dataset_type", lit("review_level"))


combined_df = app_level_combined.unionByName(
    app_month_combined, allowMissingColumns=True
).unionByName(
    review_level_combined, allowMissingColumns=True
)


combined_df.write.mode("overwrite").parquet(
    "s3://steam-dataset-2025-bucket/combined/parquet/"
)


combined_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(
    "s3://steam-dataset-2025-bucket/combined/csv/"
)


display(combined_df.count())

display(combined_df.limit(20))

combined_df.columns

# #### Review Level Data


bi_reviews_time_df.columns

# #### App level data


app_month_review_agg_df.columns

# 


app_review_agg_df.columns