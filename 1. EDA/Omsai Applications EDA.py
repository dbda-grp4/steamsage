# Generated from: Omsai Applications EDA.ipynb
# Converted at: 2026-01-28T04:21:07.585Z
# Next step (optional): refactor into modules & generate tests with RunCell
# Quick start: pip install runcell

import logging

logging.getLogger().setLevel(logging.ERROR)
logging.getLogger("ThreadMonitor").setLevel(logging.ERROR)


# Configure AWS credentials for S3 access
spark.conf.set("fs.s3a.access.key", "ASIAXXXXX")
spark.conf.set("fs.s3a.secret.key", "XXXXXXXX/XXXX/XXXX")
spark.conf.set("fs.s3a.session.token", "XXx//////////xxxxxxxxx/xxxxx+tC//xxxxx//////////xxxxx/xxxx+V4DHoWpjIDxnmh2edubIZUVqszqXGtut7YwyLcC/FeqoMo854iyv4tyuKPEdDLKQNHqH0C1hw4jlnST6MIUTe4m89dfSkLfyA9FYr9evA25CVlmkRDg+89vLyXHmaBUSDJs1vjLNpboZxtuGAwXENtlAMpaEz8A3oMWsVg6yFfrQyBImC1mysotVU4moh4Wb/7kLow8KFNCU+/AywY6nAEHdmz/FLTKqm/4yvuZn4gLMF8+h6Mz667pOvQQ6yY2ZwcDgkJo845fbI2cu2DUGOq12VOa+ZWFZJ0MgZljrGutlOZ+OeajEfxkYjTf8kh7QgvOXcyEi5bqlWfYR1EdrlNdStEn24VlqCXwKgFr7eBa8lvHIWQNtpQHMfhQdNHgkuq2v7+BhCN+jSbF3s/n8lENSv9WX8irpVZwOGQ=")
spark.conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
spark.conf.set("fs.s3a.endpoint", "s3.amazonaws.com")



applications_raw_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("multiLine", "true")      # IMPORTANT (text fields)
    .option("quote", "\"")
    .option("escape", "\"")
    .option("mode", "PERMISSIVE")
    .option("encoding", "UTF-8")
    .option("inferSchema", "false")   # VERY IMPORTANT
    .load("s3://steam-dataset-2025-bucket/Steam/steam_dataset_2025_csv_package_v1/steam_dataset_2025_csv/applications.csv")
)


applications_raw_df.printSchema()

print("Rows:", applications_df.count())
print("Columns:", len(applications_raw_df.columns))


display(applications_raw_df.limit(10))


from pyspark.sql.functions import countDistinct, count

applications_raw_df.select(
    countDistinct("appid").alias("distinct_appids"),
    count("*").alias("total_rows")
).show()


from pyspark.sql.functions import col, to_date, expr

bi_applications_df = (
    applications_raw_df
    .select(
        col("appid").cast("double").cast("long").alias("appid"),
        col("name"),
        col("type"),

        col("is_free").cast("boolean"),

        col("mat_initial_price").cast("double"),
        col("mat_final_price").cast("double"),
        col("mat_discount_percent").cast("double"),
        col("mat_currency"),

        col("metacritic_score").cast("double"),
        col("recommendations_total").cast("double").cast("long"),
        col("mat_achievement_count").cast("double").cast("long"),

        col("mat_supports_windows").cast("boolean"),
        col("mat_supports_mac").cast("boolean"),
        col("mat_supports_linux").cast("boolean"),

        to_date(col("release_date")).alias("release_date"),
        expr("try_cast(required_age as int)").alias("required_age")
    )
)

bi_applications_df.printSchema()


display(bi_applications_df.limit(10))


from pyspark.sql.functions import countDistinct

bi_applications_df.select(
    countDistinct("appid").alias("distinct_appids"),
    count("*").alias("total_rows")
).show()


from pyspark.sql.functions import countDistinct

bi_applications_df.select(
    countDistinct("appid").alias("distinct_appids"),
    count("*").alias("total_rows")
).show()


from pyspark.sql.functions import col, count, when

total_apps = bi_applications_df.count()

null_counts = bi_applications_df.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in bi_applications_df.columns
])

display(null_counts)


null_percentages = bi_applications_df.select([
    (count(when(col(c).isNull(), c)) / total_apps * 100).alias(c)
    for c in bi_applications_df.columns
])

display(null_percentages)


zero_summary = bi_applications_df.select([
    count(when(col("mat_initial_price") == 0, "mat_initial_price")).alias("initial_price_zero"),
    count(when(col("mat_final_price") == 0, "mat_final_price")).alias("final_price_zero"),
    count(when(col("mat_discount_percent") == 0, "mat_discount_percent")).alias("discount_zero"),
    count(when(col("recommendations_total") == 0, "recommendations_zero"),
    ).alias("recommendations_zero")
])

display(zero_summary)


display(
    bi_applications_df.filter(
        (col("is_free") == True) & (col("mat_final_price") > 0)
    ).limit(10)
)


display(
    bi_applications_df.filter(
        (col("is_free") == False) & (col("mat_final_price").isNull())
    ).limit(10)
)


display(
    bi_applications_df.select(
        "mat_initial_price",
        "mat_final_price",
        "mat_discount_percent",
        "recommendations_total",
        "mat_achievement_count"
    ).summary("min", "50%", "90%", "95%", "99%", "max")
)


from pyspark.sql.functions import when, col

bi_applications_capped_df = (
    bi_applications_df
    .withColumn(
        "mat_initial_price_capped",
        when(col("mat_initial_price") > 6999, 6999)
        .otherwise(col("mat_initial_price"))
    )
    .withColumn(
        "mat_final_price_capped",
        when(col("mat_final_price") > 5999, 5999)
        .otherwise(col("mat_final_price"))
    )
    .withColumn(
        "recommendations_total_capped",
        when(col("recommendations_total") > 76489, 76489)
        .otherwise(col("recommendations_total"))
    )
    .withColumn(
        "mat_achievement_count_capped",
        when(col("mat_achievement_count") > 135, 135)
        .otherwise(col("mat_achievement_count"))
    )
)


from pyspark.sql.functions import max

display(
    bi_applications_capped_df.select(
        max("mat_initial_price_capped").alias("max_initial_price_capped"),
        max("mat_final_price_capped").alias("max_final_price_capped"),
        max("recommendations_total_capped").alias("max_recommendations_capped"),
        max("mat_achievement_count_capped").alias("max_achievement_count_capped")
    )
)


from pyspark.sql.functions import when

bi_applications_capped_df = (
    bi_applications_df
    .withColumn(
        "mat_initial_price_capped",
        when(col("mat_initial_price") > price_init_p99, price_init_p99)
        .otherwise(col("mat_initial_price"))
    )
    .withColumn(
        "mat_final_price_capped",
        when(col("mat_final_price") > price_final_p99, price_final_p99)
        .otherwise(col("mat_final_price"))
    )
    .withColumn(
        "recommendations_total_capped",
        when(col("recommendations_total") > reco_p99, reco_p99)
        .otherwise(col("recommendations_total"))
    )
    .withColumn(
        "mat_achievement_count_capped",
        when(col("mat_achievement_count") > ach_p99, ach_p99)
        .otherwise(col("mat_achievement_count"))
    )
)


display(
    bi_applications_capped_df.select(
        "mat_initial_price",
        "mat_initial_price_capped",
        "recommendations_total",
        "recommendations_total_capped"
    ).orderBy(col("recommendations_total").desc()).limit(10)
)


# #### parquet master copy


bi_applications_df.write.mode("overwrite").parquet(
    "s3://steam-dataset-2025-bucket/Omsai/applications/bi_applications/parquet/"
)


# #### CSV power BI friendly


bi_applications_df.coalesce(1) \
    .write.mode("overwrite") \
    .option("header", "true") \
    .csv("s3://steam-dataset-2025-bucket/Omsai/applications/bi_applications/csv/")


# #### parquet Bi + ML export friendly


bi_applications_capped_df.write.mode("overwrite").parquet(
    "s3://steam-dataset-2025-bucket/Omsai/applications/bi_applications_capped/parquet/"
)


# #### CSV powerBi cinsuption


bi_applications_capped_df.coalesce(1) \
    .write.mode("overwrite") \
    .option("header", "true") \
    .csv("s3://steam-dataset-2025-bucket/Omsai/applications/bi_applications_capped/csv/")