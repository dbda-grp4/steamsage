# Generated from: masterdata.ipynb
# Converted at: 2026-01-28T04:21:32.115Z
# Next step (optional): refactor into modules & generate tests with RunCell
# Quick start: pip install runcell

applications_df = (
    spark.read
    .option("header", "true")
    .csv("/Volumes/workspace/default/rawdata/steam/bi_applications_capped.csv")
)

reviews_app_df = (
    spark.read
    .option("header", "true")
    .csv("/Volumes/workspace/default/rawdata/steam/app_review_agg.csv")
)


# Schema check
applications_df.printSchema()


# Row counts
print("Applications rows:", applications_df.count())


# Sample rows
display(applications_df.limit(5))



reviews_app_df.printSchema()
print("Reviews (app-level) rows:", reviews_app_df.count())
display(reviews_app_df.limit(5))

master_step1_df = (
    applications_df
    .join(
        reviews_app_df,
        on="appid",
        how="left"
    )
)


# Row count check (CRITICAL)
print("Rows after join:", master_step1_df.count())

# Check for duplication of appid
from pyspark.sql.functions import countDistinct, count

master_step1_df.select(
    countDistinct("appid").alias("distinct_appids"),
    count("*").alias("total_rows")
).show()

# Sample rows
display(master_step1_df.limit(5))


app_developers_agg_df = (
    spark.read.option("header", "true")
    .csv("/Volumes/workspace/default/rawdata/steam/processed/aggregates/app_developers/")
)

app_publishers_agg_df = (
    spark.read.option("header", "true")
    .csv("/Volumes/workspace/default/rawdata/steam/processed/aggregates/app_publishers/")
)

app_genres_agg_df = (
    spark.read.option("header", "true")
    .csv("/Volumes/workspace/default/rawdata/steam/processed/aggregates/app_genres/")
)

app_categories_agg_df = (
    spark.read.option("header", "true")
    .csv("/Volumes/workspace/default/rawdata/steam/processed/aggregates/app_categories/")
)

app_platforms_agg_df = (
    spark.read.option("header", "true")
    .csv("/Volumes/workspace/default/rawdata/steam/processed/aggregates/app_platforms/")
)


# Row counts
print("Developers agg rows:", app_developers_agg_df.count())
print("Publishers agg rows:", app_publishers_agg_df.count())
print("Genres agg rows:", app_genres_agg_df.count())
print("Categories agg rows:", app_categories_agg_df.count())
print("Platforms agg rows:", app_platforms_agg_df.count())

app_developers_agg_df.printSchema()
app_publishers_agg_df.printSchema()
app_genres_agg_df.printSchema()
app_categories_agg_df.printSchema()
app_platforms_agg_df.printSchema()

display(app_developers_agg_df.limit(5))
display(app_platforms_agg_df.limit(5))

master_step2_df = (
    master_step1_df
    .join(
        app_developers_agg_df,
        on="appid",
        how="left"
    )
)


# Row count must stay the same
print("Rows after developers join:", master_step2_df.count())

# Uniqueness check
from pyspark.sql.functions import countDistinct

master_step2_df.select(
    countDistinct("appid").alias("distinct_appids"),
    count("*").alias("total_rows")
).show()

# Sample check
display(master_step2_df.select("appid", "name", "developers").limit(10))


master_step3_df = (
    master_step2_df
    .join(
        app_publishers_agg_df,
        on="appid",
        how="left"
    )
)


# Row count check
print("Rows after publishers join:", master_step3_df.count())

# Uniqueness check
from pyspark.sql.functions import countDistinct

master_step3_df.select(
    countDistinct("appid").alias("distinct_appids"),
    count("*").alias("total_rows")
).show()

# Sample validation
display(
    master_step3_df
    .select("appid", "name", "publishers")
    .limit(10)
)


master_step4_df = (
    master_step3_df
    .join(
        app_genres_agg_df,
        on="appid",
        how="left"
    )
)


# Row count check
print("Rows after genres join:", master_step4_df.count())

# Uniqueness check
from pyspark.sql.functions import countDistinct

master_step4_df.select(
    countDistinct("appid").alias("distinct_appids"),
    count("*").alias("total_rows")
).show()

# Sample validation
display(
    master_step4_df
    .select("appid", "name", "genres")
    .limit(10)
)


master_step5_df = (
    master_step4_df
    .join(
        app_categories_agg_df,
        on="appid",
        how="left"
    )
)


# Row count check
print("Rows after categories join:", master_step5_df.count())

# Uniqueness check
from pyspark.sql.functions import countDistinct

master_step5_df.select(
    countDistinct("appid").alias("distinct_appids"),
    count("*").alias("total_rows")
).show()

# Sample validation
display(
    master_step5_df
    .select("appid", "name", "categories")
    .limit(10)
)


masterdata_df = (
    master_step5_df
    .join(
        app_platforms_agg_df,
        on="appid",
        how="left"
    )
)


# Row count check (must remain unchanged)
print("Rows after platforms join:", masterdata_df.count())

# Uniqueness check (MOST IMPORTANT)
from pyspark.sql.functions import countDistinct

masterdata_df.select(
    countDistinct("appid").alias("distinct_appids"),
    count("*").alias("total_rows")
).show()

# Sample rows (wide view)
display(
    masterdata_df.select(
        "appid", "name", "developers", "publishers",
        "genres", "categories", "platforms"
    ).limit(10)
)


display(masterdata_df.limit(5))

display(reviews_app_df.limit(5))

masterdata_df.coalesce(1) \
    .write.mode("overwrite") \
    .option("header", "true") \
    .csv("/Volumes/workspace/default/rawdata/steam/processed/masterdata_exports/masterdata/")


master_step1_df.coalesce(1) \
    .write.mode("overwrite") \
    .option("header", "true") \
    .csv("/Volumes/workspace/default/rawdata/steam/processed/masterdata_exports/master_step1_app_reviews/")


master_step2_df.coalesce(1) \
    .write.mode("overwrite") \
    .option("header", "true") \
    .csv("/Volumes/workspace/default/rawdata/steam/processed/masterdata_exports/master_step2_developers/")


master_step3_df.coalesce(1) \
    .write.mode("overwrite") \
    .option("header", "true") \
    .csv("/Volumes/workspace/default/rawdata/steam/processed/masterdata_exports/master_step3_publishers/")


master_step4_df.coalesce(1) \
    .write.mode("overwrite") \
    .option("header", "true") \
    .csv("/Volumes/workspace/default/rawdata/steam/processed/masterdata_exports/master_step4_genres/")


master_step5_df.coalesce(1) \
    .write.mode("overwrite") \
    .option("header", "true") \
    .csv("/Volumes/workspace/default/rawdata/steam/processed/masterdata_exports/master_step5_categories/")


dbutils.fs.ls("/Volumes/workspace/default/rawdata/steam/processed/masterdata_exports/")