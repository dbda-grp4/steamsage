from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import col, trim, countDistinct, count

# --------------------------------------------------
# Spark / Glue init
# --------------------------------------------------
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# --------------------------------------------------
# Paths
# --------------------------------------------------
RAW_BASE = "s3://steam-dataset-2025-bucket/Steam/steam_dataset_2025_csv_package_v1/steam_dataset_2025_csv"
SILVER_BASE = "s3://steam-dataset-2025-bucket/silver/bridges"

# --------------------------------------------------
# Read raw bridge tables
# --------------------------------------------------
app_devs_df = spark.read.option("header", "true").csv(f"{RAW_BASE}/application_developers.csv")
developers_df = spark.read.option("header", "true").csv(f"{RAW_BASE}/developers.csv")

app_pubs_df = spark.read.option("header", "true").csv(f"{RAW_BASE}/application_publishers.csv")
publishers_df = spark.read.option("header", "true").csv(f"{RAW_BASE}/publishers.csv")

app_genres_df = spark.read.option("header", "true").csv(f"{RAW_BASE}/application_genres.csv")
genres_df = spark.read.option("header", "true").csv(f"{RAW_BASE}/genres.csv")

app_categories_df = spark.read.option("header", "true").csv(f"{RAW_BASE}/application_categories.csv")
categories_df = spark.read.option("header", "true").csv(f"{RAW_BASE}/categories.csv")

app_platforms_df = spark.read.option("header", "true").csv(f"{RAW_BASE}/application_platforms.csv")
platforms_df = spark.read.option("header", "true").csv(f"{RAW_BASE}/platforms.csv")

# ==================================================
# DEVELOPERS (1 row per app per developer)
# ==================================================
app_developers_rows_df = (
    app_devs_df
    .join(developers_df, app_devs_df.developer_id == developers_df.id, "left")
    .select(
        app_devs_df.appid.cast("long").alias("appid"),
        trim(col("name")).alias("developer")
    )
    .filter(col("developer").isNotNull())
    .dropDuplicates()
)

app_developers_rows_df.write.mode("overwrite").parquet(
    f"{SILVER_BASE}/app_developers_rows/"
)

# ==================================================
# PUBLISHERS
# ==================================================
app_publishers_rows_df = (
    app_pubs_df
    .join(publishers_df, app_pubs_df.publisher_id == publishers_df.id, "left")
    .select(
        app_pubs_df.appid.cast("long").alias("appid"),
        trim(col("name")).alias("publisher")
    )
    .filter(col("publisher").isNotNull())
    .dropDuplicates()
)

app_publishers_rows_df.write.mode("overwrite").parquet(
    f"{SILVER_BASE}/app_publishers_rows/"
)

# ==================================================
# GENRES
# ==================================================
app_genres_rows_df = (
    app_genres_df
    .join(genres_df, app_genres_df.genre_id == genres_df.id, "left")
    .select(
        app_genres_df.appid.cast("long").alias("appid"),
        trim(col("name")).alias("genre")
    )
    .filter(col("genre").isNotNull())
    .dropDuplicates()
)

app_genres_rows_df.write.mode("overwrite").parquet(
    f"{SILVER_BASE}/app_genres_rows/"
)

# ==================================================
# CATEGORIES
# ==================================================
app_categories_rows_df = (
    app_categories_df
    .join(categories_df, app_categories_df.category_id == categories_df.id, "left")
    .select(
        app_categories_df.appid.cast("long").alias("appid"),
        trim(col("name")).alias("category")
    )
    .filter(col("category").isNotNull())
    .dropDuplicates()
)

app_categories_rows_df.write.mode("overwrite").parquet(
    f"{SILVER_BASE}/app_categories_rows/"
)

# ==================================================
# PLATFORMS
# ==================================================
app_platforms_rows_df = (
    app_platforms_df
    .join(platforms_df, app_platforms_df.platform_id == platforms_df.id, "left")
    .select(
        app_platforms_df.appid.cast("long").alias("appid"),
        trim(col("name")).alias("platform")
    )
    .filter(col("platform").isNotNull())
    .dropDuplicates()
)

app_platforms_rows_df.write.mode("overwrite").parquet(
    f"{SILVER_BASE}/app_platforms_rows/"
)

