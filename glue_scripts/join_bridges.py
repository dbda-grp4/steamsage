from pyspark.sql.functions import countDistinct, count
from pyspark.sql.functions import collect_set, concat_ws, col
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

sc = SparkContext.getOrCreate()
spark = SparkSession.builder.getOrCreate()

# -----------------------------
# FIXED BASE PATHS
# -----------------------------

RAW_BASE = "s3://steam-glue-roshani-2026/raw"
CURATED_BASE = "s3://steam-glue-roshani-2026/curated/dimensions"

# -----------------------------
# INPUT PATHS (RAW)
# -----------------------------

app_devs_path       = f"{RAW_BASE}/application_developers/application_developers.csv"
developers_path     = f"{RAW_BASE}/developers/developers.csv"

app_publishers_path = f"{RAW_BASE}/application_publishers/application_publishers.csv"
publishers_path     = f"{RAW_BASE}/publishers/publishers.csv"

app_genres_path     = f"{RAW_BASE}/application_genres/application_genres.csv"
genres_path         = f"{RAW_BASE}/genres/genres.csv"

app_categories_path = f"{RAW_BASE}/application_categories/application_categories.csv"
categories_path     = f"{RAW_BASE}/categories/categories.csv"

app_platforms_path  = f"{RAW_BASE}/application_platforms/application_platforms.csv"
platforms_path      = f"{RAW_BASE}/platforms/platforms.csv"

# -----------------------------
# OUTPUT PATHS (CURATED)
# -----------------------------

out_devs       = f"{CURATED_BASE}/app_developers/"
out_publishers = f"{CURATED_BASE}/app_publishers/"
out_genres     = f"{CURATED_BASE}/app_genres/"
out_categories = f"{CURATED_BASE}/app_categories/"
out_platforms  = f"{CURATED_BASE}/app_platforms/"

# -----------------------------
# READ RAW FILES
# -----------------------------

app_devs_df = spark.read.option("header", "true").csv(app_devs_path)
developers_df = spark.read.option("header", "true").csv(developers_path)

app_publishers_df = spark.read.option("header", "true").csv(app_publishers_path)
publishers_df = spark.read.option("header", "true").csv(publishers_path)

app_genres_df = spark.read.option("header", "true").csv(app_genres_path)
genres_df = spark.read.option("header", "true").csv(genres_path)

app_categories_df = spark.read.option("header", "true").csv(app_categories_path)
categories_df = spark.read.option("header", "true").csv(categories_path)

app_platforms_df = spark.read.option("header", "true").csv(app_platforms_path)
platforms_df = spark.read.option("header", "true").csv(platforms_path)

# -----------------------------
# AGGREGATIONS
# -----------------------------

app_developers_agg_df = (
    app_devs_df
    .join(developers_df,
          app_devs_df.developer_id == developers_df.id,
          how="left")
    .groupBy(app_devs_df.appid)
    .agg(concat_ws(", ", collect_set(col("name"))).alias("developers"))
)

app_publishers_agg_df = (
    app_publishers_df
    .join(publishers_df,
          app_publishers_df.publisher_id == publishers_df.id,
          how="left")
    .groupBy(app_publishers_df.appid)
    .agg(concat_ws(", ", collect_set(col("name"))).alias("publishers"))
)

app_genres_agg_df = (
    app_genres_df
    .join(genres_df,
          app_genres_df.genre_id == genres_df.id,
          how="left")
    .groupBy(app_genres_df.appid)
    .agg(concat_ws(", ", collect_set(col("name"))).alias("genres"))
)

app_categories_agg_df = (
    app_categories_df
    .join(categories_df,
          app_categories_df.category_id == categories_df.id,
          how="left")
    .groupBy(app_categories_df.appid)
    .agg(concat_ws(", ", collect_set(col("name"))).alias("categories"))
)

app_platforms_agg_df = (
    app_platforms_df
    .join(platforms_df,
          app_platforms_df.platform_id == platforms_df.id,
          how="left")
    .groupBy(app_platforms_df.appid)
    .agg(concat_ws(", ", collect_set(col("name"))).alias("platforms"))
)

# -----------------------------
# SANITY CHECK
# -----------------------------

app_developers_agg_df.select(
    countDistinct("appid").alias("distinct_appids"),
    count("*").alias("rows")
).show()

# -----------------------------
# WRITE OUTPUT
# -----------------------------

app_developers_agg_df.write.mode("overwrite").parquet(out_devs)
app_publishers_agg_df.write.mode("overwrite").parquet(out_publishers)
app_genres_agg_df.write.mode("overwrite").parquet(out_genres)
app_categories_agg_df.write.mode("overwrite").parquet(out_categories)
app_platforms_agg_df.write.mode("overwrite").parquet(out_platforms)
