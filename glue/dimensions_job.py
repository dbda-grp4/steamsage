import sys
from pyspark.context import SparkContext
from pyspark.sql.functions import collect_set, concat_ws, col
from pyspark.sql.functions import countDistinct, count
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
# Input paths (RAW)
# --------------------------------------------------
app_devs_path       = f"{RAW_BASE}/application_developers.csv"
developers_path     = f"{RAW_BASE}/developers.csv"

app_publishers_path = f"{RAW_BASE}/application_publishers.csv"
publishers_path     = f"{RAW_BASE}/publishers.csv"

app_genres_path     = f"{RAW_BASE}/application_genres.csv"
genres_path         = f"{RAW_BASE}/genres.csv"

app_categories_path = f"{RAW_BASE}/application_categories.csv"
categories_path     = f"{RAW_BASE}/categories.csv"

app_platforms_path  = f"{RAW_BASE}/application_platforms.csv"
platforms_path      = f"{RAW_BASE}/platforms.csv"


# --------------------------------------------------
# Output paths (SILVER)
# --------------------------------------------------
out_devs       = f"{SILVER_BASE}/app_developers/"
out_publishers = f"{SILVER_BASE}/app_publishers/"
out_genres     = f"{SILVER_BASE}/app_genres/"
out_categories = f"{SILVER_BASE}/app_categories/"
out_platforms  = f"{SILVER_BASE}/app_platforms/"


# --------------------------------------------------
# Read RAW CSVs
# --------------------------------------------------
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


# --------------------------------------------------
# Aggregate dimensions (1 row per appid)
# --------------------------------------------------
app_developers_agg_df = (
    app_devs_df
    .join(
        developers_df,
        app_devs_df.developer_id == developers_df.id,
        how="left"
    )
    .groupBy(app_devs_df.appid)
    .agg(
        concat_ws(", ", collect_set(col("name"))).alias("developers")
    )
)

app_publishers_agg_df = (
    app_publishers_df
    .join(
        publishers_df,
        app_publishers_df.publisher_id == publishers_df.id,
        how="left"
    )
    .groupBy(app_publishers_df.appid)
    .agg(
        concat_ws(", ", collect_set(col("name"))).alias("publishers")
    )
)

app_genres_agg_df = (
    app_genres_df
    .join(
        genres_df,
        app_genres_df.genre_id == genres_df.id,
        how="left"
    )
    .groupBy(app_genres_df.appid)
    .agg(
        concat_ws(", ", collect_set(col("name"))).alias("genres")
    )
)

app_categories_agg_df = (
    app_categories_df
    .join(
        categories_df,
        app_categories_df.category_id == categories_df.id,
        how="left"
    )
    .groupBy(app_categories_df.appid)
    .agg(
        concat_ws(", ", collect_set(col("name"))).alias("categories")
    )
)

app_platforms_agg_df = (
    app_platforms_df
    .join(
        platforms_df,
        app_platforms_df.platform_id == platforms_df.id,
        how="left"
    )
    .groupBy(app_platforms_df.appid)
    .agg(
        concat_ws(", ", collect_set(col("name"))).alias("platforms")
    )
)


# --------------------------------------------------
# Integrity check (example on developers)
# --------------------------------------------------
app_developers_agg_df.select(
    countDistinct("appid").alias("distinct_appids"),
    count("*").alias("rows")
).show()


# --------------------------------------------------
# Write SILVER dimension outputs (Parquet only)
# --------------------------------------------------
app_developers_agg_df.write.mode("overwrite").parquet(out_devs)
app_publishers_agg_df.write.mode("overwrite").parquet(out_publishers)
app_genres_agg_df.write.mode("overwrite").parquet(out_genres)
app_categories_agg_df.write.mode("overwrite").parquet(out_categories)
app_platforms_agg_df.write.mode("overwrite").parquet(out_platforms)

print("Dimensions job completed successfully.")
