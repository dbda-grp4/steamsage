from pyspark.sql.functions import countDistinct, count
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

sc = SparkContext.getOrCreate()
spark = SparkSession.builder.getOrCreate()

# -----------------------------
# FIXED BASE PATHS
# -----------------------------

CURATED_BASE = "s3://steam-glue-roshani-2026/curated"

# -----------------------------
# INPUT PATHS (CURATED)
# -----------------------------

applications_path = f"{CURATED_BASE}/applications/bi_applications_capped/parquet/"

devs_path       = f"{CURATED_BASE}/dimensions/app_developers/"
publishers_path = f"{CURATED_BASE}/dimensions/app_publishers/"
genres_path     = f"{CURATED_BASE}/dimensions/app_genres/"
categories_path = f"{CURATED_BASE}/dimensions/app_categories/"
platforms_path  = f"{CURATED_BASE}/dimensions/app_platforms/"

# -----------------------------
# OUTPUT PATHS (CURATED – FINAL)
# -----------------------------

master_out_parquet = f"{CURATED_BASE}/masterdata/parquet/"
master_out_csv     = f"{CURATED_BASE}/masterdata/csv/"

# -----------------------------
# READ INPUT DATA
# -----------------------------

applications_df = spark.read.parquet(applications_path)

applications_df.select(
    countDistinct("appid").alias("distinct_appids"),
    count("*").alias("total_rows")
).show()

app_devs_df       = spark.read.parquet(devs_path)
app_publishers_df = spark.read.parquet(publishers_path)
app_genres_df     = spark.read.parquet(genres_path)
app_categories_df = spark.read.parquet(categories_path)
app_platforms_df  = spark.read.parquet(platforms_path)

# -----------------------------
# JOIN DIMENSIONS
# -----------------------------

master_df = applications_df.join(app_devs_df, on="appid", how="left")
master_df = master_df.join(app_publishers_df, on="appid", how="left")
master_df = master_df.join(app_genres_df, on="appid", how="left")
master_df = master_df.join(app_categories_df, on="appid", how="left")
master_df = master_df.join(app_platforms_df, on="appid", how="left")

master_df.select(
    countDistinct("appid").alias("distinct_appids"),
    count("*").alias("total_rows")
).show()

# -----------------------------
# WRITE FINAL MASTER DATA
# -----------------------------

master_df.write.mode("overwrite").parquet(master_out_parquet)

master_df.coalesce(1) \
    .write.mode("overwrite") \
    .option("header", "true") \
    .csv(master_out_csv)
