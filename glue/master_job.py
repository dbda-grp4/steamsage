import sys
from pyspark.context import SparkContext
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
        'SILVER_BASE',
        'GOLD_BASE'
    ]
)

SILVER_BASE = args['SILVER_BASE']
GOLD_BASE = args['GOLD_BASE']

print(f"Starting Glue job: {args['JOB_NAME']}")
print(f"SILVER_BASE: {SILVER_BASE}")
print(f"GOLD_BASE: {GOLD_BASE}")


# --------------------------------------------------
# Glue Context
# --------------------------------------------------
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session


# --------------------------------------------------
# Input paths (SILVER)
# --------------------------------------------------
applications_path = f"{SILVER_BASE}/applications/bi_applications_capped/parquet/"

devs_path       = f"{SILVER_BASE}/dimensions/app_developers/"
publishers_path = f"{SILVER_BASE}/dimensions/app_publishers/"
genres_path     = f"{SILVER_BASE}/dimensions/app_genres/"
categories_path = f"{SILVER_BASE}/dimensions/app_categories/"
platforms_path  = f"{SILVER_BASE}/dimensions/app_platforms/"


# --------------------------------------------------
# Output path (GOLD)
# --------------------------------------------------
master_out_parquet = f"{GOLD_BASE}/masterdata/parquet/"


# --------------------------------------------------
# Read SILVER datasets
# --------------------------------------------------
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


# --------------------------------------------------
# Join all dimensions (LEFT joins to preserve apps)
# --------------------------------------------------
master_df = applications_df.join(app_devs_df, on="appid", how="left")
master_df = master_df.join(app_publishers_df, on="appid", how="left")
master_df = master_df.join(app_genres_df, on="appid", how="left")
master_df = master_df.join(app_categories_df, on="appid", how="left")
master_df = master_df.join(app_platforms_df, on="appid", how="left")


# --------------------------------------------------
# Post-join integrity check
# --------------------------------------------------
master_df.select(
    countDistinct("appid").alias("distinct_appids"),
    count("*").alias("total_rows")
).show()


# --------------------------------------------------
# Write GOLD output (Parquet only)
# --------------------------------------------------
master_df.write.mode("overwrite").parquet(master_out_parquet)

print("Master job completed successfully.")
