import sys
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, expr, when
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
        'RAW_BASE',    # Passed from Terraform
        'SILVER_BASE'  # Passed from Terraform
    ]
)

# --------------------------------------------------
# DYNAMIC PATHS (The Fix)
# --------------------------------------------------
# We use the arguments passed by Terraform instead of hardcoding
RAW_BASE = args['RAW_BASE']
SILVER_BASE = args['SILVER_BASE']

print(f"Starting Glue job: {args['JOB_NAME']}")
print(f"RAW_BASE: {RAW_BASE}")
print(f"SILVER_BASE: {SILVER_BASE}")

# Define Input/Output paths based on dynamic bases
applications_input = f"{RAW_BASE}/applications.csv"
applications_capped_parquet = f"{SILVER_BASE}/applications/bi_applications_capped/parquet/"
applications_out_parquet = f"{SILVER_BASE}/applications/bi_applications/parquet/"

# --------------------------------------------------
# Glue Context
# --------------------------------------------------
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# --------------------------------------------------
# Read RAW applications CSV
# --------------------------------------------------
applications_raw_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("multiLine", "true")
    .option("quote", "\"")
    .option("escape", "\"")
    .option("mode", "PERMISSIVE")
    .option("encoding", "UTF-8")
    .option("inferSchema", "false")
    .load(applications_input)
)

# --------------------------------------------------
# Select & cast BI-ready columns
# --------------------------------------------------
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

# --------------------------------------------------
# Data integrity check
# --------------------------------------------------
print("Performing Data Integrity Check...")
bi_applications_df.select(
    countDistinct("appid").alias("distinct_appids"),
    count("*").alias("total_rows")
).show()

# --------------------------------------------------
# Compute P99 thresholds (distribution-aware capping)
# --------------------------------------------------
# Note: approxQuantile can be expensive on large data; safe for this dataset size
price_init_p99 = bi_applications_df.approxQuantile("mat_initial_price", [0.99], 0.01)[0]
price_final_p99 = bi_applications_df.approxQuantile("mat_final_price", [0.99], 0.01)[0]
reco_p99 = bi_applications_df.approxQuantile("recommendations_total", [0.99], 0.01)[0]
ach_p99 = bi_applications_df.approxQuantile("mat_achievement_count", [0.99], 0.01)[0]

print("P99 thresholds:")
print("price_init_p99:", price_init_p99)
print("price_final_p99:", price_final_p99)
print("reco_p99:", reco_p99)
print("ach_p99:", ach_p99)

# --------------------------------------------------
# Apply capping
# --------------------------------------------------
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

# --------------------------------------------------
# Write SILVER outputs (Parquet only)
# --------------------------------------------------
print(f"Writing uncapped data to: {applications_out_parquet}")
bi_applications_df.write.mode("overwrite").parquet(applications_out_parquet)

print(f"Writing capped data to: {applications_capped_parquet}")
bi_applications_capped_df.write.mode("overwrite").parquet(applications_capped_parquet)

print("Applications job completed successfully.")
