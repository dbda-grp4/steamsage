from pyspark.sql.functions import col, to_date, expr
from pyspark.sql.functions import countDistinct, count
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import when

sc = SparkContext.getOrCreate()
spark = SparkSession.builder.getOrCreate()

# -----------------------------
# FIXED BASE PATHS
# -----------------------------

RAW_BASE = "s3://steam-glue-roshani-2026/raw"
CURATED_BASE = "s3://steam-glue-roshani-2026/curated/applications"

applications_input = f"{RAW_BASE}/applications/applications.csv"

bi_out_parquet = f"{CURATED_BASE}/bi_applications/parquet/"
bi_out_csv     = f"{CURATED_BASE}/bi_applications/csv/"

bi_capped_parquet = f"{CURATED_BASE}/bi_applications_capped/parquet/"
bi_capped_csv     = f"{CURATED_BASE}/bi_applications_capped/csv/"

# -----------------------------
# READ RAW DATA
# -----------------------------

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

# -----------------------------
# SELECT & CAST
# -----------------------------

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

bi_applications_df.select(
    countDistinct("appid").alias("distinct_appids"),
    count("*").alias("total_rows")
).show()

# -----------------------------
# P99 CAPPING
# -----------------------------

price_init_p99 = bi_applications_df.approxQuantile("mat_initial_price", [0.99], 0.01)[0]
price_final_p99 = bi_applications_df.approxQuantile("mat_final_price", [0.99], 0.01)[0]
reco_p99 = bi_applications_df.approxQuantile("recommendations_total", [0.99], 0.01)[0]
ach_p99 = bi_applications_df.approxQuantile("mat_achievement_count", [0.99], 0.01)[0]

bi_applications_capped_df = (
    bi_applications_df
    .withColumn("mat_initial_price_capped",
                when(col("mat_initial_price") > price_init_p99, price_init_p99)
                .otherwise(col("mat_initial_price")))
    .withColumn("mat_final_price_capped",
                when(col("mat_final_price") > price_final_p99, price_final_p99)
                .otherwise(col("mat_final_price")))
    .withColumn("recommendations_total_capped",
                when(col("recommendations_total") > reco_p99, reco_p99)
                .otherwise(col("recommendations_total")))
    .withColumn("mat_achievement_count_capped",
                when(col("mat_achievement_count") > ach_p99, ach_p99)
                .otherwise(col("mat_achievement_count")))
)

# -----------------------------
# WRITE OUTPUT
# -----------------------------

bi_applications_df.write.mode("overwrite").parquet(bi_out_parquet)
bi_applications_capped_df.write.mode("overwrite").parquet(bi_capped_parquet)

bi_applications_df.coalesce(1) \
    .write.mode("overwrite") \
    .option("header", "true") \
    .csv(bi_out_csv)

bi_applications_capped_df.coalesce(1) \
    .write.mode("overwrite") \
    .option("header", "true") \
    .csv(bi_capped_csv)
