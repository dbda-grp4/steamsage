from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import sys
import traceback

print("🔥 GLUE DIAGNOSTIC JOB STARTED")

try:
    # 1️⃣ Confirm Spark is alive
    spark = SparkSession.builder.getOrCreate()
    print("✅ SparkSession created successfully")

    # 2️⃣ Test READ access (this SHOULD work)
    test_read_path = (
        "s3://steam-dataset-2025-bucket/"
        "Steam/steam_dataset_2025_csv_package_v1/"
        "steam_dataset_2025_csv/application_developers.csv"
    )

    print(f"🔍 Testing READ access on: {test_read_path}")
    read_df = spark.read.option("header", "true").csv(test_read_path)
    read_count = read_df.count()

    print(f"✅ READ SUCCESS — rows read: {read_count}")

    # 3️⃣ Create a tiny test dataframe
    test_df = spark.range(1).withColumn("test_col", lit("glue_write_test"))

    # 4️⃣ Test WRITE access (this SHOULD FAIL in your environment)
    test_write_path = "s3://steam-dataset-2025-bucket/glue_write_test_output/"

    print(f"🧪 Testing WRITE access on: {test_write_path}")
    test_df.write.mode("overwrite").parquet(test_write_path)

    # If this line prints, then WRITE actually worked
    print("🚨 UNEXPECTED: WRITE SUCCEEDED (S3 write is allowed!)")

except Exception as e:
    print("❌ GLUE DIAGNOSTIC FAILURE DETECTED")
    print("📌 This failure is INTENTIONAL and DIAGNOSTIC")

    print("\n🔴 Exception type:")
    print(type(e).__name__)

    print("\n🔴 Exception message:")
    print(str(e))

    print("\n🔴 Full stack trace:")
    traceback.print_exc(file=sys.stdout)

    print("\n🧠 INTERPRETATION:")
    print(
        "If READ succeeded but WRITE failed, then:\n"
        "- Your Spark code is correct\n"
        "- Glue is working\n"
        "- S3 WRITE is blocked by IAM/SCP\n"
        "- This is an ENVIRONMENT limitation, not a code bug"
    )

    # ❗ Force Glue job to FAIL clearly
    raise RuntimeError(
        "GLUE DIAGNOSTIC FAILURE: "
        "S3 WRITE ACCESS IS BLOCKED. "
        "THIS IS NOT A PYSPARK LOGIC ERROR."
    )

finally:
    print("🧯 GLUE DIAGNOSTIC JOB FINISHED")
