import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext

# Initialize Spark and Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

print("===================================")
print("AWS GLUE JOB STARTED SUCCESSFULLY")
print("Glue version is working")
print("IAM Role attached correctly")
print("S3 access is available")
print("===================================")

# Simple Spark operation (to prove Spark works)
data = [("Roshani", "Terraform"), ("AWS", "Glue")]
df = spark.createDataFrame(data, ["Name", "]()
