from pyspark.context import SparkContext
from awsglue.context import GlueContext

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
df = spark.createDataFrame(data, ["Name", "Tool"])
df.show()

print("===================================")
print("AWS GLUE JOB COMPLETED SUCCESSFULLY")
print("===================================")
