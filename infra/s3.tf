# S3 bucket for Glue scripts and data
resource "aws_s3_bucket" "glue_bucket" {
  bucket = "steam-glue-roshani-2026"

  tags = {
    Project = "Steam Data Pipeline"
    Owner   = "Terraform"
  }
}

# Create folder for Glue scripts
resource "aws_s3_object" "glue_scripts_folder" {
  bucket = aws_s3_bucket.glue_bucket.id
  key    = "glue-scripts/"
}

# Create folder for raw data
resource "aws_s3_object" "raw_folder" {
  bucket = aws_s3_bucket.glue_bucket.id
  key    = "raw/"
}

# Create folder for curated data
resource "aws_s3_object" "curated_folder" {
  bucket = aws_s3_bucket.glue_bucket.id
  key    = "curated/"
}
