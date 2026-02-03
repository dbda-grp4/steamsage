# -----------------------------
# Glue Jobs
# -----------------------------

locals {
  glue_scripts_path = "s3://${aws_s3_bucket.data_lake.bucket}/glue-scripts"
}

# -----------------------------
# Applications Job
# -----------------------------
resource "aws_glue_job" "applications_job" {
  name     = "${var.project_name}-applications-job-${var.environment}"
  role_arn = aws_iam_role.glue_role.arn

  glue_version       = var.glue_version
  worker_type        = var.glue_worker_type
  number_of_workers  = var.glue_number_of_workers
  timeout            = var.glue_timeout

  command {
    name            = "glueetl"
    script_location = "${local.glue_scripts_path}/applications_job.py"
    python_version  = "3"
  }

  default_arguments = {
    "--RAW_BASE"    = "s3://${aws_s3_bucket.data_lake.bucket}/raw"
    "--SILVER_BASE" = "s3://${aws_s3_bucket.data_lake.bucket}/silver"
    "--job-language" = "python"
  }

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}

# -----------------------------
# Reviews Job
# -----------------------------
resource "aws_glue_job" "reviews_job" {
  name     = "${var.project_name}-reviews-job-${var.environment}"
  role_arn = aws_iam_role.glue_role.arn

  glue_version       = var.glue_version
  worker_type        = var.glue_worker_type
  number_of_workers  = var.glue_number_of_workers
  timeout            = var.glue_timeout

  command {
    name            = "glueetl"
    script_location = "${local.glue_scripts_path}/reviews_job.py"
    python_version  = "3"
  }

  default_arguments = {
    "--RAW_BASE"    = "s3://${aws_s3_bucket.data_lake.bucket}/raw"
    "--SILVER_BASE" = "s3://${aws_s3_bucket.data_lake.bucket}/silver"
    "--job-language" = "python"
  }

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}

# -----------------------------
# Dimensions Job
# -----------------------------
resource "aws_glue_job" "dimensions_job" {
  name     = "${var.project_name}-dimensions-job-${var.environment}"
  role_arn = aws_iam_role.glue_role.arn

  glue_version       = var.glue_version
  worker_type        = var.glue_worker_type
  number_of_workers  = var.glue_number_of_workers
  timeout            = var.glue_timeout

  command {
    name            = "glueetl"
    script_location = "${local.glue_scripts_path}/dimensions_job.py"
    python_version  = "3"
  }

  default_arguments = {
    "--RAW_BASE"    = "s3://${aws_s3_bucket.data_lake.bucket}/raw"
    "--SILVER_BASE" = "s3://${aws_s3_bucket.data_lake.bucket}/silver"
    "--job-language" = "python"
  }

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}

# -----------------------------
# Master Job (GOLD)
# -----------------------------
resource "aws_glue_job" "master_job" {
  name     = "${var.project_name}-master-job-${var.environment}"
  role_arn = aws_iam_role.glue_role.arn

  glue_version       = var.glue_version
  worker_type        = var.glue_worker_type
  number_of_workers  = var.glue_number_of_workers
  timeout            = var.glue_timeout

  command {
    name            = "glueetl"
    script_location = "${local.glue_scripts_path}/master_job.py"
    python_version  = "3"
  }

  default_arguments = {
    "--SILVER_BASE" = "s3://${aws_s3_bucket.data_lake.bucket}/silver"
    "--GOLD_BASE"   = "s3://${aws_s3_bucket.data_lake.bucket}/gold"
    "--job-language" = "python"
  }

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}

# -----------------------------
# Glue Crawler (Gold Layer)
# -----------------------------
resource "aws_glue_crawler" "gold_crawler" {
  name          = "${var.project_name}-gold-crawler-${var.environment}"
  database_name = aws_athena_database.steam_db.name
  role          = aws_iam_role.glue_role.arn

  s3_target {
    path = "s3://${aws_s3_bucket.data_lake.bucket}/gold/"
  }

  # Automatically add new columns if schema evolves, but only log deletions
  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}
