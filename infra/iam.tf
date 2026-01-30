########################################
# IAM ROLE FOR AWS GLUE
########################################
resource "aws_iam_role" "glue_service_role" {
  name = "terraform-glue-service-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

########################################
# AWS MANAGED GLUE POLICY
########################################
resource "aws_iam_role_policy_attachment" "glue_service_policy" {
  role       = aws_iam_role.glue_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

########################################
# GENERIC S3 ACCESS
########################################
resource "aws_iam_role_policy" "glue_s3_policy" {
  name = "terraform-glue-s3-policy"
  role = aws_iam_role.glue_service_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = "*"
      }
    ]
  })
}

########################################
# ACCESS TO GLUE BUCKET
########################################
resource "aws_iam_role_policy" "glue_s3_bucket_access" {
  name = "terraform-glue-s3-access"
  role = aws_iam_role.glue_service_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.glue_bucket.arn,
          "${aws_s3_bucket.glue_bucket.arn}/*"
        ]
      }
    ]
  })
}

########################################
# GLUE WORKFLOW + JOB + CRAWLER AUTOMATION
########################################
resource "aws_iam_role_policy" "glue_automation_policy" {
  name = "terraform-glue-automation-policy"
  role = aws_iam_role.glue_service_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "glue:StartJobRun",
          "glue:GetJobRun",
          "glue:GetJobRuns",
          "glue:StartWorkflowRun",
          "glue:GetWorkflowRun",
          "glue:GetWorkflowRuns",
          "glue:StartCrawler",
          "glue:GetCrawler",
          "glue:GetCrawlers"
        ]
        Resource = "*"
      }
    ]
  })
}

########################################
# CLOUDWATCH LOGS & METRICS (MONITORING)
########################################
resource "aws_iam_role_policy" "glue_cloudwatch_policy" {
  name = "terraform-glue-cloudwatch-policy"
  role = aws_iam_role.glue_service_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "cloudwatch:PutMetricData"
        ]
        Resource = "*"
      }
    ]
  })
}
