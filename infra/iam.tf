resource "aws_iam_role" "glue_service_role" {
  name = "terraform-glue-service-role"

  lifecycle {
    prevent_destroy = true
  }

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
      Action = "sts:AssumeRole"
    }]
  })
}
