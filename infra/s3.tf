resource "aws_s3_bucket" "glue_bucket" {
  bucket = "steam-glue-roshani-2026"

  tags = {
    Project = "Steam Data Pipeline"
    Owner   = "Terraform"
  }

  lifecycle {
    prevent_destroy = true
  }
}
