resource "aws_glue_job" "applications" {
  name     = "tf-steam-applications-etl"
  role_arn = aws_iam_role.glue_service_role.arn

  command {
    name            = "glueetl"
    script_location = "s3://steam-glue-roshani-2026/glue-scripts/applications_transformations.py"
    python_version  = "3"
  }

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 60
  max_retries       = 1

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-glue-datacatalog"          = "true"
    "--job-bookmark-option"              = "job-bookmark-enable"
    "--TempDir"                          = "s3://steam-glue-roshani-2026/temp/"
  }
}
