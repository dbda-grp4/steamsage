resource "aws_glue_job" "applications" {
  name     = "steam-applications-etl"
  role_arn = aws_iam_role.glue.arn

  command {
    name            = "glueetl"
    script_location = "s3://steam-glue-roshani-2026/glue-scripts/applications_transformations.py"
    python_version  = "3"
  }

  glue_version = "4.0"
  worker_type  = "G.1X"
  number_of_workers = 2
}

resource "aws_glue_job" "reviews" {
  name     = "steam-reviews-etl"
  role_arn = aws_iam_role.glue.arn

  command {
    name            = "glueetl"
    script_location = "s3://steam-glue-roshani-2026/glue-scripts/reviews_transformations.py"
    python_version  = "3"
  }

  glue_version = "4.0"
  worker_type  = "G.1X"
  number_of_workers = 2
}

resource "aws_glue_job" "dimensions" {
  name     = "steam-dimensions-etl"
  role_arn = aws_iam_role.glue.arn

  command {
    name            = "glueetl"
    script_location = "s3://steam-glue-roshani-2026/glue-scripts/join_bridges.py"
    python_version  = "3"
  }

  glue_version = "4.0"
  worker_type  = "G.1X"
  number_of_workers = 2
}

resource "aws_glue_job" "masterdata" {
  name     = "steam-masterdata-etl"
  role_arn = aws_iam_role.glue.arn

  command {
    name            = "glueetl"
    script_location = "s3://steam-glue-roshani-2026/glue-scripts/masterdata.py"
    python_version  = "3"
  }

  glue_version = "4.0"
  worker_type  = "G.1X"
  number_of_workers = 2
}
