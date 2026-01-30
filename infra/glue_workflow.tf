resource "aws_glue_workflow" "steam_pipeline" {
  name = "steam-etl-workflow"
}

resource "aws_glue_trigger" "applications_trigger" {
  name          = "applications-trigger"
  type          = "ON_DEMAND"
  workflow_name = aws_glue_workflow.steam_pipeline.name

  actions {
    job_name = aws_glue_job.applications.name
  }
}

resource "aws_glue_trigger" "reviews_trigger" {
  name          = "reviews-trigger"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.steam_pipeline.name

  predicate {
    conditions {
      job_name = aws_glue_job.applications.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = aws_glue_job.reviews.name
  }
}

resource "aws_glue_trigger" "dimensions_trigger" {
  name          = "dimensions-trigger"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.steam_pipeline.name

  predicate {
    conditions {
      job_name = aws_glue_job.reviews.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = aws_glue_job.dimensions.name
  }
}

resource "aws_glue_trigger" "masterdata_trigger" {
  name          = "masterdata-trigger"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.steam_pipeline.name

  predicate {
    conditions {
      job_name = aws_glue_job.dimensions.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = aws_glue_job.masterdata.name
  }
}
