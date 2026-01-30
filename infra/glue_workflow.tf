resource "aws_glue_crawler" "steam_crawler" {
  name          = "steam-curated-crawler"
  role          = aws_iam_role.glue_service_role.arn
  database_name = "steam_analytics"

  s3_target {
    path = "s3://steam-glue-roshani-2026/curated/"
  }
}
resource "aws_glue_trigger" "crawler_trigger" {
  name          = "crawler-trigger"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.steam_pipeline.name

  predicate {
    conditions {
      job_name = aws_glue_job.masterdata.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    crawler_name = aws_glue_crawler.steam_crawler.name
  }
}
