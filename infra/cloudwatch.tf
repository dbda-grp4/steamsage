resource "aws_cloudwatch_metric_alarm" "glue_job_failed" {
  alarm_name          = "glue-job-failure-alarm"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "GlueJobFailed"
  namespace           = "AWS/Glue"
  period              = 300
  statistic           = "Sum"
  threshold           = 1
  alarm_description   = "Glue job failed"
}
