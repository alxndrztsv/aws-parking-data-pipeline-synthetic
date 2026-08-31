# IAM Role allowing EventBridge to start Step Functions and publish to SNS
resource "aws_iam_role" "eventbridge_role" {
  name = "${local.prefix}-eventbridge-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "events.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy" "eventbridge_policy" {
  name = "${local.prefix}-eventbridge-policy"
  role = aws_iam_role.eventbridge_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "states:StartExecution"
        Resource = aws_sfn_state_machine.parking_pipeline_orchestrator.arn
      },
      {
        Effect   = "Allow"
        Action   = "sns:Publish"
        Resource = aws_sns_topic.parking_pipeline_alerts.arn
      }
    ]
  })
}

# EventBridge schedule rule to trigger pipeline weekly on Mondays at 08:00 UTC
resource "aws_cloudwatch_event_rule" "weekly_parking_pipeline" {
  name                = "${var.project_name}-weekly-pipeline"
  description         = "Triggers the parking data pipeline weekly on Mondays at 08:00 UTC"
  schedule_expression = "cron(0 8 ? * MON *)"
}

# EventBridge target (points to Step Functions machine state)
resource "aws_cloudwatch_event_target" "step_function_target" {
  rule      = aws_cloudwatch_event_rule.weekly_parking_pipeline.name
  target_id = "StepFunctionTarget"
  arn       = aws_sfn_state_machine.parking_pipeline_orchestrator.arn
  role_arn  = aws_iam_role.eventbridge_role.arn

  retry_policy {
    maximum_event_age_in_seconds = 3600
    maximum_retry_attempts       = 2
  }
}

resource "aws_cloudwatch_event_rule" "parking_pipeline_failed" {
  name        = "${var.project_name}-pipeline-failed"
  description = "Catches Step Functions execution failures"

  event_pattern = jsonencode({
    source      = ["aws.states"]
    detail-type = ["Step Functions Execution Status Change"]
    detail = {
      status          = ["FAILED"]
      stateMachineArn = [aws_sfn_state_machine.parking_pipeline_orchestrator.arn]
    }
  })
}

# EventBridge target to send failure events to SNS
resource "aws_cloudwatch_event_target" "failure_notification" {
  rule      = aws_cloudwatch_event_rule.parking_pipeline_failed.name
  target_id = "SNSFailureTarget"
  arn       = aws_sns_topic.parking_pipeline_alerts.arn
  role_arn  = aws_iam_role.eventbridge_role.arn
}