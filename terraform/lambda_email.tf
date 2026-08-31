data "aws_caller_identity" "current" {}

# Create IAM Role for Lambda
resource "aws_iam_role" "lambda_email_role" {
  name = "${local.prefix}-lambda-email-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })
}

# Attach policies for CloudWatch Logs, S3 read, and SES send
resource "aws_iam_role_policy" "lambda_policy" {
  name = "${local.prefix}-lambda-policy"
  role = aws_iam_role.lambda_email_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      # Allow CloudWatch logs
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${local.prefix}-email-reporter:*"
      },
      # Allow access to S3
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.layers["gold"].arn,
          "${aws_s3_bucket.layers["gold"].arn}/*"
        ]
      },
      # Allow sending email via SES
      {
        Effect = "Allow"
        Action = [
          "ses:SendEmail",
          "ses:SendRawEmail"
        ]
        Resource = "arn:aws:ses:${var.aws_region}:${data.aws_caller_identity.current.account_id}:identity/${var.sender_email}" # to any email address verified
      }
    ]
  })
}

# Package Lambda code into .zip file
data "archive_file" "lambda_zip" {
  type        = "zip"
  source_file = "../lambda_functions/email_reporter.py"
  output_path = "../lambda_functions/email_reporter.zip"
}

# Create Lambda function
resource "aws_lambda_function" "email_reporter" {
  filename         = data.archive_file.lambda_zip.output_path
  function_name    = "${local.prefix}-email-reporter"
  role             = aws_iam_role.lambda_email_role.arn
  handler          = "email_reporter.lambda_handler"
  runtime          = "python3.11"
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256

  environment {
    variables = {
      GOLD_BUCKET_NAME = aws_s3_bucket.layers["gold"].id
      SENDER_EMAIL     = var.sender_email
      RECIPIENT_EMAIL  = var.recipient_email
    }
  }

  timeout = 60
}

output "lambda_function_name" {
  value = aws_lambda_function.email_reporter.function_name
}