# Create IAM Role for Step Functions
resource "aws_iam_role" "step_functions_role" {
    name = "${local.prefix}-step-functions-role"

    assume_role_policy = jsonencode({
        Version   = "2012-10-17"    
        Statement = [
            {
                Action    = "sts:AssumeRole"
                Effect    = "Allow"        
                Principal = {
                    Service = "states.amazonaws.com"
                }
            }
        ]
    })
}

resource "aws_iam_role_policy" "step_functions_policy" {
    name = "${local.prefix}-step-functions-policy"
    role = aws_iam_role.step_functions_role.id

    policy = jsonencode({
        Version   = "2012-10-17"    
        Statement = [
            {
                Effect   = "Allow"        
                Action   = [
                    "glue:GetJob",
                    "glue:StartJobRun",
                    "glue:GetJobRun",
                    "glue:GetJobRuns",
                    "glue:StartCrawler",
                    "glue:GetCrawler",
                    "glue:GetCrawlerMetrics",
                    "glue:BatchStopJobRun",          
                    "lambda:InvokeFunction"
                ]        
                Resource = [
                    aws_glue_job.bronze_to_silver.arn,
                    aws_glue_crawler.silver_crawler.arn,
                    aws_glue_job.silver_to_gold.arn,
                    aws_lambda_function.email_reporter.arn
                ]
            },
            {
                Effect   = "Allow"
                Action   = [
                    "events:PutTargets",
                    "events:PutRule",
                    "events:DescribeRule"
                ]
                Resource = "*"
            },
            # Allow publishing to SNS for pipeline failures
            {
                Effect   = "Allow"
                Action   = "sns:Publish"
                Resource = aws_sns_topic.parking_pipeline_alerts.arn
            }
        ]
    })
}

# State Functions State Machine definition
resource "aws_sfn_state_machine" "parking_pipeline_orchestrator" {
    name     = "${local.prefix}-pipeline-orchestrator"
    role_arn = aws_iam_role.step_functions_role.arn

    definition = jsonencode({
        Comment = "Orchestrates the Bronze->Silver->SilverCrawler->Gold->Email pipeline"
        StartAt = "BronzeToSilver"
	  
        States = {
            # Bronze to Silver Glue job
            BronzeToSilver = {
                Type     = "Task"
                Resource = "arn:aws:states:::glue:startJobRun.sync"    

                Parameters = {
                    JobName = aws_glue_job.bronze_to_silver.name
                }
                
                Retry = [
                    {
                        ErrorEquals     = ["States.ALL"]
                        IntervalSeconds = 60
                        MaxAttempts     = 2
                        BackoffRate     = 2.0
                    }
                ]
                    
                Catch = [
                    {
                        ErrorEquals = ["States.ALL"]
                        Next        = "NotifyFailure"
                    }
                ]
                    
                Next = "SilverCrawlerStart"
            }

            # Silver Crawler
            # 1. Start the crawler
            SilverCrawlerStart = {
                Type     = "Task"
                Resource = "arn:aws:states:::aws-sdk:glue:startCrawler"

                Parameters = {
                    Name = aws_glue_crawler.silver_crawler.name
                }

                Catch = [
                    {
                        ErrorEquals = ["States.ALL"]
                        Next        = "NotifyFailure"
                    }
                ]
                    
                Next = "SilverCrawlerWait"
            }

            # 2. Wait for a short period before checking status (prevents API throttling)
            SilverCrawlerWait = {
                Type    = "Wait"
                Seconds = 30
                Next    = "SilverCrawlerCheck"
            }

            # 3. Check the current state of the crawler
            SilverCrawlerCheck = {
                Type     = "Task"
                Resource = "arn:aws:states:::aws-sdk:glue:getCrawler"

                Parameters = {
                    Name = aws_glue_crawler.silver_crawler.name
                }

                Catch = [
                    {
                        ErrorEquals = ["States.ALL"]
                        Next        = "NotifyFailure"
                    }
                ]

                Next = "SilverCrawlerReady"
            }

            # 4. Evaluate if the crawler has finished (State returns to "READY" when done)
            SilverCrawlerReady = {
                Type    = "Choice"
                Choices = [
                    {
                        Variable     = "$.Crawler.State"
                        StringEquals = "READY"
                        Next         = "SilverToGold"
                    }
                ]
                # If it's still "RUNNING" or "STOPPING", loop back to wait
                Default = "SilverCrawlerWait" 
            }
            
            # Silver to Gold Glue job
            SilverToGold = {
                Type     = "Task"
                Resource = "arn:aws:states:::glue:startJobRun.sync"
                    
                Parameters = {
                    JobName = aws_glue_job.silver_to_gold.name
                }
                
                Retry = [
                    {
                        ErrorEquals     = ["States.ALL"]
                        IntervalSeconds = 60
                        MaxAttempts     = 2
                        BackoffRate     = 2.0
                    }
                ]
                    
                Catch = [
                    {
                        ErrorEquals = ["States.ALL"]
                        Next        = "NotifyFailure"
                    }
                ]
                
                Next = "SendEmailReport"
            }
            
            # Send email lambda function
            SendEmailReport = {
                Type       = "Task"
                Resource   = "arn:aws:states:::lambda:invoke"
                
                Parameters = {
                    FunctionName = aws_lambda_function.email_reporter.arn
                }
                
                Retry = [
                    {
                        ErrorEquals = ["States.ALL"]
                        MaxAttempts = 2
                    }
                ]
            
                Catch = [
                    {
                        ErrorEquals = ["States.ALL"]
                        Next        = "NotifyFailure"
                    }
                ]
                
                End = true
            }
            
            # Connect to the SNS topic
            NotifyFailure = {
                Type       = "Task"
                Resource   = "arn:aws:states:::sns:publish"
                
                Parameters = {
                    TopicArn = aws_sns_topic.parking_pipeline_alerts.arn
                    Message  = "The parking data pipeline failed. Please check AWS Step Functions and CloudWatch logs for details."
                }
                
                End = true
            }
        }	  
	})
}

output "step_function_arn" {
    description = "ARN of the Step Functions state machine"
    value       = aws_sfn_state_machine.parking_pipeline_orchestrator.arn
}