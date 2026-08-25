# Define input variables used throughout Terraform configuration

variable "aws_region" {
    description = "AWS region for all resources."    
    type        = string
}

variable "environment" {
	description = "Environment (e.g., dev, staging, prod)."
	type        = string
}

variable "project_name" {
    description = "Name of project, used for resource naming."
    type        = string
}

variable "sender_email" {
	description = "Email address to send reports from (must be verified in SES)."
	type    	= string
}

variable "recipient_email" {
	description = "Email address to receive reports."
	type 		= string
}

variable "failure_notification_email" {
	description = "Email address for pipeline alerts"
	type        = string
	  
	validation {
		condition     = can(regex("^[\\w-\\.]+@([\\w-]+\\.)+[\\w-]{2,4}$", var.failure_notification_email))
		error_message = "Must be a valid email address."
	}
}
