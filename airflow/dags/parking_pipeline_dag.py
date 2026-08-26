import json
import logging
import os
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.sns import SnsHook
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerRunOperator
from airflow.providers.amazon.aws.operators.lambda_function import (
    LambdaInvokeFunctionOperator,
)
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig

log = logging.getLogger(__name__)

# Configuration from environment variables
AWS_CONN_ID = os.environ.get(
    "AWS_CONN_ID",
    "aws_default",
)

BRONZE_TO_SILVER_GLUE_JOB = os.environ.get(
    "BRONZE_TO_SILVER_GLUE_JOB",
    "parking-pipeline-dev-bronze-to-silver",
)

SILVER_TO_GOLD_GLUE_JOB = os.environ.get(
    "SILVER_TO_GOLD_GLUE_JOB",
    "parking-pipeline-dev-silver-to-gold",
)

SILVER_CRAWLER = os.environ.get(
    "SILVER_CRAWLER",
    "parking-pipeline-dev-silver-crawler",
)

EMAIL_LAMBDA_FUNCTION = os.environ.get(
    "EMAIL_LAMBDA_FUNCTION",
    "parking-pipeline-dev-email-reporter",
)

SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN")

DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/parking_analytics")
profiles_yml_path = os.path.join(DBT_PROJECT_DIR, "profiles.yml")


def notify_failure(context):
    """Publishes a formatted message to SNS on task failure."""
    task_instance = context.get("task_instance")
    logical_date = context.get('logical_date')
    exception = context.get("exception")
    message = (
        "Parking data pipeline failed.\n"
        f"DAG: {task_instance.dag_id if task_instance else 'unknown'}\n"
        f"Task: {task_instance.task_id if task_instance else 'unknown'}\n"
        f"Run ID: {context.get('run_id')}\n"
        f"Execution Date: {logical_date}\n"
        f"Error: {exception!s}\n"
        "Please check Airflow and CloudWatch logs."
    )

    if SNS_TOPIC_ARN:
        try:
            hook = SnsHook(aws_conn_id=AWS_CONN_ID)
            hook.publish_to_target(target_arn=SNS_TOPIC_ARN, message=message)
            log.info("Failure alert published to SNS topic: %s", SNS_TOPIC_ARN)
        except Exception:
            log.exception("Failed to publish failure alert to SNS")

    log.error(message)


@dag(
    dag_id = "parking_data_pipeline",
    description="Replaces AWS Step Functions with Airflow for parking data pipeline",
    schedule="0 6 * * 1",  # every monday at 06:00 UTC,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["parking", "aws", "etl", "weekly-report"],
    default_args = {
        "owner": "parking-data-pipeline",
        "depends_on_past": False,
        "retries": 0,
        "retry_delay": timedelta(minutes=2),
        "retry_exponential_backoff": True,
        "on_failure_callback": notify_failure,
    },
)
def parking_data_pipeline():

    @task
    def verify_lambda_response(response_payload: dict | str | bytes) -> dict:
        """Validates the response from the Lambda function."""
        if response_payload is None:
            raise ValueError("No response received from Lambda function.")

        if isinstance(response_payload, bytes):
            response_payload = response_payload.decode("utf-8")

        if isinstance(response_payload, str):
            try:
                result = json.loads(response_payload)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Lambda response is not valid JSON: {response_payload}") from exc
        elif isinstance(response_payload, dict):
            result = response_payload
        else:
            raise TypeError(f"Unsupported Lambda response type: {type(response_payload)}")

        log.info("Lambda response validated successfully.")
        if result.get("statusCode") != 200:
            raise RuntimeError(f"Lambda function failed with status {result.get('statusCode')}: {result.get('body')}")
            
        return result

    # --- Cosmos dbt Configuration ---
    profile_config = ProfileConfig(
        profile_name="parking_analytics",
        target_name="dev",
        profiles_yml_filepath=profiles_yml_path,
    )

    # --- Tasks ---
    bronze_to_silver = GlueJobOperator(
        task_id="bronze_to_silver",
        job_name=BRONZE_TO_SILVER_GLUE_JOB,
        aws_conn_id=AWS_CONN_ID,
        wait_for_completion=True,
        deferrable=True,
    )  

    silver_crawler = GlueCrawlerRunOperator(
        task_id="run_silver_crawler",
        crawler_name=SILVER_CRAWLER,
        aws_conn_id=AWS_CONN_ID,
        wait_for_completion=True,
        deferrable=True,
    )

    silver_to_gold = GlueJobOperator(
        task_id="silver_to_gold",
        job_name=SILVER_TO_GOLD_GLUE_JOB,
        aws_conn_id=AWS_CONN_ID,
        wait_for_completion=True,
        deferrable=True,
    )

    dbt_models = DbtTaskGroup(
        group_id="dbt_models",
        project_config=ProjectConfig(DBT_PROJECT_DIR),
        profile_config=profile_config,
        execution_config=ExecutionConfig(dbt_executable_path="dbt"),
    )

    send_email_report = LambdaInvokeFunctionOperator(
        task_id="send_email_report",
        function_name=EMAIL_LAMBDA_FUNCTION,
        aws_conn_id=AWS_CONN_ID,
    )

    # TaskFlow API automatically passes the XCom result of send_email_report to this task.
    verify_email = verify_lambda_response(send_email_report.output)

    # --- Dependencies ---
    bronze_to_silver >> silver_crawler >> silver_to_gold  >> dbt_models>> send_email_report >> verify_email

# Instantiate the DAG
parking_data_pipeline()