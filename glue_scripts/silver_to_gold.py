import json
import sys
from datetime import datetime, timedelta, timezone

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import avg, col, count, desc, round, sum, to_date, when

# Initialize Glue Context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Get parameters
args = getResolvedOptions(sys.argv, ["JOB_NAME", "source_bucket", "target_bucket"])
source_bucket = args["source_bucket"]
target_bucket = args["target_bucket"]

source_path = f"s3://{source_bucket}/processed/"
target_path = f"s3://{target_bucket}/weekly_summary/"

print(f"Reading Silver data from: {source_path}")

# Read from Silver layer
df = spark.read.option("header", "true").option("inferSchema", "true").csv(source_path)

# Calculate dates for previous week
now = datetime.now(tz=timezone.utc)
current_week_monday = now - timedelta(days=now.weekday())
last_week_monday = current_week_monday - timedelta(days=7)
last_week_sunday = current_week_monday - timedelta(days=1)

# --- Gold Layer transformations ---
# Step 1: Parse the date properly to ensure accurate filtering and sorting
df = df.withColumn("terminal_date", to_date(col("terminal_date"), "dd/MM/yyyy HH:mm"))

# Step 2: Filter for the previous week
df_filtered = df.filter(
    (col("terminal_date") >= last_week_monday) & 
    (col("terminal_date") < current_week_monday)
)

# Step 3: Aggregate by park
gold_df = df_filtered.groupBy("park") \
    .agg(
        round(sum("amount"), 2).alias("Total Revenue (EUR)"),
        count("terminal_code").alias("Total Transactions"),
        round(avg("amount"), 2).alias("Avg Transaction Value (EUR)"),
        round(avg("paid_duration_in_mins"), 0).alias("Avg Paid Duration (mins)"),
        sum(when(col("payment_mean") == "Card", 1).otherwise(0)).alias("Card Transactions"),
        sum(when(col("payment_mean") == "Coins", 1).otherwise(0)).alias("Coin Transactions")
    ) \
    .orderBy(desc("Total Revenue (EUR)"))
# --- Gold layer transformations completed ---

print(f"Writing Gold summary data to: {target_path}")

# Write to Gold layer
folder_name = (
    f"week_{last_week_monday.strftime('%d%m%Y')}_"
    f"{last_week_sunday.strftime('%d%m%Y')}/"
)
weekly_target_path = f"{target_path}{folder_name}"

# Generate unified .csv file
gold_df.coalesce(1).write.option("header", "true").mode("overwrite").csv(weekly_target_path)

# Create S3 pointer file
s3_client = boto3.client("s3")
pointer_data = {"folder_name": folder_name}
s3_client.put_object(
    Bucket=target_bucket,
    Key="weekly_summary/latest_report.json",
    Body=json.dumps(pointer_data)
)

# Commit job
job.commit()
print("Silver-to-Gold aggregation completed successfully!")