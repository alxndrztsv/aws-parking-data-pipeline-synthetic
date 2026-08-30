import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import coalesce, col, concat, floor, lit, upper, when

# Initialize Glue Context.
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Get parameters.
args = getResolvedOptions(sys.argv, ["JOB_NAME", "source_bucket", "target_bucket"])
source_bucket = args["source_bucket"]
target_bucket = args["target_bucket"]

source_path = f"s3://{source_bucket}/raw/"
target_path = f"s3://{target_bucket}/processed/"

print(f"Reading streamlined data from: {source_path}")

# Read from Bronze Layer.
df = spark.read.option("header", "true").option("inferSchema", "true").csv(source_path)

# Check 1: Input must not be empty.
assert df.limit(1).count() > 0, "Bronze input dataset is empty."

# Check 2: Required source columns must exist.
required_source_columns = [
    "PAYMENT_MEAN",
    "ORIGIN",
    "SERVER_DATE",
    "METER_DATE",
    "METER_CODE",
    "AMOUNT",
    "TOTAL_DURATION",
    "PAID_DURATION",
    "SYSTEM_ID",
    "PRINTED_ID",
    "ZONE_DESC",
    "CIRCUIT_DESC",
    "PARK_CODE",
    "PARK_NAME",
    "METER_DESC",
    "ADDRESS",
    "USER_NUMBER",
    "END_DATE",
    "FREE_DURATION",
    "CURRENCY",
    "CARD_TRANS_ID"
]

missing_columns = [
    c for c in required_source_columns
    if c not in df.columns
]

assert not missing_columns, (
    f"Missing required Bronze columns: {missing_columns}"
)

# Check 3: Certain columns must be populated.
assert (
    df.filter(
        col("METER_CODE").isNotNull() |
        col("SYSTEM_ID").isNotNull() |
        col("PARK_CODE").isNotNull()
    ).limit(1).count() > 0
), "No usable terminal/transaction/park identifiers found."

# --- PySpark transformations ---
# Step 1: Select and Rename only relevant columns.
# snake_case for athena compatability.
df = df.select(
    col("PAYMENT_MEAN").alias("payment_mean"),
    col("ORIGIN").alias("origin"),
    col("SERVER_DATE").alias("server_time"),
    col("METER_DATE").alias("terminal_date"),
    col("METER_CODE").alias("terminal_code"),
    col("AMOUNT").cast("double").alias("amount"),
    col("TOTAL_DURATION").cast("int").alias("total_duration_sec"),
    col("PAID_DURATION").cast("int").alias("paid_duration_sec"),
    col("SYSTEM_ID").alias("system_id"),
    col("PRINTED_ID").alias("printed_id"),
    col("ZONE_DESC").alias("zone_desc"),
    col("CIRCUIT_DESC").alias("circuit_desc"),
    col("PARK_CODE").alias("park_code"),
    col("PARK_NAME").alias("park"),
    col("METER_DESC").alias("terminal_description"),
    col("ADDRESS").alias("address"),
    col("USER_NUMBER").alias("user_type"),
    col("END_DATE").alias("end_date"),
    col("FREE_DURATION").cast("int").alias("free_duration"),
    col("CURRENCY").alias("currency"),
    col("CARD_TRANS_ID").alias("banking_id")
)

# Check 4: No negative values in certain columns.
negative_duration_count = df.filter(
    (col("total_duration_sec") < 0) |
    (col("paid_duration_sec") < 0) |
    (col("free_duration") < 0) |
    (col("amount") < 0)
).limit(1).count()

assert negative_duration_count == 0, (
    "Negative values detected in total_duration_sec, paid_duration_sec, free_duration or amount."
)

# Check 5: Paid duration cannot exceed total duration.
invalid_duration_count = df.filter(
    col("paid_duration_sec").isNotNull() &
    col("total_duration_sec").isNotNull() &
    (col("paid_duration_sec") > col("total_duration_sec"))
).limit(1).count()

assert invalid_duration_count == 0, (
    "Paid duration exceeds total duration."
)

# Step 2: Calculate minutes.
df = df.withColumn("total_duration_in_mins", floor(col("total_duration_sec") / 60)) \
       .withColumn("paid_duration_in_mins", floor(col("paid_duration_sec") / 60))

# Step 3: Format Durations to "X h Y m".
# Use integer division and modulo to get hours and minutes, then concatenate.
df = df.withColumn("total_duration", 
    concat(floor(coalesce(col("total_duration_sec"), lit(0)) / 3600).cast("string"), lit(" h "), 
           floor((coalesce(col("total_duration_sec"), lit(0)) % 3600) / 60).cast("string"), lit(" m")))

df = df.withColumn("paid_duration", 
    concat(floor(coalesce(col("paid_duration_sec"), lit(0)) / 3600).cast("string"), lit(" h "), 
           floor((coalesce(col("paid_duration_sec"), lit(0)) % 3600) / 60).cast("string"), lit(" m")))

# Drop the temporary seconds columns.
df = df.drop("total_duration_sec", "paid_duration_sec")

# Step 4: Clean up payment_mean.
df = df.withColumn(
    "payment_mean",
    when(upper(col("payment_mean")) == "COINS", "Coins")
    .when(upper(col("payment_mean")) == "CARD", "Card")
    .otherwise(col("payment_mean"))
)

# Check 6: raw uppercase values should no longer exist
raw_payment_count = df.filter(
    col("payment_mean").isin(["COINS", "CARD"])
).limit(1).count()

assert raw_payment_count == 0, (
    "payment_mean normalization failed: COINS or CARD still exists."
)

# Step 5: Add empty columns.
df = df.withColumn("type", lit("")) \
       .withColumn("product_name", lit("")) \
       .withColumn("user_name", lit(""))

# Step 6: Final column ordering.
final_columns = [
    "payment_mean", "origin", "server_time", "terminal_date", "terminal_code", 
    "amount", "total_duration", "paid_duration", "total_duration_in_mins", 
    "paid_duration_in_mins", "system_id", "printed_id", "zone_desc", "circuit_desc", 
    "park_code", "park", "terminal_description", "address", "type", "user_type", 
    "end_date", "free_duration", "currency", "banking_id", "product_name", "user_name"
]
# --- PySpark transformations completed ---

# Ensure all columns exist before selecting.
for c in final_columns:
    if c not in df.columns:
        df = df.withColumn(c, lit("").cast("string"))

df = df.select(final_columns)

# Check 7: Final schema must contain exactly the expected columns.
assert df.columns == final_columns, (
    f"Final schema mismatch.\n"
    f"Expected: {final_columns}\n"
    f"Actual: {df.columns}"
)

# Check 8: Final row count must be > 0
final_count = df.limit(1).count()

assert final_count > 0, (
    "Final Silver dataset is empty."
)

# Check 9: Required business identifiers should not all be NULL
identifier_check = df.filter(
    col("terminal_code").isNotNull() &
    col("system_id").isNotNull() &
    col("park_code").isNotNull()
).limit(1).count()

assert identifier_check > 0, (
    "Final dataset contains no usable identifiers."
)

print(f"Writing processed streamlined data to: {target_path}")

# Write to Silver Layer.
df.write.option("header", "true").mode("overwrite").csv(target_path)

# Commit Job.
job.commit()
print("Streamlined job completed successfully!")