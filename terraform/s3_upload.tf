# Upload scripts and synthetic data to S3

locals {
    glue_scripts_dir = "../glue_scripts"
    bronze_data_dir  = "../data/bronze"
}

# Upload all PySpark scripts to the scripts bucket
resource "aws_s3_object" "glue_scripts" {
    for_each = fileset(local.glue_scripts_dir, "*.py")

    bucket = aws_s3_bucket.layers["scripts"].id
    key    = each.value
    source = "${local.glue_scripts_dir}/${each.value}"
    etag   = filemd5("${local.glue_scripts_dir}/${each.value}")
}

# Upload all synthetic CSV data to Bronze bucket under 'raw/' folder
resource "aws_s3_object" "bronze_data" {
	# Loop through every .csv file in local data/bronze folder
    for_each = fileset(local.bronze_data_dir, "*.csv")

    bucket = aws_s3_bucket.layers["bronze"].id
    key    = "raw/${each.value}"
    source = "${local.bronze_data_dir}/${each.value}"
    etag   = filemd5("${local.bronze_data_dir}/${each.value}")
}