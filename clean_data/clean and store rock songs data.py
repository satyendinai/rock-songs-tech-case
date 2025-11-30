from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, trim, regexp_replace, from_unixtime
from pyspark.sql.types import StringType, IntegerType

# Raw source data
file_path = "s3://rockstars-techcase-rock-songs-s3/rock-songs-raw-data.txt"

# Data Destination
output_path = "s3://rockstars-techcase-rock-songs-s3/cleaned-rock-songs/"

# Read raw text file
df = spark.read.text(file_path)

# Handle misaligned rows (where column _c7 is not empty)
df_misaligned_colum_rows = df.filter(regexp_extract(col("value"), r'(?:[^;]*;){7}([^;]*)', 1) != "")

# Extract columns using regex
df_misaligned_colum_rows = (
    df_misaligned_colum_rows
        .withColumn("TIME", regexp_extract(col("value"), r'(\d{10})', 1))
        .withColumn("UNIQUE_ID", regexp_extract(col("value"), r'([A-Z]{2,6}\d{4})', 1))
        .withColumn("CALLSIGN", regexp_extract(col("UNIQUE_ID"), r'([A-Z]+)', 1))
        .withColumn("COMBINED", regexp_extract(col("value"), r'[^;]* by [^;]*', 0))
        .withColumn("COMBINED", trim(regexp_replace(col("COMBINED"), r'"', "")))
        # Use last " by " for splitting song and artist
        .withColumn("RAW_SONG", trim(regexp_extract(col("COMBINED"), r'^(.*) by [^ ]+.*$', 1)))
        .withColumn("RAW_ARTIST", trim(regexp_extract(col("COMBINED"), r' by ([^ ]+.*)$', 1)))
        .withColumn("First?", regexp_extract(col("value"), r'\b(0|1)\b', 1))
        .drop("value")
)

# Set column order
df_misaligned_columns_cleaned = df_misaligned_colum_rows.select(
    "RAW_SONG", "RAW_ARTIST", "CALLSIGN", "TIME", "UNIQUE_ID", "COMBINED", "First?"
)

# Handle correctly aligned rows
df_correct_aligned_column_rows = (
    spark.read.option("header", "true").option("delimiter", ";").csv(file_path)
)

# Filter where column _c7 is empty
df_correct_aligned_column_rows = df_correct_aligned_column_rows.filter(
    (col("_c7").isNull()) | (trim(col("_c7")) == "")
)

# Extract columns using regex (split by last " by ")
df_correct_aligned_column_rows = (
    df_correct_aligned_column_rows
        .withColumn("RAW_SONG", trim(regexp_extract(col("COMBINED"), r'^(.*) by [^ ]+.*$', 1)))
        .withColumn("RAW_ARTIST", trim(regexp_extract(col("COMBINED"), r' by ([^ ]+.*)$', 1)))
)

# Set column order
df_correct_aligned_columns_cleaned = df_correct_aligned_column_rows.select(
    "RAW_SONG", "RAW_ARTIST", "CALLSIGN", "TIME", "UNIQUE_ID", "COMBINED", "First?"
)

# Combine both sets
df_combined = df_misaligned_columns_cleaned.unionByName(
    df_correct_aligned_columns_cleaned, allowMissingColumns=True
)

# Set datatypes
df_combined = (
    df_combined.withColumn("RAW_SONG", col("RAW_SONG").cast(StringType()))
               .withColumn("RAW_ARTIST", col("RAW_ARTIST").cast(StringType()))
               .withColumn("CALLSIGN", col("CALLSIGN").cast(StringType()))
               .withColumn("UNIQUE_ID", col("UNIQUE_ID").cast(StringType()))
               .withColumn("COMBINED", col("COMBINED").cast(StringType()))
               .withColumn("TIME", from_unixtime(col("TIME").cast("long")).cast("timestamp"))
               .withColumn("First?", col("First?").cast(IntegerType()))
)

# Save cleaned data as Parquet
df_combined.write.mode("overwrite").parquet(output_path)
