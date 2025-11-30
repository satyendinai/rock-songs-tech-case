# Technical Documentation: Rock Songs ETL & Analytics Pipeline

## 1. Project Overview
This project implements an end-to-end data pipeline for rock song airplay data. It covers:
- **Extract:** Raw data stored in AWS S3.
- **Transform:** Data cleaning using AWS Glue (PySpark).
- **Load:** Store cleaned data in S3 and query via Athena.
- **Enrich:** Add artist image URLs using Spotify API.
- **Analyze:** Generate insights using Athena SQL queries.

## 2. Architecture
```
[S3 Raw Data] → [AWS Glue ETL Script] → [Cleaned Data in S3] → [AWS Glue Crawler] → [Athena Table] → [Queries & Insights]
```

## 3. Setup & Requirements
- AWS Account with S3, Glue, Athena.
- Python 3.x and PySpark.
- Spotify Developer credentials for API access.

### Required Python Libraries:
- `pyspark`
- `requests`
- `awsglue`

## 4. ETL Steps
### 4.1 Upload Raw Data
Upload the raw file `rock-songs-raw-data.txt` to an S3 bucket.

### 4.2 Data Cleaning in AWS Glue
The Glue script handles misaligned rows and columns using regex.

#### Key Steps:
- Detect misaligned rows using regex.
- Extract columns: TIME, CALLSIGN, RAW_SONG, RAW_ARTIST, UNIQUE_ID, COMBINED, First?.
- Convert TIME from epoch to timestamp.
- Infer RAW_SONG and RAW_ARTIST from COMBINED.
- Save cleaned data as Parquet in S3.

#### Code Snippet:
```python
# Read raw text file
df = spark.read.text(file_path)

# Extract columns using regex
df_misaligned_colum_rows = (
    df.withColumn("TIME", regexp_extract(col("value"), r'(\d{10})', 1))
      .withColumn("CALLSIGN", regexp_extract(col("value"), r'([A-Z]{3,4})', 1))
      .withColumn("COMBINED", regexp_extract(col("value"), r'[^;]* by [^;]*', 0))
)

# Save cleaned data
df_combined.write.mode("overwrite").parquet(output_path)
```

## 5. Enrichment with Spotify API
- Authenticate using Client ID and Secret.
- Fetch artist image URLs via Spotify Search endpoint.
- Store enriched dataset in S3 as Parquet.

#### Code Snippet:
```python
search_url = "https://api.spotify.com/v1/search"
params = {"q": artist, "type": "artist", "limit": 1}
response = requests.get(search_url, headers=HEADERS, params=params)
```

## 6. Queries & Insights
Example Athena queries:
```sql
-- Top 10 most played artists
SELECT raw_artist, COUNT(*) AS times_played
FROM rock-songscleaned_rock_songs
GROUP BY raw_artist
ORDER BY times_played DESC
LIMIT 10;

-- Hourly trends
SELECT hour(time) AS play_hour, COUNT(*) AS play_count
FROM rock-songscleaned_rock_songs
GROUP BY hour(time)
ORDER BY play_count DESC;
```

## 7. Automation & CI/CD
- Store code in GitHub for version control.

## 8. Future Enhancements
- Combine Glue scripts into a single ETL job
- Use AWS Secrets Manager for API keys.
- Optimize Athena queries with partitions.
- Validate data types during cleaning.
- Add album, genre, popularity from Spotify.
- Enrich station metadata via FCC API.
- Correlate plays with weather data using NOAA API.
- Build dashboards in QuickSight or Power BI.
