# Rock Songs ETL & Analytics Pipeline

## Overview
This project builds an end-to-end data pipeline for rock song airplay data:
- **Extract:** Raw data from S3
- **Transform:** Clean and standardize using AWS Glue (PySpark)
- **Load:** Store cleaned data in S3 and query via Athena
- **Enrich:** Add artist images using Spotify API
- **Analyze:** Generate insights with SQL queries

## Architecture
```
[S3] → [AWS Glue ETL] → [Cleaned Data in S3] → [AWS Glue Crawler] → [Athena Table] → [Queries & Insights]
```

## Setup
- AWS Account with S3, Glue, Athena
- Python 3.x, PySpark
- Spotify Developer credentials

## ETL Steps
1. Upload raw file to S3
2. Run Glue script to clean misaligned rows and normalize columns
3. Save cleaned data as Parquet in S3
4. Crawl cleaned data to create Athena table

## Enrichment
- Use Spotify API to fetch artist image URLs
- Store enriched dataset in S3 and crawl into Athena

## Example Queries
```sql
-- Top 10 most played artists
SELECT raw_artist, COUNT(*) AS times_played
FROM rock-songscleaned_rock_songs
GROUP BY raw_artist
ORDER BY times_played DESC
LIMIT 10;
```

## Improvements
- Add album, genre, popularity from Spotify
- Add station metadata via FCC API
- Automate pipeline with AWS Step Functions
