import sys
import requests
import re
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import col, udf, regexp_replace
from pyspark.sql.types import StringType

# Initialize Glue and Spark
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Spotify api creds
CLIENT_ID = "a50b3c06af264d76ba38743ca13c59c9"
CLIENT_SECRET = "63fe9cac7a424b91a34d7b65735434df"

# Connect to Spotify API
auth_response = requests.post(
    "https://accounts.spotify.com/api/token",
    data={"grant_type": "client_credentials"},
    auth=(CLIENT_ID, CLIENT_SECRET)
)
ACCESS_TOKEN = auth_response.json().get("access_token")
HEADERS = {"Authorization": f"Bearer {ACCESS_TOKEN}"}

# Load cleaned data from S3
file_path = "s3://rockstars-techcase-rock-songs-s3/cleaned-rock-songs/"
df_cleaned = spark.read.parquet(file_path)

# Get unique artists
df_artists = df_cleaned.select("RAW_ARTIST").distinct()

# Clean artist names (just to be sure)
def clean_artist(name):
    if not name:
        return None
    name = name.replace("&amp;amp;", "&amp;")
    name = re.sub(r"\s*/\s*", " ", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name

# Get Spotify image URL
def get_spotify_image(artist):
    if not artist:
        return None
    artist = clean_artist(artist)

    try:
        search_url = "https://api.spotify.com/v1/search"
        params = {"q": artist, "type": "artist", "limit": 1}
        response = requests.get(search_url, headers=HEADERS, params=params)
        data = response.json()

        if "artists" in data and data["artists"]["items"]:
            images = data["artists"]["items"][0].get("images", [])
            if images:
                return images[0]["url"]
    except Exception:
        return None

    return None

# Store URL in column image_url
get_image_udf = udf(get_spotify_image, StringType())
df_artists_with_images = df_artists.withColumn("image_url", get_image_udf(col("RAW_ARTIST")))

# Write to S3 as Parquet
output_path = "s3://rockstars-techcase-rock-songs-s3/artist-images/"

df_artists_with_images.select("RAW_ARTIST", "image_url") \
    .write.mode("overwrite").parquet(output_path)