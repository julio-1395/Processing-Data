from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col
from pyspark.sql.functions import lit
import requests
import json

# Initialize Spark session
spark = SparkSession.builder \
    .appName("API to S3 ETL with PySpark") \
    .getOrCreate()

# Define API endpoint and OAuth token
api_endpoint = "https://example.com/api/electronics"
oauth_token = "YOUR_OAUTH_TOKEN_HERE"

# Function to fetch data from API
def fetch_data_from_api():
    headers = {
        "Authorization": f"Bearer {oauth_token}"
    }
    response = requests.get(api_endpoint, headers=headers)
    return response.json()

# Fetch data from API
api_data = fetch_data_from_api()

# Convert JSON data into DataFrame
products_df = spark.createDataFrame(api_data["products"])
orders_df = spark.createDataFrame(api_data["orders"])
sales_df = spark.createDataFrame(api_data["sales"])
costs_df = spark.createDataFrame(api_data["costs"])

# Write DataFrames to S3
products_df.write.mode("overwrite").parquet("s3://your-bucket-name/products")
orders_df.write.mode("overwrite").parquet("s3://your-bucket-name/orders")
sales_df.write.mode("overwrite").parquet("s3://your-bucket-name/sales")
costs_df.write.mode("overwrite").parquet("s3://your-bucket-name/costs")

# Stop Spark session
spark.stop()
