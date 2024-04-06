import pandas as pd
import boto3
from io import StringIO

# Initialize AWS S3 client
s3 = boto3.client('s3')

# Define function to read CSV files from S3
def read_csv_from_s3(bucket, key):
    response = s3.get_object(Bucket=bucket, Key=key)
    body = response['Body'].read().decode('utf-8')
    return pd.read_csv(StringIO(body))

# Read CSV files for each month
months = ['January', 'February', 'March', 'April', 'May', 'June']
dfs = []
for month in months:
    df = read_csv_from_s3('Sales', f'{month}.csv')
    dfs.append(df)

# Merge all DataFrames into one
merged_df = pd.concat(dfs, ignore_index=True)

# Remove duplicate, null, and empty data
merged_df.drop_duplicates(inplace=True)
merged_df.dropna(inplace=True)
merged_df.replace('', pd.NA, inplace=True)
merged_df.dropna(inplace=True)

# Save merged DataFrame to CSV
csv_buffer = StringIO()
merged_df.to_csv(csv_buffer, index=False)

# Upload CSV to S3
s3.put_object(Body=csv_buffer.getvalue(), Bucket='Silver Sales', Key='1st Semester/merged_data.csv')

