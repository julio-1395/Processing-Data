import requests
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import json

# API endpoint URLs
api_base_url = 'https://api.supermarket.com'
products_url = f'{api_base_url}/products'
orders_url = f'{api_base_url}/orders'
customers_url = f'{api_base_url}/customers'
channels_url = f'{api_base_url}/channels'

# Azure Blob Storage connection string and container name
connection_string = 'your_connection_string'
container_name = 'your_container_name'

# Function to extract data from the API
def extract_data(api_url):
    response = requests.get(api_url, headers={'Authorization': 'Bearer your_oauth_token'})
    return response.json()

# Function to transform data and check for null or empty values
def transform_data(data):
    # Your transformation logic here
    transformed_data = data  # Placeholder, replace with actual transformation
    
    # Check for null or empty values
    for key, value in transformed_data.items():
        if value is None or value == "":
            print(f"Warning: Null or empty value found for {key}")

    return transformed_data

# Function to load data into Azure Blob Storage
def load_data_to_blob(data, filename):
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(filename)
    
    blob_client.upload_blob(json.dumps(data), overwrite=True)
    print(f"Data uploaded to Azure Blob Storage: {filename}")

# Main function to orchestrate the ETL process
def main():
    # Extract data from API
    products_data = extract_data(products_url)
    orders_data = extract_data(orders_url)
    customers_data = extract_data(customers_url)
    channels_data = extract_data(channels_url)

    # Transform and check for null or empty values
    transformed_products_data = transform_data(products_data)
    transformed_orders_data = transform_data(orders_data)
    transformed_customers_data = transform_data(customers_data)
    transformed_channels_data = transform_data(channels_data)

    # Load transformed data into Azure Blob Storage
    load_data_to_blob(transformed_products_data, 'products.json')
    load_data_to_blob(transformed_orders_data, 'orders.json')
    load_data_to_blob(transformed_customers_data, 'customers.json')
    load_data_to_blob(transformed_channels_data, 'channels.json')

    # Evaluate data integrity
    print("Data evaluation completed.")

if __name__ == "__main__":
    main()
