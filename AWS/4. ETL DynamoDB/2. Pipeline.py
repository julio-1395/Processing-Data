import pymongo
import boto3

# MongoDB connection parameters
MONGO_HOST = 'your_mongodb_host'
MONGO_PORT = 27017
MONGO_DB = 'your_mongodb_database'

# DynamoDB table names
DYNAMODB_TABLES = ['Distribution', 'Consumption', 'Employees', 'Marketing']

# AWS credentials and region
AWS_ACCESS_KEY_ID = 'your_access_key_id'
AWS_SECRET_ACCESS_KEY = 'your_secret_access_key'
AWS_REGION = 'your_aws_region'

# Connect to MongoDB
mongo_client = pymongo.MongoClient(host=MONGO_HOST, port=MONGO_PORT)
mongo_db = mongo_client[MONGO_DB]

# Connect to DynamoDB
dynamodb_client = boto3.client('dynamodb', aws_access_key_id=AWS_ACCESS_KEY_ID,
                               aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                               region_name=AWS_REGION)

# Function to evaluate if data has no null or empty values
def check_data_validity(data):
    for key, value in data.items():
        if value is None or value == '':
            return False
    return True

# Function to load data from MongoDB to DynamoDB
def load_data_to_dynamodb(collection_name):
    collection = mongo_db[collection_name]
    dynamodb_table_name = collection_name

    # Iterate over MongoDB documents
    for document in collection.find():
        # Check data validity
        if not check_data_validity(document):
            print(f"Skipping invalid document in collection '{collection_name}'")
            continue

        # Put item into DynamoDB
        dynamodb_client.put_item(
            TableName=dynamodb_table_name,
            Item=document
        )
        print(f"Successfully loaded document into DynamoDB table '{dynamodb_table_name}'")

# Main function
def main():
    for table_name in DYNAMODB_TABLES:
        load_data_to_dynamodb(table_name)

if __name__ == "__main__":
    main()
