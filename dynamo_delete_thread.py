import boto3
import time
from botocore.config import Config
from botocore.exceptions import ClientError
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Configure DynamoDB client with increased max_pool_connections
config = Config(
    retries={
        'max_attempts': 10,
        'mode': 'standard'
    },
    max_pool_connections=50  # Increase the connection pool size
)

# Initialize DynamoDB resource and client with custom config
dynamodb = boto3.resource('Product', config=config)
client = boto3.client('Product', config=config)

# Progress tracking variables
lock = threading.Lock()
total_items = 0
deleted_items = 0
progress_interval = 10000  # Print progress every 10,000 items

def delete_item(table_name, key):
    global deleted_items
    try:
        table = dynamodb.Table(table_name)
        table.delete_item(Key=key)
        with lock:
            deleted_items += 1
            if deleted_items % progress_interval == 0:
                print(f"Deleted {deleted_items}/{total_items} items")
    except ClientError as e:
        print(e.response['Error']['Message'])

def batch_delete_items(table_name, keys, max_workers=10):
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(delete_item, table_name, key) for key in keys]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as exc:
                print(f'Item deletion generated an exception: {exc}')

def get_all_items_to_delete(table_name):
    table = dynamodb.Table(table_name)
    response = table.scan()
    items = response.get('Items', [])
    
    # Continue to scan and get items if there are more
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        items.extend(response.get('Items', []))
    
    return items

def main(table_name):
    global total_items
    items_to_delete = get_all_items_to_delete(table_name)
    total_items = len(items_to_delete)
    keys = [{'ID': item['id'], 'product_name': item['product_name']} for item in items_to_delete]
    batch_delete_items(table_name, keys, max_workers=10)

if __name__ == "__main__":
    table_name = 'your_table_name'  # Replace with your DynamoDB table name
    main(table_name)
