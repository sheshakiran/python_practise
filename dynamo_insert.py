import boto3
import time
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key

# Initialize DynamoDB resource
dynamodb = boto3.resource('dynamodb')
table_name = 'product'
table = dynamodb.Table(table_name)

# Function to get current epoch timestamp
def get_current_epoch():
    return int(time.time())

# Function to check if an item exists with the same partition key and different sort key
def item_exists_with_different_sort_key(partition_key, sort_key):
    try:
        response = table.query(
            KeyConditionExpression=Key('ID').eq(partition_key)
        )
        for item in response.get('Items', []):
            if item['product_name'] != sort_key:
                return item
        return None
    except ClientError as e:
        print(e.response['Error']['Message'])
        return None

# Function to update the existing item with TTL
def update_item_with_ttl(partition_key, sort_key):
    ttl_value = get_current_epoch() + 3600  # TTL set to 1 hour from now
    try:
        table.update_item(
            Key={
                'ID': partition_key,
                'product_name': sort_key
            },
            UpdateExpression="SET ttl = :ttl_value",
            ExpressionAttributeValues={':ttl_value': ttl_value}
        )
    except ClientError as e:
        print(e.response['Error']['Message'])

# Function to insert items into DynamoDB using batch_write_item
def insert_items(items):
    request_items = []
    for item in items:
        partition_key = item['ID']
        sort_key = item['product_name']
        
        existing_item = item_exists_with_different_sort_key(partition_key, sort_key)
        if existing_item:
            existing_sort_key = existing_item['product_name']
            update_item_with_ttl(partition_key, existing_sort_key)
        
        # Add the item to the batch request
        dynamodb_item = {
            'ID': {'S': partition_key},
            'product_name': {'S': sort_key},
            'price': {'N':item['price']},
            'time_to_live': {'N':'0'}
            # Add more attributes as needed
        }
        request_items.append({
            'PutRequest': {
                'Item': dynamodb_item
            }
        })
        
        # Write items in batches of 25
        if len(request_items) == 25:
            batch_write_items(request_items)
            request_items = []

    # Write any remaining items
    if request_items:
        batch_write_items(request_items)

# Function to write batch items to DynamoDB
def batch_write_items(request_items):
    try:
        client = boto3.client('dynamodb')
        response = client.batch_write_item(
            RequestItems={
                table_name: request_items
            }
        )
        # Handle unprocessed items if any
        if 'UnprocessedItems' in response and response['UnprocessedItems']:
            print("Unprocessed items found, retrying...")
            batch_write_items(response['UnprocessedItems'][table_name])
    except ClientError as e:
        print(e.response['Error']['Message'])

# Example list of items to insert
items_to_insert = [
    {'ID': '1', 'product_name': 'Pen', 'price':50 },
    {'ID': '2', 'product_name': 'TV', 'price':400}
    # Add more items as needed
]

# Insert items
insert_items(items_to_insert)
