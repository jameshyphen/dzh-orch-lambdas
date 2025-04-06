import json
import boto3
import os
import datetime

from models.event.event import ProductStateUpdateEvent

dynamodb = boto3.resource('dynamodb')
event_history_table_name = os.environ.get('EVENT_HISTORY_TABLE')
table = dynamodb.Table(event_history_table_name)


def lambda_handler(event, context):
    # Extract id, message, and body from the event
    event_instance = ProductStateUpdateEvent(
        id=event.get('id'),
        message=event.get('message'),
        body=event.get('body')
    )

    # Convert the event instance to a dictionary
    item = event_instance.to_dict()
    item['timestamp'] = datetime.now().isoformat()

    # Prepare the transaction
    transact_items = [
        {
            'Put': {
                'TableName': event_history_table_name,
                'Item': item
            }
        }
    ]

    # Execute the transaction
    try:
        dynamodb.meta.client.transact_write_items(TransactItems=transact_items)
        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Data saved to DynamoDB!"})
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"message": "Failed to save data to DynamoDB", "error": str(e)})
        }
