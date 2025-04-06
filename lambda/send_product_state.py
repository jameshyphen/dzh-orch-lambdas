import json
import boto3
import os

eventbridge = boto3.client('events')
event_bus_name = os.environ.get('EVENT_BUS_NAME')

def lambda_handler(event, context):
    response = eventbridge.put_events(
        Entries=[
            {
                'Source': event.source,
                'DetailType': event.type,
                'Detail': json.dumps(event),
                'EventBusName': event_bus_name
            }
        ]
    )
    return {
        "statusCode": 200,
        "body": json.dumps({"message": "Event sent to EventBridge!", "response": response})
    }
