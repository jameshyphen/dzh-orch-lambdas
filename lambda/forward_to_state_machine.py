import boto3
import json
import os

stepfunctions = boto3.client('stepfunctions')
eventbridge = boto3.client('events')
state_machine_arn = os.environ.get('STATE_MACHINE_ARN')

def lambda_handler(event, context):
    # Log the event for traceability
    print(f"Received event: {json.dumps(event)}")

    # Start a new state machine execution if it doesn't exist
    try:
        response = stepfunctions.start_execution(
            stateMachineArn=state_machine_arn,
            name=event['eventId'],  # Use eventId as the unique execution name
            input=json.dumps(event)
        )
        print(f"Started state machine: {response}")
    except stepfunctions.exceptions.ExecutionAlreadyExists:
        print(f"State machine already exists for eventId: {event['eventId']}")

    return {
        "statusCode": 200,
        "body": json.dumps({"message": "Event processed successfully"})
    }