import json
import boto3
import psycopg2
import os


def get_db_connection(secret_arn):
    """Retrieve database connection"""
    client = boto3.client('secretsmanager')
    response = client.get_secret_value(SecretId=secret_arn)
    secret = json.loads(response['SecretString'])
    connection = psycopg2.connect(
        host=secret['host'],
        port=secret['port'],
        user=secret['username'],
        password=secret['password'],
        dbname=secret['dbname']
    )
    return connection

def lambda_handler(event, context):
    # Get the secret ARN from the environment variable
    secret_arn = os.environ['DB_SECRET_ARN']

    # Connect to the RDS database
    connection = get_db_connection(secret_arn)
    cursor = connection.cursor()

    # Process the event and update the product state
    event_id = event['eventId']
    event_type = event['eventType']

    # Check if the product exists
    cursor.execute("SELECT * FROM product_states WHERE product_id = %s", (event_id,))
    product = cursor.fetchone()

    if not product:
        # Insert a new product if it doesn't exist
        cursor.execute(
            "INSERT INTO product_states (product_id, state) VALUES (%s, %s)",
            (event_id, event_type)
        )
    else:
        # Update the product state
        cursor.execute(
            "UPDATE product_states SET state = %s WHERE product_id = %s",
            (event_type, event_id)
        )

    connection.commit()
    cursor.close()
    connection.close()

    return {"statusCode": 200, "body": "Product state updated"}