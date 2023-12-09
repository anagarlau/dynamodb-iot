import json
import os

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from table_scripts import handle_error


def create_dynamodb_client(local=True):
    return boto3.resource("dynamodb", region_name="localhost", endpoint_url="http://localhost:8000",
                        aws_access_key_id="fakeMyKeyId", aws_secret_access_key="fakeSecretAccessKey`")

def create_table(client, table_name, pk_type, sk_type):
    # Cf https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/create_table.html
    table_name = table_name
    key_schema = [
        {
            'AttributeName': 'PK',
            'KeyType': 'HASH'  # Partition key
        },
        {
            'AttributeName': 'SK',
            'KeyType': 'RANGE'  # Sort key
        }
    ]
    attribute_definitions = [
        {
            'AttributeName': 'PK',
            'AttributeType': pk_type
        },
        {
            'AttributeName': 'SK',
            'AttributeType': sk_type
        }
    ]
    provisioned_throughput = {
        'ReadCapacityUnits': 10,
        'WriteCapacityUnits': 10
    }
    try:
        table = client.create_table(
            TableName=table_name,
            KeySchema=key_schema,
            AttributeDefinitions=attribute_definitions,
            ProvisionedThroughput=provisioned_throughput
        )
        # Wait until the table exists.
        table.meta.client.get_waiter('table_exists').wait(TableName=table_name)
        print(f"Successfully created table: {table_name}")
    except ClientError as e:
        handle_error(e)
    except BotoCoreError as e:
        handle_error(e)
    except Exception as e:
        handle_error(e)

def create_gsi(table_name, gsi_name, gsi_pk, gsi_pk_type='S', gsi_sk=None, gsi_sk_type='S'):
    try:
        client=boto3.client("dynamodb", region_name="localhost", endpoint_url="http://localhost:8000",
                            aws_access_key_id="fakeMyKeyId", aws_secret_access_key="fakeSecretAccessKey`")
        # Define key schema for the GSI
        key_schema = [{'AttributeName': gsi_pk, 'KeyType': 'HASH'}]
        if gsi_sk:
            key_schema.append({'AttributeName': gsi_sk, 'KeyType': 'RANGE'})

        # Define attribute definitions for the GSI
        attribute_definitions = [{'AttributeName': gsi_pk, 'AttributeType': gsi_pk_type}]
        if gsi_sk:
            attribute_definitions.append({'AttributeName': gsi_sk, 'AttributeType': gsi_sk_type})

        # Define the GSI
        gsi = {
            'Create': {
                'IndexName': gsi_name,
                'KeySchema': key_schema,
                'Projection': {
                    'ProjectionType': 'ALL'  # or 'KEYS_ONLY' or 'INCLUDE'
                },
                'ProvisionedThroughput': {
                    'ReadCapacityUnits': 10,  # Adjust as needed
                    'WriteCapacityUnits': 10  # Adjust as needed
                }
            }
        }

        # Update the table to add the GSI
        response = client.update_table(
            TableName=table_name,
            AttributeDefinitions=attribute_definitions,
            GlobalSecondaryIndexUpdates=[gsi]
        )

        return response
    except ClientError as e:
        print(f"Error creating GSI: {e}")
        return None


def batch_write(client, items, table_name):
    try:
        table = client.Table(table_name)
        with table.batch_writer() as batch:
            for item in items:
                print(item)
                batch.put_item(
                    Item=item
                )
    except ClientError as e:
        handle_error(e)
    except Exception as e:
        handle_error(e)
    return None


def main():
    # Create the DynamoDB Client with the region you want
    dynamodb_client = create_dynamodb_client()
    table_name = 'IoT'
    create_table(dynamodb_client, table_name, 'S', 'S')
    # GSI for Plants in a given area
    create_gsi(table_name=table_name,
               gsi_name='GSI_Area_Plant',
               gsi_pk='geohash6',
               gsi_sk='SK')

    #Batch writes using pre-created JSON files
    JSON_PATH = 'C:\\Users\\ana\\PycharmProjects\\dynamodb\\maps\\json_batch_writes\\plants_batch.json'
    items = []
    f = open(JSON_PATH)
    items = json.load(f)
    f.close()
    batch_write(client=dynamodb_client,items=items, table_name=table_name)



if __name__ == "__main__":
    main()
