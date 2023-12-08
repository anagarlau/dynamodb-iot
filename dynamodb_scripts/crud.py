import json
import os
import sys

import boto3
from botocore.exceptions import ClientError, BotoCoreError

from table_scripts import handle_error


# TODO Extract into service as class
def create_dynamodb_client(local=True):
    return boto3.resource("dynamodb", region_name="localhost", endpoint_url="http://localhost:8000",
                        aws_access_key_id="fakeMyKeyId", aws_secret_access_key="fakeSecretAccessKey`")
    # return boto3.client("dynamodb", region_name="localhost", endpoint_url="http://localhost:8000",
    #                     aws_access_key_id="fakeMyKeyId", aws_secret_access_key="fakeSecretAccessKey`")



def create_table(client, table_name, pk_type, sk_type):
    # Cf https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/create_table.html
    try:
        table = client.create_table(
            TableName=table_name,
            KeySchema=[
                {
                    'AttributeName': 'PK',
                    'KeyType': 'HASH'  # Partition key
                },
                {
                    'AttributeName': 'SK',
                    'KeyType': 'RANGE'  # Sort key
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'PK',
                    'AttributeType': pk_type
                },
                {
                    'AttributeName': 'SK',
                    'AttributeType': sk_type
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 10,
                'WriteCapacityUnits': 10
            }
            # StreamSpecification={
            #     'StreamEnabled': True | False,
            #     'StreamViewType': 'NEW_IMAGE' | 'OLD_IMAGE' | 'NEW_AND_OLD_IMAGES' | 'KEYS_ONLY'
            # }
        )
        # used to pause the execution of your script until the specified DynamoDB table has been created
        client.get_waiter('table_exists').wait(
            TableName=table_name,
            WaiterConfig={
                'Delay': 11,
                'MaxAttempts': 3
            }
        )

        print(f"Successfully created table: {table_name}")
    except ClientError as e:
        handle_error(e)
    except BotoCoreError as e:
        handle_error(e)
    except Exception as e:
        handle_error(e)


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

    JSON_PATH = os.environ.get('JSON_PATH')
    # Create the DynamoDB Client with the region you want
    dynamodb_client = create_dynamodb_client()
    table_name = 'IoT'
    # create_table(dynamodb_client, table_name, 'S', 'S')
    items = []

    f = open(JSON_PATH)

    items = json.load(f)

    f.close()
    #batch_write(client=dynamodb_client,items=items, table_name=table_name)


if __name__ == "__main__":
    main()
