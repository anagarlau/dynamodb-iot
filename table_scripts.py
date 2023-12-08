import boto3
from boto3.dynamodb.types import TypeSerializer
from botocore.exceptions import ClientError, BotoCoreError

ERROR_HELP_STRINGS = {
    # Common Errors
    'InternalServerError': 'Internal Server Error, generally safe to retry with exponential back-off',
    'ProvisionedThroughputExceededException': 'Request rate is too high. If you\'re using a custom retry strategy make sure to retry with exponential back-off.' +
                                              'Otherwise consider reducing frequency of requests or increasing provisioned capacity for your table or secondary index',
    'ResourceNotFoundException': 'One of the tables was not found, verify table exists before retrying',
    'ServiceUnavailable': 'Had trouble reaching DynamoDB. generally safe to retry with exponential back-off',
    'ThrottlingException': 'Request denied due to throttling, generally safe to retry with exponential back-off',
    'UnrecognizedClientException': 'The request signature is incorrect most likely due to an invalid AWS access key ID or secret key, fix before retrying',
    'ValidationException': 'The input fails to satisfy the constraints specified by DynamoDB, fix input before retrying',
    'RequestLimitExceeded': 'Throughput exceeds the current throughput limit for your account, increase account level throughput before retrying',
    'ConditionalCheckFailedException': 'Conditional Check Failed for current operation',
     'ResourceInUseException': 'Table already exists.'
}

def handle_error(error):
    if type(error) == Exception:
        print('An exception has occured. Error message: {error_message}'
              .format(error_message=str(error)))
    else:
        error_code = error.response['Error']['Code'] if not None else error.response['Error']['Code']

        error_message = error.response['Error']['Message']

        error_help_string = ERROR_HELP_STRINGS[error_code]

        print('[{error_code}] {help_string}. Error message: {error_message}'
              .format(error_code=error.response,
                      help_string=error_help_string,
                      error_message=error.response))


def create_dynamodb_client(local=True):
    return boto3.client("dynamodb", region_name="localhost", endpoint_url="http://localhost:8000",
                        aws_access_key_id="fakeMyKeyId", aws_secret_access_key="fakeSecretAccessKey`")


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
        client.get_waiter('table_exists').wait(
            TableName=table_name,
            WaiterConfig={
                'Delay': 11,
                'MaxAttempts': 3
            }
        )
        # used to pause the execution of your script until the specified DynamoDB table has been created
        print(f"Successfully created table {table_name}")
    except ClientError as e:
        handle_error(e)
    except BotoCoreError as e:
        handle_error(e)
    except Exception as e:
        handle_error(e)
    # SCHEMALESS except for primary key schema => other attributes are added as you go,
    # no set schema. just add an entry

    # # Add an item with a string set attribute
    # serializer = TypeSerializer()
    #
    # # Serialize the set
    # courses_set = serializer.serialize(set(['English', 'Math', 'Physics']))['SS']
    # try:
    #     # Put the item
    #     response = client.put_item(
    #         TableName=table_name,
    #         Item={
    #             'PK': {'S': 'patata@gmail.com'},
    #             'SK': {'S': "John van Patata"},
    #             'grade': {'N': '12'},
    #             'courses': {'SS': courses_set}  # string set
    #         },
    #         ConditionExpression='attribute_not_exists(PK)'
    #     )
    #     print("Item added successfully.")
    # except ClientError as e:
    #     handle_error(e)

def create_get_item_input():
    return {
        "TableName": "Employee",
        "Key": {
            "LoginAlias": {"S": "johns"}
        }
    }


def execute_get_item(dynamodb_client, input):
    try:
        response = dynamodb_client.get_item(**input)
        print("Successfully get item.")
        print(response)
        # Handle response
    except ClientError as error:
        handle_error(error)
        # except BaseException as error:
        print("Unknown error while getting item: ")
        handle_error(error)


def main():
    # Create the DynamoDB Client with the region you want
    dynamodb_client = create_dynamodb_client()

    # Create the dictionary containing arguments for get_item call
    get_item_input = create_get_item_input()

    # Call DynamoDB's get_item API
    execute_get_item(dynamodb_client, get_item_input)
    create_table(dynamodb_client, 'Student', 'S', 'S')



if __name__ == "__main__":
    main()
