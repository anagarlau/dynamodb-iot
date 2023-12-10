import json
import os
import uuid

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from shapely import Point

from dynamodbgeo.dynamodbgeo import GeoDataManagerConfiguration, GeoDataManager, GeoTableUtil, GeoPoint, \
    QueryRadiusRequest, PutPointInput
from sensors_new.sensors import json_to_array, parse_sensor_data, visualize_results
from table_scripts import handle_error

dynamodb = boto3.client("dynamodb", region_name="localhost", endpoint_url="http://localhost:8000",
                            aws_access_key_id="fakeMyKeyId", aws_secret_access_key="fakeSecretAccessKey`")
config = GeoDataManagerConfiguration(dynamodb, 'IoT')

config.hashKeyAttributeName = 'PK'
config.rangeKeyAttributeName = 'SK'
geoDataManager = GeoDataManager(config)

# Pick a hashKeyLength appropriate to your usage
config.hashKeyLength = 6

# Use GeoTableUtil to help construct a CreateTableInput.
table_util = GeoTableUtil(config)
create_table_input = table_util.getCreateTableRequest()

# tweaking the base table parameters as a dict
create_table_input["ProvisionedThroughput"]['ReadCapacityUnits'] = 10

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
        print(response)
        return response
    except ClientError as e:
        print(f"Error creating GSI: {e}")
        return None

def insert_plants():
    dynamodb = boto3.client("dynamodb", region_name="localhost", endpoint_url="http://localhost:8000",
                            aws_access_key_id="fakeMyKeyId", aws_secret_access_key="fakeSecretAccessKey`")
    config = GeoDataManagerConfiguration(dynamodb, 'IoT')

    config.hashKeyAttributeName = 'PK'
    config.rangeKeyAttributeName = 'SK'
    geoDataManager = GeoDataManager(config)

    # Pick a hashKeyLength appropriate to your usage
    config.hashKeyLength = 6

    # Use GeoTableUtil to help construct a CreateTableInput.
    table_util = GeoTableUtil(config)
    create_table_input = table_util.getCreateTableRequest()

    # tweaking the base table parameters as a dict
    # create_table_input["ProvisionedThroughput"]['ReadCapacityUnits'] = 10

    # Use GeoTableUtil to create the table
    table_util.create_table(create_table_input)
    create_gsi(table_name='IoT',
               gsi_name='GSI_Area_Plant',
               gsi_pk='plant_type',
               gsi_sk='geohash6')
    #Read Plants
    JSON_PATH = 'C:\\Users\\ana\\PycharmProjects\\dynamodb\\maps\\data\\to_json.json'
    f = open(JSON_PATH)
    items = json.load(f)
    f.close()
    for item in items:
        print(item)
        PutItemInput = {
            'TableName': 'IoT',
            'Item': {
                'plant_type': {'S': item['PK']},
                'geohash8': {'S': item['geohash8']},
                'geohash6': {'S': item['geohash6']},
                'details': {
                    'M': {
                        'latin_name': {'S': item['details']['latin_name']},
                        'family': {'S': item['details']['family']}
                    }
                },
                'optimal_temperature': {'S': item['optimal_temperature']},
                'optimal_humidity': {'S': item['optimal_humidity']},
                'optimal_soil_ph': {'S': item['optimal_soil_ph']},
                'water_req_mm_per_week': {'S': item['water_req_mm_per_week']},
                'sunlight_req_h_per_day': {'S': item['sunlight_req_h_per_day']}

            },
            'ConditionExpression': "attribute_not_exists(hashKey)"
            # ... Anything else to pass through to `putItem`, eg ConditionExpression

        }

        geoDataManager.put_Point(PutPointInput(
            GeoPoint(item['SK'][0], item['SK'][1]),  # latitude then latitude longitude
            str(uuid.uuid4()),  # Use this to ensure uniqueness of the hash/range pairs.
            PutItemInput  # pass the dict here
        ))
    # response = dynamodb.put_item(
    #     TableName='IoT',
    #     Item = PutItemInput,
    #     ConditionExpression='attribute_not_exists(PK)'
    # )
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

def insert_sensor_points():

    table_util.create_table(create_table_input)
    # Read Sensors
    items = json_to_array()
    for item in items:
        print(item)
        PutItemInput = {
            'TableName': 'IoT',
            'Item': {
                'sensor_id': {'S': item['sensor_id']},
                'sensor_type': {'S':item['sensor_type']},
                'geohash8': {'S': item['geohash8']},
                'geohash6': {'S': item['geohash6']}

            },
            'ConditionExpression': "attribute_not_exists(hashKey)"
            # ... Anything else to pass through to `putItem`, eg ConditionExpression

        }

        geoDataManager.put_Point(PutPointInput(
            GeoPoint(item['point_coordinates'][1], item['point_coordinates'][0]),  # latitude then latitude longitude
            str(uuid.uuid4()),  # Use this to ensure uniqueness of the hash/range pairs.
            PutItemInput  # pass the dict here
        ))


def get_sensors_in_radius(center_point, radius_meters):
    # Prepare the filter expression and attribute values
    lat, lon = center_point.y, center_point.x
    # query_radius_input = {
    #     "FilterExpression": "Country = :val1",
    #     "ExpressionAttributeValues": {
    #         ":val1": {"S": country_filter},
    #     }
    # }

    # Perform the radius query
    response = geoDataManager.queryRadius(
        QueryRadiusRequest(
            GeoPoint(lat, lon),  # center point
            radius_meters,  # search radius in meters
           # query_radius_input,  # additional filter input
            sort=False  # sort by distance from the center point
        )
    )

    data = parse_sensor_data(response['results'])
    map = visualize_results(center_point, radius_meters, data)
    print(data[:2])
    map.save("sensors-radius.html")
    return data

def main():
    #insert_sensor_points()
    center_point = Point(28.1250063, 46.6334964)
    get_sensors_in_radius(center_point, 1200)





if __name__ == "__main__":
    main()
