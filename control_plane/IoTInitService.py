import uuid
from botocore.exceptions import ClientError
from dynamodbgeo.dynamodbgeo import GeoDataManagerConfiguration, GeoDataManager, GeoTableUtil, GeoPoint, PutPointInput
from utils.polygon_def import create_dynamodb_client
from utils.sensor_events.sensor_events_generation import process_events_for_db
from utils.sensors.sensors import json_to_array, csv_to_json
from table_scripts import handle_error


class IoTInitService:
    def __init__(self):
        self.dynamodb = create_dynamodb_client()
        self.config = GeoDataManagerConfiguration(self.dynamodb, 'IoT')
        self.config.hashKeyAttributeName = 'PK'
        self.config.rangeKeyAttributeName = 'SK'
        self.geoDataManager = GeoDataManager(self.config)
        self.config.hashKeyLength = 6
        self.table_util = GeoTableUtil(self.config)
        self.create_table_input = self.table_util.getCreateTableRequest()
        self.table_name='IoT'
        self.table_util = GeoTableUtil(self.config)

    def batch_write(self, items):
        try:
            #use resource instead of client for easier batching from json
            dynamodb = create_dynamodb_client(resource=True)
            table = dynamodb.Table(self.table_name)
            with table.batch_writer() as batch:
                for item in items:
                    print(item)
                    batch.put_item(
                        Item=item
                    )
        except ClientError as e:
            handle_error(e)
        except Exception as e:
            print(e)
        return None

    def create_gsi(self, gsi_name, gsi_pk, gsi_pk_type='S', gsi_sk=None, gsi_sk_type='S'):
        try:
            # Key schema for the GSI
            key_schema = [{'AttributeName': gsi_pk, 'KeyType': 'HASH'}]
            if gsi_sk:
                key_schema.append({'AttributeName': gsi_sk, 'KeyType': 'RANGE'})

            # Attribute definitions for the GSI
            attribute_definitions = [{'AttributeName': gsi_pk, 'AttributeType': gsi_pk_type}]
            if gsi_sk:
                attribute_definitions.append({'AttributeName': gsi_sk, 'AttributeType': gsi_sk_type})

            # GSI definition
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
            response = self.dynamodb.update_table(
                TableName=self.table_name,
                AttributeDefinitions=attribute_definitions,
                GlobalSecondaryIndexUpdates=[gsi]
            )
            print(response)
            return response
        except ClientError as e:
            print(f"Error creating GSI: {e}")
            return None

    def create_gsis_for_table(self):
        # Create all GSIs in one go
        # GSI: sensors in a radiu according to type
        self.create_gsi(
                       gsi_name='GSI_SensorType_Radius',
                       gsi_pk='sensor_type',
                       gsi_pk_type='S', gsi_sk='SK', gsi_sk_type='S')

    def insert_sensor_points(self):
        # Insert all the sensors from csv/json file
        create_table_input = self.table_util.getCreateTableRequest()
        create_table_input["ProvisionedThroughput"]['ReadCapacityUnits'] = 10
        self.table_util.create_table(create_table_input)
        # Read csv into json
        csv_to_json()
        # Read Sensors from json file
        items = json_to_array()
        for item in items:
            print(item)
            PutItemInput = {
                'TableName': 'IoT',
                'Item': {
                    'sensor_id': {'S': item['sensor_id']},
                    'sensor_type': {'S': item['sensor_type']},
                    'geohash8': {'S': item['geohash8']},
                    'geohash6': {'S': item['geohash6']}

                },
                'ConditionExpression': "attribute_not_exists(hashKey)"
                # ... Anything else to pass through to `putItem`, eg ConditionExpression
            }

            self.geoDataManager.put_Point(PutPointInput(
                GeoPoint(item['point_coordinates'][1], item['point_coordinates'][0]),
                # latitude then latitude longitude
                str(uuid.uuid4()),  # Use this to ensure uniqueness of the hash/range pairs.
                PutItemInput  # pass the dict here
            ))

    def insert_sensor_events(self):
        json_array = process_events_for_db()
        self.batch_write(items=json_array)
        print(len(json_array))

if __name__ == "__main__":
    initService = IoTInitService()
    initService.insert_sensor_points()
    initService.create_gsis_for_table()
    initService.insert_sensor_events()