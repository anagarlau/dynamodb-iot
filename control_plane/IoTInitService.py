import random
import time
import uuid
from botocore.exceptions import ClientError

from backend.models.MaintenanceEnum import MaintenanceType
from dynamodbgeo.dynamodbgeo import GeoDataManagerConfiguration, GeoDataManager, GeoTableUtil, GeoPoint, PutPointInput, \
    S2Manager
from utils.parcels.parcels_from_csv import read_and_process_parcels_from_json
from utils.polygon_def import create_dynamodb_client, hashKeyLength
from utils.sensor_events.sensor_events_generation import process_events_for_db, convert_to_unix_epoch, \
    random_date_string
from utils.sensors.sensors_from_csv import json_to_array, csv_to_json
from utils.handle_error import handle_error
from utils.users_and_roles.user_and_roles_generation import read_users_from_json


class IoTInitService:
    def __init__(self):
        self.dynamodb = create_dynamodb_client()
        self.config = GeoDataManagerConfiguration(self.dynamodb, 'IoT')
        self.config.hashKeyAttributeName = 'PK'
        self.config.rangeKeyAttributeName = 'SK'
        self.geoDataManager = GeoDataManager(self.config)
        self.config.hashKeyLength = hashKeyLength
        self.table_util = GeoTableUtil(self.config)
        self.create_table_input = self.table_util.getCreateTableRequest()
        self.table_name = 'IoT'
        self.table_util = GeoTableUtil(self.config)

    def delete_dynamodb_table_if_exists(self):
        try:
            # Check if the table exists before deleting, else exception
            self.dynamodb.describe_table(TableName=self.table_name)
            # Delete if the table exists
            response = self.dynamodb.delete_table(TableName=self.table_name)
            print(f"Table {self.table_name} is being deleted. Status: {response['TableDescription']['TableStatus']}")

            # Polling for table deletion
            while True:
                try:
                    # Table lookup
                    self.dynamodb.describe_table(TableName=self.table_name)
                    print(f"Waiting for table {self.table_name} to be deleted...")
                    time.sleep(5)
                except self.dynamodb.exceptions.ResourceNotFoundException:
                    # Table not found, stop polling
                    print(f"Table {self.table_name} has been successfully deleted.")
                    break
        except self.dynamodb.exceptions.ResourceNotFoundException:
            # If the table does not exist at all
            print(f"Table {self.table_name} does not exist or has already been deleted.")
        except ClientError as e:
            print(f"An error occurred: {e.response['Error']['Message']}")

    def batch_write(self, items):
        try:
            # use resource instead of client for easier batching from json
            dynamodb = create_dynamodb_client(resource=True)
            table = dynamodb.Table(self.table_name)
            with table.batch_writer() as batch:
                for item in items:
                    # print("Item in Batch func", item)
                    # print("Data Point Type:", type(item['data_point']), "Battery Level Type:",
                    #       type(item['battery_level']))
                    res = batch.put_item(
                        Item=item
                    )
        except ClientError as e:
            handle_error(e)
        except Exception as e:
            print('Exception:', e)
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
            return response
        except ClientError as e:
            print(f"Error creating GSI: {e}")
            return None

    def custom_gsi_waiter(self, gsi_name):
        print(f"Waiting for GSI {gsi_name} to become active...")
        while True:
            table_description = self.dynamodb.describe_table(TableName=self.table_name)
            # Find the GSI in the table description
            gsi_status = None
            if "GlobalSecondaryIndexes" in table_description["Table"]:
                for gsi in table_description["Table"]["GlobalSecondaryIndexes"]:
                    if gsi["IndexName"] == gsi_name:
                        gsi_status = gsi["IndexStatus"]
                        break

            # Check if GSI is active
            if gsi_status == "ACTIVE":
                print(f"GSI {gsi_name} is now active.")
                break
            # Wait for a short period before checking again
            time.sleep(5)
        return table_description

    def insert_sensor_points(self):
        # Insert all the sensors from csv/json file
        create_table_input = self.table_util.getCreateTableRequest()
        create_table_input["ProvisionedThroughput"]['ReadCapacityUnits'] = 10
        self.table_util.create_table(create_table_input)
        # Read csv into json
        csv_to_json()
        # Read Sensors from json file
        sensor_items = json_to_array()
        #Read Users from json file
        user_items = read_users_from_json()
        # Process Sensor objects
        processed_items = []
        for item in sensor_items:
            # print(item)
            geopoint = GeoPoint(item['point_coordinates'][1], item['point_coordinates'][0])
            geohash = S2Manager().generateGeohash(geopoint)
            hashKey = S2Manager().generateHashKey(geohash, self.config.hashKeyLength)
            sensor_details = {
                'PK': f"Sensor#{item['sensor_id']}",
                'SK': f"Metadata#{item['sensor_type']}#{item['sensor_id']}",
                'sensor_type': item['sensor_type'],
                'geoJson': "{},{}".format(geopoint.getLatitude(), geopoint.getLongitude()),
                'hash_key': str(hashKey),
                'geohash': f"Metadata#{item['sensor_type']}#{geohash}",
                'curr_parcelid': item['parcel_id'],
                'manufacturer': item['manufacturer'],
                'firmware': item['firmware'],
                'model': item['model']
            }
            # Tracks sensor location history
            sensor_location_event = {
                'PK': f"Sensor#{item['sensor_id']}",
                'SK': f"Location#{convert_to_unix_epoch('2020-01-01T04:35:53')}",
                # 'moved_date': '',
                'sensortype': item['sensor_type'],  # In order for potential GSIs type not to fetch it
                'geoJson': "{},{}".format(geopoint.getLatitude(), geopoint.getLongitude()),
                'hash_key': str(hashKey),
                'geohash': f"Location#{geohash}",
                'id_parcel': item['parcel_id'],
                'placed_at':convert_to_unix_epoch('2020-01-01T04:35:53')
            }
            # Maintenance Operations
            for i in range(random.randint(0, 5)):
                SK =f"Maintenance#{convert_to_unix_epoch(random_date_string())}"
                sensor_maintenance = {
                    'PK': f"Sensor#{item['sensor_id']}",
                    'SK': SK,
                    'maintenance_type': MaintenanceType.get_random().value,
                    'details': 'Maintenance details',
                    'GSI_PK': random.choice(user_items).get("GSI_SK"),
                    'GSI_SK': SK,
                    'concluded_on': random_date_string()
                }
                processed_items.append(sensor_maintenance)

            processed_items.append(sensor_details)
            processed_items.append(sensor_location_event)
        processed_items.extend(user_items)
            # PutItemInput = {
            #     'TableName': 'IoT',
            #     'Item': {
            #         'sensor_id': {'S': item['sensor_id']},
            #         'sensor_type': {'S': item['sensor_type']}
            #         #TODO add maintenance stuff
            #     },
            #     'ConditionExpression': "attribute_not_exists(hashKey)"
            #     # ... Anything else to pass through to `putItem`, eg ConditionExpression
            # }
            #
            # self.geoDataManager.put_Point(PutPointInput(
            #     GeoPoint(item['point_coordinates'][1], item['point_coordinates'][0]),
            #     # latitude then latitude longitude
            #     str(uuid.uuid4()),  # Use this to ensure uniqueness of the hash/range pairs.
            #     PutItemInput  # pass the dict here
            # ))
        print(f'Inserted Number of Sensor Points: {len(processed_items)}')
        self.batch_write(items=processed_items)

    def insert_sensor_events(self):
        json_array = process_events_for_db()
        self.batch_write(items=json_array)
        print(f"Inserted Number of Events in base table: {len(json_array)}")

    def insert_parcels(self):
        json_array = read_and_process_parcels_from_json()
        self.batch_write(items=json_array)
        print(f"Inserted Number of Parcels in base table: {len(json_array)}")


if __name__ == "__main__":
    initService = IoTInitService()
    initService.delete_dynamodb_table_if_exists()
    initService.insert_sensor_points()
    initService.insert_parcels()
    gsi_name = 'GSI_Sensor_By_Parcel'
    initService.create_gsi(
        gsi_name=gsi_name,
        gsi_pk='curr_parcelid',
        gsi_pk_type='S', gsi_sk='SK', gsi_sk_type='S')
    initService.custom_gsi_waiter(gsi_name)
    gsi_name = 'GSI_Active_Parcels'
    initService.create_gsi(
        gsi_name=gsi_name,
        gsi_pk='active',
        gsi_pk_type='N', gsi_sk='SK', gsi_sk_type='S') # Bool not supported for partition keys
    initService.custom_gsi_waiter(gsi_name)
    gsi_name = 'GSI_Users_Roles_Maintenance'
    initService.create_gsi(
        gsi_name=gsi_name,
        gsi_pk='GSI_PK',
        gsi_pk_type='S', gsi_sk='GSI_SK', gsi_sk_type='S')
    initService.custom_gsi_waiter(gsi_name)
    # gsi_name = 'GSI_AllSensors_By_Type'
    # initService.create_gsi(
    #     gsi_name=gsi_name,
    #     gsi_pk='sensor_type',
    #     gsi_pk_type='S', gsi_sk='geohash', gsi_sk_type='S')
    # initService.custom_gsi_waiter(gsi_name)
    # gsi_name = 'GSI_ActiveSensor_By_Type'
    # initService.create_gsi(
    #     gsi_name=gsi_name,
    #     gsi_pk='sensor_type',
    #     gsi_pk_type='S', gsi_sk='curr_parcelid', gsi_sk_type='S')
    # initService.custom_gsi_waiter(gsi_name)
    gsi_name = f'GSI_Geohash{initService.config.hashKeyLength}_FullGeohash'
    initService.create_gsi(
        gsi_name=gsi_name,
        gsi_pk='hash_key',
        gsi_pk_type='S', gsi_sk='geohash', gsi_sk_type='S')
    initService.custom_gsi_waiter(gsi_name)
    gsi_name = f'GSI_TypeGeohash{initService.config.hashKeyLength}_FullGeohash'
    initService.create_gsi(
        gsi_name=gsi_name,
        gsi_pk='hash_key',
        gsi_pk_type='S', gsi_sk='geohash', gsi_sk_type='S')
    initService.custom_gsi_waiter(gsi_name)
    initService.insert_sensor_events()
    gsi_name = 'GSI_Events_By_Sensor'
    initService.create_gsi(
        gsi_name=gsi_name,
        gsi_pk='s_id',
        gsi_pk_type='S', gsi_sk='SK', gsi_sk_type='S')
    initService.custom_gsi_waiter(gsi_name)
    gsi_name = 'GSI_AllSensorEvents_Parcel'
    initService.create_gsi(
        gsi_name=gsi_name,
        gsi_pk='parcel_id',
        gsi_pk_type='S', gsi_sk='SK', gsi_sk_type='S')
    response = initService.custom_gsi_waiter(gsi_name)
    #print(response)
    # Table summaries
    attribute_definitions = {attr['AttributeName']: attr['AttributeType'] for attr in
                             response['Table']['AttributeDefinitions']}

    # Base table item count and key schema with data types
    base_table_item_count = response['Table']['ItemCount']
    base_table_key_schema = ", ".join(
        [f"{key['AttributeName']} ({key['KeyType']} - {attribute_definitions[key['AttributeName']]})" for key in
         response['Table']['KeySchema']]
    )
    print(f"Base table item count: {base_table_item_count}, Key Schema: {base_table_key_schema}")
    # Base table columns
    attribute_definitions = response['Table']['AttributeDefinitions']
    print(f"Base table attributes and their data types:")
    for attribute in attribute_definitions:
        print(f" - {attribute['AttributeName']} ({attribute['AttributeType']})")
    # GSI details
    for index in response['Table'].get('GlobalSecondaryIndexes', []):
        index_name = index['IndexName']
        index_item_count = index['ItemCount']
        key_schema_details = ", ".join([f"{key['AttributeName']} ({key['KeyType']})" for key in index['KeySchema']])

        print(f"Index Name: {index_name}, Item Count: {index_item_count}, Key Schema: {key_schema_details}")

