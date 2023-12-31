import asyncio
from datetime import datetime, timedelta
from itertools import chain

import aioboto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError, BotoCoreError
from dateutil.relativedelta import relativedelta
from shapely import Point

from backend.models.SensorEvent import SensorEvent
from backend.service.SensorService import SensorService
from dynamodbgeo.dynamodbgeo import GeoDataManager, GeoDataManagerConfiguration
from playground import calculate_pks
from utils.polygon_def import hashKeyLength, create_dynamodb_client, aws_access_key_id, aws_secret_access_key, \
    region_name, endpoint_url
from utils.sensor_events.sensor_events_generation import convert_to_unix_epoch, get_first_of_month_as_unix_timestamp


class SensorEventService:
    def __init__(self):
        self.dynamodb = create_dynamodb_client()
        self.table_name = 'IoT'
        self.config = GeoDataManagerConfiguration(self.dynamodb, self.table_name)
        self.config.hashKeyAttributeName = 'PK'
        self.config.rangeKeyAttributeName = 'SK'
        self.geoDataManager = GeoDataManager(self.config)
        self.config.hashKeyLength = hashKeyLength
        self.sensor_service=SensorService()

    def add_sensor_event(self, sensor_event_json):
        try:
            sensor_event_entry = SensorEvent.from_json(sensor_event_json).to_entity()
            self.dynamodb.put_item(TableName=self.table_name, Item=sensor_event_entry)
            return sensor_event_json['sensorId']
        except (ClientError, BotoCoreError, Exception) as e:
            print(f"Error adding sensor event to DB: {e}")
            return None

    def query_sensorevents_by_sensorid_in_time_range(self, sensor_id, start_range, end_range):
        try:
            start_range_unix = convert_to_unix_epoch(start_range)
            end_range_unix = convert_to_unix_epoch(end_range)
            last_evaluated_key = None
            all_items = []
            consumed_capacity=0
            while True:
                if last_evaluated_key:
                    response = self.dynamodb.query(
                        TableName=self.table_name,
                        KeyConditionExpression=f"PK = :pval AND SK BETWEEN :sval AND :eval",
                        ExpressionAttributeValues={
                            ':pval': {'S': f"Event#{sensor_id}"},
                            ':sval': {'S': f"Timestamp#{start_range_unix}"},
                            ':eval': {'S': f"Timestamp#{end_range_unix}"}
                        },
                        ReturnConsumedCapacity='TOTAL',
                        ExclusiveStartKey=last_evaluated_key
                    )
                else:
                    response = self.dynamodb.query(
                        TableName=self.table_name,
                        KeyConditionExpression=f"PK = :pval AND SK BETWEEN :sval AND :eval",
                        ExpressionAttributeValues={
                            ':pval': {'S': f"Event#{sensor_id}"},
                            ':sval': {'S': f"Timestamp#{start_range_unix}"},
                            ':eval': {'S': f"Timestamp#{end_range_unix}"}
                        },
                        ReturnConsumedCapacity='TOTAL'
                    )
                consumed_capacity += response.get('ConsumedCapacity')['CapacityUnits']
                items = response.get('Items', [])
                all_items.extend(items)
                print('Consumed Capacity', consumed_capacity)
                last_evaluated_key = response.get('LastEvaluatedKey')
                if not last_evaluated_key:
                    break
            return all_items
        except (ClientError, BotoCoreError, Exception) as e:
            print(f"An error occurred while retrieving sensor events for sensor {sensor_id}:", e)
            return None

    def query_latest_n_sensorevents_by_sensorid(self, sensor_id, n):
        try:
            items = []
            last_evaluated_key = None
            remaining_items = n
            consumed_capacity=0
            while remaining_items > 0:
                if last_evaluated_key:
                    response = self.dynamodb.query(
                        TableName=self.table_name,
                        KeyConditionExpression=f"PK = :pval AND begins_with(SK, :skval)",
                        ExpressionAttributeValues={
                            ':pval': {'S': f"Event#{sensor_id}"},
                            ':skval': {'S': 'Timestamp#'}
                        },
                        ScanIndexForward=False,
                        Limit=remaining_items,
                        ExclusiveStartKey=last_evaluated_key,
                        ReturnConsumedCapacity='TOTAL'
                    )
                else:
                    response = self.dynamodb.query(
                        TableName=self.table_name,
                        KeyConditionExpression=f"PK = :pval AND begins_with(SK, :skval)",
                        ExpressionAttributeValues={
                            ':pval': {'S': f"Event#{sensor_id}"},
                            ':skval': {'S': 'Timestamp#'}
                        },
                        ScanIndexForward=False,
                        Limit=remaining_items,
                        ReturnConsumedCapacity='TOTAL'
                    )
                batch_items = response.get('Items', [])
                items.extend(batch_items)
                remaining_items -= len(batch_items)
                consumed_capacity += response.get('ConsumedCapacity')['CapacityUnits']
                print('Consumed Capacity', consumed_capacity)
                last_evaluated_key = response.get('LastEvaluatedKey')
                if not last_evaluated_key:
                    break
            return items
        except (ClientError, BotoCoreError, Exception) as e:
            print("Boto3 client error:", e)
            return [], None

    def query_sensorevents_for_entire_field_in_time_range(self, start_range, end_range, sensor_types_filters=None):
        try:
            # Calculate partition keys and convert time ranges to Unix epoch
            pks = calculate_pks(start_range, end_range)
            start_range_unix = convert_to_unix_epoch(start_range)
            end_range_unix = convert_to_unix_epoch(end_range)

            # Query variables
            partition_key = 'month'
            sort_key = 'SK'
            type_key = 'data_type'
            items = []
            consumed_capacity = 0


            # Iterate through each partition key (month)
            for pk in pks:
                partition_val = get_first_of_month_as_unix_timestamp(pk)

                # Base query params
                query_params = {
                    'TableName': self.table_name,
                    'IndexName': 'GSI_AllSensorEvents_TimeRange',
                    'KeyConditionExpression': f"#{partition_key} = :pval AND #{sort_key} BETWEEN :sval AND :eval",
                    'ExpressionAttributeNames': {
                        f"#{partition_key}": partition_key,
                        f"#{sort_key}": sort_key
                    },
                    'ExpressionAttributeValues': {
                        ':pval': {'N': str(partition_val)},
                        ':sval': {'S': f'Timestamp#{start_range_unix}#'},
                        ':eval': {'S': f'Timestamp#{end_range_unix}#zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz'}
                    },
                    'ReturnConsumedCapacity': 'INDEXES'
                }

                if sensor_types_filters:
                    type_filter_expressions = []
                    for i, sensor_type in enumerate(sensor_types_filters):
                        type_key_placeholder = f":typeval{i}"
                        query_params['ExpressionAttributeValues'][type_key_placeholder] = {'S': sensor_type}
                        type_filter_expressions.append(f"#{type_key} = {type_key_placeholder}")

                    query_params['FilterExpression'] = " OR ".join(type_filter_expressions)
                    query_params['ExpressionAttributeNames'][f"#{type_key}"] = type_key

                response = self.dynamodb.query(**query_params)
                items.extend(response.get('Items', []))
                consumed_capacity += response.get('ConsumedCapacity')['CapacityUnits']
            print("Number of queries for range is", len(pks))
            print('Consumed Capacity', consumed_capacity)
            return items
        except ClientError as e:
            print("Boto3 client error:", e)
            return [], None
    def get_previous_month_timestamp(self,current_month_unix_timestamp):
        # Convert current month timestamp to datetime (not necessary here but for reusability purposes)
        current_month_datetime = datetime.fromtimestamp(current_month_unix_timestamp)
        first_day_of_current_month = datetime(current_month_datetime.year, current_month_datetime.month, 1)
        first_day_of_previous_month = first_day_of_current_month - relativedelta(months=1)
        # First of month as Unix timestamp
        return int(first_day_of_previous_month.timestamp())
    def query_latest_n_sensor_events_for_field(self, num_reads):
        try:
            current_datetime_str = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
            current_month_partition = get_first_of_month_as_unix_timestamp(current_datetime_str)
            items = []
            last_evaluated_key = None
            remaining_reads = num_reads
            total_consumed_capacity = 0

            while remaining_reads > 0:
                #print("Remaining reads", remaining_reads)
                query_params = {
                    'TableName': self.table_name,
                    'IndexName': 'GSI_AllSensorEvents_TimeRange',
                    'KeyConditionExpression': "#PK = :pval",
                    'ExpressionAttributeNames': {
                        "#PK": "month"
                    },
                    'ExpressionAttributeValues': {
                        ':pval': {'N': str(current_month_partition)}
                    },
                    'ScanIndexForward': False,  # Descending order
                    'Limit': remaining_reads,
                    'ReturnConsumedCapacity': 'TOTAL'  # Tracking consumed capacity
                }

                if last_evaluated_key:
                    query_params['ExclusiveStartKey'] = last_evaluated_key

                response = self.dynamodb.query(**query_params)
                items.extend(response.get('Items', []))
                remaining_reads -= len(response.get('Items', []))
                last_evaluated_key = response.get('LastEvaluatedKey')

                # Update consumed capacity
                consumed_capacity = response.get('ConsumedCapacity', {}).get('CapacityUnits', 0)
                total_consumed_capacity += consumed_capacity

                # Move to the previous month if needed
                if not last_evaluated_key and remaining_reads > 0:
                    #print("current month part", current_month_partition)
                    current_month_partition = self.get_previous_month_timestamp(current_month_partition)
            print('Total Consumed Capacity:', total_consumed_capacity)
            return items
        except ClientError as e:
            print("Boto3 client error:", e)
            return []

    def query_sensor_events_by_parcelid_in_time_range(self, parcel_id, from_date, to_date, sensor_type_filters=None):
        print(f"Area Id is {parcel_id}")
        start_range_unix = convert_to_unix_epoch(from_date)
        end_range_unix = convert_to_unix_epoch(to_date)
        try:
            query_params = {
                'TableName': self.table_name,
                'IndexName': 'GSI_AllSensorEvents_Parcel',
                'KeyConditionExpression': "#parcel_id = :pid AND #sk BETWEEN :start_range AND :end_range",
                'ExpressionAttributeNames': {
                    "#parcel_id": "parcel_id",
                    "#sk": "SK"
                },
                'ExpressionAttributeValues': {
                    ':pid': {'S': parcel_id},
                    ':start_range': {'S': f"Timestamp#{start_range_unix}#"},
                    ':end_range': {'S': f"Timestamp#{end_range_unix}#zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"}
                },
                'ReturnConsumedCapacity': 'TOTAL'
            }

            if sensor_type_filters:
                type_key='data_type'
                type_filter_expressions = []
                for i, sensor_type in enumerate(sensor_type_filters):
                    type_key_placeholder = f":typeval{i}"
                    query_params['ExpressionAttributeValues'][type_key_placeholder] = {'S': sensor_type}
                    type_filter_expressions.append(f"#{type_key} = {type_key_placeholder}")

                query_params['FilterExpression'] = " OR ".join(type_filter_expressions)
                query_params['ExpressionAttributeNames'][f"#{type_key}"] = type_key

            response = self.dynamodb.query(**query_params)
            items = response.get('Items', [])
            consumed_capacity = response.get('ConsumedCapacity')['CapacityUnits']
            print('Consumed Capacity', consumed_capacity)
            return items

        except ClientError as e:
            print(f"An error occurred: {e.response['Error']['Message']}")
            return []

    #TODO
    #def query_events_in_radius_for_currently_active_sensors(self, center_point, radius:
    #def query_events_in_radius_for_currently_active_sensors_by_sensor_type(self):
def main():
    service = SensorEventService()
    # events = await service.get_sensor_events('3cec4677-92d7-4a88-9b66-1a1323c6288d', 'Humidity', 1583276400, 1694123999)
    # center_point = Point(28.1250063, 46.6334964)
    # events = await service.get_sensors_events_in_radius_per_data_type(center_point,
    #                                                           200,
    #                                                           'Humidity',
    #                                                           '2021-07-02T16:27:30',
    #                                                           '2022-10-16T23:52:35')
    # events = service.query_sensorevents_for_entire_field_in_time_range(
    #     '2020-01-01T04:35:53',
    #     '2020-02-07T14:56:50',['Humidity', 'Light', 'Temperature']
    #     )
    # print(len(events))
    # print(events[0])
    events = service.query_sensorevents_by_sensorid_in_time_range(
                 '32c3ecce-6589-445f-8f64-4d7422d4f1bf',
                        '2022-01-01T11:51:38',
                        '2024-12-01T03:51:11')
    print(len(events))
    print(events[0])
    events = service.query_latest_n_sensorevents_by_sensorid('32c3ecce-6589-445f-8f64-4d7422d4f1bf',4)
    print(len(events))
    for item in events:
        print(item['SK']['S'])
    # events = service.query_latest_n_sensor_events_for_field(100)
    # print(len(events))
    # print(events[0])
    # events = service.query_sensor_events_by_parcelid_in_time_range(
    #     'Chickpeas#3225eba0-4695-48ee-9616-62dc5256b4e2',
    #     '2020-01-01T11:51:38',
    #     '2020-02-01T03:51:11'
    # ,['Humidity', 'Light', 'Temperature'])
    # print(len(events))
    # print(events[0])
    json = {
        "sensorId": "32c3ecce-6589-445f-8f64-4d7422d4f1bf",
        "metadata": {
            "location": "(46.63366128235294, 28.12680874117647)",
            "battery_level": 33,
            "status": "Active",
            "parcel_id": "Chickpeas#af8ed50d-68c4-4cf9-b04e-bba5432d4b8e"
        },
        "data": {
            "dataType": "SoilPH",
            "dataPoint": 1,
            "timestamp": "2023-12-22T16:01:00"
        }
    }
    service.add_sensor_event(json)

# Run the async main function
if __name__ == "__main__":
    main()