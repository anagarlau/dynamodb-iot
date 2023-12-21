import asyncio
from datetime import datetime, timedelta
from itertools import chain

import aioboto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
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

    # # TODO refactor
    # async def get_sensor_events(self, sensor_id, data_type, unix_from_date, unix_to_date):
    #     session = aioboto3.Session(
    #         aws_access_key_id=aws_access_key_id,
    #         aws_secret_access_key=aws_secret_access_key,  # Optional: Only if not set elsewhere
    #         region_name=region_name
    #     )
    #     async with session.client('dynamodb', endpoint_url=endpoint_url) as client:
    #         response = await client.query(
    #             TableName=self.table_name,
    #             KeyConditionExpression='PK = :pk_val and SK between :sk_start and :sk_end',
    #             ExpressionAttributeValues={
    #                 ':pk_val': {'S': sensor_id},
    #                 ':sk_start': {'S': f"{data_type}#{unix_from_date}"},
    #                 ':sk_end': {'S': f"{data_type}#{unix_to_date}"}
    #             },
    #             ReturnConsumedCapacity='INDEXES'
    #         )
    #         print("Consumed Capacity:", response.get('ConsumedCapacity'))
    #         return response['Items']
    #
    # async def get_sensors_events_in_radius_per_data_type(self, center_point, radius_meters, data_type='Humidity',
    #                                                      from_date="2020-03-04T00:00:00",
    #                                                      to_date="2023-09-07T23:59:59"):
    #     sensor_ids = self.sensor_service.get_sensors_in_radius_acc_to_type(center_point, radius_meters,
    #                                                                        sensor_type=data_type)
    #
    #     unix_from_date = convert_to_unix_epoch(from_date)
    #     unix_to_date = convert_to_unix_epoch(to_date)
    #
    #     tasks = [self.get_sensor_events(sensor['sensor_id'], data_type, unix_from_date, unix_to_date) for sensor in
    #              sensor_ids]
    #     # Nested list
    #     nested_events = await asyncio.gather(*tasks)
    #     # Flatten
    #     all_events = list(chain.from_iterable(nested_events))
    #     return all_events


    def add_sensor_event(self, sensor_event_json):
        try:
            sensor_event_entry = SensorEvent.from_json(sensor_event_json).to_entity()
            self.dynamodb.put_item(TableName=self.table_name, Item=sensor_event_entry)
            return sensor_event_json['sensorId']
        except ClientError as e:
            print(e.response['Error']['Message'])
            return "Error occurred during operation."

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
                        ':sval': {'S': f'TimeRange#{start_range_unix}#'},
                        ':eval': {'S': f'TimeRange#{end_range_unix}#zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz'}
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


    def query_sensorevents_by_sensorid_in_time_range(self, sensor_id, start_range, end_range):
        try:
            # Convert time ranges to Unix epoch
            start_range_unix = convert_to_unix_epoch(start_range)
            end_range_unix = convert_to_unix_epoch(end_range)

            # Query parameters
            partition_key = 'PK'
            sort_key = 'SK'
            items = []
            consumed_capacity = 0

            # Construct sort key range for BETWEEN clause
            lower_bound = f"TimeRange#{start_range_unix}#"
            upper_bound = f"TimeRange#{end_range_unix}#zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"

            # Query the DynamoDB table
            response = self.dynamodb.query(
                TableName=self.table_name,
                KeyConditionExpression=f"#{partition_key} = :pval AND #{sort_key} BETWEEN :sval AND :eval",
                ExpressionAttributeNames={
                    f"#{partition_key}": partition_key,
                    f"#{sort_key}": sort_key
                },
                ExpressionAttributeValues={
                    ':pval': {'S': f"Event#{sensor_id}"},
                    ':sval': {'S': lower_bound},
                    ':eval': {'S': upper_bound}
                },
                ReturnConsumedCapacity='TOTAL'
            )

            items.extend(response.get('Items', []))
            consumed_capacity += response.get('ConsumedCapacity')['CapacityUnits']
            print('Consumed Capacity', consumed_capacity)
            return items
        except ClientError as e:
            print("Boto3 client error:", e)
            return [], None


    def query_latest_n_sensorevents_by_sensorid(self, sensor_id, n):
        try:
            items = []
            consumed_capacity = 0

            # Query the DynamoDB table
            response = self.dynamodb.query(
                TableName=self.table_name,
                KeyConditionExpression=f"PK = :pval AND begins_with(SK, :skval)",
                ExpressionAttributeValues={
                    ':pval': {'S': f"Event#{sensor_id}"},
                    ':skval': {'S': 'TimeRange#'}
                },
                ScanIndexForward=False,  # Query in descending order
                Limit=n,  # Limit the number of items processed
                ReturnConsumedCapacity='TOTAL'
            )

            items.extend(response.get('Items', []))
            consumed_capacity += response.get('ConsumedCapacity')['CapacityUnits']
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
                    ':start_range': {'S': f"TimeRange#{start_range_unix}#"},
                    ':end_range': {'S': f"TimeRange#{end_range_unix}#zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"}
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
def main():
    service = SensorEventService()
    # events = await service.get_sensor_events('3cec4677-92d7-4a88-9b66-1a1323c6288d', 'Humidity', 1583276400, 1694123999)
    # center_point = Point(28.1250063, 46.6334964)
    # events = await service.get_sensors_events_in_radius_per_data_type(center_point,
    #                                                           200,
    #                                                           'Humidity',
    #                                                           '2021-07-02T16:27:30',
    #                                                           '2022-10-16T23:52:35')
    events = service.query_sensorevents_for_entire_field_in_time_range(
        '2020-01-01T04:35:53',
        '2020-02-07T14:56:50',['Humidity', 'Light', 'Temperature']
        )
    print(len(events))
    print(events[0])
    events = service.query_sensorevents_by_sensorid_in_time_range(
                 '57bf26c3-d792-4906-a717-a90c5e400e61',
                        '2020-01-01T11:51:38',
                        '2020-02-01T03:51:11')
    print(len(events))
    print(events[0])
    events = service.query_latest_n_sensorevents_by_sensorid('57bf26c3-d792-4906-a717-a90c5e400e61',4)
    print(len(events))
    for item in events[:1]:
        print(item['SK']['S'])
    events = service.query_latest_n_sensor_events_for_field(100)
    print(len(events))
    print(events[0])
    events = service.query_sensor_events_by_parcelid_in_time_range(
        'Chickpeas#3225eba0-4695-48ee-9616-62dc5256b4e2',
        '2020-01-01T11:51:38',
        '2020-02-01T03:51:11'
    ,['Humidity', 'Light', 'Temperature'])
    print(len(events))
    print(events[0])
    json = {
        "sensorId": "60acb1d3-bf3a-4f25-aa73-c75d0f495a8b",
        "metadata": {
            "location": "(46.63366128235294, 28.12680874117647)",
            "battery_level": 33,
            "status": "Maintenance",
            "parcel_id": "Chickpeas#957000a4-6b4a-4ff7-979d-9764d086ca01"
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
    #asyncio.run(main())
    main()