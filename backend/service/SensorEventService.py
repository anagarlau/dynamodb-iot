import asyncio
from itertools import chain

import aioboto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from shapely import Point

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

    async def get_sensor_events(self, sensor_id, data_type, unix_from_date, unix_to_date):
        session = aioboto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,  # Optional: Only if not set elsewhere
            region_name=region_name
        )
        async with session.client('dynamodb', endpoint_url=endpoint_url) as client:
            response = await client.query(
                TableName=self.table_name,
                KeyConditionExpression='PK = :pk_val and SK between :sk_start and :sk_end',
                ExpressionAttributeValues={
                    ':pk_val': {'S': sensor_id},
                    ':sk_start': {'S': f"{data_type}#{unix_from_date}"},
                    ':sk_end': {'S': f"{data_type}#{unix_to_date}"}
                },
                ReturnConsumedCapacity='INDEXES'
            )
            print("Consumed Capacity:", response.get('ConsumedCapacity'))
            return response['Items']

    async def get_sensors_events_in_radius_per_data_type(self, center_point, radius_meters, data_type='Humidity',
                                                         from_date="2020-03-04T00:00:00",
                                                         to_date="2023-09-07T23:59:59"):
        sensor_ids = self.sensor_service.get_sensors_in_radius_acc_to_type(center_point, radius_meters,
                                                                           sensor_type=data_type)
        #TODO monitor the usage of radius query via response[;]
        unix_from_date = convert_to_unix_epoch(from_date)
        unix_to_date = convert_to_unix_epoch(to_date)

        tasks = [self.get_sensor_events(sensor['sensor_id'], data_type, unix_from_date, unix_to_date) for sensor in
                 sensor_ids]
        # Nested list
        nested_events = await asyncio.gather(*tasks)
        # Flatten
        all_events = list(chain.from_iterable(nested_events))
        return all_events



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
                    ':pval': {'S': sensor_id},
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
                    ':pval': {'S': sensor_id},
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
def main():
    service = SensorEventService()
    #events = await service.get_sensor_events('3cec4677-92d7-4a88-9b66-1a1323c6288d', 'Humidity', 1583276400, 1694123999)
    # center_point = Point(28.1250063, 46.6334964)
    # events = await service.get_sensors_events_in_radius_per_data_type(center_point,
    #                                                           200,
    #                                                           'Humidity',
    #                                                           '2021-07-02T16:27:30',
    #                                                           '2022-10-16T23:52:35')
    events = service.query_sensorevents_for_entire_field_in_time_range(
        '2020-01-01T04:35:53',
        '2020-02-07T14:56:50',['Humidity', 'Light', 'Temperature']
        ) #1.5 for all nb of data points
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
    for item in events:
        print(item['SK']['S'])

# Run the async main function
if __name__ == "__main__":
    #asyncio.run(main())
    main()