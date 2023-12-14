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

    #GSi with month as PK, SK design Type#timerange
    def query_sensorevents_by_type_for_entire_field_in_time_range(self,start_range, end_range, sensor_types):
        try:
            pks = calculate_pks(start_range, end_range)

            start_range = convert_to_unix_epoch(start_range)
            end_range=convert_to_unix_epoch(end_range)
            partition_key='month'
            sort_key='SK'
            items = []
            consumed_capacity=0
            #["Humidity", "Rain", "Light", "SoilPH", "SoilMoisture", "Temperature"]
            for sensor in sensor_types:
                for pk in pks:
                    partition_val = get_first_of_month_as_unix_timestamp(pk)
                    print(partition_val)
                    response = self.dynamodb.query(
                        TableName=self.table_name,
                        IndexName='GSI_AllSensorEvents_TimeRange',
                        KeyConditionExpression=f"#{partition_key} = :pval AND #{sort_key} BETWEEN :sval AND :eval",
                        ExpressionAttributeNames={
                            f"#{partition_key}": partition_key,
                            f"#{sort_key}": sort_key
                        },
                        ExpressionAttributeValues={
                            ':pval': {'N': str(partition_val)},
                            ':sval': {'S': f'{sensor}#{start_range}'},
                            ':eval': {'S': f'{sensor}#{end_range}'}
                        },
                        ReturnConsumedCapacity='INDEXES'
                    )
                    items.append(response.get('Items', []))
                    print(response.get('ConsumedCapacity'))
                    consumed_capacity+=response.get('ConsumedCapacity')['CapacityUnits']
        except ClientError as e:
            print("Boto3 client error:", e)
            return [], None
        return [item for sublist in items for item in sublist], consumed_capacity

    def query_sensorevents_for_entire_field_in_time_range(self, start_range, end_range, sensor_type):
        try:
            # Calculate partition keys
            pks = calculate_pks(start_range, end_range)

            # Convert time ranges to Unix epoch
            start_range_unix = convert_to_unix_epoch(start_range)
            end_range_unix = convert_to_unix_epoch(end_range)

            # Initialize variables
            partition_key = 'month'
            sort_key = 'SK'
            items = []
            consumed_capacity = 0
            type_key='type'
            # Iterate through each partition key (month)
            for pk in pks:
                # Convert partition key to Unix timestamp format
                partition_val = get_first_of_month_as_unix_timestamp(pk)
                print(partition_val)
                print(start_range_unix)
                print(end_range_unix)
                # Query the DynamoDB table
                response = self.dynamodb.query(
                    TableName=self.table_name,
                    IndexName='GSI_AllSensorEvents_TimeRange',
                    KeyConditionExpression=f"#{partition_key} = :pval AND #{sort_key} BETWEEN :sval AND :eval",
                    #FilterExpression=f"#{type_key} = :typeval",
                    ExpressionAttributeNames={
                        f"#{partition_key}": partition_key,
                        f"#{sort_key}": sort_key
                        #,f"#{type_key}": type_key
                    },
                    ExpressionAttributeValues={
                        ':pval': {'N': str(partition_val)},
                        ':sval': {'S': f'{start_range_unix}'},
                        ':eval': {'S': f'{end_range_unix}'}
                        #':typeval': {'S': sensor_type}  # Expression Attribute Value for the filter
                    },
                    ReturnConsumedCapacity='INDEXES'
                )

                # Collect items and consumed capacity
                items.extend(response.get('Items', []))
                print(consumed_capacity)
                consumed_capacity += response.get('ConsumedCapacity')['CapacityUnits']

        except ClientError as e:
            print("Boto3 client error:", e)
            return [], None

        return items, consumed_capacity
def main():
    service = SensorEventService()
    #events = await service.get_sensor_events('3cec4677-92d7-4a88-9b66-1a1323c6288d', 'Humidity', 1583276400, 1694123999)
    # center_point = Point(28.1250063, 46.6334964)
    # events = await service.get_sensors_events_in_radius_per_data_type(center_point,
    #                                                           200,
    #                                                           'Humidity',
    #                                                           '2021-07-02T16:27:30',
    #                                                           '2022-10-16T23:52:35')
    # events = service.query_sensorevents_by_type_for_entire_field_in_time_range(
    #     '2020-01-01T04:35:53',
    #     '2020-02-07T14:56:50',
    #     ["Humidity", "Rain", "Light", "SoilPH", "SoilMoisture", "Temperature"]) #4 for all depends on nb of data points
    events = service.query_sensorevents_for_entire_field_in_time_range(
        '2020-01-01T04:35:53',
        '2020-02-07T14:56:50', None) #1.5 for all nb of data points
    print(len(events[0]))
    print(events[0])
    print(events[1])

# Run the async main function
if __name__ == "__main__":
    #asyncio.run(main())
    main()