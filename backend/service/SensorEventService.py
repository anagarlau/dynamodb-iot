import asyncio
from itertools import chain

import aioboto3
from boto3.dynamodb.conditions import Key
from shapely import Point

from backend.service.SensorService import SensorService
from dynamodbgeo.dynamodbgeo import GeoDataManager, GeoDataManagerConfiguration
from utils.polygon_def import hashKeyLength, create_dynamodb_client, aws_access_key_id, aws_secret_access_key, \
    region_name, endpoint_url
from utils.sensor_events.sensor_events_generation import convert_to_unix_epoch


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

        unix_from_date = convert_to_unix_epoch(from_date)
        unix_to_date = convert_to_unix_epoch(to_date)

        tasks = [self.get_sensor_events(sensor['sensor_id'], data_type, unix_from_date, unix_to_date) for sensor in
                 sensor_ids]
        # Nested list
        nested_events = await asyncio.gather(*tasks)
        # Flatten
        all_events = list(chain.from_iterable(nested_events))
        return all_events


async def main():
    service = SensorEventService()
    #events = await service.get_sensor_events('3cec4677-92d7-4a88-9b66-1a1323c6288d', 'Humidity', 1583276400, 1694123999)
    center_point = Point(28.1250063, 46.6334964)
    events = await service.get_sensors_events_in_radius_per_data_type(center_point,
                                                              200,
                                                              'Humidity',
                                                              '2021-07-02T16:27:30',
                                                              '2022-10-16T23:52:35')

    print(len(events))

# Run the async main function
if __name__ == "__main__":
    asyncio.run(main())