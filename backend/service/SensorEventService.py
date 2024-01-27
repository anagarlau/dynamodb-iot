from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Tuple, List, Dict, Optional

import shapely
from botocore.exceptions import ClientError, BotoCoreError
from dateutil.relativedelta import relativedelta
from shapely import Point

from backend.models.AggregateData import AggregateData
from backend.models.SensorEvent import SensorEvent, DataType
from backend.service.SensorService import SensorService
from dynamodbgeo.dynamodbgeo import GeoDataManager, GeoDataManagerConfiguration
from playground import calculate_pks
from utils.polygon_def import hashKeyLength, create_dynamodb_client
from utils.sensor_events.sensor_events_generation import convert_to_unix_epoch, get_first_of_month_as_unix_timestamp, \
    format_date


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

    def add_records(self, records: List[Dict]):
        self.dynamodb.transact_write_items(TransactItems=records)

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
                        IndexName='GSI_Events_By_Sensor',
                        KeyConditionExpression=f"s_id = :pval AND SK BETWEEN :sval AND :eval",
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
                        IndexName='GSI_Events_By_Sensor',
                        KeyConditionExpression=f"s_id = :pval AND SK BETWEEN :sval AND :eval",
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
            return [SensorEvent.from_entity(item) for item in all_items]
        except (ClientError, BotoCoreError, Exception) as e:
            print(f"An error occurred while retrieving sensor events for sensor {sensor_id}:", e)
            return []

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
                        IndexName='GSI_Events_By_Sensor',
                        KeyConditionExpression=f"s_id = :pval AND begins_with(SK, :skval)",
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
                        IndexName='GSI_Events_By_Sensor',
                        KeyConditionExpression=f"s_id = :pval AND begins_with(SK, :skval)",
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
            return [SensorEvent.from_entity(item) for item in items]
        except (ClientError, BotoCoreError, Exception) as e:
            print("Boto3 client error:", e)
            return [], None

    def query_sensor_events_for_field_in_time_range_by_type(self, start_range, end_range, data_type):
        try:
            pks = calculate_pks(start_range, end_range)
            start_range_unix = convert_to_unix_epoch(start_range)
            end_range_unix = convert_to_unix_epoch(end_range)
            items = []
            consumed_capacity = 0
            network_calls_count = 0
            for pk in pks:
                first_of_month = get_first_of_month_as_unix_timestamp(pk)
                last_evaluated_key = None
                while True:
                    query_params = {
                        'TableName': self.table_name,
                        'KeyConditionExpression': f"PK = :pval AND SK BETWEEN :sval AND :eval",
                        'ExpressionAttributeValues': {
                            ':pval': {'S': f"{data_type}#{first_of_month}"},
                            ':sval': {'S': f'Timestamp#{start_range_unix}#'},
                            ':eval': {'S': f'Timestamp#{end_range_unix}#'}
                        },
                        'ReturnConsumedCapacity': 'TOTAL'
                    }
                    if last_evaluated_key:
                        query_params['ExclusiveStartKey'] = last_evaluated_key
                    response = self.dynamodb.query(**query_params)
                    items.extend(response.get('Items', []))
                    consumed_capacity += response.get('ConsumedCapacity')['CapacityUnits']
                    network_calls_count += 1
                    last_evaluated_key = response.get('LastEvaluatedKey')
                    if not last_evaluated_key:
                        break
            print("Number of network calls for range is", network_calls_count)
            print('Consumed Capacity', consumed_capacity)
            return [SensorEvent.from_entity(item) for item in items]
        except (ClientError, BotoCoreError, ValueError, Exception) as e:
            print("Boto3 client error:", e)
            return None
    def get_previous_month_timestamp(self, current_month_unix_timestamp):
        # Convert current month timestamp to datetime (not necessary here but for reusability purposes)
        current_month_datetime = datetime.fromtimestamp(current_month_unix_timestamp)
        first_day_of_current_month = datetime(current_month_datetime.year, current_month_datetime.month, 1)
        first_day_of_previous_month = first_day_of_current_month - relativedelta(months=1)
        # First of month as Unix timestamp
        return int(first_day_of_previous_month.timestamp())

    def query_sensor_events_by_parcelid_in_time_range(self, parcel_id, from_date, to_date, sensor_type_filters=None):
        start_range_unix = convert_to_unix_epoch(from_date)
        end_range_unix = convert_to_unix_epoch(to_date)
        try:
            query_params = {
                'TableName': self.table_name,
                'IndexName': 'GSI_AllSensorEvents_Parcel',
                'KeyConditionExpression': "parcel_id = :pid AND SK BETWEEN :start_range AND :end_range",
                'ExpressionAttributeValues': {
                    ':pid': {'S': parcel_id},
                    ':start_range': {'S': f"Timestamp#{start_range_unix}#"},
                    ':end_range': {'S': f"Timestamp#{end_range_unix}#"}
                },
                'ReturnConsumedCapacity': 'TOTAL'
            }
            if sensor_type_filters:
                type_filter_expressions = []
                for i, sensor_type in enumerate(sensor_type_filters):
                    type_key_placeholder = f":typeval{i}"
                    query_params['ExpressionAttributeValues'][type_key_placeholder] = {'S': sensor_type}
                    type_filter_expressions.append(f"data_type = {type_key_placeholder}")
                query_params['FilterExpression'] = " OR ".join(type_filter_expressions)
            response = self.dynamodb.query(**query_params)
            items = [SensorEvent.from_entity(item) for item in response.get('Items', [])]
            consumed_capacity = response.get('ConsumedCapacity')['CapacityUnits']
            print('Consumed Capacity', consumed_capacity)
            grouped_items = defaultdict(list)
            for item in items:
                grouped_items[item.data.dataType].append(item)
            return grouped_items
        except (ClientError, BotoCoreError, Exception) as e:
            print(f"An error occurred: {e}")
            return None

    #TODO
    def query_events_in_radius_for_timerange(self,
                                center_point: shapely.geometry.point.Point,
                                radius_meters: float,
                                from_date,
                                to_date):
        try:
            from backend.service.SensorService import SensorService
            sensor_service = SensorService()
            active_sensors_in_radius = sensor_service.get_active_sensors_in_radius_for_time_range(center_point, radius_meters, from_date, to_date)
            total = 0
            data_by_type = {}
            for active_sensor in active_sensors_in_radius:
                sensor_events = self.query_sensorevents_by_sensorid_in_time_range(active_sensor.sensor_id, from_date, to_date)
                total+=len(sensor_events)
                if active_sensor.sensor_type not in data_by_type.keys():
                    data_by_type[active_sensor.sensor_type] = sensor_events
                else:
                    data_by_type[active_sensor.sensor_type].extend(sensor_events)
            print(f"Total {total}")
            return data_by_type
        except (ClientError, BotoCoreError, Exception) as e:
            print(f"An error occurred: {e}")
            return []

    def query_events_in_rectangle_for_timerange(self, polygon_coords: List[Tuple[float, float]], from_date: str, to_date: str):
        try:
            from backend.service.SensorService import SensorService
            sensor_service = SensorService()
            active_sensors_in_radius = sensor_service.get_active_sensors_in_rectangle_for_time_range(polygon_coords, from_date, to_date)
            total = 0
            data_by_type = {}
            for active_sensor in active_sensors_in_radius:
                sensor_events = self.query_sensorevents_by_sensorid_in_time_range(active_sensor.sensor_id, from_date, to_date)
                total+=len(sensor_events)
                if active_sensor.sensor_type not in data_by_type.keys():
                    data_by_type[active_sensor.sensor_type] = sensor_events
                else:
                    data_by_type[active_sensor.sensor_type].extend(sensor_events)
            print(f"Total {total}")
            return data_by_type
        except (ClientError, BotoCoreError, Exception) as e:
            print(f"An error occurred: {e}")
            return []

    def query_aggregates(self, data_types: List[str] = None, date: str = None, month_year: Optional[Tuple[int, int]] = None):
        try:
            if data_types is None:
                raise ValueError("Missing data_types parameter. Please provide the data types to query by.")
            if (date is not None and month_year is not None) or (date is None and month_year is None):
                raise ValueError("Please provide either a date or a month&year, not both or neither.")
            elif date:
                print(date)
                day = format_date(date)
                first_of_month = get_first_of_month_as_unix_timestamp(day)
                unix_date = convert_to_unix_epoch(day)
                print(unix_date)
                prefix = 'Day'
            else:
                month, year = month_year
                first_of_month = convert_to_unix_epoch(datetime(year, month, 1).strftime("%Y-%m-%dT%H:%M:%S"))
                prefix = 'Month'
            DataType.validate_data_types(data_types)
            items = []
            consumed_capacity = 0
            with ThreadPoolExecutor(max_workers=len(data_types)) as executor:
                futures = []
                for type in data_types:
                    query_params = {
                        'TableName': self.table_name,
                        'KeyConditionExpression': "PK = :pk_val AND SK = :sk_val",
                        'ExpressionAttributeValues': {
                            ':pk_val': {'S': f"{type}#{first_of_month}"},
                            ':sk_val': {'S': f"Agg#{prefix}#{unix_date if prefix == 'Day' else first_of_month}"}
                        },
                        'ReturnConsumedCapacity': 'TOTAL'
                    }
                    future = executor.submit(self.dynamodb.query, **query_params)
                    futures.append(future)
                for future in as_completed(futures):
                    response = future.result()
                    items.extend(response.get('Items', []))
                    consumed_capacity += response.get('ConsumedCapacity')['CapacityUnits']
            print('Consumed Capacity', consumed_capacity)
            return [AggregateData(item) for item in items]
        except (ClientError, BotoCoreError, Exception) as e:
            print(f"An error occurred: {e}")





def main():
    service = SensorEventService()

    # agg = service.query_aggregates(data_types=['Humidity', 'SoilPH', 'Rain', 'Temperature'], date="2020-03-16") #month_year=(3, 2020)
    # print(agg)

    center_point = Point(28.1250063, 46.6334964)
    events = service.query_events_in_radius_for_timerange(
        center_point,
        500,
      '2023-01-12T00:00:00',
        '2023-12-12T16:00:00'    )
    print(events)
    rectangle = [(28.1250063, 46.6334964), (28.1256516, 46.6322131), (28.1285698, 46.6329204), (28.1278188, 46.6341654),
                 (28.1250063, 46.6334964)]
    events = service.query_events_in_rectangle_for_timerange(
        polygon_coords=rectangle,
        from_date='2020-01-12T00:00:00',
        to_date='2021-01-12T16:00:00')
    print(events)
    events = service.query_sensor_events_for_field_in_time_range_by_type("2020-03-14T00:00:00", "2020-03-14T23:59:59", "Rain")
    print(events)
    # # events = await service.get_sensor_events('3cec4677-92d7-4a88-9b66-1a1323c6288d', 'Humidity', 1583276400, 1694123999)
    # # center_point = Point(28.1250063, 46.6334964)
    # # events = await service.get_sensors_events_in_radius_per_data_type(center_point,
    # #                                                           200,
    # #                                                           'Humidity',
    # #                                                           '2021-07-02T16:27:30',
    # #                                                           '2022-10-16T23:52:35')
    # # events = service.query_sensorevents_for_entire_field_in_time_range(
    # #     '2020-01-01T04:35:53',
    # #     '2020-02-07T14:56:50',['Humidity', 'Light', 'Temperature']
    # #     )
    # # print(len(events))
    # # print(events[0])
    # events = service.query_sensorevents_by_sensorid_in_time_range(
    #              '32c3ecce-6589-445f-8f64-4d7422d4f1bf',
    #                     '2020-03-16T04:21:21',
    #                     '2021-11-27T08:02:50')
    # print(len(events))
    # # print(events[0])
    # events = service.query_latest_n_sensorevents_by_sensorid('32c3ecce-6589-445f-8f64-4d7422d4f1bf',4)
    # print(len(events))
    # for item in events:
    #     print(item['SK']['S'])
    #
    # json = {
    #     "sensorId": "32c3ecce-6589-445f-8f64-4d7422d4f1bf",
    #     "metadata": {
    #         "location": "(46.63366128235294, 28.12680874117647)",
    #         "battery_level": 33,
    #         "status": "Active",
    #         "parcel_id": "Chickpeas#af8ed50d-68c4-4cf9-b04e-bba5432d4b8e"
    #     },
    #     "data": {
    #         "dataType": "SoilPH",
    #         "dataPoint": 1,
    #         "timestamp": "2023-12-22T16:01:00"
    #     }
    # }
    # service.add_sensor_event(json)
    # events = service.query_sensor_events_by_parcelid_in_time_range("Chickpeas#af8ed50d-68c4-4cf9-b04e-bba5432d4b8e",
    #                                                                "2020-01-07T20:47:52", "2020-01-20T06:16:31",
    #                                                                ["Humidity", "SoilMoisture"])
    # for ev in events:
    #     print(ev, events[ev])
# Run the async main function
if __name__ == "__main__":
    main()