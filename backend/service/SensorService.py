import uuid
from datetime import datetime

from botocore.exceptions import ClientError, BotoCoreError
from shapely import Point, Polygon

from backend.models.SensorMaintenance import SensorMaintenance
from backend.service.ParcelService import ParcelService
from backend.models.SensorDetails import SensorDetails
from backend.models.SensorEvent import DataType
from backend.models.SensorLocationHistory import SensorLocationHistory
from backend.models.SensorMetadata import SensorMetadata
from dynamodbgeo.dynamodbgeo import GeoDataManagerConfiguration, GeoDataManager, QueryRadiusRequest, GeoPoint, \
    QueryRectangleRequest
from utils.sensor_events.sensor_events_generation import convert_to_unix_epoch
from utils.sensors.sensor_placing_generation import is_point_in_parcel
from utils.sensors.sensors_from_csv import parse_sensor_data, visualize_results, visualize_results_in_rectangle
from utils.polygon_def import create_dynamodb_client, hashKeyLength


class SensorService:
    def __init__(self):
        self.dynamodb = create_dynamodb_client()
        self.config = GeoDataManagerConfiguration(self.dynamodb, 'IoT')
        self.config.hashKeyAttributeName = 'PK'
        self.config.rangeKeyAttributeName = 'SK'
        self.geoDataManager = GeoDataManager(self.config)
        self.config.hashKeyLength = hashKeyLength
        self.table_name = 'IoT'
        self.parcel_service = ParcelService()

    def get_sensor_details_by_id(self, sensor_id):
        try:
            response = self.dynamodb.query(
                TableName=self.table_name,
                KeyConditionExpression="PK = :pk_val AND begins_with(SK, :sk_val)",
                ExpressionAttributeValues={
                    ":pk_val": {'S': f'Sensor#{sensor_id}'},
                    ":sk_val": {'S': 'Metadata#'}
                },
                Limit=1,
                ReturnConsumedCapacity='TOTAL'
            )
            items = response.get('Items', [])
            if items:
                sensor_details = SensorDetails(items[0])
                consumed_capacity = response.get('ConsumedCapacity', {}).get('CapacityUnits', 0)
                print(f'Sensor#Metadata for ID {sensor_id}, Consumed Capacity Units: {consumed_capacity}')
                return sensor_details
            else:
                print(f"No item found with Sensor ID: {sensor_id}")
                return None
        except (BotoCoreError, ClientError) as error:
            print(f"An error occurred: {error}")
            return None

    def add_sensor(self, lon, lat, sensor_details):
        try:
            sensor_metadata = SensorMetadata(lon, lat, sensor_details)
            all_parcels = self.parcel_service.get_all_active_parcels_in_field()
            parcel_for_point = is_point_in_parcel(sensor_metadata.location, all_parcels)
            if not parcel_for_point:
                raise Exception(f"The point does not fall within the coordinates of active parcels")
            sensor_id = uuid.uuid4()
            parcel_id = parcel_for_point.SK
            sensor_metadata_record = sensor_metadata.get_sensor_metadata_record(sensor_id, parcel_id)
            sensor_location_record = sensor_metadata.get_sensor_location_record(sensor_id, parcel_id,
                                                                                datetime.now().strftime("%Y-%m-%dT%H:%M:%S"))
            transact_items = [
                {'Put': {'TableName': self.table_name, 'Item': sensor_metadata_record,
                         'ConditionExpression':'attribute_not_exists(PK) AND attribute_not_exists(SK)'}},
                {'Put': {'TableName': self.table_name, 'Item': sensor_location_record}}
            ]
            self.dynamodb.transact_write_items(TransactItems=transact_items)
            return sensor_id
        except (ClientError, BotoCoreError, ValueError, Exception) as e:
            print(f"Error adding sensor to DB: {e}")
            return None

    def get_all_active_sensors_in_field_or_with_optional_parcel_id(self, parcel_id=None, sensor_type=None):
        gsi_name = 'GSI_Sensor_By_Parcel'
        params = {'TableName': self.table_name,
                  'IndexName': gsi_name,
                  'ReturnConsumedCapacity': 'TOTAL'}
        if parcel_id:
            key_condition = 'curr_parcelid = :parcelId'
            expression_attribute_values = {':parcelId': {'S': parcel_id}}
            if sensor_type:
                key_condition += f" AND begins_with(SK, :type)"
                expression_attribute_values[':type'] = {'S': f'Metadata#{sensor_type}'}
            params.update({
                'KeyConditionExpression': key_condition,
                'ExpressionAttributeValues': expression_attribute_values
            })
            dynamodb_operation = self.dynamodb.query
        else:
            dynamodb_operation = self.dynamodb.scan
            if sensor_type:
                filter_expression = f"begins_with(SK, :type)"
                expression_attribute_values = {':type': {'S': f'Metadata#{sensor_type}'}}

                params.update({
                    'FilterExpression': filter_expression,
                    'ExpressionAttributeValues': expression_attribute_values
                })
        response = dynamodb_operation(**params)
        data = response.get('Items', [])
        total_consumed_capacity = response.get('ConsumedCapacity', {}).get('CapacityUnits', 0)
        while 'LastEvaluatedKey' in response:
            params['ExclusiveStartKey'] = response['LastEvaluatedKey']
            response = dynamodb_operation(**params)
            data.extend(response.get('Items', []))
            total_consumed_capacity += response.get('ConsumedCapacity', {}).get('CapacityUnits', 0)
        print(f'Total active sensors retrieved: {len(data)}, Consumed Capacity Units {total_consumed_capacity}')
        parsed_data = parse_sensor_data(data)
        map = visualize_results(center_point=None, radius=None, sensors=parsed_data)
        map.save("vis_out/sensorservice/sensors-field-all.html")
        return parsed_data

    def get_all_active_sensors_in_radius_by_type(self, center_point, radius_meters, sensor_type):
        lat, lon = center_point.y, center_point.x
        query_radius_input = {
            'GSI': {
                'Name': 'GSI_TypeGeohash6_FullGeohash',
                'PK': {'name': 'hash_key', 'type': 'S'},
                'SK': {'name': 'SK', 'value': f"Metadata#{sensor_type}#", 'type': 'S', 'composite': True}
            }
            # ,"Filters": "attribute_exists(curr_parcelid)",
            # "ExpressionAttributeValues": {
            #     ':metadataPrefix': {'S': "METADATA#"}
            # }
        } #begins_with(SK, :metadataPrefix) AND
        # Perform the radius query
        response = self.geoDataManager.queryRadius(
            QueryRadiusRequest(
                GeoPoint(lat, lon),  # center point
                radius_meters,  # search radius in meters
                query_radius_input,  # additional filter input
                sort=True  # sort by distance from the center point
            )
        )

        data = parse_sensor_data(response['results'])
        map = visualize_results(center_point, radius_meters, data)
        sensor_ids = [item['sensor_id'].split("#")[1] for item in data]
        print(sensor_ids)
        print(len(sensor_ids))
        print('>>All Active in radius: Total data', len(data), 'with consumed Capacity Units',
              response['consumed_capacity'])
        map.save("vis_out/sensorservice/sensors-active-radius.html")
        return data

    def get_active_sensors_in_radius_for_time_range(self, center_point, radius_meters, from_date, to_date):
        start_range_unix = convert_to_unix_epoch(from_date)
        end_range_unix = convert_to_unix_epoch(to_date)

        lat, lon = center_point.y, center_point.x
        query_radius_input = {
            'GSI': {
                'Name': 'GSI_Geohash6_FullGeohash',
                'PK': {'name': 'hash_key', 'type': 'S'},
                'SK': {'name': 'geohash', 'type': 'S'}
            },
            "Filters": "SK <= :sk_end  AND (attribute_not_exists(moved_at) OR (moved_at >= :startDate AND moved_at <= :endDate) OR (moved_at >= :startDate AND moved_at >= :endDate))",
            "ExpressionAttributeValues": {
                ':startDate': {'N': f"{start_range_unix}"},
                ':endDate': {'N': f"{end_range_unix}"},
                # ':locationPrefix': {'S': 'Location#'},
                # ':sk_start': {'S': f"Location#{start_range_unix}#"},
                ':sk_end': {'S': f"Location#{end_range_unix}#zzzzzzzz"}
            }
        }
        # Perform the radius query
        response = self.geoDataManager.queryRadius(
            QueryRadiusRequest(
                GeoPoint(lat, lon),  # center point
                radius_meters,  # search radius in meters
                query_radius_input,  # additional filter input
                sort=True  # sort by distance from the center point
            )
        )

        data = parse_sensor_data(response['results'])
        map = visualize_results(center_point, radius_meters, data)

        print('>>Radius Time Range: Total data', len(response['results']), 'with consumed Capacity Units',
              response['consumed_capacity'])
        map.save("vis_out/sensorservice/sensors-radius-timerange.html")
        return data

    def get_active_sensors_in_rectangle_for_time_range(self, polygon_coords, from_date, to_date):
        start_range_unix = convert_to_unix_epoch(from_date)
        end_range_unix = convert_to_unix_epoch(to_date)
        polygon = Polygon(polygon_coords)
        min_lon, min_lat, max_lon, max_lat = polygon.bounds
        query_rectangle_input = {
            'GSI': {
                'Name': 'GSI_Geohash6_FullGeohash',
                'PK': {'name': 'hash_key', 'type': 'S'},
                'SK': {'name': 'geohash', 'type': 'S'}
            },
            "Filters": "SK <= :sk_end  AND (attribute_not_exists(moved_at) OR (moved_at >= :startDate AND moved_at <= :endDate) OR (moved_at >= :startDate AND moved_at >= :endDate))",
            "ExpressionAttributeValues": {
                ':startDate': {'N': f"{start_range_unix}"},
                ':endDate': {'N': f"{end_range_unix}"},
                # ':locationPrefix': {'S': 'Location#'},
                # ':sk_start': {'S': f"Location#{start_range_unix}#"},
                ':sk_end': {'S': f"Location#{end_range_unix}#zzzzzzzz"}
            }
        }
        # Rectangle query
        response = self.geoDataManager.queryRectangle(
            QueryRectangleRequest(
                GeoPoint(min_lat, min_lon),
                GeoPoint(max_lat, max_lon), query_rectangle_input))

        print('Point in polygon', polygon.contains(Point(28.12680874117647, 46.63242435294118)))
        data = parse_sensor_data(response['results'])
        map = visualize_results_in_rectangle(subpolygon=polygon, sensors=data)

        print('>>Rectangle Time Range: Total data', len(response['results']), 'with consumed Capacity Units',
              response['consumed_capacity'])
        map.save("vis_out/sensorservice/sensors-rectangle-timerange.html")
        # return data

    def get_active_sensors_in_radius_acc_to_type(self, center_point, radius_meters, sensor_type='Humidity'):
        # Prepare the filter expression and attribute values
        lat, lon = center_point.y, center_point.x
        query_radius_input = {
            'GSI': {
                'Name': 'GSI_Geohash6_FullGeohash',
                'PK': {'name': 'hash_key', 'type': 'S'},
                'SK': {'name': 'geohash', 'type': 'S'}
            },
            "Filters": "begins_with(SK, :metadataPrefix) AND (attribute_exists(curr_parcelid)) AND sensor_type = :st",
            "ExpressionAttributeValues": {
                ':metadataPrefix': {'S': "METADATA#"},
                ':st': {'S': sensor_type}
            }
        }
        # Perform the radius query
        response = self.geoDataManager.queryRadius(
            QueryRadiusRequest(
                GeoPoint(lat, lon),  # center point
                radius_meters,  # search radius in meters
                query_radius_input,  # additional filter input
                sort=True  # sort by distance from the center point
            )
        )

        data = parse_sensor_data(response['results'])
        # print(data[:2])
        print('>>In Radius by type: Total data', len(data), 'with consumed Capacity Units',
              response['consumed_capacity'])
        map = visualize_results(center_point, radius_meters, data)
        map.save("vis_out/sensorservice/sensors-radius-type.html")
        return data

    def get_sensor_location_history(self, sensor_id, filter_moved_at=False):
        params = {
            'TableName': self.table_name,
            'KeyConditionExpression': 'PK = :sensorId AND begins_with(SK, :locationPrefix)',
            'ExpressionAttributeValues': {
                ':sensorId': {'S': f"Sensor#{sensor_id}"},
                ':locationPrefix': {'S': 'Location#'}
            },
            'ReturnConsumedCapacity': 'TOTAL'
        }
        if filter_moved_at:
            params['FilterExpression'] = 'attribute_not_exists(moved_at)'

        try:
            response = self.dynamodb.query(**params)
            items = response.get('Items', [])
            consumed_capacity = response.get('ConsumedCapacity', {}).get('CapacityUnits', 0)
            print(
                f'>> Queried sensor locations for ID {sensor_id}, found {len(items)} records, Consumed Capacity Units: {consumed_capacity}')
            location_histories = [SensorLocationHistory(item) for item in items]
            return location_histories

        except ClientError as e:
            print(f"Error retrieving sensor location history: {e.response['Error']['Message']}")
            return None

    def batch_get_sensor_locations_histories(self, sensor_ids, filter_moved_at=False):
        all_sensor_histories = {}
        for sensor_id in sensor_ids:
            sensor_history = self.get_sensor_location_history(sensor_id, filter_moved_at)
            all_sensor_histories[sensor_id] = sensor_history
        return all_sensor_histories

    def move_sensor(self, sensor_type, sensor_id, new_lon, new_lat):
        # 1: Retrieve the sensor's current location history
        current_location = self.get_sensor_location_history(sensor_id=sensor_id, filter_moved_at=True)
        sensor_details = self.get_sensor_details_by_id(sensor_id)
        print(sensor_details)
        if len(current_location) < 1:
            print(f"No location history found for sensor ID: {sensor_id}")
            return None
        # Check if coordinates are valid and fall in one of the parcels
        new_point = Point(new_lon, new_lat)
        all_parcels = self.parcel_service.get_all_active_parcels_in_field()
        parcel_for_point = is_point_in_parcel(new_point, all_parcels)
        if not parcel_for_point:
            print(f"The point does not fall within the coordinates of active parcels")
            return None
        # 2: Create a new location history record and update current curr_parcelid in METADATA# and old location
        current_time_unix = convert_to_unix_epoch(datetime.now().strftime("%Y-%m-%dT%H:%M:%S"))
        geohash, hashkey = SensorMetadata.get_geohash_and_hashkey(new_lon, new_lat, hashKeyLength)
        # Create new location record
        new_location_history = {
            'Put': {
                'TableName': self.table_name,
                'Item': {
                    'PK': {'S': f"Sensor#{sensor_id}"},
                    'SK': {'S': f'Location#{geohash}'},
                    'geoJson': {'S': f'{new_lat},{new_lon}'},
                    'hash_key': {'S': str(hashkey)},
                    'geohash': {'S': str(geohash)},
                    'id_parcel': {'S': parcel_for_point.SK},
                    'sensortype': {'S': current_location[0].sensor_type},
                    'placed_at': {'N': str(current_time_unix)}
                }
            }
        }

        # Delete former Metadata Record and create new one with the new data
        # SKs cannot be updated in DDB
        delete_old_metadata = {
            'Delete': {
                'TableName': self.table_name,
                'Key': {
                    'PK': {'S': f"Sensor#{sensor_id}"},
                    'SK': {'S': f'Metadata#{sensor_type}#{current_location[0].geohash}'}
                }
            }
        }

        # Insert new Metadata Record
        new_metadata_record = {
            'Put': {
                'TableName': self.table_name,
                'Item': {
                    'PK': {'S': f"Sensor#{sensor_id}"},
                    'SK': {'S': f'Metadata#{sensor_type}#{geohash}'},
                    'sensor_type': {'S': sensor_type},
                    'curr_parcelid': {'S': parcel_for_point.SK},
                    'geoJson': {'S': f'{new_lat},{new_lon}'},
                    'hash_key': {'S': str(hashkey)},
                    'geohash': {'S': str(geohash)},
                    'manufacturer': {'S': sensor_details.manufacturer},
                    'firmware': {'S': sensor_details.firmware},
                    'model': {'S': sensor_details.model}
                }
            }
        }

       # Update Old Location History Record
        update_old_location = {
            'Update': {
                'TableName': self.table_name,
                'Key': {
                    'PK': {'S': f"Sensor#{sensor_id}"},
                    'SK': {'S': current_location[0].sk}
                },
                'UpdateExpression': 'SET moved_at = :timestamp',
                'ExpressionAttributeValues': {
                    ':timestamp': {'N': str(current_time_unix)}
                }
            }
        }
        transact_items = [delete_old_metadata, new_location_history, new_metadata_record, update_old_location]
        try:
            self.dynamodb.transact_write_items(TransactItems=transact_items)
            print(f"Sensor {sensor_id} moved to new location: {new_lat}, {new_lon} in parcel {parcel_for_point.SK}")
        except ClientError as e:
            print(f"Error moving sensor: {e.response['Error']['Message']}")

    def retire_sensor(self, sensor_type, sensor_id):
        current_location = self.get_sensor_location_history(sensor_id=sensor_id, filter_moved_at=True)
        current_time_unix = convert_to_unix_epoch(datetime.now().strftime("%Y-%m-%dT%H:%M:%S"))
        if len(current_location) < 1:
            print(f"No location history found for sensor ID: {sensor_id}")
            return None
        update_metadata_record = {
            'Update': {
                'TableName': self.table_name,
                'Key': {
                    'PK': {'S': f"Sensor#{sensor_id}"},
                    'SK': {'S': f'Metadata#{sensor_type}#{current_location[0].geohash}'}
                },
                'UpdateExpression': 'REMOVE curr_parcelid, hash_key'
            }
        }
        update_old_location = {
            'Update': {
                'TableName': self.table_name,
                'Key': {
                    'PK': {'S': f"Sensor#{sensor_id}"},
                    'SK': {'S': current_location[0].sk}
                },
                'UpdateExpression': 'SET moved_at = :timestamp',
                'ExpressionAttributeValues': {
                    ':timestamp': {'N': str(current_time_unix)}
                }
            }
        }
        transact_items = [update_metadata_record, update_old_location]
        try:
            self.dynamodb.transact_write_items(TransactItems=transact_items)
            print(f"Sensor {sensor_type}{sensor_id} retired")
        except ClientError as e:
            print(f"Error moving sensor: {e.response['Error']['Message']}")

    # TODO : update sensor details (SK METADATA)


# Example usage of the class
if __name__ == "__main__":
    center_point = Point(28.1250063, 46.6334964)
    sensor_service = SensorService()
    subpolygon = [(28.1250063, 46.6334964), (28.1256516, 46.6322131), (28.1284625, 46.6330088), (28.127733, 46.6341875),
                  (28.1250063, 46.6334964)]
    sensor_det = sensor_service.get_sensor_details_by_id("5459217a-400a-4102-ad36-f628cd1adbeb")
    # print(sensor_det)
    sensor_locations = sensor_service.get_sensor_location_history("1d835be7-5984-4604-8c84-9a99b59201bb")
    # print(sensor_locations)

    # sensor_id=sensor_service.add_sensor( 28.12595, 46.63357,{
    #     'sensor_type': 'SoilMoisture',
    #     'manufacturer': 'Panasonic',
    #     'model': '12f',
    #     'firmware': 'a34'
    # })
    # print(sensor_id)
    # sensor_service.get_active_sensors_in_rectangle_for_time_range(subpolygon,
    #                                                           '2023-12-23T00:00:00',
    #                                                           '2023-12-18T16:00:00')

    sensor_service.get_all_active_sensors_in_field_or_with_optional_parcel_id(sensor_type="Humidity")
    #sensor_service.retire_sensor("SoilMoisture","a2ac35b9-f32a-43be-a6fa-2aa5f969440e")
    #sensor_service.move_sensor("SoilMoisture", "a93e5c4f-b143-498c-b4d5-939e513ff2df",   28.15071,46.62401)
    #sensor_service.retire_sensor("SoilMoisture", "a93e5c4f-b143-498c-b4d5-939e513ff2df")
    sensor_service.get_all_active_sensors_in_radius_by_type(center_point, 200, 'Temperature')
   # sensor_service.get_all_active_sensors_in_field_or_with_optional_parcel_id('Chickpeas#13bcb3a9-78c9-461c-99a7-2e18dbe3671a', "Humidity")  # 174
