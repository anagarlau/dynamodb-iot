import uuid
from datetime import datetime

from botocore.exceptions import ClientError
from shapely import Point

from backend.service.ParcelService import ParcelService
from backend.models.SensorDetails import SensorDetails
from backend.models.SensorEvent import DataType
from backend.models.SensorLocationHistory import SensorLocationHistory
from backend.models.SensorMetadata import SensorMetadata
from dynamodbgeo.dynamodbgeo import GeoDataManagerConfiguration, GeoDataManager, QueryRadiusRequest, GeoPoint
from utils.sensor_events.sensor_events_generation import convert_to_unix_epoch
from utils.sensors.sensor_placing_generation import is_point_in_parcel
from utils.sensors.sensors_from_csv import parse_sensor_data, visualize_results
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

    def add_sensor(self, lon, lat, sensor_type):
        try:
            sensor_metadata = SensorMetadata(lon, lat, sensor_type)
            all_parcels = self.parcel_service.get_all_active_parcels_in_field()
            parcel_for_point = is_point_in_parcel(sensor_metadata.location, all_parcels)
            if not parcel_for_point:
                raise Exception(f"The point does not fall within the coordinates of active parcels")
            sensor_id = uuid.uuid4()
            parcel_id = parcel_for_point.SK
            print(parcel_id)
            print(sensor_id)
            sensor_metadata_record = sensor_metadata.get_sensor_metadata_item_batch_record(sensor_id, parcel_id)
            sensor_location_record = sensor_metadata.get_sensor_location_event_batch_record(sensor_id, parcel_id,
                                                                                            datetime.now().strftime(
                                                                                                "%Y-%m-%dT%H:%M:%S"))
            batch = [
                {'PutRequest': {'Item': sensor_location_record}},
                {'PutRequest': {'Item': sensor_metadata_record}},
            ]
            response = self.dynamodb.batch_write_item(
                RequestItems={self.table_name: batch}
            )
            print(f"Batch lengh {len(batch)} write completed.")
            return sensor_id
        except ClientError as e:
            print(f"Error adding sensor to DB: {e.response['Error']['Message']}")
        except ValueError as e:
            print(f"Validation error: {e}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")

    def get_all_active_sensors_in_field_or_with_optional_parcel_id(self, parcel_id=None):
        gsi_name = 'GSI_Sensor_By_Parcel'
        params = {
            'TableName': self.table_name,
            'IndexName': gsi_name,
            'ReturnConsumedCapacity': 'TOTAL'
        }
        if parcel_id:
            params.update({
                'KeyConditionExpression': 'curr_parcelid = :parcelId',
                'ExpressionAttributeValues': {':parcelId': {'S': parcel_id}}
            })
            dynamodb_operation = self.dynamodb.query
        else:
            dynamodb_operation = self.dynamodb.scan
        response = dynamodb_operation(**params)
        data = response.get('Items', [])
        total_consumed_capacity = response.get('ConsumedCapacity', {}).get('CapacityUnits', 0)

        # Pagination
        while 'LastEvaluatedKey' in response:
            params['ExclusiveStartKey'] = response['LastEvaluatedKey']
            response = self.dynamodb.scan(**params)  # Change this line to use scan
            data.extend(response.get('Items', []))
            total_consumed_capacity += response.get('ConsumedCapacity', {}).get('CapacityUnits', 0)

        print(
            f'>>All Active in field with optional Parcel Id: Total data {len(data)}, with consumed Capacity Units {total_consumed_capacity}')
        parsed_data = parse_sensor_data(data)
        map = visualize_results(center_point=None, radius=None, sensors=parsed_data)
        map.save("vis_out/sensorservice/sensors-field-all.html")
        return parsed_data

    def get_all_active_sensors_in_radius(self, center_point, radius_meters):
        print("Finding in radius")
        lat, lon = center_point.y, center_point.x
        query_radius_input = {
            'GSI': {
                'Name': 'GSI_Geohash6_FullGeohash',
                'PK': {'name': 'hash_key', 'type': 'S'},
                'SK': {'name': 'geohash', 'type': 'S'}
            },
            "Filters": "begins_with(SK, :metadataPrefix) AND (attribute_exists(curr_parcelid))",
            "ExpressionAttributeValues": {
                ':metadataPrefix': {'S': "METADATA#"}
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
        sensor_ids = [item['sensor_id'].split("#")[1] for item in data]
        print(sensor_ids)
        print(len(sensor_ids))
        print('>>All Active in radius: Total data', len(data), 'with consumed Capacity Units',
              response['consumed_capacity'])
        map.save("vis_out/sensorservice/sensors-active-radius.html")
        return data

    def get_all_active_sensors_of_type_in_field(self, sensor_type):
        if not any(sensor_type == member.value for member in DataType):
            raise ValueError(f"Invalid sensor type: {sensor_type}")

        params = {
            'TableName': self.table_name,
            'IndexName': 'GSI_ActiveSensor_By_Type',
            'KeyConditionExpression': 'sensor_type = :st',
            'ExpressionAttributeValues': {
                ':st': {'S': sensor_type}
            },
            'ReturnConsumedCapacity': 'TOTAL'
        }

        response = self.dynamodb.query(**params)
        total_consumed_capacity = response.get('ConsumedCapacity', {}).get('CapacityUnits', 0)
        print('>>In Field by type: Total data', len(response['Items']), 'with consumed Capacity Units',
              total_consumed_capacity)
        parsed_data = parse_sensor_data(response['Items'])
        # sensor_ids = [item['sensor_id'].split("#")[1] for item in parsed_data]
        map = visualize_results(center_point=None, radius=None, sensors=parsed_data)
        map.save("vis_out/sensorservice/sensors-field-type.html")
        return parsed_data

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
            "Filters": 'SK <= :sk_end AND (attribute_not_exists(moved_at) OR (moved_at >= :startDate AND moved_at <= :endDate))',
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

    def get_sensors_in_radius_acc_to_type(self, center_point, radius_meters, sensor_type='Humidity'):
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

    def get_sensor_details_by_id(self, sensor_id):
        # SK with pattern 'METADATA#sensor_id'
        sk = f'METADATA#{sensor_id}'
        params = {
            'TableName': self.table_name,
            'Key': {
                'PK': {'S': sensor_id},
                'SK': {'S': sk}
            },
            'ReturnConsumedCapacity': 'TOTAL'  # Include consumed capacity in the response
        }

        try:
            response = self.dynamodb.get_item(**params)
            item = response.get('Item', None)

            if item:
                sensor_details = SensorDetails(item)
                consumed_capacity = response.get('ConsumedCapacity', {}).get('CapacityUnits', 0)
                print(f'>> Queried sensor metadata for ID {sensor_id}, Consumed Capacity Units: {consumed_capacity}')
                return sensor_details
            else:
                print(f"No item found with Sensor ID: {sensor_id}")
                return None

        except ClientError as e:
            print(f"Error retrieving sensor details: {e.response['Error']['Message']}")
            return None

    def get_sensor_location_history(self, sensor_id, filter_moved_at=False):
        params = {
            'TableName': self.table_name,
            'KeyConditionExpression': 'PK = :sensorId',
            'ExpressionAttributeValues': {
                ':sensorId': {'S': f"Location#{sensor_id}"}
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

    def move_sensor(self, sensor_id, new_lon, new_lat):
        # 1: Retrieve the sensor's current location history
        current_location = self.get_sensor_location_history(sensor_id=sensor_id, filter_moved_at=True)
        print(current_location)
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
                    'PK': {'S': f"Location#{sensor_id}"},
                    'SK': {'S': f'Location#{current_time_unix}#{sensor_id}'},
                    'geoJson': {'S': f'{new_lat},{new_lon}'},
                    'hash_key': {'S': str(hashkey)},
                    'geohash': {'S': str(geohash)},
                    'id_parcel': {'S': parcel_for_point.SK},
                    'sensortype': {'S': current_location[0].sensor_type}
                }
            }
        }

        # Update Current Metadata Record
        update_metadata_record = {
            'Update': {
                'TableName': self.table_name,
                'Key': {
                    'PK': {'S': sensor_id},
                    'SK': {'S': f'METADATA#{sensor_id}'}
                },
                'UpdateExpression': 'SET curr_parcelid = :newParcelId, geoJson = :newGeoJson, hash_key = :newHashKey, geohash = :newGeohash',
                'ExpressionAttributeValues': {
                    ':newParcelId': {'S': parcel_for_point.SK},
                    ':newGeoJson': {'S': f'{new_lat},{new_lon}'},
                    ':newHashKey': {'S': str(hashkey)},
                    ':newGeohash': {'S': str(geohash)}
                }
            }
        }

        # Update Old Location History Record
        update_old_location = {
            'Update': {
                'TableName': self.table_name,
                'Key': {
                    'PK': {'S': f"Location#{sensor_id}"},
                    'SK': {'S': current_location[0].sk}
                },
                'UpdateExpression': 'SET moved_at = :timestamp',
                'ExpressionAttributeValues': {
                    ':timestamp': {'N': str(current_time_unix)}
                }
            }
        }
        transact_items = [new_location_history, update_metadata_record, update_old_location]
        try:
            self.dynamodb.transact_write_items(TransactItems=transact_items)
            print(f"Sensor {sensor_id} moved to new location: {new_lat}, {new_lon} in parcel {parcel_for_point.SK}")
        except ClientError as e:
            print(f"Error moving sensor: {e.response['Error']['Message']}")

    # TODO : update sensor details (SK METADATA)
    # def update_sensor_details(self):


# Example usage of the class
if __name__ == "__main__":
    center_point = Point(28.1250063, 46.6334964)
    sensor_service = SensorService()
    sensor_service.get_all_active_sensors_in_radius(center_point, 200)
    # sensor_service.get_all_active_sensors_in_field_or_with_optional_parcel_id()
    # sensor_service.get_all_active_sensors_in_field_or_with_optional_parcel_id(
    #     'Chickpeas#3225eba0-4695-48ee-9616-62dc5256b4e2')  # 174
    # sensor_service.get_all_active_sensors_of_type_in_field("Humidity")
    sensor_service.get_sensors_in_radius_acc_to_type(center_point, 200, "Temperature")
    sensor_service.get_active_sensors_in_radius_for_time_range(center_point,
                                                               200,
                                                               '2020-01-01T00:00:00',
                                                               '2025-12-21T16:00:00')
    # sensor_service.get_all_active_sensors_in_field_or_with_optional_parcel_id(
    #     "Grapevine#dcce31f9-ccf6-4f0c-985d-91d8a2276091")  # 21
    # sensor_service.add_sensor(28.12565, 46.63328, 'SoilMoisture')  # bottom point 28.14247, 46.62179
    # sensor_service.get_all_active_sensors_in_field_or_with_optional_parcel_id()  # 175
    # sensor_service.get_all_active_sensors_in_field_or_with_optional_parcel_id(
    #     "Grapevine#dcce31f9-ccf6-4f0c-985d-91d8a2276091")  # 22
    # sensor = sensor_service.get_sensor_details_by_id(
    #     "924bb19d-ea3c-44d8-9980-4ece23fecdde")  # parcelid Grapevine#dcce31f9-ccf6-4f0c-985d-91d8a2276091
    # print(sensor)
    # location_history = sensor_service.get_sensor_location_history("e5352d72-3165-4f93-935f-0ed3ab17a871")
    # print(location_history)

    # sensor_service.move_sensor("7c627c03-e82b-483a-83c8-1f507b9515fd",   28.14247, 46.62179)
    # sensor_service.get_all_active_sensors_in_field_or_with_optional_parcel_id(
    #     "Chickpeas#957000a4-6b4a-4ff7-979d-9764d086ca01")  # 21
    # sensor = sensor_service.get_sensor_details_by_id("1988c59f-782b-4bc8-91a2-5041370b4595")
    # print(sensor)
    location_history = sensor_service.get_sensor_location_history("7c627c03-e82b-483a-83c8-1f507b9515fd")
    print(location_history)
    #
    # sensor_service.get_all_active_sensors_of_type_in_field("Temperature")
    # sensor_service.get_active_sensors_in_radius_for_time_range(center_point, 200) #2
    # sensor_service.get_sensors_in_radius_acc_to_type(center_point, 200, sensor_type='Temperature')#1
    # sensor_service.get_sensors_in_radius_acc_to_type(center_point, 200, sensor_type='SoilPH')#1
    # sensor_service.add_sensor( 28.14247,46.62179, 'Humidity')
    # sensor_service.get_all_sensors_with_optional_parcel_id()  # 174
