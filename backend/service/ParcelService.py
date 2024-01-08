import ast
import uuid
from datetime import datetime

import dateutil.utils
from boto3.dynamodb.conditions import Key
from botocore.exceptions import BotoCoreError, ClientError
from shapely import Polygon

from backend.models.Parcel import Parcel
from dynamodbgeo.dynamodbgeo import GeoDataManagerConfiguration, GeoDataManager, QueryRectangleRequest, GeoPoint
from utils import polygon_def
from utils.polygon_def import create_dynamodb_client, hashKeyLength
from utils.sensor_events.sensor_events_generation import convert_to_unix_epoch


class ParcelService:
    def __init__(self):
        self.dynamodb = create_dynamodb_client()
        self.config = GeoDataManagerConfiguration(self.dynamodb, 'IoT')
        self.config.hashKeyAttributeName = 'PK'
        self.config.rangeKeyAttributeName = 'SK'
        self.geoDataManager = GeoDataManager(self.config)
        self.config.hashKeyLength = hashKeyLength

    def add_parcel(self, entry):
        try:
            parcel_polygon = Polygon(entry['polygon_coord'])
            field_bounds = polygon_def.polygon
            is_in_field = field_bounds.contains(parcel_polygon)
            if not is_in_field:
                raise Exception("Polygon is not in field")
            existing_parcels = self.get_all_active_parcels_in_field()
            for parcel in existing_parcels:
                existing_parcel_polygon = parcel.polygon
                if parcel_polygon.intersects(existing_parcel_polygon):
                    raise Exception(f"Planned parcel clashes with currently active parcel id {parcel.PK}")
            parcel_id = uuid.uuid4()
            new_parcel = {
                'PK': 'Parcel',
                'SK': f"{entry['plant_type']}#{parcel_id}",
                'polygon_coord': str(entry['polygon_coord']),
                'plant_type': entry['plant_type'],
                'details': {
                    'latin_name': entry['details']['latin_name'],
                    'family': entry['details']['family']
                },
                'optimal_temperature': entry['optimal_temperature'],
                'optimal_humidity': entry['optimal_humidity'],
                'optimal_soil_ph': entry['optimal_soil_ph'],
                'water_requirements_mm_per_week': entry['water_requirements_mm_per_week'],
                'sunlight_requirements_hours_per_day': entry['sunlight_requirements_hours_per_day']
            }
            if 'active' in entry:
                new_parcel['active'] = entry['active']
            self.dynamodb.put_item(TableName=self.config.tableName, Item=new_parcel)
            return parcel_id
        except (BotoCoreError, ClientError, Exception) as error:
            print(f"An error occurred: {error}")
            return None

    def parse_area_response(self, items):
        parsed_data = []

        for item in items:
            args = {
                'water_requirements_mm_per_week': item.get('water_requirements_mm_per_week', {}).get('S', '0'),
                'optimal_humidity': item.get('optimal_humidity', {}).get('S', '(0, 0)'),
                'optimal_soil_ph': item.get('optimal_soil_ph', {}).get('S', '(0.0, 0.0)'),
                'optimal_temperature': item.get('optimal_temperature', {}).get('S', '(0, 0)'),
                'sunlight_requirements_hours_per_day': item.get('sunlight_requirements_hours_per_day', {}).get('S',
                                                                                                               '0'),
                'polygon': item.get('polygon_coord', {}).get('S', '[]'),
                'SK': item.get('SK', {}).get('S', ''),
                'details': {k: v['S'] for k, v in item.get('details', {}).get('M', {}).items()},
                'PK': item.get('PK', {}).get('S', ''),
                'plant_type': item.get('plant_type', {}).get('S', '')
            }
            parsed_data.append(Parcel(**args))

        return parsed_data

    def get_all_active_parcels_in_field_optionally_by_plant_type(self, plant_type=None):
        key_condition_expression = "active = :pk_val"
        expression_attribute_values = {
            ":pk_val": {'N': '1'}
        }
        if plant_type is not None:
            key_condition_expression += f" AND begins_with(SK, :sk_val)"
            expression_attribute_values[":sk_val"] = {'S': f"{plant_type.capitalize()}#"}
        response = self.dynamodb.query(
            TableName=self.config.tableName,
            IndexName='GSI_Active_Parcels',
            KeyConditionExpression=key_condition_expression,
            ExpressionAttributeValues=expression_attribute_values,
            ReturnConsumedCapacity='Indexes'
        )
        items = response.get('Items', [])
        data = self.parse_area_response(items)
        return data

    def get_all_parcels_optionally_by_plant_type(self, plant_type=None):
        try:
            key_condition_expression = "PK = :pk_val"
            expression_attribute_values = {
                ":pk_val": {'S': 'Parcel'}
            }
            if plant_type is not None:
                key_condition_expression += " AND begins_with(SK, :sk_val)"
                expression_attribute_values[":sk_val"] = {'S': f"{plant_type.capitalize()}#"}
            response = self.dynamodb.query(
                TableName=self.config.tableName,
                KeyConditionExpression=key_condition_expression,
                ExpressionAttributeValues=expression_attribute_values,
                ReturnConsumedCapacity='Indexes'
            )
            print(response['ConsumedCapacity']['CapacityUnits'])
            items = response.get('Items', [])
            data = self.parse_area_response(items)
            return data
        except (BotoCoreError, ClientError) as error:
            print(f"An error occurred: {error}")
        return None

    def get_all_active_parcels_in_field(self):
        return self.get_all_active_parcels_in_field_optionally_by_plant_type()

    def get_parcel_by_id(self, id):
        response = self.dynamodb.get_item(
            TableName=self.config.tableName,
            Key={
                self.config.hashKeyAttributeName: {'S': 'Parcel'},
                self.config.rangeKeyAttributeName: {'S': id}
            },
            ReturnConsumedCapacity='TOTAL'
        )
        print(response.get('ConsumedCapacity', {}).get('CapacityUnits', 0))
        item = response.get('Item', None)
        if item:
            return self.parse_area_response([item])[0]
        return None

    # TODO add timespan active between
    def retire_parcel(self, parcel_id):
        from backend.service.SensorService import SensorService
        sensor_service = SensorService()
        sensors_details = sensor_service.get_all_active_sensors_in_field_or_with_optional_parcel_id(parcel_id)
        sensor_ids = [sensor['sensor_id'].split("#")[1] for sensor in sensors_details]
        sensors_location_histories = sensor_service.batch_get_sensor_locations_histories(sensor_ids, True)
        transact_items = []
        transact_items.append({
            'Update': {
                'TableName': self.config.tableName,
                'Key': {
                    'PK': {'S': f'Parcel'},
                    'SK': {'S': f'{parcel_id}'}
                },
                'UpdateExpression': 'SET active_to = :val REMOVE active',
                'ExpressionAttributeValues': {
                    ':val': {'S': str(dateutil.utils.today())}
                }
            }
        })
        for key, value in sensors_location_histories.items():
            sensor_id = key
            transact_items.append({
                'Update': {
                    'TableName': self.config.tableName,
                    'Key': {
                        'PK': {'S': f"Sensor#{sensor_id}"},
                        'SK': {'S': f"Metadata#{value[0].sensor_type}#{value[0].geohash}"}
                    },
                    'UpdateExpression': 'REMOVE curr_parcelid, hash_key'
                }
            })
            current_date = convert_to_unix_epoch(datetime.now().strftime("%Y-%m-%dT%H:%M:%S"))
            transact_items.append({
                'Update': {
                    'TableName': self.config.tableName,
                    'Key': {
                        'PK': {'S': f"Sensor#{sensor_id}"},
                        'SK': {'S': f"{value[0].sk}"}
                    },
                    'UpdateExpression': 'SET moved_at = :movedAt',
                    'ExpressionAttributeValues': {
                        ':movedAt': {'S': str(current_date)}
                    }
                }
            })
        response = self.dynamodb.transact_write_items(TransactItems=transact_items)
        print(response)


if __name__ == "__main__":
    service = ParcelService()
    res = service.retire_parcel("Chickpeas#13bcb3a9-78c9-461c-99a7-2e18dbe3671a")
    #res = service.get_parcel_by_id("Chickpeas#13bcb3a9-78c9-461c-99a7-2e18dbe3671a")
    res = service.get_all_parcels_optionally_by_plant_type()
    # print(res[:1])
    print(len(res))
    res = service.get_all_parcels_optionally_by_plant_type("ChickpeAS")
    # print(res[:1])
    print(len(res))
    # res = service.get_all_active_parcels_in_field()
    # print(res[:1])
    # print(len(res))
    res = service.get_all_active_parcels_in_field_optionally_by_plant_type()
    # print(res[:1])
    print(len(res))
    parcel = {
        'polygon_coord': [(40.7128, -74.0060), (40.7129, -74.0061), (40.7130, -74.0062), (40.7131, -74.0063)],
        'plant_type': 'Chickpeas',
        'active': 1,  # Optional, can be omitted if not needed
        'details': {
            'latin_name': 'Cicer arietinum',
            'family': 'Fabaceae'
        },
        'optimal_temperature': 20,  # in degrees Celsius
        'optimal_humidity': 60,  # in percentage
        'optimal_soil_ph': 6.5,  # pH level
        'water_requirements_mm_per_week': 25,  # in millimeters
        'sunlight_requirements_hours_per_day': 6  # in hours
    }
    print(service.add_parcel(parcel))
    # res = service.get_all_active_parcels_in_field_by_type("Chickpeas")
    # print(res[:1])
    # print(len(res))
    # area = service.get_parcel_by_id('Chickpeas#3225eba0-4695-48ee-9616-62dc5256b4e2')
    # print(area)
