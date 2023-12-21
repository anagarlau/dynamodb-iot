import ast
from datetime import datetime

from boto3.dynamodb.conditions import Key
from shapely import Polygon

from backend.models.Parcel import Parcel
from dynamodbgeo.dynamodbgeo import GeoDataManagerConfiguration, GeoDataManager, QueryRectangleRequest, GeoPoint
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

    def get_all_active_parcels_in_field_by_type(self, plant_type=None):
        # Base KeyConditionExpression and ExpressionAttributeValues
        key_condition_expression = "active = :pk_val"
        expression_attribute_values = {
            ":pk_val": {'N': '1'}
        }

        if plant_type is not None:
            key_condition_expression += f" AND begins_with({self.config.rangeKeyAttributeName}, :sk_val)"
            expression_attribute_values[":sk_val"] = {'S': f"{plant_type}#"}

        # Query
        response = self.dynamodb.query(
            TableName=self.config.tableName,
            IndexName='GSI_Active_Parcels',
            KeyConditionExpression=key_condition_expression,
            ExpressionAttributeValues=expression_attribute_values,
            ReturnConsumedCapacity='Indexes'
        )

        print(response['ConsumedCapacity']['CapacityUnits'])
        items = response.get('Items', [])
        data = self.parse_area_response(items)
        return data

    def get_all_active_parcels_in_field(self):
        return self.get_all_active_parcels_in_field_by_type()

    def get_all_parcels_in_field(self):
        #regardless if active or not
        response = self.dynamodb.query(
            TableName=self.config.tableName,
            KeyConditionExpression=f"{self.config.hashKeyAttributeName} = :pk_val",
            ExpressionAttributeValues={
                ":pk_val": {'S': 'PARCEL'}
            },
            ReturnConsumedCapacity='INDEXES'
        )
        print(response['ConsumedCapacity']['CapacityUnits'])
        items = response.get('Items', [])
        data = self.parse_area_response(items)
        return data

    def get_parcel_by_id(self, id):
        response = self.dynamodb.get_item(
            TableName=self.config.tableName,
            Key={
                self.config.hashKeyAttributeName: {'S': 'PARCEL'},
                self.config.rangeKeyAttributeName: {'S': id}
            },
            ReturnConsumedCapacity='TOTAL'
        )
        print(response.get('ConsumedCapacity', {}).get('CapacityUnits', 0))
        item = response.get('Item', None)
        if item:
            return self.parse_area_response([item])[0]
        return None
    #TODO add timespan active between
    def retire_area(self, parcel_id):
        from backend.service.SensorService import SensorService
        sensor_service = SensorService()
        # Get parcel in order to remove active columns
        area = self.get_parcel_by_id(parcel_id)
        # For all sensors remove METADATA curr_parcelid column
        sensors_details=sensor_service.get_all_active_sensors_in_field_or_with_optional_parcel_id(parcel_id)
        #for all sensors update location history Location#... update moved_at to current date
        sensor_ids = [sensor['sensor_id'].split("#")[1] for sensor in sensors_details]
        print(sensor_ids)
        sensors_location_histories=sensor_service.batch_get_sensor_locations_histories(sensor_ids, True)
        print(area)
        print(sensors_details)
        print(sensors_location_histories)
        transact_items = []

        # Update the parcel to take it off the active GSI
        transact_items.append({
            'Update': {
                'TableName': self.config.tableName,
                'Key': {
                    'PK': {'S': f'PARCEL'},
                    'SK': {'S': f'{parcel_id}'}
                },
                'UpdateExpression': 'REMOVE active'
            }
        })

        #Update each sensor history and metadata
        for key, value in sensors_location_histories.items():
            sensor_id = key
            transact_items.append({
                'Update': {
                    'TableName': self.config.tableName,
                    'Key': {
                        'PK': {'S': sensor_id},
                        'SK': {'S': f"METADATA#{sensor_id}"}
                    },
                    'UpdateExpression': 'REMOVE curr_parcelid'
                }
            })

            # Update item to add 'moved_at' attribute to location history
            current_date = convert_to_unix_epoch(datetime.now().strftime("%Y-%m-%dT%H:%M:%S"))
            transact_items.append({
                'Update': {
                    'TableName': self.config.tableName,
                    'Key': {
                        'PK': {'S': f"Location#{sensor_id}"},
                        'SK': {'S': f"{value[0].sk}"}
                    },
                    'UpdateExpression': 'SET moved_at = :movedAt',
                    'ExpressionAttributeValues': {
                        ':movedAt': {'S': str(current_date)}
                    }
                }
            })

        # Step 3: Execute the transaction
        response = self.dynamodb.transact_write_items(TransactItems=transact_items)
        print(response)

if __name__ == "__main__":
    service = ParcelService()
    res = service.retire_area('Chickpeas#3225eba0-4695-48ee-9616-62dc5256b4e2')
    res = service.get_all_parcels_in_field()
    print(res[:1])
    print(len(res))
    res = service.get_all_active_parcels_in_field()
    print(res[:1])
    print(len(res))
    res = service.get_all_active_parcels_in_field_by_type("Chickpeas")
    print(res[:1])
    print(len(res))
    area = service.get_parcel_by_id('Chickpeas#3225eba0-4695-48ee-9616-62dc5256b4e2')
    print(area)
