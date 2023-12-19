import ast

from boto3.dynamodb.conditions import Key
from shapely import Polygon

from backend.models.Parcel import Parcel
from dynamodbgeo.dynamodbgeo import GeoDataManagerConfiguration, GeoDataManager, QueryRectangleRequest, GeoPoint
from utils.polygon_def import create_dynamodb_client, hashKeyLength


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
    def get_all_parcels_in_field_by_type(self, plant_type):
        response = self.dynamodb.query(
            TableName=self.config.tableName,
            KeyConditionExpression=f"{self.config.hashKeyAttributeName} = :pk_val AND begins_with({self.config.rangeKeyAttributeName}, :sk_val)",
            ExpressionAttributeValues={
                ":pk_val": {'S': 'Areas'},
                ":sk_val": {'S': f"{plant_type}#"}
            },
            ReturnConsumedCapacity='Indexes'
        )
        print(response['ConsumedCapacity']['CapacityUnits'])
        items = response.get('Items', [])
        data = self.parse_area_response(items)
        return data

    def get_all_parcels_in_field(self):
        response = self.dynamodb.query(
            TableName=self.config.tableName,
            KeyConditionExpression=f"{self.config.hashKeyAttributeName} = :pk_val",
            ExpressionAttributeValues={
                ":pk_val": {'S': 'Areas'}
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
                self.config.hashKeyAttributeName: {'S': 'Areas'},
                self.config.rangeKeyAttributeName: {'S': id}
            },
            ReturnConsumedCapacity='TOTAL'
        )
        print(response.get('ConsumedCapacity', {}).get('CapacityUnits', 0))
        item = response.get('Item', None)
        if item:
            return self.parse_area_response([item])[0]
        return None



if __name__ == "__main__":
    service = ParcelService()
    res = service.get_all_parcels_in_field()
    print(res[:1])
    print(len(res))
    res = service.get_all_parcels_in_field_by_type("Chickpeas")
    print(res[:1])
    print(len(res))
    area = service.get_parcel_by_id('Chickpeas#3225eba0-4695-48ee-9616-62dc5256b4e2')
    print(area)
    #service.get_sensor_events_by_area_in_given_range(area, '2020-01-01T04:35:53', '2020-01-06T03:39:07')