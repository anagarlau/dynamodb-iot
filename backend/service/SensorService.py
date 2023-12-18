import s2sphere
from s2sphere import LatLng, LatLngRect
from shapely import Point
from dynamodbgeo.dynamodbgeo import GeoDataManagerConfiguration, GeoDataManager, QueryRadiusRequest, GeoPoint, \
    S2Manager, S2Util, QueryRectangleRequest
from utils.sensor_events.sensor_events_generation import convert_to_unix_epoch
from utils.sensors.sensors_from_csv import parse_sensor_data, visualize_results
from utils.polygon_def import center_point_field, radius, create_dynamodb_client, hashKeyLength, polygon


class SensorService:
    def __init__(self):
        self.dynamodb = create_dynamodb_client()
        self.config = GeoDataManagerConfiguration(self.dynamodb, 'IoT')
        self.config.hashKeyAttributeName = 'PK'
        self.config.rangeKeyAttributeName = 'SK'
        self.geoDataManager = GeoDataManager(self.config)
        self.config.hashKeyLength = hashKeyLength
        self.table_name='IoT'

    def get_all_sensors_of_type_in_field(self, sensor_type):
        min_lon, min_lat, max_lon, max_lat = polygon.bounds
        # print(f"Min Point: {min_lat}, {min_lon}")
        # print(f"Max Point: {max_lat}, {max_lon}")
        query_radius_input = {
            'GSI': {
                'Name': 'GSI_SensorType_Radius',
                'PK': {'name': 'sensor_type', 'type': 'S', 'value': sensor_type},
                'SK': {'name': 'PK', 'type': 'S'}
            }
        }
        response = self.geoDataManager.queryRectangle(
            QueryRectangleRequest(
                GeoPoint(min_lat, min_lon),
                GeoPoint(max_lat, max_lon), query_radius_input))
        data = response['results']
        print('>>In Field by type: Total data', len(data), 'with consumed Capacity Units',
              response['consumed_capacity'])
        parsed_data = parse_sensor_data(data)
        map = visualize_results(center_point=None, radius=None, sensors=parsed_data)
        map.save("vis_out/sensorservice/sensors-field-type.html")
        return parsed_data
    def get_active_sensors_in_radius_for_time_range(self, center_point, radius_meters):
        # ':startDate': {'S': "1577849753#"},
        # ':endDate': {'S': "1677849753#zzzzz"}
        # Prepare the filter expression and attribute values
        lat, lon = center_point.y, center_point.x
        query_radius_input = {
            'GSI': {
                'Name': 'GSI_Geohash6_FullGeohash',
                'PK': {'name': 'hash_key', 'type': 'S'},
                'SK': {'name': 'geohash', 'type': 'S'}
            },
            "Filters": 'SK <= :endDate AND (attribute_not_exists(moved_date) OR moved_date >= :startDate)',
            "ExpressionAttributeValues": {
            ':startDate': {'S': "Location#1577849753#"},
            ':endDate': {'S': "Location#1577849753#zzzzz"}
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
        #print(data[:2])
        print('>>All in radius: Total data', len(data), 'with consumed Capacity Units',response['consumed_capacity'])
        map.save("vis_out/sensorservice/sensors-radius.html")
        return data

    def get_sensors_in_radius_acc_to_type(self,center_point, radius_meters, sensor_type='Humidity'):
        # Prepare the filter expression and attribute values
        lat, lon = center_point.y, center_point.x
        query_radius_input = {
            'GSI': {
                'Name': 'GSI_SensorType_Radius',
                'PK': {'name': 'sensor_type', 'type': 'S', 'value': sensor_type},
                'SK': {'name': 'PK', 'type': 'S'}
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
        #print(data[:2])
        print('>>In Radius by type: Total data', len(data), 'with consumed Capacity Units',response['consumed_capacity'])
        map = visualize_results(center_point, radius_meters, data)
        map.save("vis_out/sensorservice/sensors-radius-type.html")
        return data

    def get_all_sensors_with_optional_parcel_id(self, parcel_id=None):
        gsi_name = 'GSI_Sensor_By_Parcel'
        params = {
            'TableName': self.table_name,
            'IndexName': gsi_name,
            'ReturnConsumedCapacity': 'TOTAL'  # Add this line to see consumed capacity
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

        print(f'>>All in field with optional Parcel Id: Total data {len(data)}, with consumed Capacity Units {total_consumed_capacity}')
        parsed_data = parse_sensor_data(data)
        map = visualize_results(center_point=None, radius=None, sensors=parsed_data)
        map.save("vis_out/sensorservice/sensors-field-all.html")
        return parsed_data

    def get_sensor_by_id(self, sensor_id):
        params = {
            'TableName': self.table_name,
            'KeyConditionExpression': 'PK = :sensorId AND begins_with(SK, :metadataPrefix)',
            'ExpressionAttributeValues': {
                ':sensorId': {'S': sensor_id},
                ':metadataPrefix': {'S': 'METADATA#'}
            },
            'ReturnConsumedCapacity': 'TOTAL'  # Include consumed capacity in the response
        }
        response = self.dynamodb.query(**params)
        data = response.get('Items', [])
        consumed_capacity = response.get('ConsumedCapacity', {}).get('CapacityUnits', 0)
        print(f'>> Queried sensor metadata for ID {sensor_id}, Consumed Capacity Units: {consumed_capacity}')
        return data




# Example usage of the class
if __name__ == "__main__":
    center_point = Point(28.1250063, 46.6334964)
    sensor_service = SensorService()
    sensor_service.get_all_sensors_with_optional_parcel_id('Chickpeas#3225eba0-4695-48ee-9616-62dc5256b4e2') #174
    sensor = sensor_service.get_sensor_by_id("136dce58-9352-40e6-81b1-c0cd4684c6f8")
    print(sensor)
    # sensor_service.get_all_sensors_of_type_in_field("Temperature")
    #sensor_service.get_active_sensors_in_radius_for_time_range(center_point, 200) #2
    # sensor_service.get_sensors_in_radius_acc_to_type(center_point, 200, sensor_type='Temperature')#1
    # sensor_service.get_sensors_in_radius_acc_to_type(center_point, 200, sensor_type='SoilPH')#1
