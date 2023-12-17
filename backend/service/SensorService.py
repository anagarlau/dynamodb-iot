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

    def get_sensors_in_radius(self, center_point, radius_meters):
        # Prepare the filter expression and attribute values
        lat, lon = center_point.y, center_point.x
        query_radius_input = {
            'GSI': {
                'Name': 'GSI_Geohash6_FullGeohash',
                'PK': {'name': 'hash_key', 'type': 'S'},
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

    def get_all_sensors(self):
        #self.get_sensors_in_radius(center_point=center_point_field, radius_meters=radius)
        min_lon, min_lat, max_lon, max_lat = polygon.bounds
        # print(f"Min Point: {min_lat}, {min_lon}")
        # print(f"Max Point: {max_lat}, {max_lon}")
        query_radius_input = {
            'GSI': {
                'Name': 'GSI_Geohash6_FullGeohash',
                'PK': {'name': 'hash_key', 'type': 'S'},
                'SK': {'name': 'PK', 'type': 'S'}
            }
        }
        response= self.geoDataManager.queryRectangle(
                QueryRectangleRequest(
                GeoPoint(min_lat, min_lon),
                GeoPoint(max_lat, max_lon), query_radius_input))
        data = response['results']
        print('>>All in field: Total data', len(data), 'with consumed Capacity Units',
             response['consumed_capacity'])
        parsed_data = parse_sensor_data(data)
        map = visualize_results(center_point=None, radius=None, sensors=parsed_data)
        map.save("vis_out/sensorservice/sensors-field-all.html")
        return parsed_data

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

# Example usage of the class
if __name__ == "__main__":
    center_point = Point(28.1250063, 46.6334964)
    sensor_service = SensorService()
    sensor_service.get_all_sensors() #174
    sensor_service.get_all_sensors_of_type_in_field("Humidity")
    sensor_service.get_sensors_in_radius(center_point, 200) #2
    sensor_service.get_sensors_in_radius_acc_to_type(center_point, 500, sensor_type='Light')#2
    sensor_service.get_sensors_in_radius_acc_to_type(center_point, 300, sensor_type='SoilPH')#1
