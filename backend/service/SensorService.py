from shapely import Point
from dynamodbgeo.dynamodbgeo import GeoDataManagerConfiguration, GeoDataManager, QueryRadiusRequest, GeoPoint
from utils.sensor_events.sensor_events_generation import convert_to_unix_epoch
from utils.sensors.sensors import parse_sensor_data, visualize_results
from utils.polygon_def import center_point_field, radius, create_dynamodb_client, hashKeyLength


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

        # Perform the radius query
        response = self.geoDataManager.queryRadius(
            QueryRadiusRequest(
                GeoPoint(lat, lon),  # center point
                radius_meters,  # search radius in meters
                # query_radius_input,  # additional filter input
                sort=False  # sort by distance from the center point
            )
        )
        data = parse_sensor_data(response['results'])
        map = visualize_results(center_point, radius_meters, data)
        print(data[:2])
        print(len(data))
        map.save("vis_out/sensorservice/sensors-radius.html")
        return data

    def get_sensors_in_radius_acc_to_type(self,center_point, radius_meters, sensor_type='Humidity'):
        # Prepare the filter expression and attribute values
        lat, lon = center_point.y, center_point.x
        query_radius_input = {
            'GSI': {
                'Name': 'GSI_SensorType_Radius',
                'PK': {'name': 'sensor_type', 'type': 'S', 'value': sensor_type},
                'SK': {'name': 'SK', 'type': 'S'}
            }
            # 'FilterExpression': {
            #     "FilterExpression": "sensor_type = :val1",
            #     "ExpressionAttributeValues": {
            #         ":val1": {"S": sensor_type},
            #     }
            # }
        }
        # query_radius_input = {
        #
        #         "FilterExpression": "Country = :val1",
        #         "ExpressionAttributeValues": {
        #             ":val1": {"S": '1'},
        #         }
        # }
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
        print(len(data))
        map = visualize_results(center_point, radius_meters, data)
        map.save("vis_out/sensorservice/sensors-radius-type.html")
        return data

    def get_all_sensors(self):
        self.get_sensors_in_radius(center_point=center_point_field, radius_meters=radius)




# Example usage of the class
if __name__ == "__main__":
    center_point = Point(28.1250063, 46.6334964)
    sensor_service = SensorService()
    sensor_service.get_all_sensors() #174
    sensor_service.get_sensors_in_radius(center_point, 200) #2
    sensor_service.get_sensors_in_radius_acc_to_type(center_point, 200, sensor_type='Humidity')#1
    sensor_service.get_sensors_in_radius_acc_to_type(center_point, 200, sensor_type='Temperature')
