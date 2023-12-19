from shapely import Point


class SensorDetails:
    def __init__(self, response_item):
        self.hash_key = response_item['hash_key']['S']
        self.geohash = response_item['geohash']['S']
        self.sk = response_item['SK']['S']
        self.parcel_id = response_item['curr_parcelid']['S']
        self.sensor_id = response_item['PK']['S']
        self.sensor_type = response_item['sensor_type']['S']
        # Parse geoJson to create a Point
        lat, lon = map(float, response_item['geoJson']['S'].split(','))
        self.location = Point(lon, lat)

    def __repr__(self):
        return (f"SensorDetails(SensorID={self.sensor_id}, SK={self.sk}, SensorType={self.sensor_type}, "
                f"HashKey={self.hash_key}, Geohash={self.geohash}, "
                f"ParcelID={self.parcel_id}, Location=({self.location.y}, {self.location.x}))")
