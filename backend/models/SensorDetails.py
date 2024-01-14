from shapely import Point


class SensorDetails:
    def __init__(self, response_item):
        self.hash_key =  response_item['hash_key']['S'] if 'hash_key' in response_item.keys() else None
        self.geohash = response_item['geohash']['S']
        self.sk = response_item['SK']['S']
        self.parcel_id = response_item['curr_parcelid']['S'] if 'curr_parcelid' in response_item.keys() else None
        self.sensor_id = response_item['PK']['S'].split("#")[1]
        if 'sensor_type' in response_item:
            self.sensor_type = response_item['sensor_type']['S']
        elif 'sensortype' in response_item:
            self.sensor_type = response_item['sensortype']['S']
        else:
            self.sensor_type = None
        self.manufacturer = response_item['manufacturer']['S'] if 'manufacturer' in response_item else None
        self.model = response_item['model']['S'] if 'model' in response_item else None
        self.firmware = response_item['firmware']['S'] if 'firmware' in response_item else None
        # Parse geoJson to create a Point
        lat, lon = map(float, response_item['geoJson']['S'].split(','))
        self.location = Point(lon, lat)

    def __repr__(self):
        return (f"SensorDetails(SensorID={self.sensor_id}, SK={self.sk}, SensorType={self.sensor_type}, "
                f"Manufacturer={self.manufacturer}, Model={self.model}, Firmware={self.firmware}, "
                f"HashKey={self.hash_key}, Geohash={self.geohash}, "
                f"ParcelID={self.parcel_id}, Location=({self.location.y}, {self.location.x}))")
