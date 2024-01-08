from shapely import Point

from backend.models.SensorEvent import DataType
from dynamodbgeo.dynamodbgeo import GeoPoint, S2Manager
from utils.polygon_def import hashKeyLength
from utils.sensor_events.sensor_events_generation import convert_to_unix_epoch


class SensorMetadata:
    def __init__(self, longitude, latitude, sensor_details):
        if 'sensor_type' not in sensor_details.keys():
            raise Exception("Sensor type must be specified")
        sensor_type = sensor_details['sensor_type']
        if not any(sensor_type == member.value for member in DataType):
            raise ValueError(f"Invalid sensor type: {sensor_type}")

        self.longitude = longitude
        self.latitude = latitude
        self.sensor_type = sensor_type
        self.location = self.create_point(longitude, latitude)
        self.create_geohash_and_hashkey()
        self.geoJson = f"{self.location.y},{self.location.x}"
        self.manufacturer=sensor_details['manufacturer']
        self.firmware=sensor_details['firmware']
        self.model = sensor_details['model']


    def create_point(self, longitude, latitude):
        return Point(longitude, latitude)

    def create_geohash_and_hashkey(self):
        s2_manager = S2Manager()
        longitude, latitude = self.location.x, self.location.y
        geopoint = GeoPoint(latitude, longitude)
        self.geohash = s2_manager.generateGeohash(geopoint)
        self.hashkey = s2_manager.generateHashKey(self.geohash, hashKeyLength)

    def get_sensor_metadata_record(self, sensor_id, parcel_id):
        sensor_metadata_record = {
            'PK': f"Sensor#{sensor_id}",
            'SK': f"Metadata#{self.sensor_type}#{sensor_id}",
            'sensor_type': self.sensor_type,
            'geoJson': self.geoJson,
            'hash_key': str(self.hashkey),
            'geohash': str(self.geohash),
            'curr_parcelid': parcel_id,
            'manufacturer': self.manufacturer,
            'firmware': self.firmware,
            'model': self.model
        }
        return {key: {'S': str(value)} for key, value in sensor_metadata_record.items()}

    def get_sensor_location_record(self, sensor_id, parcel_id, timestamp):
        sensor_location_record = {
            'PK': f"Sensor#{sensor_id}",
            'SK': f"Location#{convert_to_unix_epoch(timestamp)}",
            'sensortype': self.sensor_type, # In order for GSI for active in radius by type not to fetch it
            'geoJson': self.geoJson,
            'hash_key': str(self.hashkey),
            'geohash': str(self.geohash),
            'id_parcel': parcel_id
        }
        return {key: {'S': str(value)} for key, value in sensor_location_record.items()}
    def __repr__(self):
        return f"Sensor(longitude={self.longitude}, latitude={self.latitude}, sensor_type={self.sensor_type}, location={self.location}, model={self.model}, manufacturer={self.manufacturer}, firmware={self.firmware})"

    @staticmethod
    def get_geohash_and_hashkey(longitude, latitude, hashKeyLength):
        s2_manager = S2Manager()
        geopoint = GeoPoint(latitude, longitude)
        geohash = s2_manager.generateGeohash(geopoint)
        hashkey = s2_manager.generateHashKey(geohash, hashKeyLength)
        return geohash, hashkey
