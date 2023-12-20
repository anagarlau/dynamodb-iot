from shapely import Point

from utils.sensor_events.sensor_events_generation import unix_to_iso


class SensorLocationHistory:
    def __init__(self, response_item):
        self.sensor_id = response_item['PK']['S']
        self.sk = response_item['SK']['S']
        self.parse_placed_at(self.sk)
        self.sensor_type = response_item['sensortype']['S']
        self.geo_json = response_item['geoJson']['S']
        self.hash_key = response_item['hash_key']['S']
        self.geohash = response_item['geohash']['S']
        self.parcel_id = response_item.get('id_parcel', {}).get('S')
        if 'moved_at' in response_item.keys():
            self.moved_at = unix_to_iso(int(response_item['moved_at']['N']))
        else:
            self.moved_at = None
        lat, lon = map(float, self.geo_json.split(','))
        self.location = Point(lon, lat)

    def parse_placed_at(self, sk):
        # Extract timestamp from SK
        parts = sk.split('#')
        if len(parts) > 2 and parts[0] == 'Location':
            # Convert unix timestamp to ISO date
            try:
                self.placed_at = unix_to_iso(int(parts[1]))
            except ValueError:
                return None
        return None

    def __repr__(self):
        return (f"SensorLocationHistory(SensorID={self.sensor_id}, PlacedAt={self.placed_at}, MovedAt={self.moved_at}, "
                f"ParcelID={self.parcel_id}, SensorType={self.sensor_type},"
                f"Geohash={self.geohash} "
                f"Location=({self.location.y}, {self.location.x}))")