import json
from enum import Enum
from datetime import datetime
from typing import List

from dateutil import parser


# from utils.sensor_events.sensor_events_generation import convert_to_unix_epoch, get_first_of_month_as_unix_timestamp


# Enums
class SensorStatus(Enum):
    BUSY = 'Busy'
    IDLE = 'Idle'
    MAINTENANCE = 'Maintenance'
    ACTIVE = 'Active'


class DataType(Enum):
    TEMP = 'Temperature'
    LIGHT = 'Light'
    SOILMOISTURE = 'SoilMoisture'
    HUMIDITY = 'Humidity'
    RAIN = 'Rain'
    SOIL_PH = 'SoilPH'

    @classmethod
    def validate_data_types(self, data_types: List[str]):
        valid_types = {data_type.value for data_type in DataType}
        for data_type_str in data_types:
            if data_type_str not in valid_types:
                raise ValueError(f"Invalid data type: {data_type_str}")


# Sensor Event

class SensorEvent:

    def to_entity(self):
        from utils.sensor_events.sensor_events_generation import convert_to_unix_epoch, \
            get_first_of_month_as_unix_timestamp
        lat, lon = self.metadata.location
        geoJson = "{},{}".format(lat, lon)
        timestamp_str = self.data.timestamp
        sk_formatted = f"Event#{convert_to_unix_epoch(timestamp_str)}#{self.sensorId}"
        start_of_month = get_first_of_month_as_unix_timestamp(self.data.timestamp)
        return {
            'PK': {'S': f"{self.data.dataType}#{str(start_of_month)}"},
            'SK': {'S': sk_formatted},
            's_id': {'S': f"Event#{self.sensorId}"},
            'data_point': {'N': str(self.data.dataPoint)},
            'geoJson': {'S': geoJson},
            'parcel_id': {'S': self.metadata.parcel_id},
            'battery_level': {'N': str(self.metadata.batteryLevel)},
            'data_type': {'S': self.data.dataType}
        }

    class Metadata:
        def __convert_location_to_tuple(self, location_str):
            lat_str, lon_str = location_str.strip("()").split(",")
            lat = float(lat_str.strip())
            lon = float(lon_str.strip())
            return lat, lon

        def __init__(self, location, parcel_id, battery_level):
            self.location = self.__convert_location_to_tuple(location)  # Expected to be a tuple (longitude, latitude)
            self.batteryLevel = battery_level
            self.parcel_id = parcel_id

    class Data:
        def __init__(self, dataType, dataPoint, timestamp):
            self.dataType = DataType(dataType).value
            self.dataPoint = dataPoint
            self.timestamp = parser.parse(timestamp).strftime("%Y-%m-%dT%H:%M:%S")

    def __init__(self, sensorId, metadata, data):
        self.sensorId = sensorId
        self.metadata = self.Metadata(**metadata)
        self.data = self.Data(**data)

    def to_json(self):
        return {
            "sensorId": self.sensorId,
            "metadata": {
                "location": self.metadata.location,
                "battery_level": self.metadata.batteryLevel,
                "parcel_id": self.metadata.parcel_id
            },
            "data": {
                "dataType": self.data.dataType,
                "dataPoint": self.data.dataPoint,
                "timestamp": self.data.timestamp if self.data.timestamp else None
            }
        }


    def __repr__(self):
        metadata_repr = (f"Metadata(location={self.metadata.location}, "
                         f"parcel_id='{self.metadata.parcel_id}', "
                         f"battery_level={self.metadata.batteryLevel}")

        data_repr = (f"Data(dataType='{self.data.dataType}', "
                     f"dataPoint={self.data.dataPoint}, "
                     f"timestamp='{self.data.timestamp}')")

        return f"SensorEvent(sensorId='{self.sensorId}', metadata={metadata_repr}, data={data_repr})"

    @classmethod
    def from_json(cls, json_data):
        if isinstance(json_data, str):
            json_data = json.loads(json_data)
        if 'timestamp' in json_data['data']:
            json_data['data']['timestamp'] = datetime.fromisoformat(json_data['data']['timestamp'])
        return cls(sensorId=json_data['sensorId'],
                   metadata=json_data['metadata'],
                   data=json_data['data'])

    @classmethod
    def from_entity(cls, entity):
        pk = entity["PK"]["S"]
        sensor_id = entity["s_id"]["S"].split("#")[1]
        location = entity["geoJson"]["S"]
        parcel_id = entity["parcel_id"]["S"]
        battery_level = float(entity["battery_level"]["N"])
        metadata = {
            "location": location,
            "parcel_id": parcel_id,
            "battery_level": battery_level
        }
        data_type = entity["data_type"]["S"]
        data_point = float(entity["data_point"]["N"])
        from utils.sensor_events.sensor_events_generation import unix_to_iso
        timestamp = unix_to_iso(int(entity["SK"]["S"].split("#")[1]))
        data = {
            "dataType": data_type,
            "dataPoint": data_point,
            "timestamp": timestamp
        }
        sensor_event = cls(sensor_id, metadata, data)
        sensor_event.PK = pk
        return sensor_event

