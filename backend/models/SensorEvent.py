import json
from enum import Enum
from datetime import datetime


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
    RAIN = 'rain'
    SOIL_PH = 'SoilPH'


# Sensor Event
class SensorEvent:
    class Metadata:
        def __convert_location_to_tuple(self, location_str):
            lat_str, lon_str = location_str.strip("()").split(",")
            lat = float(lat_str.strip())
            lon = float(lon_str.strip())
            return lat, lon

        def __init__(self, location, parcel_id, battery_level, status):
            self.location = self.__convert_location_to_tuple(location)  # Expected to be a tuple (longitude, latitude)
            self.batteryLevel = battery_level
            self.status = SensorStatus(status)
            self.parcel_id = parcel_id

    class Data:
        def __init__(self, dataType, dataPoint, timestamp):
            self.dataType = DataType(dataType)
            self.dataPoint = dataPoint
            self.timestamp = timestamp  # Expected to be a datetime object

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
                "status": self.metadata.status.value,
                "parcel_id": self.metadata.parcel_id
            },
            "data": {
                "dataType": self.data.dataType.value,
                "dataPoint": self.data.dataPoint,
                "timestamp": self.data.timestamp if self.data.timestamp else None
            }
        }

    def to_entity(self):
        from utils.sensor_events.sensor_events_generation import convert_to_unix_epoch, \
            get_first_of_month_as_unix_timestamp
        lat, lon = self.metadata.location
        geoJson = "{},{}".format(lat, lon)
        timestamp_str = self.data.timestamp.strftime("%Y-%m-%dT%H:%M:%S")
        sk_formatted = f"Timestamp#{convert_to_unix_epoch(timestamp_str)}"
        start_of_month = get_first_of_month_as_unix_timestamp(self.data.timestamp.strftime("%Y-%m-%dT%H:%M:%S"))
        return {
            'PK': {'S': f"Event#{self.sensorId}"},
            'SK': {'S': sk_formatted},
            'type_month': {'S': f"{self.data.dataType.value}#{str(start_of_month)}"},
            'data_point': {'N': str(self.data.dataPoint)},
            'geoJson': {'S': geoJson},
            'parcel_id': {'S': self.metadata.parcel_id},
            'battery_level': {'N': str(self.metadata.batteryLevel)},
            'status': {'S': self.metadata.status.value},
            'data_type': {'S': self.data.dataType.value}
        }
    def __repr__(self):
        metadata_repr = (f"Metadata(location={self.metadata.location}, "
                         f"parcel_id='{self.metadata.parcel_id}', "
                         f"battery_level={self.metadata.batteryLevel}, "
                         f"status='{self.metadata.status.name}')")

        data_repr = (f"Data(dataType='{self.data.dataType.name}', "
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


# if __name__ == "__main__":
#     # sensor_event = SensorEvent(
#     #     sensorId='60acb1d3-bf3a-4f25-aa73-c75d0f495a8b',
#     #     metadata={
#     #         'location': '(46.63366128235294,28.12680874117647)',
#     #         'battery_level': 33,
#     #         'status': 'Maintenance',
#     #         'parcel_id': 'Chickpeas#957000a4-6b4a-4ff7-979d-9764d086ca01'
#     #     },
#     #     data={
#     #         'dataType': 'SoilPH',
#     #         'dataPoint': 1,
#     #         'timestamp': '2023-12-21T16:00:00'
#     #     }
#     # )
#     # # print(sensor_event.to_entity())
#     # # print(sensor_event.to_json())
#     # json = {
#     #     "sensorId": "60acb1d3-bf3a-4f25-aa73-c75d0f495a8b",
#     #     "metadata": {
#     #         "location": "(46.63366128235294, 28.12680874117647)",
#     #         "battery_level": 33,
#     #         "status": "Maintenance",
#     #         "parcel_id": "Chickpeas#957000a4-6b4a-4ff7-979d-9764d086ca01"
#     #     },
#     #     "data": {
#     #         "dataType": "SoilPH",
#     #         "dataPoint": 1,
#     #         "timestamp": "2023-12-21T16:00:00"
#     #     }
#     # }
#     # print(json)
#     # print(SensorEvent.from_json(json).to_entity())
