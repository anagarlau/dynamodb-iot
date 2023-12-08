from enum import Enum
from datetime import datetime
#Enums
class SensorStatus(Enum):
    BUSY = 'busy'
    IDLE = 'idle'
    MAINTENANCE = 'maintenance'

class DataType(Enum):
    TEMP = 'Temperature'
    LIGHT = 'Light'
    SOILMOISTURE = 'SoilMoisture'
    HUMIDITY = 'Humidity'
    RAIN = 'rain'
    SOIL_PH = 'SoilPH'

#Sensor Event
class SensorEvent:
    class Metadata:
        def __convert_location_to_tuple(self,location_str):
            lat_str, lon_str = location_str.strip("()").split(",")
            lat = float(lat_str.strip())
            lon = float(lon_str.strip())
            # Return as a tuple
            return lat, lon

        def __init__(self, location, batteryLevel, status):
            self.location = self.__convert_location_to_tuple(location) # Expected to be a tuple (longitude, latitude)
            print(self.location)
            self.batteryLevel = batteryLevel
            self.status = SensorStatus(status)

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
                "batteryLevel": self.metadata.batteryLevel,
                "status": self.metadata.status.value
            },
            "data": {
                "dataType": self.data.dataType.value,
                "dataPoint": self.data.dataPoint,
                "timestamp": self.data.timestamp if self.data.timestamp else None
            }
        }

