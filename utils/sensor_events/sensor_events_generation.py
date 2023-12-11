import time
from decimal import Decimal

import pandas as pd
import json
import random
from datetime import datetime

from backend.models.SensorEvent import SensorStatus
from dynamodbgeo.dynamodbgeo import S2Manager, GeoPoint
from utils.polygon_def import hashKeyLength
from utils.sensors.sensors import json_to_array

# Script for generation of the mock data and timestamps
# Generates mock data for sensors csv
# Creates a json file

jsonFilepath = 'C:\\Users\\ana\\PycharmProjects\\dynamodb\\maps\\data\\sensorsevents_to_json.json'
csvFilePath = 'C:\\Users\\ana\\PycharmProjects\\dynamodb\\maps\\data\\sensors_locations.csv'
excelFilepath = 'C:\\Users\\ana\\PycharmProjects\\dynamodb\\maps\\data\\sensor_events_analytics.xlsx'


def generate_mock_data(sensor_type):
    mock_data = {
        'Light': random.randint(0, 1000),  # Luminosity in lux
        'Temperature': random.uniform(-20, 50),  # Temperature in Celsius
        'SoilMoisture': random.uniform(0, 100),  # Soil moisture in percentage
        'Rain': random.choice([1, 0]),  # Rain presence as boolean
        'SoilPH': random.uniform(3, 9),  # Soil pH
        'Humidity': random.uniform(0, 100)  # Humidity in percentage
    }
    return mock_data.get(sensor_type, None)


def generate_random_timestamps(num_events):
    timestamps = []
    for _ in range(num_events):
        random_year = random.randint(2020, 2023)
        random_month = random.randint(1, 12)
        random_day = random.randint(1, 28)  # Keeping it simple to avoid month/day mismatch
        random_hour = random.randint(0, 23)
        random_minute = random.randint(0, 59)
        random_second = random.randint(0, 59)
        random_timestamp = datetime(random_year, random_month, random_day, random_hour, random_minute, random_second)
        timestamps.append(random_timestamp.isoformat())
    return timestamps


def csv_to_dynamodb_json(csv_file_path=csvFilePath, json_file_path=jsonFilepath):
    df = pd.read_csv(csv_file_path)
    processed_data = []
    excel_data = []

    for _, row in df.iterrows():
        # Random number of events per sensor
        num_events = random.randint(1, 20)
        timestamps = generate_random_timestamps(num_events)

        for timestamp in timestamps:
            point_str = row['point_coordinates'].replace('POINT (', '').replace(')', '')
            point = [float(coord.strip()) for coord in point_str.split()]

            sensor_event = {
                "sensorId": row['sensor_id'],
                "metadata": {
                    "location": point,
                    "batteryLevel": round(random.uniform(0, 100), 2),  # Random battery level
                    "status": random.choice([SensorStatus.BUSY, SensorStatus.IDLE, SensorStatus.MAINTENANCE]).value
                },
                "data": {
                    "dataType": row['sensor_type'],
                    "dataPoint": generate_mock_data(row['sensor_type']),
                    "timestamp": timestamp
                }
            }
            sensor_event_excel = {
                "sensorId": row['sensor_id'],
                "location": point,
                "batteryLevel": round(random.uniform(0, 100), 2),
                "status": random.choice([SensorStatus.BUSY, SensorStatus.IDLE, SensorStatus.MAINTENANCE]).value,
                "dataType": row['sensor_type'],
                "dataPoint": generate_mock_data(row['sensor_type']),
                "timestamp": timestamp
            }
            processed_data.append(sensor_event)
            excel_data.append(sensor_event_excel)
    # Writing to JSON file
    with open(json_file_path, 'w') as json_file:
        json.dump(processed_data, json_file, indent=4)

    events_df = pd.DataFrame(excel_data)
    events_df.to_excel(excelFilepath, index=False)


def events_json_to_array():
    return json_to_array(json_filepath=jsonFilepath)


def convert_to_unix_epoch(timestamp_str):
    dt = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S")
    # Convert to Unix epoch timestamp
    unix_epoch = int(time.mktime(dt.timetuple()))
    return unix_epoch # = continuous count of seconds


def process_events_for_db():
    # Read generated json and process for batch writes
    sensor_events = events_json_to_array()
    s2_manager = S2Manager()
    database_entries = []
    for event in sensor_events:
        lon, lat = event['metadata']['location']
        geopoint = GeoPoint(lat, lon)
        geoJson = "{},{}".format(lat, lon)
        geohash = s2_manager.generateGeohash(geopoint)
        hashKey = s2_manager.generateHashKey(geohash, hashKeyLength)
        sk_formated = "{}#{}".format(
            geohash,
            convert_to_unix_epoch(event['data']['timestamp'])
        )
        sensor_event = {
            'PK': event['data']['dataType'],
            'SK': sk_formated,
            'sensor_id': event['sensorId'],
            'data_point': Decimal(event['data']['dataPoint']),
            'geoJson': geoJson,
            'battery_level': Decimal(str(event['metadata']['batteryLevel'])),
            'status': event['metadata']['status']
        }
        print(sensor_event)
        database_entries.append(sensor_event)
    return database_entries


# Call to generate a new batch of sensor events
#csv_to_dynamodb_json()
#process_events_for_db()
