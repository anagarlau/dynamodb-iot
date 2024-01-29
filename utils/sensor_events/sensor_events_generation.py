import random
import time
from datetime import datetime, timezone, timedelta
from decimal import Decimal, getcontext, ROUND_HALF_UP

import pandas as pd
import simplejson as json

from dynamodbgeo.dynamodbgeo import S2Manager, GeoPoint
from utils.polygon_def import hashKeyLength, get_project_path
from utils.sensors.sensors_from_csv import json_to_array

# Script for generation of the mock data and timestamps
# Generates mock data for sensors csv
# Creates a json file

jsonFilepath = f"{get_project_path()}\\maps\\data\\sensorsevents_to_json.json"
csvFilePath = f"{get_project_path()}\\maps\\data\\sensors_locations.csv"
excelFilepath = f"{get_project_path()}\\maps\\data\\sensor_events_analytics.xlsx"

getcontext().prec = 4  # Set the precision for Decimal operations

def generate_decimal(min_val, max_val):
    # Generate a random Decimal within the specified range
    range = Decimal(max_val) - Decimal(min_val)
    random_fraction = Decimal(str(random.random()))  # Convert float to Decimal immediately
    return min_val + range * random_fraction
def generate_mock_data(sensor_type):
    mock_data = {
        'Light': random.randint(0, 1000),  # Luminosity in lux
        'Temperature': generate_decimal(-20, 50),  # Temperature in Celsius
        'SoilMoisture': generate_decimal(0, 100),  # Soil moisture in percentage
        'Rain': random.choice([1, 0]),  # Rain presence as boolean
        'SoilPH': generate_decimal(3, 9),  # Soil pH
        'Humidity': generate_decimal(0, 100)  # Humidity in percentage
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


def generate_sensor_events_from_locations_csv_into_json(csv_file_path=csvFilePath, json_file_path=jsonFilepath):
    df = pd.read_csv(csv_file_path)
    processed_data = []
    excel_data = []

    for _, row in df.iterrows():
        # Random number of events per sensor
        point_str = row['point_coordinates'].replace('POINT (', '').replace(')', '')
        lon, lat = point_str.split(" ")
        point = [Decimal(coord.strip()) for coord in point_str.split()]
        # s2_manager = S2Manager()
        # geopoint = GeoPoint(float(lat), float(lon))
        # sensor_id = s2_manager.generateGeohash(geopoint)
        num_events = random.randint(1, 20)
        timestamps = generate_random_timestamps(num_events)


        for timestamp in timestamps:

            battery_level = generate_decimal(0, 100)# Random battery level
            data_point = generate_mock_data(row['sensor_type'])

            sensor_event = {
                "sensorId": str(row['sensor_id']),
                "metadata": {
                    "location": point,
                    'parcel_id': row['parcel_id'],
                    "batteryLevel": battery_level
                },
                "data": {
                    "dataType": row['sensor_type'],
                    "dataPoint": data_point,
                    "timestamp": timestamp
                }
            }
            sensor_event_excel = {
                "sensorId": str(row['sensor_id']),
                "location": point,
                'parcel_id': row['parcel_id'],
                "batteryLevel": battery_level,
                "dataType": row['sensor_type'],
                "dataPoint": data_point,
                "timestamp": timestamp
            }
            processed_data.append(sensor_event)
            excel_data.append(sensor_event_excel)
    # Writing to JSON file
    with open(json_file_path, 'w') as json_file:
        json.dump(processed_data, json_file,indent=4)

    events_df = pd.DataFrame(excel_data)
    events_df.to_excel(excelFilepath, index=False)

def convert_floats_to_decimals(sensor_event):
    precision = Decimal('0.01')
    battery_level = sensor_event['metadata']['batteryLevel']
    if isinstance(battery_level, float):
        sensor_event['metadata']['batteryLevel'] = Decimal(battery_level).quantize(precision, rounding=ROUND_HALF_UP)
    # Convert default float and round data point, handling different types
    data_point = sensor_event['data']['dataPoint']
    if isinstance(data_point, float):
        sensor_event['data']['dataPoint'] = Decimal(data_point).quantize(precision, rounding=ROUND_HALF_UP)
    elif isinstance(data_point, int):
        sensor_event['data']['dataPoint'] = Decimal(data_point)
    return sensor_event

def events_json_to_array():
    sensor_events = json_to_array(json_filepath=jsonFilepath)
    return [convert_floats_to_decimals(event) for event in sensor_events]

def calculate_month_diff(start_time, end_time):
    start_date = datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%S")
    end_date = datetime.strptime(end_time, "%Y-%m-%dT%H:%M:%S")

    year_diff = end_date.year - start_date.year
    month_diff = end_date.month - start_date.month

    return year_diff * 12 + month_diff


def format_date(date_str):
    try:
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        date_obj = date_obj.replace(hour=0, minute=0, second=0)
        formatted_date_str = date_obj.strftime("%Y-%m-%dT%H:%M:%S")
        return formatted_date_str
    except ValueError:
        raise ValueError("Invalid date format. Please use the format %Y-%m-%d.")


def get_start_of_month(timestamp_str):
    timestamp = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S")
    start_of_month = timestamp.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    return start_of_month.strftime("%Y-%m-%dT%H:%M:%S")
def convert_to_unix_epoch(timestamp_str):
    dt = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S")
    # Convert to Unix epoch timestamp
    unix_epoch = int(time.mktime(dt.timetuple()))
    return unix_epoch # = continuous count of seconds

def random_date_string():
    start_date = datetime(2020, 1, 1)
    end_date = datetime.now()
    random_date = start_date + timedelta(days=random.randint(0, (end_date - start_date).days))
    return random_date.strftime('%Y-%m-%dT%H:%M:%S')

def get_first_of_month_as_unix_timestamp(timestamp_str):
    return convert_to_unix_epoch(get_start_of_month(timestamp_str))

def unix_to_iso(unix_timestamp):
    offset = time.timezone if (time.localtime().tm_isdst == 0) else time.altzone
    local_tz = timezone(timedelta(seconds=-offset))
    timestamp_datetime = datetime.fromtimestamp(unix_timestamp, tz=local_tz)
    iso_timestamp = timestamp_datetime.isoformat()
    return iso_timestamp

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
        timestamp=event['data']['timestamp']
        start_of_month=get_first_of_month_as_unix_timestamp(timestamp)
        sk_formated = "{}#{}#{}".format(
             "Event",
            convert_to_unix_epoch(timestamp),
            event['sensorId']
            #Issue:not unique if 2 sensors send the same timestamp! i need a unique identifier
        )
        sensor_event = {
            'PK': f"{event['data']['dataType']}#{start_of_month}",#f"Event#{event['sensorId']}",
            'SK': sk_formated,
            #'type_month': f"{event['data']['dataType']}#{start_of_month}",
            's_id': f"Event#{event['sensorId']}",
            'data_point': event['data']['dataPoint'],
            'geoJson': geoJson,
            'parcel_id': event['metadata']['parcel_id'],
            'battery_level': event['metadata']['batteryLevel'],
            #'status': event['metadata']['status'],
            'data_type': event['data']['dataType']
        }
        #print(sensor_event)
        database_entries.append(sensor_event)
    return database_entries


# Call to generate a new batch of sensor events
#generate_sensor_events_from_locations_csv_into_json()
# Test
# process_events_for_db()