import csv
import datetime
import json
import uuid
from random import random, randint

import dateutil.utils
from shapely import Polygon

from utils.parcels.parcels_generation import plot_polygons_on_map
from utils.polygon_def import get_project_path

jsonFilepath = f'{get_project_path()}\\maps\\data\\plants_to_json.json'
csvFilePath = f'{get_project_path()}\\maps\\data\\plant_type_to_parcels.csv'


def csv_to_json(csv_filepath=csvFilePath, json_filepath=jsonFilepath):
    json_data = []
    with open(csv_filepath, mode='r', newline='') as csvfile:
        csv_reader = csv.DictReader(csvfile)
        for row in csv_reader:
            # Convert WKT polygon to an array of coordinates
            if 'polygon' in row:
                # Strip 'POLYGON ((' and '))', then split into pairs
                polygon_data = row['polygon'].lstrip('POLYGON ((').rstrip('))')
                # Split into coordinate pairs and convert each to a tuple of floats
                row['polygon'] = [tuple(map(float, coord_pair.split())) for coord_pair in polygon_data.split(', ')]
            json_data.append(row)

    with open(json_filepath, 'w', encoding='utf-8') as jsonfile:
        json.dump(json_data, jsonfile, ensure_ascii=False, indent=4)


def read_and_process_parcels_from_json(json_filepath=jsonFilepath):
    csv_to_json()
    database_entries = []
    with open(json_filepath, 'r', encoding='utf-8') as jsonfile:
        data = json.load(jsonfile)
        for entry in data:
            if 'polygon' in entry:
                entry['polygon'] = [tuple(coord) for coord in entry['polygon']]
            pk = 'Parcel'
            parcel_active = {
                'PK': pk,
                'SK': f"{entry['parcel_id']}",
                'polygon_coord': str(entry['polygon']),
                'plant_type': entry['plant_type'],
                'active': 1, # Bool not supported for partition keys
                'details': {
                    'latin_name': entry['latin_name'],
                    'family': entry['family']
                },
                'optimal_temperature': entry['optimal_temperature'],
                'optimal_humidity': entry['optimal_humidity'],
                'optimal_soil_ph': entry['optimal_soil_ph'],
                'water_requirements_mm_per_week': entry['water_requirements_mm_per_week'],
                'sunlight_requirements_hours_per_day': entry['sunlight_requirements_hours_per_day'],
                'active_in_year': dateutil.utils.today().year,
                'active_from': str(datetime.date(dateutil.utils.today().year, 3,15))
            }
            plant_types = ['Chickpeas', 'Grapevine']
            random_index = randint(0, len(plant_types) - 1)
            random_id = f"{plant_types[random_index]}#{uuid.uuid4()}"
            parcel_not_active = {
                'PK': pk,
                'SK': random_id,
                'polygon_coord': str(entry['polygon']),
                'plant_type': entry['plant_type'],
                'details': {
                    'latin_name': entry['latin_name'],
                    'family': entry['family']
                },
                'optimal_temperature': entry['optimal_temperature'],
                'optimal_humidity': entry['optimal_humidity'],
                'optimal_soil_ph': entry['optimal_soil_ph'],
                'water_requirements_mm_per_week': entry['water_requirements_mm_per_week'],
                'sunlight_requirements_hours_per_day': entry['sunlight_requirements_hours_per_day'],
                'active_in_year': 2022,
                'active_from': str(datetime.date(2022,3,15)),
                'active_to':str(datetime.date(2022,11,15)),
            }
            database_entries.append(parcel_active)
            database_entries.append(parcel_not_active)
    return database_entries

def read_parcels_from_json(json_filepath=jsonFilepath):
    csv_to_json()
    parcels = []
    with open(json_filepath, 'r', encoding='utf-8') as jsonfile:
        data = json.load(jsonfile)
        for entry in data:
            if 'polygon' in entry:
                entry['polygon'] = [tuple(coord) for coord in entry['polygon']]
            parcels.append(entry)
    return parcels

def sanity_check_parcels():
    data = read_parcels_from_json()
    processed = [{**item, 'polygon': Polygon(item['polygon'])} for item in data]
    # print(processed)
    map = plot_polygons_on_map(processed)
    map.save('../../maps/parcels.html')

#sanity_check_parcels()
