import csv
import json
import os
from ast import literal_eval as make_tuple
# or ast.literal_eval(tuple_string)

def read_plants_csv_to_json(csv_filepath):
    items = []
    with open(csv_filepath, mode='r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            item = {
                'PK': f"PLANT#{row['plant_type']}",
                'SK': row['geohash8'],
                'geohash6': row['geohash6'],
                 'details': {
                     'latin_name': row['latin_name'],
                     'family': row['family']
                 },
                'optimal_temperature': row['optimal_temperature'],
                'optimal_humidity': row['optimal_humidity'],
                'optimal_soil_ph': row['optimal_soil_ph'],
                'water_req_mm_per_week':  row['water_requirements_mm_per_week'],
                'sunlight_req_h_per_day': row['sunlight_requirements_hours_per_day']
            }
            items.append(item)

    with open(os.environ.get('JSON_PATH'), 'w', encoding='utf-8') as jsonfile:
        json.dump(items, jsonfile, indent=4)
    return items

