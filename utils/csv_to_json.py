import csv
import json
import os
from ast import literal_eval as make_tuple

import geohash2
from dynamodbgeo.model.GeoPoint import GeoPoint
from dynamodbgeo.s2.S2Manager import S2Manager


# or ast.literal_eval(tuple_string)

def read_plants_csv_to_json(csv_filepath):
    items = []
    with open(csv_filepath, mode='r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            #print("Current Geohash 8 and 6")
            #print(row['geohash8'], row['geohash6'])
            lat, lon, lat_err, lon_err = geohash2.decode_exactly(row['geohash8'])

            #print(geohash)
            item = {
                'PK': f"PLANT#{row['plant_type']}",
                'SK': (lat, lon),
                'geohash8':row['geohash8'],
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
            print(item)
            items.append(item)

    with open('C:\\Users\\ana\\PycharmProjects\\dynamodb\\maps\\data\\to_json.json', 'w', encoding='utf-8') as jsonfile:
        json.dump(items, jsonfile, indent=4)
    return items

#read_plants_csv_to_json('C:\\Users\\ana\\PycharmProjects\\dynamodb\\maps\\data\\plant_type_to_geohashes.csv')