import csv
import json

from shapely import Polygon

from utils.parcels.parcels_generation import plot_polygons_on_map

jsonFilepath = 'C:\\Users\\ana\\PycharmProjects\\dynamodb\\maps\\data\\plants_to_json.json'
csvFilePath = 'C:\\Users\\ana\\PycharmProjects\\dynamodb\\maps\\data\\plant_type_to_parcels.csv'


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
            parcel = {
                'PK': 'Areas',
                'SK': f"{entry['parcel_id']}",
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
                'sunlight_requirements_hours_per_day': entry['sunlight_requirements_hours_per_day']
            }
            database_entries.append(parcel)
    return database_entries


def sanity_check_parcels():
    data = read_and_process_parcels_from_json()
    processed = [{**item, 'polygon': Polygon(item['polygon'])} for item in data]
    # print(processed)
    map = plot_polygons_on_map(processed)
    map.save('test.html')

# sanity_check_parcels()
