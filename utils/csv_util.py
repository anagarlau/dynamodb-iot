import csv

from shapely import Point

from utils.parcels.parcels_from_csv import csv_to_json
from utils.sensors import sensors_from_csv


def list_to_csv_for_plant_type(filename, plant_polygons, headers):
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=headers)
        writer.writeheader()
        for plant in plant_polygons:
            # Flatten specs within the plant dictionary and prepare polygon WKT
            flattened_specs = {k: v for k, v in plant.items() if k in headers}
            flattened_specs['polygon'] = flattened_specs['polygon'].wkt  # Convert Polygon to WKT format
            writer.writerow(flattened_specs)
    csv_to_json()

def list_to_csv_for_sensor_type(filename, list_sensors, sensor_headers):
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=sensor_headers)
        writer.writeheader()
        for sensor in list_sensors:
            row = {
                'sensor_id': sensor['sensor_id'],
                'sensor_type': sensor['sensor_type'],
                'point_coordinates': Point(sensor['coordinates'][1], sensor['coordinates'][0]),
                'parcel_id': sensor['parcel_id'],
                'manufacturer': sensor['manufacturer'],
                'model': sensor['model'],
                'firmware': sensor['firmware']
            }
            writer.writerow(row)
    sensors_from_csv.csv_to_json()
def export_data_to_csv(list_sensors, list_plants, path='maps/data'):
    # Headers for plant_type_to_parcels
    plant_type_headers = ['parcel_id', 'plant_type', 'latin_name', 'family',
                          'optimal_temperature', 'optimal_humidity', 'optimal_soil_ph',
                          'water_requirements_mm_per_week', 'sunlight_requirements_hours_per_day', 'polygon']

    # Headers for list_sensors
    sensors_headers = ['sensor_id', 'sensor_type', 'point_coordinates', 'parcel_id', 'manufacturer', 'model', 'firmware']

    # plant_type_to_geohashes to CSV
    list_to_csv_for_plant_type(f'{path}/plant_type_to_parcels.csv', list_plants, plant_type_headers)

    list_to_csv_for_sensor_type(f'{path}/sensors_locations.csv', list_sensors, sensors_headers)





