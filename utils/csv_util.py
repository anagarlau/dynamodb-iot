import csv

from shapely import Point

from utils.polygon_def import polygon,coordinates

def dict_to_csv_for_geohash6_info(filename, dictionary, headers):
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=headers)
        writer.writeheader()
        for geohash6, data in dictionary.items():
            for geohash8 in data['geohash8']:
                row = {
                    'geohash6': geohash6,
                    'geohash8': geohash8,
                    'plant': data['plant']
                }
                row.update(data['specs'])  # Add plant specifications
                writer.writerow(row)

def dict_to_csv_for_plant_type(filename, dictionary, headers):
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=headers)
        writer.writeheader()
        for plant_type, geohashes in dictionary.items():
            for geohash6, data in geohashes.items():
                # Extract and flatten specs
                flattened_specs = {f'{k}': v for k, v in data['specs'].items()}
                for geohash8 in data['geohash8']:
                    row = {
                        'plant_type': plant_type,
                        'geohash6': geohash6,
                        'geohash8': geohash8,
                    }
                    row.update(flattened_specs)  # Add plant specifications
                    writer.writerow(row)

def list_to_csv_for_sensor_type(filename, list_sensors, sensor_headers):
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=sensor_headers)
        writer.writeheader()
        for sensor in list_sensors:
            row = {
                'sensor_id': sensor['sensor_id'],
                'sensor_type': sensor['sensor_type'],
                'geohash6': sensor['geohash8'][0:6],
                'geohash8': sensor['geohash8'],
                'point_coordinates': Point(sensor['coordinates'][1], sensor['coordinates'][0])
            }
            writer.writerow(row)

def export_data_to_csv(plant_type_to_geohashes, geohash6_info,  list_sensors, path='maps/data'):
    # Headers for plant_type_to_geohashes
    plant_type_headers = ['plant_type', 'geohash6', 'geohash8', 'latin_name', 'family',
                          'optimal_temperature', 'optimal_humidity', 'optimal_soil_ph',
                          'water_requirements_mm_per_week', 'sunlight_requirements_hours_per_day']

    # Headers for geohash6_info
    geohash6_info_headers = ['geohash6', 'geohash8', 'plant', 'latin_name', 'family',
                             'optimal_temperature', 'optimal_humidity', 'optimal_soil_ph',
                             'water_requirements_mm_per_week', 'sunlight_requirements_hours_per_day']
    # Headers for list_sensors
    sensors_headers = ['sensor_id', 'geohash6', 'geohash8','sensor_type', 'point_coordinates']

    # plant_type_to_geohashes to CSV
    dict_to_csv_for_plant_type(f'{path}/plant_type_to_geohashes.csv', plant_type_to_geohashes, plant_type_headers)

    # geohash6_info to CSV
    dict_to_csv_for_geohash6_info(f'{path}/geohash6_info.csv', geohash6_info, geohash6_info_headers)

    list_to_csv_for_sensor_type(f'{path}/sensors_locations.csv', list_sensors, sensors_headers)





