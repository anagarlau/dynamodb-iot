import itertools
import random
import uuid
from typing import List

from folium import Marker, Icon
from shapely import Polygon
from shapely.geometry import Point

from backend.models.Parcel import Parcel
from dynamodbgeo.dynamodbgeo import GeoPoint, S2Manager


def find_polygon_for_point(point, polygon_data_list):
    for polygon_data in polygon_data_list:
        if polygon_data['polygon'].contains(point):
            return polygon_data
    return None


def is_point_in_parcel(point, parcel_list: List[Parcel]):
    for parcel in parcel_list:
        if parcel.polygon.contains(point):
            return parcel
    return None


def create_uniform_sensor_grid(polygon, crop_assignment):
    processed_crop_assignments = [{**item, 'polygon': Polygon(item['polygon'])} for item in crop_assignment]
    # print("crop assignment dict")
    # print(processed_crop_assignments)
    sensor_distribution = {
        'Temperature': 50,
        'Light': 50,
        'SoilMoisture': 50,
        'Humidity': 50,
        'Rain': 50,
        'SoilPH': 50
    }

    sensor_manufacturing_details = {
        'Temperature': [
            {'manufacturer': 'Honeywell', 'model': 'T10', 'firmware': 'v1.2'},
            {'manufacturer': 'Siemens', 'model': 'TH100', 'firmware': 'v1.3'}
        ],
        'Light': [
            {'manufacturer': 'Philips', 'model': 'L200', 'firmware': 'v2.1'},
            {'manufacturer': 'Osram', 'model': 'LX1', 'firmware': 'v2.3'}
        ],
        'SoilMoisture': [
            {'manufacturer': 'Bosch', 'model': 'SM10', 'firmware': 'v3.0'},
            {'manufacturer': 'Texas Instruments', 'model': 'SM200', 'firmware': 'v3.2'}
        ],
        'Humidity': [
            {'manufacturer': 'Sensirion', 'model': 'SHT31', 'firmware': 'v4.0'},
            {'manufacturer': 'Panasonic', 'model': 'HC2-H-DC', 'firmware': 'v4.1'}
        ],
        'Rain': [
            {'manufacturer': 'Davis Instruments', 'model': 'R1', 'firmware': 'v5.0'},
            {'manufacturer': 'La Crosse Technology', 'model': 'Rain200', 'firmware': 'v5.1'}
        ],
        'SoilPH': [
            {'manufacturer': 'Atlas Scientific', 'model': 'PH10', 'firmware': 'v6.0'},
            {'manufacturer': 'Hanna Instruments', 'model': 'HI98103', 'firmware': 'v6.1'}
        ]
    }

    # Determine the total number of sensors and the grid size
    total_sensors = sum(sensor_distribution.values())
    grid_size = int(total_sensors ** 0.5)  # Square root of total sensors for grid dimension

    minx, miny, maxx, maxy = polygon.bounds
    lat_step = (maxy - miny) / grid_size
    lon_step = (maxx - minx) / grid_size
    # Weighted list of sensors for random placement
    weighted_sensors = [[sensor] * count for sensor, count in sensor_distribution.items()]
    weighted_sensors = list(itertools.chain.from_iterable(weighted_sensors))
    random.shuffle(weighted_sensors)  # shuffle for randomness
    s2_manager = S2Manager()
    sensor_locations = []
    for y in range(grid_size + 1):
        for x in range(grid_size + 1):
            current_lat = miny + (y * lat_step)
            current_lon = minx + (x * lon_step)
            point = Point(current_lon, current_lat)
            parcel = find_polygon_for_point(point, processed_crop_assignments)
            geopoint = GeoPoint(current_lat, current_lon)
            geohash = s2_manager.generateGeohash(geopoint)
            if polygon.contains(point) and weighted_sensors:
                sensor_type = weighted_sensors.pop()  # Randomly get a sensor type
                sensor_details = random.choice(sensor_manufacturing_details[sensor_type])
                # geohash = gh.encode(current_lat, current_lon, precision)
                sensor_locations.append(
                    {'sensor_id': uuid.uuid4(),
                     'sensor_type': sensor_type,
                     'parcel_id': parcel['parcel_id'],
                     'coordinates': (current_lat, current_lon),
                     'manufacturer': sensor_details['manufacturer'],
                     'model': sensor_details['model'],
                     'firmware': sensor_details['firmware']
                     })

    return sensor_locations


def visualize_sensor_locations_on_existing_map(sensor_locations, existing_map,
                                               map_filename='maps/sensor_map_generated.html'):
    sensor_colors = {
        'Light': 'blue',
        'Temperature': 'red',
        'SoilMoisture': 'green',
        'Rain': 'purple',
        'SoilPH': 'orange',
        'Humidity': 'darkblue'
    }
    if not sensor_locations:
        print("No sensor locations provided.")
        return
    # Sanity check marker
    # Marker([46.6334964, 28.1250063], popup="Test Marker").add_to(existing_map)
    print("Number of sensors to place:", len(sensor_locations))
    for sensor in sensor_locations:
        lat, lon = sensor['coordinates']
        # print(lat, lon)
        popup_text = f"Type:{sensor['sensor_type']}<br>Geohash: {sensor['sensor_id']} <br> Parcel id {sensor['parcel_id']}"
        icon_color = sensor_colors.get(sensor['sensor_type'], 'gray')
        marker_icon = Icon(color=icon_color)
        # print(f"Placing sensor on map at {lat}, {lon}")
        Marker([lat, lon], popup=popup_text, icon=marker_icon).add_to(existing_map)

    existing_map.save(map_filename)
    print(f"Map saved to {map_filename}")
    return existing_map
