import itertools
from random import random
import uuid
import pygeohash as gh
from folium import folium, Marker, Icon
from shapely.geometry import Point
import random


def create_uniform_sensor_grid(polygon, precision=8):
    sensor_distribution = {
            'Temperature': 50,
            'Light': 50,
            'SoilMoisture': 50,
            'Humidity': 50,
            'Rain': 50,
            'SoilPH': 50
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
    random.shuffle(weighted_sensors)  # Shuffle the list for randomness

    sensor_locations = []
    for y in range(grid_size + 1):
        for x in range(grid_size + 1):
            current_lat = miny + (y * lat_step)
            current_lon = minx + (x * lon_step)
            point = Point(current_lon, current_lat)
            if polygon.contains(point) and weighted_sensors:
                sensor_type = weighted_sensors.pop()  # Randomly get a sensor type
                geohash = gh.encode(current_lat, current_lon, precision)
                sensor_locations.append(
                    {'sensorId': uuid.uuid4(),
                     'sensor_type': sensor_type,
                     'geohash8': geohash,
                     'coordinates': (current_lat, current_lon)})

    return sensor_locations


def visualize_sensor_locations_on_existing_map(sensor_locations, existing_map, map_filename='maps/sensor_map.html'):
    sensor_colors = {
        'Temperature': 'red',
        'Light': 'green',
        'SoilMoisture': 'blue',
        'Humidity': 'purple',
        'Rain': 'darkblue',
        'SoilPH': 'orange'
    }
    if not sensor_locations:
        print("No sensor locations provided.")
        return
    # Sanity check marker
    # Marker([46.6334964, 28.1250063], popup="Test Marker").add_to(existing_map)
    print("Number of sensors to place:", len(sensor_locations))
    for sensor in sensor_locations:
        lat, lon = sensor['coordinates']
        #print(lat, lon)
        popup_text = f"Type:{sensor['sensor_type']}<br>Geohash:{sensor['geohash8']}"
        icon_color = sensor_colors.get(sensor['sensor_type'], 'gray')
        marker_icon = Icon(color=icon_color)
        # print(f"Placing sensor on map at {lat}, {lon}")
        Marker([lat, lon], popup=popup_text, icon=marker_icon).add_to(existing_map)

    existing_map.save(map_filename)
    print(f"Map saved to {map_filename}")
    return existing_map
