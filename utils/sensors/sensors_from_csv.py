import csv
import simplejson as json

from folium import Marker, Icon, Circle

from utils.parcels.parcels_generation import create_map_with_polygon
from utils.polygon_def import polygon

jsonFilepath='C:\\Users\\ana\\PycharmProjects\\dynamodb\\maps\\data\\sensors_to_json.json'
csvFilePath='C:\\Users\\ana\\PycharmProjects\\dynamodb\\maps\\data\\sensors_locations.csv'
def csv_to_json(csv_filepath=csvFilePath, json_filepath=jsonFilepath):
    # Create a list to store the data
    data_list = []

    # Open the CSV file for reading
    with open(csv_filepath, 'r', encoding='utf-8') as csv_file:
        # Create a CSV reader object
        csv_reader = csv.DictReader(csv_file)

        # Iterate over each row in the CSV file
        for row in csv_reader:
            # Convert the POINT string to a list of floats
            point_str = row['point_coordinates'].replace('POINT (', '').replace(')', '')
            point = [float(coord.strip()) for coord in point_str.split()]

            # Replace the string point coordinates with the list of floats
            row['point_coordinates'] = point

            # Add the row (which is a dictionary) to the list
            data_list.append(row)

    # Write the list to a JSON file
    with open(json_filepath, 'w', encoding='utf-8') as json_file:
        json.dump(data_list, json_file, indent=4)
def json_to_array(json_filepath=jsonFilepath):
    # Open the JSON file for reading
    with open(json_filepath, 'r', encoding='utf-8') as json_file:
        # Load the JSON content into a Python list
        data_array = json.load(json_file)
    return data_array

def add_sensor_markers_to_map(sensor_data,map_object=None):
    if map_object is None:
        map_object = create_map_with_polygon(polygon.exterior.coords)
    # Define a dictionary to map sensor types to colors
    sensor_type_colors = {
        'Light': 'blue',
        'Temperature': 'red',
        'SoilMoisture': 'green',
        'Rain': 'purple',
        'SoilPH': 'orange',
        'Humidity': 'darkblue'
    }

    # Iterate over the sensor data
    for sensor in sensor_data:
        #print(sensor)
        longitude, latitude = sensor['point_coordinates']
        # Get the sensor type and corresponding color
        sensor_type = sensor['sensor_type']
        marker_color = sensor_type_colors.get(sensor_type, 'gray')  # Default to gray if sensor type is not found
        Marker(
            [latitude, longitude],
            popup=f"{sensor_type}<br> Sensor: {sensor['sensor_id']}<br> Parcel id {sensor['parcel_id']}",
            icon=Icon(color=marker_color)
        ).add_to(map_object)
    return map_object

# Method for parsing DynamoDB sensor payloads
def parse_sensor_data(sensor_data):
    parsed_data = []

    for item in sensor_data:
        #print(item)
        sensor_id = item.get('SK', {}).get('S', None)
        sensor_type = item.get('sensor_type', {}).get('S') or item.get('sensortype', {}).get('S')
        geoJson_str = item.get('geoJson', {}).get('S', None)
        parcel_id = item.get('curr_parcelid') or item.get('id_parcel')

        if sensor_id and geoJson_str and sensor_type:
            # Split the string by comma to get the coordinates
            coords = geoJson_str.split(",")
            if len(coords) == 2:
                try:
                    # Convert strings to float
                    latitude, longitude = float(coords[0]), float(coords[1])

                    parsed_data.append({
                        'sensor_id': sensor_id,
                        'sensor_type': sensor_type,  # Including sensor type
                        'point_coordinates': [longitude,latitude],
                        'parcel_id': parcel_id
                    })
                except ValueError:
                    print("Invalid coordinates:", coords)

    return parsed_data


def visualize_results(center_point, radius, sensors, color='green'):

    map_object = add_sensor_markers_to_map(sensors)
    if center_point and radius:
    # Draw the center point as a marker
        lat, lon = center_point.y, center_point.x
        Marker(
            location=[center_point.y, center_point.x],
            popup=f"Center Point: {center_point}</br> {lat}{lon}"
        ).add_to(map_object)

        # Draw a circle around the center point with the given radius
        Circle(
            location=[center_point.y, center_point.x],
            radius=radius,  # The radius in meters
            color=color,
            fill=True,
            fill_color='red',
            fill_opacity=0.2,
            popup=f"Radius: {radius} meters"
        ).add_to(map_object)

    return map_object

# Visualize the inserted sensors from the csv file
def sanity_check_from_csv(map=None):
    csv_to_json()
    jsonArray = json_to_array()
    #print(jsonArray[0:2])
    map = add_sensor_markers_to_map(jsonArray, map)
    return map
# map = sanity_check_from_csv()
# print("saving...")
# map.save("../../maps/sensors_from_csv.html")