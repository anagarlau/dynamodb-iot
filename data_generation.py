from utils.csv_util import export_data_to_csv
from utils.polygon_def import polygon, coordinates, split_points
from utils.parcels.parcels_generation import create_map_with_polygon, plot_polygons_on_map, split_in_parcels
from utils.sensor_events.sensor_events_generation import generate_sensor_events_from_locations_csv_into_json
from utils.sensors.sensor_placing_generation import create_uniform_sensor_grid, visualize_sensor_locations_on_existing_map
from utils.sensors.sensors_from_csv import sanity_check_from_csv


def main():

    # Split the polygon into parcels and assign to crop types randomly
    crop_assignment = split_in_parcels(polygon, split_points)
    # print(polygon)
    # print(len(crop_assignment))
    # print(crop_assignment)
    #Create the map with the polygon
    map_with_polygon = create_map_with_polygon(coordinates)
    map_with_polygon.save("maps/map_with_polygon.html")
    # # #CREATE SENSOR GRID and place to CSV
    sensor_grid = create_uniform_sensor_grid(polygon, crop_assignment)
    # #Visualize newly generated sensor_grid
    updated_map = visualize_sensor_locations_on_existing_map(sensor_grid, map_with_polygon)
    map = plot_polygons_on_map(crop_assignment, polygon,map_with_polygon)
    # Save sensor grid and crop parcels to csv file
    export_data_to_csv(list_sensors=sensor_grid, list_plants=crop_assignment)
    # Generate sensor events based on the newly created sensor grid
    generate_sensor_events_from_locations_csv_into_json()
    # Sanity check sensor map from csv vs map with parcels and added sensors from csv
    sanity_check_from_csv(map)
    map.save("maps/map_with_sensors_and_parcels.html")



if __name__ == "__main__":
    main()
