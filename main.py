from shapely import Polygon

from utils.csv_util import export_data_to_csv
from utils.polygon_def import polygon,coordinates
from utils.geomapping import get_geohashes_from_polygon, get_from_max_precision, create_map_from_geohash_set, \
    assign_geohashes_to_parcels, build_dictionaries_from_crop_assignment, draw_map_parcels_with_crop, \
    create_map_with_polygon
from utils.sensor_placing import create_uniform_sensor_grid, visualize_sensor_locations_on_existing_map


def main():


    # Create the map with the polygon
    map_with_polygon = create_map_with_polygon(coordinates)

    # Display the map

    filtered_geohashes = get_geohashes_from_polygon(polygon)
    create_map_from_geohash_set(geohash_set=filtered_geohashes, name_of_map='geohash_map')
    refiltered = get_from_max_precision(higher_precision=7, geohashes_list=filtered_geohashes)
    create_map_from_geohash_set(geohash_set=refiltered, name_of_map='geohash_map_7')

    dict = assign_geohashes_to_parcels(list_precision_7_parcels=list(refiltered),
                                      list_precision_8_parcels=list(filtered_geohashes))
    dictTuple = build_dictionaries_from_crop_assignment(crop_assignment=dict)
    crop_map = draw_map_parcels_with_crop(list_precision_8_parcels=list(filtered_geohashes), crop_assignment=dict)
    print(dict)
    print(dictTuple[0]['Grapevine'])
    print(dictTuple[1])

    sensor_grid = create_uniform_sensor_grid(polygon, precision=8)
    updated_map = visualize_sensor_locations_on_existing_map(sensor_grid, crop_map)
    export_data_to_csv(plant_type_to_geohashes=dictTuple[0], geohash6_info=dictTuple[1], list_sensors=sensor_grid)
    #print(updated_map)
if __name__ == "__main__":
    main()
