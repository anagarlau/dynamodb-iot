from shapely import Polygon

from utils.geomapping import get_geohashes_from_polygon, get_from_max_precision, create_map_from_geohash_set, \
    assign_geohashes_to_parcels, build_dictionaries_from_crop_assignment, draw_map_parcels_with_crop, \
    create_map_with_polygon


def main():

    coordinates = [
        (28.1250063, 46.6334964),
        (28.1334177, 46.6175812),
        (28.1556478, 46.6224742),
        (28.1456915, 46.638609),
        (28.1250063, 46.6334964)  # Closing the loop
    ]

    polygon = Polygon(coordinates)
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
    draw_map_parcels_with_crop(list_precision_8_parcels=list(filtered_geohashes), crop_assignment=dict)
    # print(dict)
    # print(dictTuple[0])
    # print(dictTuple[1])


if __name__ == "__main__":
    main()
