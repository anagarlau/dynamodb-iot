import geopandas as gpd
import folium
import matplotlib.pyplot as plt
import pygeohash
from pyproj import CRS
from shapely.geometry import Polygon,box
import shapely.geometry
#import geohash as gh
import geohash2 as gh
import hashlib
#from utils.polygon_def import polygon,coordinates
import matplotlib.pyplot as plt

import geohash

#https://medium.com/bukalapak-data/geolocation-search-optimization-5b2ff11f013b


def create_map_with_polygon(coordinates):
    # Folium requires coordinates in (latitude, longitude) format
    # Reverse the coordinates from (longitude, latitude) to (latitude, longitude)
    reversed_coordinates = [(lat, lon) for lon, lat in coordinates]

    # Calculate the centroid of the polygon for the initial map view
    centroid_lat = sum([point[0] for point in reversed_coordinates]) / len(reversed_coordinates)
    centroid_lon = sum([point[1] for point in reversed_coordinates]) / len(reversed_coordinates)

    # Create a map centered around the centroid
    folium_map = folium.Map(location=[centroid_lat, centroid_lon], zoom_start=13)

    # Add the polygon to the map
    folium.Polygon(
        locations=reversed_coordinates,
        color='blue',
        weight=3,
        fill_color='pink',
        fill_opacity=0.3
    ).add_to(folium_map)
    folium_map.save("maps/map_with_polygon.html")
    return folium_map







# Calculate Area for test purposes. Google: 3.32 sq km

# Create a GeoDataFrame
# gdf = gpd.GeoDataFrame(index=[0], crs='EPSG:4326', geometry=[polygon])
# # With CRS utm
# utm_crs = CRS(f'EPSG:326{int((30 + coordinates[0][0]) // 6) + 1}')
# gdf_projected = gdf.to_crs(utm_crs)
# area_utm = gdf_projected.geometry.area[0] / 1e6
#
# print("Area of the polygon in square meters:", round(area_utm,2))
#
# albers_crs = CRS.from_proj4("+proj=aea +lat_1=45 +lat_2=55 +lon_0=28")
# #  Albers Equal Area
# gdf_projected = gdf.to_crs(albers_crs)
# area_sqm = gdf_projected.geometry.area[0]
# area_sqkm = area_sqm / 1e6  # Convert to square kilometers
#
# print("Area of the polygon in square meters:", round(area_sqkm,2))

#MAP
def get_geohashes_from_polygon(polygon, precision=8):
    minx, miny, maxx, maxy = polygon.bounds
    step = precision *50
    geohashes = set()
    lat_step = (maxy - miny) / step
    lon_step = (maxx - minx) / step
    current_lat = miny
    while current_lat <= maxy:
        current_lon = minx
        while current_lon <= maxx:
            geohash = gh.encode(current_lat, current_lon, precision)
            geohashes.add(geohash)
            current_lon += lon_step
        current_lat += lat_step

    # filtered
    filtered_geohashes = set()
    for geohash in geohashes:
        lat, lon, lat_err, lon_err = gh.decode_exactly(geohash)
        geohash_box = box(lon - lon_err, lat - lat_err, lon + lon_err, lat + lat_err)
        if geohash_box.intersects(polygon):
            filtered_geohashes.add(geohash)
    #print(filtered_geohashes)
    #print(len(filtered_geohashes))
    return filtered_geohashes

# add geohashes to map
def add_geohash_to_map(geohash, map_obj):
    lat_centroid, lon_centroid, lat_err, lon_err = gh.decode_exactly(geohash)
    south_west = (lat_centroid - lat_err, lon_centroid - lon_err)
    north_east = (lat_centroid + lat_err, lon_centroid + lon_err)
    folium.Rectangle(
        bounds=[south_west, north_east],
        color='#ff7800',
        fill=True,
        fill_opacity=0.4,
    ).add_to(map_obj)

def create_map_from_geohash_set(geohash_set, name_of_map):
    if geohash_set:
        first_geohash = list(geohash_set)[0]
        lat, lon = gh.decode(first_geohash)
        m = folium.Map(location=[lat, lon], zoom_start=12)

        # Add each geohash to the map
        for geohash in geohash_set:
            add_geohash_to_map(geohash, m)

        m.save(f'maps/{name_of_map}.html')
    else:
        print("No geohashes were generated within the polygon.")

def get_from_max_precision(higher_precision, geohashes_list):
    s = set()
    for str in geohashes_list:
        s.add(str[0:higher_precision])
    return s


def find_neighbors(ghash):
    neighbors = {
        'N': geohash.neighbors(ghash)[0],
        'NE': geohash.neighbors(geohash.neighbors(ghash)[0])[1],
        'E': geohash.neighbors(ghash)[1],
        'SE': geohash.neighbors(geohash.neighbors(ghash)[2])[1],
        'S': geohash.neighbors(ghash)[2],
        'SW': geohash.neighbors(geohash.neighbors(ghash)[2])[3],
        'W': geohash.neighbors(ghash)[3],
        'NW': geohash.neighbors(geohash.neighbors(ghash)[0])[3]
    }
    return list(neighbors.values())

def assign_geohashes_to_parcels(list_precision_7_parcels, list_precision_8_parcels, testing=False):
    #sort list to always get the same parcels
    #print('Length of prec 8', len(list_precision_8_parcels))
    list_precision_7_parcels = sorted(list_precision_7_parcels)
    list_precision_8_parcels= sorted(list_precision_8_parcels)


    level_7_to_8_mapping = {parcel_7: [parcel_8 for parcel_8 in list_precision_8_parcels if parcel_8.startswith(parcel_7)]
                            for parcel_7 in list_precision_7_parcels}
    crop_assignment = {}
    small_area_buffer = {}

    # initial asignment, save smallest for later or draw rows
    for i, (parcel_7, parcels_8) in enumerate(level_7_to_8_mapping.items()):
        if len(parcels_8) <= 30:
            #print(f'{parcel_7} has less than 30 geohashes, precision 8... Adding to buffer')
            small_area_buffer[parcel_7] = parcels_8
        else:
            crop = 'Chickpeas' if i % 2 == 0 else 'Grapevine'
            for parcel_8 in parcels_8:
                crop_assignment[parcel_8] = crop

    # for smaller areas find neighbours
    for parcel_7, parcels_8 in small_area_buffer.items():
        neighbors_7 = find_neighbors(parcel_7)
        # Collect crops for neighboring parcels
        neighbor_crops = []
        for n in neighbors_7:
            neighbor_crops.extend([crop_assignment.get(p8) for p8 in level_7_to_8_mapping.get(n, [])])
        # Filter out None values
        neighbor_crops = [crop for crop in neighbor_crops if crop is not None]
        # join with smallest crop in the vicinity
        # or assign a default if no neighbors have been assigned
        if neighbor_crops:
            majority_crop = min(set(neighbor_crops), key=neighbor_crops.count)
        else:
            # default crop
            majority_crop = 'Chickpeas'

        for parcel_8 in parcels_8:
            crop_assignment[parcel_8] = majority_crop
    #print(crop_assignment)
    return crop_assignment

def draw_map_parcels_with_crop(list_precision_8_parcels,crop_assignment):
    first_geohash = list(list_precision_8_parcels)[0]

    lat, lon = gh.decode(first_geohash)
    # Visualization (using Folium)
    map = folium.Map(location=[lat, lon],
                     zoom_start=12)  # Set field_lat, field_lon to your field's location
    for parcel in crop_assignment:
        # Add each parcel to the map with a popup showing the crop
        # Decode the geohash to get the bounding box
        lat_centroid, lon_centroid, lat_err, lon_err = gh.decode_exactly(parcel)
        south_west = (lat_centroid - lat_err, lon_centroid - lon_err)
        north_east = (lat_centroid + lat_err, lon_centroid + lon_err)
        folium.Rectangle(
            bounds=[south_west, north_east],
            color="#ff7800" if crop_assignment[parcel] == 'Chickpeas' else "#0000ff",
            fill=True,
            fill_opacity=0.4,
            popup=crop_assignment[parcel]
        ).add_to(map)

    map.save('maps/field_parcels.html')

def build_dictionaries_from_crop_assignment(crop_assignment):
    # Plant specs
    plant_specs = {
        'Chickpeas': {
            'latin_name': 'Cicer arietinum',
            'family': 'Fabaceae',
            'optimal_temperature': (20, 25),  # degrees Celsius
            'optimal_humidity': (30, 50),     # percentage
            'optimal_soil_ph': (6.0, 7.0),
            'water_requirements_mm_per_week': 25,  # mm per week
            'sunlight_requirements_hours_per_day': 8,  # hours per day
        },
        'Grapevine': {
            'latin_name': 'Vitis vinifera',
            'family': 'Vitaceae',
            'optimal_temperature': (15, 22),
            'optimal_humidity': (50, 70),
            'optimal_soil_ph': (5.5, 6.5),
            'water_requirements_mm_per_week': 20,
            'sunlight_requirements_hours_per_day': 6,
        }
    }

    plant_type_to_geohashes = {'Chickpeas': {}, 'Grapevine': {}}
    geohash6_info = {}

    for geohash8, plant in crop_assignment.items():
        geohash6 = geohash8[:6]

        # Append geohashes and plant specs to plant_type_to_geohashes dictionary
        if geohash6 not in plant_type_to_geohashes[plant]:
            plant_type_to_geohashes[plant][geohash6] = {'specs': plant_specs[plant], 'geohash8': [geohash8]}
        else:
            if geohash8 not in plant_type_to_geohashes[plant][geohash6]['geohash8']:
                plant_type_to_geohashes[plant][geohash6]['geohash8'].append(geohash8)

        # Append geohashes and plant specs to geohash6_info dictionary
        if geohash6 not in geohash6_info:
            geohash6_info[geohash6] = {'plant': plant, 'specs': plant_specs[plant], 'geohash8': [geohash8]}
        else:
            if geohash8 not in geohash6_info[geohash6]['geohash8']:
                geohash6_info[geohash6]['geohash8'].append(geohash8)
    return plant_type_to_geohashes, geohash6_info



