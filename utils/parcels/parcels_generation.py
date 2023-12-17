import math
import random
import uuid

import boto3
import numpy as np
from shapely import LineString, Point, MultiLineString, MultiPolygon, GeometryCollection
from shapely.affinity import rotate
from shapely.ops import split, unary_union

import dynamodbgeo
import geohash2
import geopandas as gpd
import folium
import matplotlib.pyplot as plt
import pygeohash
from geopy.distance import great_circle
from pyproj import CRS
from shapely.geometry import Polygon,box
import shapely.geometry
#import geohash as gh
import geohash2 as gh
import hashlib
#from utils.polygon_def import polygon,coordinates
import matplotlib.pyplot as plt

import geohash
from utils.polygon_def import coordinates, polygon


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
    #folium_map.save("maps/map_with_polygon.html")
    return folium_map


def split_in_parcels(polygon, split_points, bearing=193):
    """
    Splits a larger polygon into subareas based on a series of split points, aligned to a given bearing.
    """
    plant_specs = {
        'Chickpeas': {
            'latin_name': 'Cicer arietinum',
            'family': 'Fabaceae',
            'optimal_temperature': (20, 25),  # degrees Celsius
            'optimal_humidity': (30, 50),  # percentage
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
    # Sort points by their x-coordinate to ensure the line cuts through the polygon systematically
    sorted_points = sorted(split_points, key=lambda p: p.x)

    # Calculate the vector for the bearing
    dx = math.cos(math.radians(bearing))
    dy = math.sin(math.radians(bearing))

    # Generate lines that are parallel to the bearing and extend through the split points
    lines = []
    for p in sorted_points:
        # Extend the line well beyond the actual polygon to ensure it cuts through entirely
        line = LineString([(p.x - dx * 10, p.y - dy * 10), (p.x + dx * 10, p.y + dy * 10)])
        lines.append(line)

    # Split the polygon using these lines
    split_polygons = [polygon]
    for line in lines:
        new_split = []
        for poly in split_polygons:
            split_parts = split(poly, line)
            # Check if the result is a GeometryCollection and iterate through it
            if isinstance(split_parts, GeometryCollection):
                for part in split_parts.geoms:
                    if isinstance(part, Polygon):
                        new_split.append(part)
            else:
                new_split.append(poly)
        split_polygons = new_split

    plant_polygons = [
        {
            'area_id': uuid.uuid4(),
            'plant_type': 'Chickpeas' if index % 2 == 0 else 'Grapevine',
            'polygon': polygon,
            **plant_specs['Chickpeas' if index % 2 == 0 else 'Grapevine']
        }
        for index, polygon in enumerate(split_polygons)
    ]
    # print('Dictionary')
    # print(plant_polygons)
    return plant_polygons
def plot_polygons_on_map(original_polygon, plant_polygons, folium_map=None):
    min_lon, min_lat, max_lon, max_lat = original_polygon.bounds
    map_center = [(min_lat + max_lat) / 2, (min_lon + max_lon) / 2]
    if folium_map is None:
        folium_map = folium.Map(location=map_center, zoom_start=12)

    # Add the original polygon to the map with an outline
    folium.Polygon(locations=[(y, x) for x, y in original_polygon.exterior.coords],
                   color='black', weight=2, fill_opacity=0).add_to(folium_map)

    # Add each split polygon to the map with the color according to the plant_type
    for item in plant_polygons:
        plant_type = item['plant_type']
        poly = item['polygon']
        color = "#ff7800" if plant_type == 'Chickpeas' else "#0000ff"

        folium.Polygon(locations=[(y, x) for x, y in poly.exterior.coords],
                       color=color, fill=True, fill_opacity=0.5).add_to(folium_map)

    return folium_map
# Calculate Area for test purposes. Google: 3.32 sq km

# Create a GeoDataFrame
# gdf = gpd.GeoDataFrame(index=[0], crs='EPSG:4326', geometry=[polygon])
# # # With CRS utm
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
# area_sqkm = area_sqm / 1e6  # Google Maps results match
#
# print("Area of the polygon in square meters:", round(area_sqkm,2))

#MAP


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
        # lat_centroid, lon_centroid, lat_err, lon_err = gh.decode_exactly(geohash8)
        # lat_centroid, lon_centroid, lat_err, lon_err = gh.decode_exactly(geohash6)
        # p = dynamodbgeo.GeoPoint(lat_centroid, lon_centroid)
        # print(f"point {p}")
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



