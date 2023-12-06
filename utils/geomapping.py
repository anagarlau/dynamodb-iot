import geopandas as gpd
import folium
import matplotlib.pyplot as plt
import pygeohash
from pyproj import CRS
from shapely.geometry import Polygon,box
import shapely.geometry
#import geohash as gh
import geohash2 as gh

import matplotlib.pyplot as plt

#https://medium.com/bukalapak-data/geolocation-search-optimization-5b2ff11f013b
# Define your polygon coordinates
coordinates = [
    (28.1250063, 46.6334964),
    (28.1334177, 46.6175812),
    (28.1556478, 46.6224742),
    (28.1456915, 46.638609),
    (28.1250063, 46.6334964)  # Closing the loop
]

polygon = Polygon(coordinates)

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

    return folium_map



# Create the map with the polygon
map_with_polygon = create_map_with_polygon(coordinates)

# Display the map
map_with_polygon.save("map_with_polygon.html")



# Calculate Area for test purposes. Google: 3.32 sq km

# Create a GeoDataFrame
gdf = gpd.GeoDataFrame(index=[0], crs='EPSG:4326', geometry=[polygon])
# With CRS utm
utm_crs = CRS(f'EPSG:326{int((30 + coordinates[0][0]) // 6) + 1}')
gdf_projected = gdf.to_crs(utm_crs)
area_utm = gdf_projected.geometry.area[0] / 1e6

print("Area of the polygon in square meters:", round(area_utm,2))

albers_crs = CRS.from_proj4("+proj=aea +lat_1=45 +lat_2=55 +lon_0=28")
#  Albers Equal Area
gdf_projected = gdf.to_crs(albers_crs)
area_sqm = gdf_projected.geometry.area[0]
area_sqkm = area_sqm / 1e6  # Convert to square kilometers

print("Area of the polygon in square meters:", round(area_sqkm,2))

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
    print(filtered_geohashes)
    print(len(filtered_geohashes))
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

        m.save(f'{name_of_map}.html')
    else:
        print("No geohashes were generated within the polygon.")

def get_from_max_precision(higher_precision, geohashes_list):
    s = set()
    for str in geohashes_list:
        s.add(str[0:higher_precision])
    return s




filtered_geohashes = get_geohashes_from_polygon(polygon)
create_map_from_geohash_set(geohash_set=filtered_geohashes, name_of_map='geohash_map')

refiltered = get_from_max_precision(higher_precision=6, geohashes_list=filtered_geohashes)
print("Refiltered")
print(refiltered)
create_map_from_geohash_set(geohash_set=refiltered, name_of_map='geohash_map_6')

