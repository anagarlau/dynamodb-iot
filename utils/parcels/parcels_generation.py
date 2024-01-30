import math
import uuid

import folium
from shapely import LineString, GeometryCollection, Point
from shapely.geometry import Polygon
from shapely.ops import split

from utils.polygon_def import polygon, split_points


#https://medium.com/bukalapak-data/geolocation-search-optimization-5b2ff11f013b

def create_map_with_polygon(coordinates):
    reversed_coordinates = [(lat, lon) for lon, lat in coordinates]
    centroid_lat = sum([point[0] for point in reversed_coordinates]) / len(reversed_coordinates)
    centroid_lon = sum([point[1] for point in reversed_coordinates]) / len(reversed_coordinates)
    folium_map = folium.Map(location=[centroid_lat, centroid_lon], zoom_start=13)
    folium.Polygon(
        locations=reversed_coordinates,
        color='blue',
        weight=3,
        fill_color='pink',
        fill_opacity=0.3
    ).add_to(folium_map)
    #folium_map.save("maps/map_with_polygon.html")
    return folium_map

# Created with ChatGPT support
def calculate_bearing_from_bbox():
    # Get the minimum rotated rectangle (MRR) that encloses the polygon
    mrr = polygon.minimum_rotated_rectangle

    # Extract the coordinates of the MRR
    mrr_coords = list(mrr.exterior.coords)

    # Calculate the length of the rectangle's sides
    # The MRR will have 5 coordinates, with the first repeating, so we need only 4
    edge_lengths = [Point(mrr_coords[i]).distance(Point(mrr_coords[i + 1])) for i in range(4)]

    # Determine the longer edge of the rectangle
    long_edge_index = edge_lengths.index(max(edge_lengths))

    # Calculate the orientation of the long edge
    dx = mrr_coords[long_edge_index + 1][0] - mrr_coords[long_edge_index][0]
    dy = mrr_coords[long_edge_index + 1][1] - mrr_coords[long_edge_index][1]

    # Calculate the angle in radians, and convert to degrees
    angle_rad = math.atan2(dy, dx)
    angle_deg = math.degrees(angle_rad)

    # Convert the angle to a bearing
    bearing = (angle_deg + 360) % 360

    return bearing

# Created with ChatGPT support
def split_in_parcels():
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
    bearing = calculate_bearing_from_bbox()
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
            'parcel_id': f"Chickpeas#{uuid.uuid4()}" if index % 2 == 0 else f"Grapevine#{uuid.uuid4()}",
            'plant_type': 'Chickpeas' if index % 2 == 0 else 'Grapevine',
            'polygon': polygon,
            **plant_specs['Chickpeas' if index % 2 == 0 else 'Grapevine']
        }
        for index, polygon in enumerate(split_polygons)
    ]
    # print('Dictionary')
    # print(plant_polygons)
    return plant_polygons


def plot_polygons_on_map(plant_polygons, original_polygon=polygon,folium_map=None):
    min_lon, min_lat, max_lon, max_lat = original_polygon.bounds
    map_center = [(min_lat + max_lat) / 2, (min_lon + max_lon) / 2]
    if folium_map is None:
        folium_map = folium.Map(location=map_center, zoom_start=12)
    folium.Polygon(locations=[(y, x) for x, y in original_polygon.exterior.coords],
                   color='black', weight=2, fill_opacity=0).add_to(folium_map)
    for item in plant_polygons:
        plant_type = item['plant_type']
        poly = item['polygon']
        color = "#ff7800" if plant_type == 'Chickpeas' else "#0000ff"

        folium.Polygon(locations=[(y, x) for x, y in poly.exterior.coords],
                       color=color,
                       fill=True,
                       popup=f"Parcel_id{item['parcel_id']}",
                       fill_opacity=0.5).add_to(folium_map)

    return folium_map

# pl = split_in_parcels()
# map = plot_polygons_on_map(pl)
#map.save("testing.html")
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


