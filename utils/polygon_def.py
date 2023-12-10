# Define your polygon coordinates
# Cf. geomapping.py for additional methods
# to create
# geohashes of different precisions,
# fields with parcels and
# different maps with folium
from shapely import Polygon, Point
from geopy.distance import geodesic

coordinates = [
    (28.1250063, 46.6334964),
    (28.1334177, 46.6175812),
    (28.1556478, 46.6224742),
    (28.1456915, 46.638609),
    (28.1250063, 46.6334964)  # Closing the loop
]

polygon = Polygon(coordinates)


# Calculate the centroid of the polygon
center_point = polygon.centroid
center_coord = (center_point.y, center_point.x)  # Latitude, Longitude

# Calculate the radius as the maximum distance from the center to a vertex
radius = max([geodesic(center_coord, (coord[1], coord[0])).meters for coord in coordinates])

print(f"Center: {center_point}, Radius: {radius} meters")