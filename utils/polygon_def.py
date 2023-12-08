# Define your polygon coordinates
# Cf. geomapping.py for additional methods
# to create
# geohashes of different precisions,
# fields with parcels and
# different maps with folium
from shapely import Polygon

coordinates = [
    (28.1250063, 46.6334964),
    (28.1334177, 46.6175812),
    (28.1556478, 46.6224742),
    (28.1456915, 46.638609),
    (28.1250063, 46.6334964)  # Closing the loop
]

polygon = Polygon(coordinates)