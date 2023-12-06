import geopandas as gpd
import folium
import matplotlib.pyplot as plt
from shapely.geometry import Polygon,box
import shapely.geometry
#import geohash as gh
import geohash2 as gh

import matplotlib.pyplot as plt

#https://medium.com/bukalapak-data/geolocation-search-optimization-5b2ff11f013b
# Define your polygon coordinates
coordinates = [
    (28.0975448, 46.6192589),
    (28.0975448, 46.6116535),
    (28.1141101, 46.6116683),
    (28.1140458, 46.6193178),
    (28.0975448, 46.6192589)  # Closing the loop
]

# Create a Shapely polygon from your coordinates
polygon = Polygon(coordinates)

# Create a GeoDataFrame
gdf = gpd.GeoDataFrame(index=[0], crs='EPSG:4326', geometry=[polygon])

# Plot the polygon
gdf.plot(marker='o', color='red', edgecolor='black')
plt.xlabel("Longitude")
plt.ylabel("Latitude")
plt.title("Polygon Visualization")

# Show the plot
plt.show()


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

# Define the polygon coordinates
coordinates = [
    (28.1250063, 46.6334964),
    (28.1334177, 46.6175812),
    (28.1556478, 46.6224742),
    (28.1456915, 46.638609),
    (28.1250063, 46.6334964)  # Closing the loop
]

# Create the map with the polygon
map_with_polygon = create_map_with_polygon(coordinates)

# Display the map
map_with_polygon.save("map_with_polygon.html")



