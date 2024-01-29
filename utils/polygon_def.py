import os

import boto3
from shapely import Polygon, Point
from geopy.distance import geodesic

coordinates = [
    (28.1250063, 46.6334964),
    (28.1334177, 46.6175812),
    (28.1556478, 46.6224742),
    (28.1456915, 46.638609),
    (28.1250063, 46.6334964)  # Closes the loop
]

polygon = Polygon(coordinates)
# Global variable for split points
split_points = [
    Point(28.1357351, 46.6331402),
    Point(28.1368938, 46.6311952),
    Point(28.1381813, 46.6286902),
    Point(28.1391683, 46.6270693),
    Point(28.1402841, 46.625242),
    Point(28.1411854, 46.6235326),
    Point(28.1424728, 46.6217936)
]

# Centroid of the polygon
center_point_field = polygon.centroid
center_coord = (center_point_field.y, center_point_field.x)  # Latitude, Longitude

# Radius as the maximum distance from the center to a vertex
radius = max([geodesic(center_coord, (coord[1], coord[0])).meters for coord in coordinates])
split_point = Point(28.13781, 46.63099)


client="dynamodb"
region_name="localhost"
endpoint_url="http://localhost:8000"
aws_access_key_id="fakeMyKeyId"
aws_secret_access_key="fakeSecretAccessKey`"
#print(f"Center: {center_point_field}, Radius: {radius} meters")
hashKeyLength=6
def create_dynamodb_client(resource=False):
    if not resource:
        return boto3.client(client, region_name=region_name, endpoint_url=endpoint_url,
                                     aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    else:
        return boto3.resource(client, region_name=region_name, endpoint_url=endpoint_url,
                                     aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

def get_project_path():
    current_script_path = os.path.abspath(__file__)
    project_root = os.path.dirname(os.path.dirname(current_script_path))
    return project_root

print(get_project_path())