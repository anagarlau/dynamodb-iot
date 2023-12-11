import json
import uuid
from cmath import cos
from collections import defaultdict
from math import radians
import boto3
import dynamodbgeo
import geohash2
import s2sphere
from botocore.exceptions import ClientError, BotoCoreError

from folium import folium, Marker, Polygon, Icon, CircleMarker, vector_layers, Circle, Rectangle

from shapely import Point

from utils.geomapping import create_map_with_polygon, add_geohash_to_map, assign_geohashes_to_parcels, \
    get_from_max_precision, get_geohashes_from_polygon, draw_map_parcels_with_crop, calculate_geohash_area
from utils.polygon_def import polygon, coordinates


# TODO Extract into service as class
def create_dynamodb_client(local=True):
    # return boto3.resource("dynamodb", region_name="localhost", endpoint_url="http://localhost:8000",
    #                     aws_access_key_id="fakeMyKeyId", aws_secret_access_key="fakeSecretAccessKey`")
    return boto3.client("dynamodb", region_name="localhost", endpoint_url="http://localhost:8000",
                        aws_access_key_id="fakeMyKeyId", aws_secret_access_key="fakeSecretAccessKey`")






def visualize_geohashes(map_object, center_point, radius, geohashes_p8, color='green'):
    # Draw each geohash at precision 8
    for gh in geohashes_p8:
        add_geohash_to_map(gh, map_object, color=color)

    # Draw the center point as a marker
    Marker(
        location=[center_point.y, center_point.x],
        popup=f"Center Point: {center_point}</br> {geohash2.encode(center_point.y, center_point.x, 8)}"
    ).add_to(map_object)

    # Draw a circle around the center point with the given radius
    Circle(
        location=[center_point.y, center_point.x],
        radius=radius,  # The radius in meters
        color='red',
        fill=True,
        fill_color='red',
        fill_opacity=0.2,
        popup=f"Radius: {radius} meters"
    ).add_to(map_object)

    return map_object





def query_records_using_gsi(client, table_name, index_name, geohash6_ranges):
    items = []
    i = 0
    try:
        # Iterate over the geohash6_ranges dictionary
        for gh6, gh8_range in geohash6_ranges.items():
            print(f"Query {i}")
            i += 1

            # Construct the key condition and expression attribute values
            key_condition = f"geohash6 = :pk_val AND SK BETWEEN :min_sk_val AND :max_sk_val"
            expr_attr_values = {
                ":pk_val": {"S": gh6},
                ":min_sk_val": {"S": gh8_range['min']},
                ":max_sk_val": {"S": gh8_range['max']}
            }

            # Perform the query
            response = client.query(
                TableName=table_name,
                IndexName=index_name,
                KeyConditionExpression=key_condition,
                ExpressionAttributeValues=expr_attr_values,
                ReturnConsumedCapacity='INDEXES'
            )

            # Extend the items list with the response items
            items.extend(response.get('Items', []))
            # Uncomment below if you want to print out query counts and capacities
            # print('Query Count', response['Count'], 'Total Scanned', response['ScannedCount'])
            # print(response['ConsumedCapacity'])

        return items
    except Exception as e:
        print(f"Error querying records: {e}")
        return None

def get_areas_by_plant_type(dynamodb_client, plant_type,table_name='IoT'):
    try:
        response = dynamodb_client.query(
            TableName=table_name,
            IndexName='GSI_Area_Plant',
            KeyConditionExpression='plant_type = :plant_type',
            ExpressionAttributeValues={
                ':plant_type': {'S': plant_type}
            },
            ReturnConsumedCapacity='INDEXES'
        )
        items = response['Items']
        map = create_map_with_polygon(polygon.exterior.coords)
        # Add points to the map
        i=0
        for item in items:
            # Extract the GeoJSON string
            geoJson_str = item.get('geoJson', {}).get('S', '')

            # Check if the string is not empty
            if geoJson_str:
                # Split the string by comma
                coords = geoJson_str.split(",")

                # Ensure that there are exactly two elements (longitude, latitude)
                if len(coords) == 2:
                    try:
                        # Convert strings to float
                        latitude, longitude = float(coords[0]), float(coords[1])
                        print("Longitude:", longitude, "Latitude:", latitude)
                        geohash = geohash2.encode(latitude, longitude, 8)
                        print(geohash)
                        add_geohash_to_map(geohash, map, "pink")
                    except ValueError:
                        print("Invalid coordinates:", coords)
                else:
                    print("Incorrect number of elements in coordinates:", coords)
            else:
                print("No GeoJSON data found")
            i+=1
        map.save('test_map.html')
        print(i)
        return items
    except ClientError as e:
        print(f"An error occurred: {e.response['Error']['Message']}")
        return None




def main():


    # Create the DynamoDB Client with the region you want
    dynamodb_client = create_dynamodb_client()
    get_areas_by_plant_type(dynamodb_client=dynamodb_client, plant_type='PLANT#Chickpeas', table_name='IoT')
    center_point = Point(28.1250063, 46.6334964)  # Point(28.135, 46.625)
    # radius = 120
    # items = query_plants_in_radius_within_polygon(
    #     dynamodb_client,
    #     'IoT',
    #     'GSI_Area_Plant',
    #     center_point, radius,
    #     polygon)
    # print(geohash2.encode(28.1250063, 46.6334964))

    # get_items(dynamodb_client)
    # sort_key_conditions = [
    #     {
    #     'condition': "SK BETWEEN :sk_start_val AND :sk_end_val",
    #     'values': {
    #         ":sk_start_val": {"S": "u8k10nq"},
    #         ":sk_end_val": {"S": "u8k10nu"}
    #     }
    #     },
    #     {
    #     'condition': "SK BETWEEN :sk_start_val AND :sk_end_val",
    #     'values': {
    #         ":sk_start_val": {"S": "u8k122u"},
    #         ":sk_end_val": {"S": "u8k122v"}
    #     }
    #     }
    # ]
    # query_records_using_gsi(client=dynamodb_client,
    #                         table_name='IoT',
    #                         index_name='GSI_Area_Plant',
    #                         partition_key_values=[{'key': 'geohash6', 'value': 'u8k10n'},
    #                                               {'key':'geohash6', 'value': 'u8k122'}],
    #                         sort_key_conditions=sort_key_conditions)


if __name__ == "__main__":
    main()
