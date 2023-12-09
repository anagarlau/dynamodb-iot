from cmath import cos
from collections import defaultdict
from math import radians
import boto3
import geohash2
from botocore.exceptions import ClientError, BotoCoreError
from folium import folium, Marker, Polygon, Icon, CircleMarker, vector_layers, Circle
from geopy.distance import great_circle
from shapely import Point
import shapely

import geohash
from table_scripts import handle_error
from utils.geomapping import create_map_with_polygon, add_geohash_to_map, assign_geohashes_to_parcels, \
    get_from_max_precision, get_geohashes_from_polygon, draw_map_parcels_with_crop, calculate_geohash_area
from utils.polygon_def import polygon, coordinates


# TODO Extract into service as class
def create_dynamodb_client(local=True):
    # return boto3.resource("dynamodb", region_name="localhost", endpoint_url="http://localhost:8000",
    #                     aws_access_key_id="fakeMyKeyId", aws_secret_access_key="fakeSecretAccessKey`")
    return boto3.client("dynamodb", region_name="localhost", endpoint_url="http://localhost:8000",
                        aws_access_key_id="fakeMyKeyId", aws_secret_access_key="fakeSecretAccessKey`")


def get_items(dynamodb_client, plant_type='PLANT#Chickpeas'):
    try:
        response = dynamodb_client.query(
            ExpressionAttributeValues={
                ':pk': {
                    'S': plant_type,
                },
            },
            KeyConditionExpression='PK = :pk',
            # ProjectionExpression='SongTitle',
            TableName='IoT'
        )
        print("Successfully get item.")
        print(type(response))
        print(len(response))
        print(response.keys())
        print(len(response['Items']))  # data is a list here
        print(response['Count'], response['ScannedCount'])
        items = response['Items']
        map = create_map_with_polygon(polygon.exterior.coords)
        # Add points to the map
        for item in items:
            geohash = item.get('SK', {}).get('S', '')
            if geohash:
                add_geohash_to_map(geohash=geohash, map_obj=map, color='#ff00dd')
        map.save('test_map.html')
        return map
    except ClientError as e:
        handle_error(e)
    except Exception as e:
        handle_error(e)
    return None

 #inspired by https://stackoverflow.com/questions/36705355/finding-geohashes-of-certain-length-within-radius-from-a-point
 #mbr = min bounding rectangle
def calculate_mbr(center_point, radius, precision=8):
    lat, lon = center_point.y, center_point.x
    directions = {'north': 0, 'east': 90, 'south': 180, 'west': 270}
    points = []
    for direction in directions.values():
        point = great_circle(kilometers=radius / 1000).destination((lat, lon), direction)
        points.append((point.longitude, point.latitude))

    # Create a polygon from these points and compute its bounding box
    mbr = shapely.Polygon(points)
    print(mbr)
    circle = Point(lon, lat).buffer(radius / 111320)

    # Includes Filter geohashes out of bounds
    return get_geohashes_from_polygon(mbr, polygon, circle)


def calculate_adjacent_geohashes(center_point, radius, precision=8):
    geohashes = calculate_mbr(center_point, radius, precision)
    print(geohashes)
    print(len(geohashes))# 🥰 Geohashes outside of field have been successfully filtered out
    return geohashes


def filter_geohashes_by_polygon(geohashes, polygon=polygon):
    # Implement logic to filter geohashes by whether they intersect with the polygon
    # You can use libraries like Shapely for polygon-geohash intersection checks
    pass

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

def query_plants_in_radius_within_polygon(dynamodb_client, table_name, gsi_name, location_point, radius, polygon):
    # Calculate geohashes within the radius
    geohashes_in_radius = calculate_adjacent_geohashes(location_point, radius, precision=8)

    #visualize results
    filtered_geohashes = get_geohashes_from_polygon(polygon)
    refiltered = get_from_max_precision(higher_precision=7, geohashes_list=filtered_geohashes)
    dict = assign_geohashes_to_parcels(list_precision_7_parcels=list(refiltered),
                                       list_precision_8_parcels=list(filtered_geohashes))
    crop_map = draw_map_parcels_with_crop(list_precision_8_parcels=list(filtered_geohashes), crop_assignment=dict)
    m = visualize_geohashes(crop_map, location_point, radius, geohashes_in_radius)
    m.save("neighbours-test.html")
    # Query DynamoDB for each geohash and aggregate results
    #TODO
    # Prepare partition key values (unique geohash6) and sort key conditions (geohash8)
    geohash6_ranges = defaultdict(lambda: {'min': None, 'max': None})

    for gh8 in geohashes_in_radius:
        gh6 = gh8[:6]
        # Update the min and max geohash8 values for this geohash6
        if geohash6_ranges[gh6]['min'] is None or gh8 < geohash6_ranges[gh6]['min']:
            geohash6_ranges[gh6]['min'] = gh8
        if geohash6_ranges[gh6]['max'] is None or gh8 > geohash6_ranges[gh6]['max']:
            geohash6_ranges[gh6]['max'] = gh8
    print(geohash6_ranges)

    # Query the DynamoDB GSI
    items = query_records_using_gsi(dynamodb_client, table_name, gsi_name, geohash6_ranges)
    # Additional post-query filtering can be applied if needed
    geohashes_items = [obj['SK']['S'] for obj in items]
    print(geohashes_items)
    print(len(geohashes_items))
    m = visualize_geohashes(crop_map, location_point, radius, geohashes_items, 'darkblue')
    m.save("test-range.html")
    return items


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


def main():
    # Create the DynamoDB Client with the region you want
    dynamodb_client = create_dynamodb_client()
    center_point = Point(28.1250063, 46.6334964) #Point(28.135, 46.625)
    radius = 120
    items = query_plants_in_radius_within_polygon(
        dynamodb_client,
        'IoT',
        'GSI_Area_Plant',
        center_point, radius,
        polygon)
    print(geohash2.encode(28.1250063, 46.6334964))



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
