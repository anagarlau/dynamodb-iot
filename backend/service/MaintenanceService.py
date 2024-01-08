from botocore.exceptions import BotoCoreError, ClientError

from backend.models.MaintenanceOperationByUser import MaintenanceOperation
from backend.models.SensorMaintenance import SensorMaintenance
from dynamodbgeo.dynamodbgeo import GeoDataManagerConfiguration, GeoDataManager
from utils.polygon_def import create_dynamodb_client, hashKeyLength
from utils.sensor_events.sensor_events_generation import convert_to_unix_epoch


class MaintenanceService:
    def __init__(self):
        self.dynamodb = create_dynamodb_client()
        self.config = GeoDataManagerConfiguration(self.dynamodb, 'IoT')
        self.config.hashKeyAttributeName = 'PK'
        self.config.rangeKeyAttributeName = 'SK'
        self.geoDataManager = GeoDataManager(self.config)
        self.config.hashKeyLength = hashKeyLength
        self.table_name = 'IoT'
        self.gsi_name = 'GSI_Users_Roles_Maintenance'

    def get_maintenance_operations_by_user(self, user_email, start_date=None, end_date=None):
        if (start_date is None) != (end_date is None):
            raise ValueError("Either both start_date and end_date must be provided or neither")
        params = {
            'TableName': self.table_name,
            'IndexName': 'GSI_Users_Roles_Maintenance',
            'KeyConditionExpression': '#pk = :userEmail',
            'ExpressionAttributeNames': {
                '#pk': 'GSI_PK'
            },
            'ExpressionAttributeValues': {
                ':userEmail': {'S': f"User#{user_email}"}
            },
            'ReturnConsumedCapacity': 'TOTAL'
        }
        # Optional time range filtering on SK
        if start_date and end_date:
            start_timestamp = convert_to_unix_epoch(start_date)
            end_timestamp = convert_to_unix_epoch(end_date)
            print(start_timestamp, end_timestamp)
            params['KeyConditionExpression'] += ' AND #sk BETWEEN :start AND :end'
            params['ExpressionAttributeNames']['#sk'] = 'GSI_SK'
            params['ExpressionAttributeValues'][':start'] = {'S': f"Maintenance#{start_timestamp}"}
            params['ExpressionAttributeValues'][':end'] = {'S': f"Maintenance#{end_timestamp}"}

        response = self.dynamodb.query(**params)
        items = response.get('Items', [])
        consumed_capacity_units = response.get('ConsumedCapacity', {}).get('CapacityUnits')
        if consumed_capacity_units is not None:
            print(f"Maintenance Operation by User {user_email}, Consumed Capacity Units: {consumed_capacity_units}")
        return [MaintenanceOperation(item) for item in items]

    def get_latest_n_maintenance_operations_for_sensor(self, sensor_id, n):
        response_items = []
        last_evaluated_key = None
        try:
            while True:
                response = self.dynamodb.query(
                    TableName=self.table_name,
                    KeyConditionExpression="PK = :pk and begins_with(SK, :sk)",
                    ExpressionAttributeValues={
                        ":pk": {'S': f"Sensor#{sensor_id}"},
                        ":sk": {'S': "Maintenance#"}
                    },
                    ScanIndexForward=False,
                    Limit=n,
                    #Pagination
                    **({'ExclusiveStartKey': last_evaluated_key} if last_evaluated_key else {}),
                    ReturnConsumedCapacity='TOTAL'
                )
                response_items.extend(response.get('Items', []))
                last_evaluated_key = response.get('LastEvaluatedKey')
                consumed_capacity = response.get('ConsumedCapacity', {}).get('CapacityUnits', 0)
                print(f'Sensor#Maintenance for ID {sensor_id}, Consumed Capacity Units: {consumed_capacity}')
                if len(response_items) >= n or not last_evaluated_key:
                    break
        except (BotoCoreError, ClientError) as error:
            print(f"An error occurred: {error}")
            return None
        return [SensorMaintenance(item) for item in response_items]

if __name__ == "__main__":
    maintenance_service = MaintenanceService()
    operations = maintenance_service.get_maintenance_operations_by_user("foxpaul@example.net")
    print(len(operations))
    operations = maintenance_service.get_maintenance_operations_by_user("foxpaul@example.net", "2020-02-10T00:00:00","2020-07-23T00:00:00")
    for op in operations:
        print(op)
    print(len(operations))
    maintenance_operation = maintenance_service.get_latest_n_maintenance_operations_for_sensor("1d835be7-5984-4604-8c84-9a99b59201bb", 5)
    print(len(maintenance_operation))