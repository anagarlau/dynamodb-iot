from botocore.exceptions import BotoCoreError, ClientError

from backend.models.MaintenanceEnum import MaintenanceType
from backend.models.MaintenanceOperationByUser import MaintenanceOperation
from backend.models.SensorDetails import SensorDetails
from backend.models.SensorMaintenance import SensorMaintenance, MaintenanceDetails
from dynamodbgeo import GeoDataManagerConfiguration, GeoDataManager
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

    def get_sensors_scheduled_or_in_maintenance(self, scheduled=False, assigned_to=None, from_date=None, to_date=None):
        response_items = []
        consumed_capacity=0
        params = {
            "TableName": self.table_name,
            "IndexName": 'GSI_Users_Roles_Maintenance',
            "KeyConditionExpression": "GSI_PK = :pk",
            "ExpressionAttributeValues": {
                ":pk": {'S': "Maintenance" if not scheduled else "PlannedMaintenance"}
            },
            "ReturnConsumedCapacity": 'TOTAL'
        }
        if assigned_to is not None and scheduled:
            params["KeyConditionExpression"] += " AND begins_with(GSI_SK, :assigned_to)"
            params["ExpressionAttributeValues"][":assigned_to"] = {'S': f"User#{assigned_to}"}
        if not scheduled and from_date and to_date:
            params["KeyConditionExpression"] += " AND GSI_SK BETWEEN :from_date AND :to_date"
            params["ExpressionAttributeValues"][":from_date"] = {'S': f"Maintenance#{convert_to_unix_epoch(from_date)}"}
            params["ExpressionAttributeValues"][":to_date"] = {'S': f"Maintenance#{convert_to_unix_epoch(to_date)}"}
        try:
            while True:
                response = self.dynamodb.query(**params)
                response_items.extend(response.get('Items', []))
                consumed_capacity += response.get('ConsumedCapacity', {}).get('CapacityUnits', 0)
                if 'LastEvaluatedKey' not in response:
                    break
                params['ExclusiveStartKey'] = response['LastEvaluatedKey']
            print(f'Sensors in Maintenance: {len(response_items)}, Consumed Capacity Units: {consumed_capacity}')
            return [SensorDetails(item) for item in response_items]
        except (BotoCoreError, ClientError, Exception) as error:
            print(f"An error occurred: {error}")
            return response_items



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
        consumed_capacity = 0
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
                    # Pagination
                    **({'ExclusiveStartKey': last_evaluated_key} if last_evaluated_key else {}),
                    ReturnConsumedCapacity='TOTAL'
                )
                response_items.extend(response.get('Items', []))
                last_evaluated_key = response.get('LastEvaluatedKey')
                consumed_capacity += response.get('ConsumedCapacity', {}).get('CapacityUnits', 0)
                if len(response_items) >= n or not last_evaluated_key:
                    break
            print(f'Sensor#Maintenance for ID {sensor_id}, Consumed Capacity Units: {consumed_capacity}')
        except (BotoCoreError, ClientError) as error:
            print(f"An error occurred: {error}")
            return None
        return [SensorMaintenance(item) for item in response_items]

    def put_sensor_into_maintenance(self, maintenance_details):
        try:
            from backend.service.SensorService import SensorService
            sensor_service = SensorService()
            sensor_details = sensor_service.get_sensor_details_by_id(maintenance_details.sensor_id)
            updated_metadata_record = maintenance_details.generate_updated_metadata_record(self.table_name)
            new_maintenance_record = maintenance_details.generate_new_maintenance_record(self.table_name, sensor_details)
            transact_items = [updated_metadata_record, new_maintenance_record]
            self.dynamodb.transact_write_items(TransactItems=transact_items)
            print(f"Added maintenance operation for {maintenance_details.SK} ")
            return True
        except (ClientError, BotoCoreError, Exception) as e:
            print(f"Error adding maintenance operation: {e}")
            return False

    def conclude_maintenance_operation(self, sensor_id, sensor_type):

        try:
            latest_maintenance_operation = self.get_latest_n_maintenance_operations_for_sensor(sensor_id, 1)
            if len(latest_maintenance_operation) > 0:
                updated_metadata_record = latest_maintenance_operation[0].generate_updated_metadata_record(self.table_name, sensor_type)
                updated_maintenance_record = latest_maintenance_operation[0].generate_updated_maintenance_record(self.table_name)
                transact_items = [updated_metadata_record, updated_maintenance_record]
                self.dynamodb.transact_write_items(TransactItems=transact_items)
                print(f"Concluded maintenance operation for {sensor_id} ")
                return True
            else:
                return False
        except (ClientError, BotoCoreError, Exception) as e:
            print(f"Error concluding maintenance operation: {e}")

    def schedule_sensor_maintenance(self, sensor_id, sensor_type, user_email):
        try:
            assigned_operation = SensorMaintenance.generate_scheduled_maintenance_record(self.table_name,
                                                                                       sensor_id, sensor_type,
                                                                                       user_email)
            transact_items = [assigned_operation]
            self.dynamodb.transact_write_items(TransactItems=transact_items)
            print(f"Scheduled maintenance operation for {sensor_id}, assigned to {user_email} ")
        except (ClientError, BotoCoreError, Exception) as e:
            print(f"Error concluding maintenance operation: {e}")
            return False




if __name__ == "__main__":
    maintenance_service = MaintenanceService()
    maintenance_operations = maintenance_service.get_latest_n_maintenance_operations_for_sensor("69fd190a-8bd4-450c-8161-263b4212dcbe", 5)
    print(len(maintenance_operations))
    # maintenance_service.schedule_sensor_maintenance("69fd190a-8bd4-450c-8161-263b4212dcbe",
    #                                                 "Light",
    #                                                 "foxpaul@example.net")
    # maintenance_service.put_sensor_into_maintenance(
    #     MaintenanceDetails("a7b33a26-31fc-408a-8d18-f26c6cd87119",
    #                        "Light",
    #                        "foxpaul@example.net",
    #                        "2024-03-19T00:00:00",
    #                        MaintenanceType.get_random().value))

    #maintenance_service.conclude_maintenance_operation("69fd190a-8bd4-450c-8161-263b4212dcbe", "Light")
    # print(maintenance_operation)
    operations = maintenance_service.get_maintenance_operations_by_user("carlyjones@example.com")
    print(len(operations))
    sensors_in_maintenance=maintenance_service.get_sensors_scheduled_or_in_maintenance()
    print(sensors_in_maintenance)
    #operations = maintenance_service.get_maintenance_operations_by_user("foxpaul@example.net", "2024-02-10T00:00:00","2024-07-23T00:00:00")
    # for op in operations:
    #     print(op)
    # print(len(operations))



