from botocore.exceptions import BotoCoreError, ClientError

from backend.models.User import User
from dynamodbgeo import GeoDataManagerConfiguration, GeoDataManager
from utils.polygon_def import create_dynamodb_client, hashKeyLength

class UserService:
    def __init__(self):
        self.dynamodb = create_dynamodb_client()
        self.config = GeoDataManagerConfiguration(self.dynamodb, 'IoT')
        self.config.hashKeyAttributeName = 'PK'
        self.config.rangeKeyAttributeName = 'SK'
        self.geoDataManager = GeoDataManager(self.config)
        self.config.hashKeyLength = hashKeyLength
        self.table_name = 'IoT'
        self.gsi_name = 'GSI_Users_Roles_Maintenance'

    def get_users_by_role(self, role):
        try:

            response = self.dynamodb.query(
                TableName=self.table_name,
                IndexName='GSI_Users_Roles_Maintenance',
                KeyConditionExpression="GSI_PK = :role",
                ExpressionAttributeValues={":role": {'S': role}},
                ReturnConsumedCapacity='TOTAL'
            )
            consumed_capacity_units = response.get('ConsumedCapacity', {}).get('CapacityUnits')
            if consumed_capacity_units is not None:
                print(f"Users by role {role}, Consumed Capacity Units: {consumed_capacity_units}")
            users_items = response.get('Items')
            return [User(item) for item in users_items]
        except (BotoCoreError, ClientError) as error:
            print(f"An error occurred: {error}")
            return None

    def get_user_details(self, user_email):
        try:

            response = self.dynamodb.get_item(
                TableName=self.table_name,
                Key={
                    'PK': {'S': f'User#{user_email}'},
                    'SK': {'S': f'User#{user_email}'}
                },
                ReturnConsumedCapacity='TOTAL'
            )
            consumed_capacity_units = response.get('ConsumedCapacity', {}).get('CapacityUnits')
            if consumed_capacity_units is not None:
                print(f"User details for e-mail {user_email}, Consumed Capacity Units: {consumed_capacity_units}")
            user_item = response.get('Item')
            return User(user_item) if user_item else None
        except (BotoCoreError, ClientError) as error:
            print(f"An error occurred: {error}")
            return None



if __name__ == "__main__":
    user_service = UserService()
    user_details = user_service.get_user_details("jennifer10@example.net")
    print(user_details)
    users_by_role = user_service.get_users_by_role("Admin")
    print(len(users_by_role))
