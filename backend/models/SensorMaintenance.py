from datetime import datetime

from utils.sensor_events.sensor_events_generation import convert_to_unix_epoch


class SensorMaintenance:
    def __init__(self, response_item):
        self.sensor_id = response_item['PK']['S'].split("#")[1]
        self.maintenance_sk = response_item['SK']['S']
        self.maintenance_type = response_item['maintenance_type']['S']
        self.details = response_item['details']['S']
        self.performed_by = response_item['GSI_PK']['S']
        self.gsi_sk=response_item['GSI_SK']['S']
        self.concluded_on = response_item['concluded_on']['S'] if 'concluded_on' in response_item.keys() else None

    def __repr__(self):
        return (f"SensorMaintenance(SensorID={self.sensor_id}, MaintenanceSK={self.maintenance_sk}, "
                f"Type={self.maintenance_type}, Details={self.details}, PerformedBy={self.performed_by})")

    def generate_updated_metadata_record(self, table_name, sensor_type):
        return {
            'Update': {
                'TableName': table_name,
                'Key': {
                    'PK': {'S': f"Sensor#{self.sensor_id}"},
                    'SK': {'S': f'Metadata#{sensor_type}#{self.sensor_id}'}
                },
                'UpdateExpression': 'REMOVE GSI_PK, GSI_SK'
            }
        }

    def generate_updated_maintenance_record(self, table_name):
        return {
            'Update': {
                'TableName': table_name,
                'Key': {
                    'PK': {'S': f"Sensor#{self.sensor_id}"},
                    'SK': {'S': self.maintenance_sk}
                },
                'UpdateExpression': 'SET concluded_on = :concluded_on',
                'ExpressionAttributeValues': {
                    ':concluded_on': {'S': datetime.now().strftime("%Y-%m-%dT%H:%M:%S")}
                }
            }
        }

class MaintenanceDetails:
    def __init__(self, sensor_id, sensor_type, user_email, start_timestamp, maintenance_type, concluded_at = None):
        self.PK = f"Sensor#{sensor_id}"
        self.sensor_id = sensor_id
        self.SK = f"Maintenance#{convert_to_unix_epoch(start_timestamp)}"
        self.maintenance_type = maintenance_type
        self.details = 'Maintenance details'
        self.GSI_PK = f"User#{user_email}"
        self.GSI_SK = self.SK
        self.concluded_on = concluded_at
        self.sensor_type = sensor_type


    def generate_updated_metadata_record(self, table_name):
        return {
            'Update': {
                'TableName': table_name,
                'Key': {
                    'PK': {'S': self.PK},
                    'SK': {'S': f'Metadata#{self.sensor_type}#{self.sensor_id}'}
                },
                'UpdateExpression': 'SET GSI_PK = :gsi_pk, GSI_SK = :gsi_sk',
                'ExpressionAttributeValues': {
                    ':gsi_pk': {'S': 'Maintenance'},
                    ':gsi_sk': {'S': self.PK}
                }
            }
        }


    def generate_new_maintenance_record(self, table_name):
        return {
            'Put': {
                'TableName': table_name,
                'Item': {
                    'PK': {'S': self.PK},
                    'SK': {'S': self.SK},
                    'maintenance_type': {'S': self.maintenance_type},
                    'details': {'S': self.details},
                    'GSI_PK': {'S': self.GSI_PK},
                    'GSI_SK': {'S': self.GSI_SK}
                }
            }
        }

    def __repr__(self):
        return f"Maintenance Details: PK={self.PK}, SK={self.SK}, maintenance_type={self.maintenance_type}, details={self.details}, GSI_PK={self.GSI_PK}, GSI_SK={self.GSI_SK}, concluded_on={self.concluded_on}"
