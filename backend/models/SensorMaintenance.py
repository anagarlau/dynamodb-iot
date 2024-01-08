class SensorMaintenance:
    def __init__(self, response_item):
        self.sensor_id = response_item['PK']['S'].split("#")[1]
        self.maintenance_sk = response_item['SK']['S']
        self.maintenance_type = response_item['maintenance_type']['S']
        self.details = response_item['details']['S']
        self.performed_by = response_item['GSI_PK']['S']
        self.gsi_sk=response_item['GSI_SK']['S']

    def __repr__(self):
        return (f"SensorMaintenance(SensorID={self.sensor_id}, MaintenanceSK={self.maintenance_sk}, "
                f"Type={self.maintenance_type}, Details={self.details}, PerformedBy={self.performed_by})")
