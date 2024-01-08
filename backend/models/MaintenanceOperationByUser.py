class MaintenanceOperation:
    def __init__(self, item):
        self.pk = item['PK']['S'].split("#")[1]
        self.sk = item['SK']['S']
        self.maintenance_type = item['maintenance_type']['S']
        self.details = item['details']['S']
        self.gsi_pk = item['GSI_PK']['S']
        self.gsi_sk = item['GSI_SK']['S']

    def __repr__(self):
        return f"MaintenanceOperation(pk={self.pk}, sk={self.sk}, maintenance_type={self.maintenance_type}, details={self.details}, gsi_pk={self.gsi_pk}, gsi_sk={self.gsi_sk})"
