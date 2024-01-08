class User:
    def __init__(self, item):
        self.pk = item['PK']['S']
        self.sk = item['SK']['S']
        self.role = item['GSI_PK']['S']
        self.gsi_sk = item['GSI_SK']['S']
        self.date_of_birth = item['date_of_birth']['S']
        self.contact = item['contact']['S']
        self.last_name = item['last_name']['S']
        self.first_name = item['first_name']['S']

    def __repr__(self):
        return (f"User(pk={self.pk}, sk={self.sk}, role={self.role}, gsi_sk={self.gsi_sk}, "
                f"date_of_birth={self.date_of_birth}, contact={self.contact}, "
                f"last_name={self.last_name}, first_name={self.first_name})")
