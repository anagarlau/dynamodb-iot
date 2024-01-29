import json
import random
from enum import Enum

from faker import Faker

from utils.polygon_def import get_project_path

# Filepath for generated user file
jsonFilepath = f"{get_project_path()}\\maps\\data\\user_and_roles.json"
# Role Enum
class UserRole(Enum):
#    MANAGER = "Manager"
    AGRONOMIST = "Agronomist"
    DATA_ANALYST = "Data Analyst"
#    FIELD_TECHNICIAN = "Field Technician"
    ADMIN = "Admin"


# Method for User and role Generation
def generate_user_data(file_name=jsonFilepath):
    fake = Faker()
    users = []
    for _ in range(20):
        email=fake.email()
        PK = f"User#{email}"
        user = {
        "PK": PK,
        "SK": PK,
        "last_name": fake.last_name(),
        "first_name": fake.first_name(),
        "GSI_PK": random.choice(list(UserRole)).value,
        "GSI_SK": PK,
        "date_of_birth": fake.date_of_birth(minimum_age=25, maximum_age=70).strftime("%Y-%m-%d"),
        "contact": email
    }
        users.append(user)
    with open(file_name, 'w') as file:
        json.dump(users, file, indent=4)

def read_users_from_json(file_name=jsonFilepath):
    try:
        with open(file_name, 'r') as file:
            return json.load(file)
    except FileNotFoundError:
        print(f"The file {file_name} was not found.")
        return []
    except json.JSONDecodeError:
        print(f"Error decoding JSON from {file_name}.")
        return []

