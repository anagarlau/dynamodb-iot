from enum import Enum
from random import  choice


class MaintenanceType(Enum):
    REGULAR_CHECKUP = "RegularCheckup"
    CALIBRATION = "Calibration"
    SOFTWARE_UPDATE = "SoftwareUpdate"
    BATTERY_REPLACEMENT = "BatteryReplacement"
    REPAIR = "Repair"
    EMERGENCY_RESPONSE = "EmergencyResponse"


    @staticmethod
    def get_random():
        return choice(list(MaintenanceType))


#random_maintenance_type = MaintenanceType.get_random()
#print(random_maintenance_type.value)