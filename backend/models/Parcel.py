import ast

from shapely import Polygon


class Parcel:
    def __init__(self, water_requirements_mm_per_week, optimal_humidity, optimal_soil_ph,
                 optimal_temperature, sunlight_requirements_hours_per_day, polygon, SK, details, PK, plant_type, is_active=None):
        self.water_requirements_mm_per_week = int(water_requirements_mm_per_week)
        self.optimal_humidity = tuple(map(float, optimal_humidity.strip("()").split(", ")))
        self.optimal_soil_ph = tuple(map(float, optimal_soil_ph.strip("()").split(", ")))
        self.optimal_temperature = tuple(map(float, optimal_temperature.strip("()").split(", ")))
        self.sunlight_requirements_hours_per_day = int(sunlight_requirements_hours_per_day)
        self.polygon = Polygon(ast.literal_eval(polygon))
        self.SK = SK
        self.details = details
        self.PK = PK
        self.plant_type = plant_type
        self.is_active = True if is_active else False

    def __str__(self):
        return (f"Parcel(ID: {self.SK}, Type: {self.plant_type}, "
                f"Polygon: {str(self.polygon)}, "
                f"Water Requirements: {self.water_requirements_mm_per_week} mm/week, "
                f"Optimal Temperature: {self.optimal_temperature} °C, "
                f"Optimal Humidity: {self.optimal_humidity}%, "
                f"Optimal Soil pH: {self.optimal_soil_ph}, "
                f"Sunlight Requirements: {self.sunlight_requirements_hours_per_day} hours/day, "
                f"Details: {self.details})")

    def __repr__(self):
        return self.__str__()