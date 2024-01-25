import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, time

import numpy as np

from backend.models.SensorEvent import DataType
from backend.service.SensorEventService import SensorEventService


class WorkerService:
    def __init__(self):
        self.data_service=SensorEventService()

    #add aggregations to table
    def add_field_aggregates_to_table(self, date: datetime = None):
        if not date:
            date = datetime.now()
        start_of_day = datetime.combine(date, time.min).strftime("%Y-%m-%dT%H:%M:%S")
        end_of_day = datetime.combine(date, time.max).strftime("%Y-%m-%dT%H:%M:%S")
        print(start_of_day, end_of_day)
        data_types = [member.value for member in DataType]
        print(data_types)
        results = {}
        with ThreadPoolExecutor() as executor:
            futures = {
                data_type: executor.submit(
                    self.data_service.query_sensor_events_for_field_in_time_range_by_type,
                    start_of_day,
                    end_of_day,
                    data_type
                ) for data_type in data_types
            }
            for data_type, future in futures.items():
                try:
                    results[data_type] = future.result()
                    print(f'Data for {data_type}:', results[data_type])
                except Exception as e:
                    print(f'Exception occurred for {data_type}: {e}')
        for data_type, result in results.items():
            if result and data_type != DataType.RAIN.value:
                result_array = [obj.data.dataPoint for obj in result]
                print(result_array)
                avg_value = np.mean(result_array)
                min_value = np.min(result_array)
                max_value = np.max(result_array)
                median = np.median(result_array)
                print(f'Data for {data_type}: Avg = {avg_value}, Min = {min_value}, Max = {max_value}, Median = {median}')
            else:
                print(f'Data for {data_type}: No data available')


    #retrieve aggregations from table

def main():
    worker_service = WorkerService()
    worker_service.add_field_aggregates_to_table(datetime(2020, 3, 14))


if __name__ == '__main__':
    main()