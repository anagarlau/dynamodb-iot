from calendar import monthrange
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, time
from typing import Tuple, Optional

import pandas as pd
from botocore.exceptions import ClientError, BotoCoreError

from backend.models.SensorEvent import DataType
from backend.service.SensorEventService import SensorEventService
from utils.sensor_events.sensor_events_generation import convert_to_unix_epoch


class WorkerService:
    def __init__(self):
        self.data_service=SensorEventService()

    # Add aggregations to table
    def calculate_aggregates_per_period(self, date: datetime = None, month_year: Optional[Tuple[int, int]] = None):
        try:
            if date is not None and month_year is not None:
                raise ValueError("Please provide either a date or a month_year tuple, not both.")
            elif date:
                start_date = datetime.combine(date, time.min).strftime("%Y-%m-%dT%H:%M:%S")
                end_date = datetime.combine(date, time.max).strftime("%Y-%m-%dT%H:%M:%S")
                prefix = 'Day'
            elif month_year:
                month, year = month_year
                start_date = datetime(year, month, 1).strftime("%Y-%m-%dT%H:%M:%S")
                end_date = datetime(year, month, monthrange(year, month)[1]).strftime("%Y-%m-%dT%H:%M:%S")
                prefix = 'Month'
            else:
                yesterday = datetime.now() - timedelta(days=1)
                start_date = datetime.combine(yesterday, time.min).strftime("%Y-%m-%dT%H:%M:%S")
                end_date = datetime.combine(yesterday, time.max).strftime("%Y-%m-%dT%H:%M:%S")
                prefix = 'Day'

            data_types = [member.value for member in DataType]
            results = {}
            with ThreadPoolExecutor() as executor:
                futures = {
                    data_type: executor.submit(
                        self.data_service.query_sensor_events_for_field_in_time_range_by_type,
                        start_date,
                        end_date,
                        data_type
                    ) for data_type in data_types
                }
            for data_type, future in futures.items():
                try:
                    results[data_type] = future.result()
                except Exception as e:
                    print(f'Exception occurred for {data_type}: {e}')
            all_data = pd.DataFrame()
            for data_type, result in results.items():
                if not result:
                    continue
                if result and data_type != DataType.RAIN.value:
                    data = [{'data_type': obj.data.dataType, 'parcel_id': obj.metadata.parcel_id, 'PK': obj.PK,
                             'data': obj.data.dataPoint} for obj in result]
                    df = pd.DataFrame(data)
                    all_data = pd.concat([all_data, df])
            if all_data.empty:
                print(f"No data found for date: {start_date}")
                return False
            aggregated_for_field = all_data.groupby('PK')['data'].agg(['mean', 'min', 'max', 'median'])
            aggregated_by_parcel = all_data.groupby(['PK', 'parcel_id'])['data'].agg(['mean', 'min', 'max', 'median'])
            transact_items = []
            for pk, agg_data in aggregated_for_field.iterrows():
                   parcel_agg = aggregated_by_parcel.xs(pk, level='PK').to_dict('index')
                   item = {
                        'PK': {'S': pk},
                        'SK': {'S': f'Agg#{prefix}#{convert_to_unix_epoch(start_date)}'},
                        'mean': {'N': str(agg_data['mean'])},
                        'min': {'N': str(agg_data['min'])},
                        'max': {'N': str(agg_data['max'])},
                        'median': {'N': str(agg_data['median'])},
                        'parcel_agg': {'M': {parcel_id:
                                                 {'M': {key: {'N': str(val)} for key, val in parcel_stats.items()}}
                                        for parcel_id, parcel_stats in parcel_agg.items()}}
                   }
                   transact_items.append({'Put': {'TableName': self.data_service.table_name, 'Item': item}})
            self.data_service.add_records(transact_items)
            return True
        except (ClientError, BotoCoreError, Exception) as e:
            print(f"An error occurred: {e}")


def main():
    worker_service = WorkerService()
    worker_service.calculate_aggregates_per_period(date=datetime(2020, 3, 19)) #month_year=(3, 2020)


if __name__ == '__main__':
    main()