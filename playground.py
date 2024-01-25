from datetime import datetime

from dateutil.relativedelta import relativedelta

from utils.sensor_events.sensor_events_generation import unix_to_iso, calculate_month_diff, get_start_of_month, \
    convert_to_unix_epoch

# unix = 1577833200
# fromdate=1577985948
# todate=1578508596
#
# print(unix_to_iso(unix))
# print(unix_to_iso(fromdate), unix_to_iso(todate))
# print(convert_to_unix_epoch("2020-03-28T03:37:07"))
# print(unix_to_iso(1581289200))
# print(unix_to_iso(1595455200))

start_time = "2021-05-14T22:33:04"
end_time = "2021-05-20T10:30:00"
#print(calculate_month_diff(start_time, end_time))

def calculate_pks(start_date_str, end_date_str):
    start_date = datetime.strptime(get_start_of_month(start_date_str), "%Y-%m-%dT%H:%M:%S")
    end_date = datetime.strptime(get_start_of_month(end_date_str), "%Y-%m-%dT%H:%M:%S")

    if start_date == end_date:
        return [start_date.strftime("%Y-%m-%dT%H:%M:%S")]

    months = []
    current_date = start_date

    while current_date <= end_date:
        months.append(current_date.strftime("%Y-%m-%dT%H:%M:%S"))
        # Increment current_date
        current_date += relativedelta(months=1)
    return months

#print(calculate_pks(start_time, end_time))