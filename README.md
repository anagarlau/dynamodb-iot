# DynamoDB IoT Prototype in Single-Table-Design

This project illustrates a small SSCM application using DynamoDB Single-Table-Design for the data layer.

## Branches

The project contains two branches:
- `main`: Implementation of the prototype.
- `cherry-picks`: Forked and modified version of `dynamodbgeo` library. See [dynamodbgeo on PyPI](https://pypi.org/project/dynamodbgeo/).

## Requirements

- NoSQL Workbench
- Installed dependencies in the main project `requirements.txt` (Run `pip install -r requirements.txt`)
- Installed adjusted `dynamodbgeo` in folder `dynamodbgeo` (Run `pip install -e .\dynamodbgeo\`)

## Data Generation

- Geodata is based on the polygon definition in `utils/polygon_def.py` where the parameters for the local setup of the DynamoDB client can also be found. 
- The script `data_generation.py` allows the creation of a new batch of data using a variety of methods from the `utils` folder, depending on the entity type.
- Generated data is saved to CSV and JSON files (used for batch-writing to DynamoDB) in the `maps/data` folder.
- Maps were created using the folium library and can be found in `maps` in HTML format.
- An overview of inserted sensor events can be found in `sensor_events_analytics.xlsx` (updated in every data generation round).
- `sensor_events_analytics.xlsx` allows filtering of events and testing for time ranges in human-readable form as timestamps are saved in Unix format to DynamoDB.

## Data Insertion

- Pre-existing data (in `maps/data`) can be inserted using the script in the IoTInitService class (`control_plane/IoTInitService.py`).  
- The script creates the IoT table and 6 GSIs.
- Maintenance entries are randomly created in the IoTInitService. Entries for sensors currently in Maintenance or PlannedMaintenance are not included and have to be inserted manually using `backend/service/MaintenanceService.py`.
- Aggregate data can be inserted using the mock Worker in `backend/service/WorkerService.py`.
- `backend/service/vis_out/sensorservice` contains visual output of methods in SensorService.py in the form of folium maps

## Notes on AI Support

Splitting the fields into subpolygons (parcels) was achieved with ChatGPT support starting from the author's idea of splitting based on set points and then rotating according to a calculated bearing. See `parcels_generation.py` and [Google Maps](https://www.google.com/maps/d/edit?mid=1zJrRQ74tlJzs8GSxvTNt5PcW9Bfs-nM&usp=sharing).

ChatGPT supported with trigonometry skills the author unfortunately lacks. ChatGPT also supported the efforts to create a weighted sensor grid in `sensor_placing_generation.py`. The relevant methods are marked as such.
