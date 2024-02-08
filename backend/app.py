from flask import Flask, request, jsonify

from backend.models.SensorEvent import SensorEvent
from backend.service.SensorEventService import SensorEventService

app = Flask(__name__)

# Sample endpoint for persisting sensor events
# Please use the json format provided in maps/data/sample-sensor-event.json for testing
@app.route('/data', methods=['POST'])
def receive_sensor_event():
    data = request.json
    if not data:
        return jsonify({"error": "Request must be JSON"}), 400
    required_keys = ['sensorId', 'metadata', 'data']
    missing_keys = [key for key in required_keys if key not in data]
    if missing_keys:
        return jsonify({"error": f"Missing keys in request data: {', '.join(missing_keys)}"}), 400

    try:
        sensor_event = SensorEvent(
            sensorId=data['sensorId'],
            metadata=data['metadata'],
            data=data['data']
        )

        service = SensorEventService()
        service.add_sensor_event(sensor_event)
        return jsonify({"message": "Sensor event received successfully", "sensor_event": sensor_event.to_json()}), 200
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": "Internal server error", "message": f"{str(e)}"}), 500

if __name__ == '__main__':
    app.run(debug=True)