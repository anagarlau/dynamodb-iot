from flask import Flask, request, jsonify

from models.SensorEvent import SensorEvent

app = Flask(__name__)


@app.route('/data', methods=['POST'])
def receive_sensor_event():
    data = request.json

    # Check if the request data is JSON
    if not data:
        return jsonify({"error": "Request must be JSON"}), 400

    # Validate the required keys
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

        # Add to DDB

        return jsonify({"message": "Sensor event received successfully", "sensor_event": sensor_event.to_json()}), 200
    except ValueError as e:
        # Handle specific value errors (like invalid enum values)
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        # Catch any other unexpected errors
        return jsonify({"error": "Internal server error", "message": f"{str(e)}"}), 500

if __name__ == '__main__':
    app.run(debug=True)