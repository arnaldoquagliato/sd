from flask import Flask, jsonify, Response
from flask_cors import CORS, cross_origin
from subprocess import Popen
import atexit
app = Flask(__name__)
CORS(app)
producer_process = None
consumer_process = None
sensor_data = []



@app.route("/")
@cross_origin()
def helloWorld():
  return "Hello, cross-origin-world!"

@app.route('/start-generating', methods=['GET'])
def start_generating():
    global producer_process
    if producer_process is None or producer_process.poll() is not None:
        producer_process = Popen(['python', 'producer.py'])
        return jsonify({'status': 'success', 'message': 'Data generation started'})
    else:
        return jsonify({'status': 'error', 'message': 'Data generation already in progress'})

@app.route('/stop-generating', methods=['GET'])
def stop_generating():
    global producer_process
    if producer_process is not None and producer_process.poll() is None:
        producer_process.terminate()
        producer_process.wait()
        return jsonify({'status': 'success', 'message': 'Data generation stopped'})
    else:
        return jsonify({'status': 'error', 'message': 'No data generation in progress'})

@app.route('/consume-sensor-data', methods=['GET'])
def consume_sensor_data():
    global consumer_process
    if consumer_process is None or consumer_process.poll() is not None:
        consumer_process = Popen(['python', 'consumer.py'])
        print(consumer_process)
        atexit.register(consumer_process.terminate)
        return jsonify({'status': 'success', 'message': 'Sensor data consumption started'})
    else:
        return jsonify({'status': 'error', 'message': 'Sensor data consumption already in progress'})

@app.route('/sensor-data', methods=['GET'])
def get_sensor_data():
    return jsonify(sensor_data)

@app.route('/sensor-data-stream')
def sensor_data_stream():
    def generate():
        for data in sensor_data:
            yield f'data: {data}\n\n'
    print("dddd",Response(generate(), mimetype='application/json'))
    return Response(generate(), mimetype='application/json')

def update_sensor_data(msg):
    sensor_data.append(msg.value().decode("utf-8"))

if __name__ == '__main__':
    app.run(debug=True)


