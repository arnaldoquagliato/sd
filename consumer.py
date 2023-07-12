from confluent_kafka import Consumer

# ['Air Humidity', 'Temperature', 'Soil Moisture', 'Water Flow']

data_dict = {
    'Air Humidity': {
        'Minimum': 0,
        'Maximum': 0,
        'Average': 0,
        'Array': [],
    }, 
    'Temperature': {
        'Minimum': 0,
        'Maximum': 0,
        'Average': 0,
        'Array': [],
    }, 
    'Soil Moisture': {
        'Minimum': 0,
        'Maximum': 0,
        'Average': 0,
        'Array': [],
    }, 
    'Water Flow': {
        'Minimum': 0,
        'Maximum': 0,
        'Array': [],
        'Average': 0
    }, 
}

def update_values(value, key):
    if(data_dict[key]["Maximum"] == 0): 
        data_dict[key]["Maximum"] = value
    if(data_dict[key]["Minimum"] == 0): 
        data_dict[key]["Minimum"] = value
    
    if value > data_dict[key]["Maximum"]:
        data_dict[key]["Maximum"]
    if value < data_dict[key]["Minimum"]:
        data_dict[key]["Minimum"]

    data_dict[key]["Array"].append(value)
    data_dict[key]["Average"] = sum(data_dict[key]["Array"])/len(data_dict[key]["Array"])

    
def process_sensor_data(sensor_data):
    print("o que esta chegnado", sensor_data)

    value = float(sensor_data.split(': ')[-1])
    sensor_type = ""

    if 'Air Humidity' in sensor_data:
        sensor_type = 'Air Humidity'
    elif 'Temperature' in sensor_data:
        sensor_type = 'Temperature'
    elif 'Soil Moisture' in sensor_data:
        sensor_type = 'Soil Moisture'
    elif 'Water Flow' in sensor_data:
        sensor_type = 'Water Flow'

    update_values(value, sensor_type)
    
def consume_sensor_data():
    consumer = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'sensor_consumer',
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe(['sensor_data'])

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f'Consumer error: {msg.error()}')
                continue

            print(f'Received sensor data: {msg.value().decode("utf-8")}')
            process_sensor_data(msg.value().decode("utf-8"))
            print(f'Processed data: {data_dict}')
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == '__main__':
    consume_sensor_data()
