# Sensor
from confluent_kafka import Producer
import random
import time

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def generate_sensor_data(producer):
    sensor_types = ['Air Humidity', 'Temperature', 'Soil Moisture', 'Water Flow']
    while True:
        sensor_type = random.choice(sensor_types)
        sensor_value = random.uniform(0, 100)
        message = f'Sensor: {sensor_type}, Value: {sensor_value}'
        producer.produce('sensor_data', message.encode('utf-8'), callback=delivery_report)
        producer.poll(0)  # Libere o buffer do produtor
        time.sleep(1)  # Aguarde 1 segundo antes de gerar a próxima mensagem

    producer.flush()  # Aguarde a entrega de todas as mensagens

if __name__ == '__main__':
    producer = Producer({'bootstrap.servers': 'localhost:9092'})
    generate_sensor_data(producer)
