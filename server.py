from kafka import KafkaConsumer
import socketio

# Configuração do Socket.IO
sio = socketio.Server(cors_allowed_origins='http://localhost:3000')

# Configuração do consumidor do Kafka
consumer = KafkaConsumer(
    'sensor_data',
    bootstrap_servers='localhost:9092',
    group_id='my_consumer_group'
)

@sio.event
def connect(sid, environ):
    print('Novo cliente conectado:', sid)

@sio.event
def disconnect(sid):
    print('Cliente desconectado:', sid)

# Função para enviar dados do consumidor via Socket.IO
def send_sensor_data():
    for message in consumer:
        data = message.value.decode('utf-8')
        sio.emit('sensor_data', data)

if __name__ == '__main__':
    app = socketio.WSGIApp(sio)
    send_sensor_data()  # Inicia o envio dos dados do consumidor via Socket.IO
    socketio.server(eventlet.listen(('', 3005)), app)
