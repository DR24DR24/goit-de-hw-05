from kafka import KafkaConsumer
from kafka import KafkaProducer
from configs import kafka_config
import json
import datetime

# Створення Kafka Consumer
consumer = KafkaConsumer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',  # Зчитування повідомлень з початку
    enable_auto_commit=True,       # Автоматичне підтвердження зчитаних повідомлень
    group_id='rogalev'   # Ідентифікатор групи споживачів
)

# Назва топіку

topic_name = 'rogalev_building_sensors'

# Підписка на тему
consumer.subscribe([topic_name])

print(f"Subscribed to topic '{topic_name}'")



producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_temperature = 'rogalev_temperature_alerts'
topic_humidity = 'rogalev_humidity_alerts'
print(f" topic_temperature: {topic_temperature}")
print(f"topic_humidity: {topic_humidity}")




# Обробка повідомлень з топіку
try:
    for message in consumer:
        
        print(f"Received message: {message.value} with key: {message.key}, partition {message.partition} ")
        ts = message.value['timestamp']
        dt = datetime.datetime.fromtimestamp(ts)
        print(dt)  # 2025-11-29 15:42:10
        if message.value['sensorType']=='temperature':
            value = message.value['value']
            if value > 40:
                print(f"ALERT! High temperature detected: {value}°C")
                data = message.value.copy()   # ← важно: делаем копию!
                data["alert"] = f"ALERT! High temperature detected: {value}°C"
                producer.send(topic_temperature, value=data)
                print(f"Message  sent to topic '{topic_temperature}' successfully. data: {data}")
        
        elif message.value['sensorType']=='humidity':
            value = message.value['value']
            if value < 20 or value > 80:
                print(f"ALERT! Abnormal humidity level detected: {value}%")
                data = message.value.copy()   # ← важно: делаем копию!
                data["alert"] = f"ALERT! Abnormal humidity level detected: {value}%"
                producer.send(topic_humidity,  value=data)
                print(f"Message  sent to topic '{topic_humidity}' successfully. data: {data}")               
        
        producer.flush()  # Очікування, поки всі повідомлення будуть відправлені 
except KeyboardInterrupt:
    print("\nStopped by user.")
    
except Exception as e:
    print(f"An error occurred: {e}")



finally:
    consumer.close()  # Закриття consumer
    producer.close()  # Закриття producer
