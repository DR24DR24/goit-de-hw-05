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
    group_id='rogalev_alerts'   # Ідентифікатор групи споживачів
)

# Назва топіку

topic_temperature = 'rogalev_temperature_alerts'
topic_humidity = 'rogalev_humidity_alerts'

# Підписка на тему
consumer.subscribe([topic_humidity,topic_temperature])

print(f"Subscribed to topic '{topic_humidity,topic_temperature}'")


# Обробка повідомлень з топіку
try:
    for message in consumer:
        
        print(f"Received message: {message.value} with key: {message.key}, partition {message.partition}")
        ts = message.value['timestamp']
        dt = datetime.datetime.fromtimestamp(ts)
        print(dt)
except KeyboardInterrupt:
    print("\nStopped by user.")
    
except Exception as e:
    print(f"An error occurred: {e}")



finally:
    consumer.close()  # Закриття consumer

