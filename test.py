from kafka.admin import KafkaAdminClient
from configs import kafka_config


admin = KafkaAdminClient(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password']

)
topic = admin.describe_topics(["rogalev_building_sensors"])
print(topic)

[print(topic) for topic in admin.list_topics() if "rogalev" in topic]