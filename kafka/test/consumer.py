import json
from kafka import KafkaConsumer
c = KafkaConsumer("streaming", bootstrap_servers="kafkainterface.servebeer.com:9092",
                  value_deserializer=lambda v: json.loads(v.decode()))
for msg in c:
    print(msg.key, msg.value)

