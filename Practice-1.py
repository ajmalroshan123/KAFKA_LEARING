from kafka import KafkaProducer
import json
from datatime import datatime

#Mention server and topic
bootstrap_servers = ''
kafka_topic = ''

#create Kafka producer instances
producer = KafkaProducer(
    bootstrap_servers = bootstrap_servers,
    value_serializer = lamda x: json.dumps(v).encode('utf-8')
)

#Produce Messages to the topic
for i in range(10):
    message_data = {
        'key':f'key_{i}',
        'value':f'This is the Message{i}'
    }
    producer.send(kafka_topic,key=str(i),value=message_data) 

producer.flush()
producer.close()