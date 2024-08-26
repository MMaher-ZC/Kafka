from confluent_kafka import Consumer, KafkaException, KafkaError,Producer
import sys


# Group ID
me='Mohamed_Maher-1'
group_id = me +'-Upload'

#producer
conf = {'bootstrap.servers': '34.138.205.183:9094,34.138.104.233:9094,34.138.118.154:9094',
        'client.id': me}
producer = Producer(conf)
# Configuration consumer
conf = {
    'bootstrap.servers': '34.138.205.183:9094,34.138.104.233:9094,34.138.118.154:9094',
    'enable.auto.commit':True,
    'group.id': group_id,                    
    'auto.offset.reset': 'latest'          
}
import random
def detect_object(id):
    return random.choice(['car', 'house', 'person'])
import requests
# Create Consumer 
consumer = Consumer(conf)
success_topic='Mohamed_Maher_success'
topics=[me]
consumer.subscribe(topics)


try:
    while True:
        # Poll for new messages
        msg = consumer.poll(timeout=1.0)  # Timeout in seconds

        if msg is None:
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f"Reached end of partition {msg.partition()}")
            else:
                raise KafkaException(msg.error())
        else:
            import requests
            message_value = msg.value().decode('utf-8').split('.')[0]
            
            requests.put('http://127.0.0.1:5000/object/' + message_value, json={"object": detect_object(id)})
            producer.produce(success_topic, key=None, value="task_completed")
            print(f"Consumer group {group_id}: Received message: {message_value}")
            


except KeyboardInterrupt:
    print("Consumer interrupted")

finally:
    consumer.close()
