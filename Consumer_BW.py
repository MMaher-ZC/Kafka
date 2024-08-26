from confluent_kafka import Consumer, KafkaException, KafkaError,Producer
import sys
from PIL import Image
import os
# Group ID
me='Mohamed_Maher-1'
group_id = me +'-BW'

#Producer
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

def process_image(file_path):
    with Image.open(file_path) as img:
        #direct=file_path.split('//'[0])
        #exten=file_path.split('//')[1].split('.')[-1]
        #file_name=file_path.split('.')[0]
        bw_img = img.convert('L')  # Convert to black and white
        bw_img.save(file_path)  # Overwrite the image with the black and white version
        print(f"Processed and saved black and white image: {file_path}")



import requests
# Create Consumer 
consumer = Consumer(conf)
topics=[me]
consumer.subscribe(topics)
success_topic='Mohamed_Maher_success'


try:
    while True:
        # Poll for new messages
        msg = consumer.poll(timeout=3.0)  # Timeout in seconds

        if msg is None:
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f"Reached end of partition {msg.partition()}")
            else:
                raise KafkaException(msg.error())
        else:
            file_path = msg.value().decode('utf-8')
            if os.path.exists('images/'+file_path):
                
                process_image('images/'+file_path)
                producer.produce(success_topic, key=None, value="task_completed")
            else:
                print(f"File not found: {file_path}")

except KeyboardInterrupt:
    print("Consumer interrupted")

finally:
    consumer.close()
