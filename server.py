import os
import random
import sqlite3
import sys
import uuid
from confluent_kafka import Producer,Consumer
import socket
from threading import Thread


from flask import (Flask, redirect, render_template_string, request,
                   send_from_directory)
from flask_socketio import SocketIO, emit


IMAGES_DIR = "images"
MAIN_DB = "main.db"
error_topic='Mohamed_Maher_error'
success_topic='Mohamed_Maher_success'

me1 = 'Mohamed_Maher-1'
conf = {'bootstrap.servers': '34.138.205.183:9094,34.138.104.233:9094,34.138.118.154:9094',
        'client.id': me1}
producer = Producer(conf)
topic1=me1



app = Flask(__name__)
socketio = SocketIO(app)


def get_db_connection():
    conn = sqlite3.connect(MAIN_DB)
    conn.row_factory = sqlite3.Row
    return conn

con = get_db_connection()
con.execute("CREATE TABLE IF NOT EXISTS image(id, filename, object)")
if not os.path.exists(IMAGES_DIR):
   os.mkdir(IMAGES_DIR)

@app.route('/', methods = ['GET'])
def index():
   con = get_db_connection()
   cur = con.cursor()
   res = cur.execute("SELECT * FROM image")
   images = res.fetchall()
   con.close()
   return render_template_string("""
<!DOCTYPE html>
<html>
<head>
<style>
.container {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
  grid-auto-rows: minmax(100px, auto);
  gap: 20px;
}
img {
   display: block;
   max-width:100%;
   max-height:100%;
   margin-left: auto;
   margin-right: auto;
}
.img {
   height: 270px;
}
.label {
   height: 30px;
  text-align: center;
}
</style>
</head>
<body>
<form method="post" enctype="multipart/form-data">
  <div>
    <label for="file">Choose file to upload</label>
    <input type="file" id="file" name="file" accept="image/x-png,image/gif,image/jpeg" />
  </div>
  <div>
    <button>Submit</button>
  </div>
</form>
<div class="container">
{% for image in images %}
<div>
<div class="img"><img src="/images/{{ image.filename }}"></img></div>
<div class="label">{{ image.object | default('undefined', true) }}</div>
</div>
{% endfor %}
</div>
<script src="https://cdnjs.cloudflare.com/ajax/libs/socket.io/4.4.1/socket.io.js"></script>
<script type="text/javascript">
    var socket = io();

    socket.on('message', function(data) {
        if (data.task === 'task_completed') {
            location.reload();  // Refreshes the entire page
        } else if (data.task === 'error') {
            alert('An error occurred: ' + data.data);
        }
    });
</script>
</body>
</html>
   """, images=images)

@app.route('/images/<path:path>', methods = ['GET'])
def image(path):
    return send_from_directory(IMAGES_DIR, path)

@app.route('/object/<id>', methods = ['PUT'])
def set_object(id):
   con = get_db_connection()
   cur = con.cursor()
   json = request.json
   object = json['object']
   cur.execute("UPDATE image SET object = ? WHERE id = ?", (object, id))
   con.commit()
   con.close()
   return '{"status": "OK"}'

@app.route('/', methods = ['POST'])
def upload_file():
   try:
      f = request.files['file']
      ext = f.filename.split('.')[-1]
      id = uuid.uuid4().hex
      filename = "{}.{}".format(id, ext)
      f.save("{}/{}".format(IMAGES_DIR, filename))
      con = get_db_connection()
      cur = con.cursor()
      cur.execute("INSERT INTO image (id, filename, object) VALUES (?, ?, ?)", (id, filename, ""))
      con.commit()
      con.close()
      producer.produce(topic1, key=None, value=filename)
   except Exception as e:
      producer.produce(error_topic, key=None, value=str(e))
      socketio.emit('message', {'data': str(e)})
   


   return redirect('/')

@socketio.on('connect')
def handle_connect():
    emit('message', {'data': 'Connected to WebSocket'})
    
def consumer_notification():
   group_id = me1 +'-Notification'
   conf = {
    'bootstrap.servers': '34.138.205.183:9094,34.138.104.233:9094,34.138.118.154:9094',
    'enable.auto.commit':True,
    'group.id': group_id,                    
    'auto.offset.reset': 'latest'          
   }
   error_topic='Mohamed_Maher_error'
   success_topic='Mohamed_Maher_success'

   consumer = Consumer(conf)
   topics=[success_topic,error_topic]
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
            if msg.topic() == success_topic:
               print(f"Emitting task_completed message: {msg.value().decode('utf-8')}")
               socketio.emit('message', {'task': 'task_completed', 'data': msg.value().decode('utf-8')})
            elif msg.topic() == error_topic:
               print(f"Emitting error message: {msg.value().decode('utf-8')}")
               socketio.emit('message', {'task': 'error', 'data': msg.value().decode('utf-8')})
            socketio.sleep(1)
            
            


   except KeyboardInterrupt:
      print("Consumer interrupted")

   finally:
      consumer.close()


if __name__ == '__main__':
   consumer_thread = Thread(target=consumer_notification)
   consumer_thread.start()
   
   socketio.run(app,debug = True, port=(int(sys.argv[1]) if len(sys.argv) > 1 else 5000))