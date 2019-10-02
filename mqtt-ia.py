import paho.mqtt.client as mqtt
from room_pred import room_pred
import json

broker="up02"
port=1883
timelive=60
client_id="mqtt-ia"
username="xxxx"
password="xxxxx"
will_topic = "home/last_wills/"

rpred = room_pred(reload=True) 

def room_ia(client, userdata, msg):
    data = json.loads(msg.payload.decode())
    data['room'] = rpred.score_room(data)
    client.publish('home/BT/rooms', payload=json.dumps(data),)

def on_connect(client, userdata, flags, rc):
  print("Connected with result code "+str(rc))
  client.publish(will_topic+client_id, payload="{\"status\":\"Online\"}", qos=0, retain=True)
  client.subscribe("home/BT/presence/#")
  client.message_callback_add("home/BT/presence/#",room_ia)
def on_message(client, userdata, msg):
    print(msg.payload.decode())
    
client = mqtt.Client(client_id)
client.username_pw_set(username, password)
client.will_set(will_topic+client_id, payload="{\"status\":\"Offline\"}", qos=0, retain=True)
client.connect(broker,port,timelive)
client.on_connect = on_connect
client.on_message = on_message
client.loop_forever()
